"""Eligibility and evidence checks for discovery recall; never a source of authority.

No model calls, index rebuild, personal-memory parsing, or unbounded corpus reads.
Matching source bytes establish currency against a recorded snapshot, not truth.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

MAX_SOURCE_BYTES = 256 * 1024
MAX_SOURCE_REFS = 4
TEXT_SUFFIXES = {".md", ".py", ".sql", ".toml", ".yaml", ".yml", ".txt"}


def safe_path(value: object, root: Path) -> Path | None:
    """Resolve a canonical relative path inside this checkout, including symlinks."""
    if not isinstance(value, str) or not value or len(value) > 512:
        return None
    if any(char in value for char in ("\\", ":", "\x00", "*", "?")):
        return None
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in value.split("/")):
        return None
    try:
        resolved = (root / value).resolve()
        return resolved if resolved.is_relative_to(root.resolve()) else None
    except (OSError, RuntimeError, ValueError):
        return None


def private_path(value: str) -> bool:
    normalized = value.replace("\\", "/").casefold()
    return normalized == "planning/product" or normalized.startswith("planning/product/")


def context_scope(cwd: object, root: Path) -> str:
    """Private context requires an actual cwd inside the private product checkout."""
    if isinstance(cwd, str):
        try:
            if Path(cwd).is_absolute() and Path(cwd).resolve().is_relative_to((root / "planning/product").resolve()):
                return "private"
        except (OSError, RuntimeError, ValueError):
            pass
    return "public"


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be text")
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return timestamp.astimezone(UTC)


def assess(row: dict, *, root: Path, scope: str = "public", now: datetime | None = None) -> dict | None:
    """Return a bounded recall record or exclude an ineligible/invalid finding.

    Legacy findings remain unverified leads. An explicit invalidation always wins,
    even if its replacement is missing. Changed evidence yields a recheck lead
    whose text contains no cached claim. Scope is coordination, not an OS sandbox.
    """
    if scope not in {"public", "private"}:
        raise ValueError("scope must be public or private")
    row_scope = row.get("scope")
    if "scope" in row and (not isinstance(row_scope, str) or row_scope not in {"public", "private"}):
        return None
    repo_path = row.get("repo_path")
    refs = row.get("source_refs")
    if "repo_path" in row and safe_path(repo_path, root) is None:
        return None
    # Existing Siting rows predate explicit scope metadata. Never broaden these
    # just because a prompt mentions planning or because a private path exists.
    is_private = row_scope == "private" or row.get("domain") == "siting"
    is_private |= isinstance(repo_path, str) and private_path(repo_path)
    if repo_path is not None:
        resolved_detail = safe_path(repo_path, root)
        if resolved_detail is None:
            return None
        is_private |= private_path(resolved_detail.relative_to(root.resolve()).as_posix())
    if is_private and (scope != "private" or row_scope == "public"):
        return None
    status = row.get("status", "active")
    if status != "active" or "superseded_by" in row:
        return None
    revision = row.get("revision", 1)
    if type(revision) is not int or revision < 1:
        return None
    now = now or datetime.now(UTC)
    state, reason = "unverified", "No verified source snapshot; recheck before relying on this lead."
    paths: list[str] = []
    observations: list[str] = []
    try:
        if "valid_from" in row and _timestamp(row["valid_from"]) > now:
            return None
        if "verified_at" in row and _timestamp(row["verified_at"]) > now:
            return None
        expired = "expires_at" in row and _timestamp(row["expires_at"]) <= now
        if "source_refs" in row:
            if not isinstance(refs, list) or not 1 <= len(refs) <= MAX_SOURCE_REFS or "verified_at" not in row:
                return None
            # Validate every reference and its scope before opening any source.
            for ref in refs:
                if not isinstance(ref, dict) or set(ref) != {"path", "sha256"}:
                    return None
                path, digest = ref["path"], ref["sha256"]
                resolved = safe_path(path, root)
                if resolved is None or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    return None
                if (scope != "private" or row_scope == "public") and (
                    private_path(path) or private_path(resolved.relative_to(root.resolve()).as_posix())
                ):
                    return None
                if resolved.suffix.lower() not in TEXT_SUFFIXES:
                    return None
                paths.append(path)
            state, reason = "source-matched", "Recorded source bytes match; this does not certify the claim."
            for ref in refs:
                source = safe_path(ref["path"], root)
                try:
                    if source is None or not source.is_file():
                        raise OSError("source is not a regular file")
                    with source.open("rb") as stream:
                        content = stream.read(MAX_SOURCE_BYTES + 1)
                    observed = hashlib.sha256(content).hexdigest() if len(content) <= MAX_SOURCE_BYTES else "oversized"
                except OSError:
                    observed = "unavailable"
                observations.append(observed)
                if observed != ref["sha256"]:
                    state, reason = "recheck", "Supporting evidence changed or is unavailable; reopen the source."
        if expired:
            state, reason = "recheck", "Recorded validity expired; reverify the source before reuse."
    except (TypeError, ValueError, OverflowError):
        return None
    if state == "recheck" and not paths and isinstance(repo_path, str):
        paths.append(repo_path)
    identity = {"record": row, "state": state, "observations": observations, "scope": scope}
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
    text = row["discovery"] if state != "recheck" else reason
    return {
        **row,
        "_recall": {"state": state, "text": text, "reason": reason, "sources": paths, "key": f"{row['id']}@{digest}"},
    }


def select(rows: list[dict], *, root: Path, scope: str = "public", now: datetime | None = None) -> list[dict]:
    """Exclude conflicting duplicate IDs rather than choosing a convenient version."""
    by_id: dict[str, dict[str, dict]] = {}
    for row in rows:
        identity = json.dumps(row, sort_keys=True, ensure_ascii=True)
        by_id.setdefault(row["id"], {})[identity] = row
    result = []
    for versions in by_id.values():
        if len(versions) != 1:
            continue
        try:
            candidate = assess(next(iter(versions.values())), root=root, scope=scope, now=now)
        except (OSError, TypeError, ValueError, AttributeError):
            continue  # isolate a malformed record; do not suppress other useful leads
        if candidate is not None:
            result.append(candidate)
    return result
