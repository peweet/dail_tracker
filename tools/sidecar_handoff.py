#!/usr/bin/env python
"""Validate, queue, and confirm a cross-session Codex sidecar handoff.

This is deliberately a thin wrapper around the installed ``codex queue`` command.
It does not schedule agents or keep an active-task ledger.  Its only jobs are to:

* require one small evidence-first Markdown packet;
* bind the packet to a stable task key and current Git snapshot;
* add a deterministic handoff id and block duplicate concurrent work;
* record the local queue receipt without storing the packet body; and
* distinguish ``accepted_unconsumed`` from ``delivered`` in the target transcript.

Usage through the repository command surface::

    python tools/dev.py sidecar-handoff template
    python tools/dev.py sidecar-handoff snapshot --root . --read-path tools/example.py
    python tools/dev.py sidecar-handoff validate --thread <uuid> --source-root . --file handoff.md
    python tools/dev.py sidecar-handoff queue --thread <uuid> --source-root . --file handoff.md
    python tools/dev.py sidecar-handoff status --thread <uuid> --handoff-id sc-0123abcd...
    python tools/dev.py sidecar-handoff recover --thread <uuid> --task-key <key> \
        --handoff-id sc-0123abcd... --resolution <accepted|failed>

The queue, claim, and receipt log are local coordination aids, not release evidence.
Ambiguous outcomes retain the claim until an operator checks the target and runs the
explicit recovery command. The target session remains the integration owner and must
adjudicate every finding.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from collections.abc import Callable, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TypedDict

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / "logs" / "sidecar_handoffs.jsonl"
DEFAULT_CLAIMS = ROOT / "logs" / "sidecar_handoff_claims"
MAX_PACKET_BYTES = 48_000
MAX_MESSAGE_CHARS = 12_000
SCHEMA_VERSION = 1
REQUIRED_SECTIONS = (
    "Scope",
    "Ownership",
    "Findings",
    "Checks run",
    "Checks not run",
    "Integration",
)
HANDOFF_ID_RE = re.compile(r"^sc-[0-9a-f]{16}$")
TASK_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,79}$")
SNAPSHOT_RE = re.compile(r"^git:[0-9a-f]{12}@[0-9a-f]{16}\+(?:clean|dirty):[0-9a-f]{16}$")
MAX_READ_PATTERNS = 64
MAX_DECLARED_FILES = 5_000
MAX_UNTRACKED_FILES = 20_000
REQUIRED_METADATA = (
    "task_key",
    "source_snapshot_id",
    "role",
    "read_paths",
    "write_owner",
    "write_paths",
    "verification_status",
    "supersedes",
)
ACTIVE_STATES = frozenset({"accepted_unconsumed", "delivered", "unknown", "accepted", "consumed"})
ACCEPTED_STATES = frozenset({"accepted_unconsumed", "delivered", "accepted", "consumed"})
RECEIPT_RE = re.compile(r"Queued message (?P<receipt>[0-9a-f-]{36}) for thread (?P<thread>[0-9a-f-]{36})\.")

TEMPLATE = """---
task_key: replace-with-stable-task-key
source_snapshot_id: run-the-snapshot-command
role: reviewer
read_paths: comma-separated exact paths or globs
write_owner: target
write_paths: none
verification_status: reported
supersedes: none
---
# Sidecar handoff

## Scope

State the bounded question and explicitly name completed work that was not repeated.

## Ownership

Name the integration owner, writer count, repository root, and files or systems inspected.

## Findings

List severity, evidence, consequence, and the smallest action. Say "No material findings" when applicable.

## Checks run

List exact commands and observed results. Label mocked, static, or focused checks accurately.

## Checks not run

List relevant checks that remain unrun and why.

## Integration

Separate target-session actions from unrelated backlog, superseded packets, and unresolved uncertainty.
"""


class HandoffError(ValueError):
    """A user-actionable handoff contract failure."""


class RenderedHandoff(TypedDict):
    handoff_id: str
    marker: str
    message: str
    message_chars: int
    sha256: str
    source_session: str
    source_snapshot_id: str
    supersedes: str
    target_session: str
    task_key: str
    verification_status: str


def _canonical_uuid(value: str, label: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise HandoffError(f"{label} must be a UUID, got {value!r}") from exc
    canonical = str(parsed)
    if value.lower() != canonical:
        raise HandoffError(f"{label} must use canonical UUID form: {canonical}")
    return canonical


def _normalise_packet(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n").strip() + "\n"


def _read_path_patterns(value: str) -> tuple[str, ...]:
    raw_patterns = [item.strip() for item in value.split(",")]
    if not raw_patterns or any(not item for item in raw_patterns):
        raise HandoffError("read_paths must be a comma-separated list of non-empty relative paths or globs")
    if len(raw_patterns) > MAX_READ_PATTERNS:
        raise HandoffError(f"read_paths has {len(raw_patterns)} entries; maximum is {MAX_READ_PATTERNS}")

    patterns: list[str] = []
    for raw in raw_patterns:
        pattern = raw.replace(chr(92), "/")
        windows_path = PureWindowsPath(pattern)
        posix_path = PurePosixPath(pattern)
        folded = pattern.casefold().rstrip("/")
        if (
            folded in {".", "all", "*", "**", "**/*"}
            or posix_path.is_absolute()
            or windows_path.is_absolute()
            or windows_path.drive
            or ":" in pattern
        ):
            raise HandoffError(f"read_paths entry must be relative and bounded: {raw!r}")
        if ".." in posix_path.parts:
            raise HandoffError(f"read_paths entry escapes the source root: {raw!r}")
        first = posix_path.parts[0] if posix_path.parts else ""
        if not first or glob.has_magic(first):
            raise HandoffError(f"read_paths glob must start with a literal bounded directory: {raw!r}")
        patterns.append(pattern)
    return tuple(patterns)


def _front_matter(text: str) -> dict[str, str]:
    if not text.startswith("---\n"):
        raise HandoffError("packet must start with flat YAML-style metadata between --- lines")
    boundary = text.find("\n---\n", 4)
    if boundary < 0:
        raise HandoffError("packet metadata is missing its closing --- line")
    metadata: dict[str, str] = {}
    for number, line in enumerate(text[4:boundary].splitlines(), start=2):
        if not line.strip() or ":" not in line:
            raise HandoffError(f"metadata line {number} must be one non-empty key: value pair")
        key, value = (part.strip() for part in line.split(":", 1))
        if not key or not value:
            raise HandoffError(f"metadata line {number} must be one non-empty key: value pair")
        if key in metadata:
            raise HandoffError(f"packet repeats metadata field: {key}")
        metadata[key] = value

    missing = [name for name in REQUIRED_METADATA if name not in metadata]
    unknown = sorted(set(metadata).difference(REQUIRED_METADATA))
    if missing:
        raise HandoffError(f"packet is missing metadata field(s): {', '.join(missing)}")
    if unknown:
        raise HandoffError(f"packet has unsupported metadata field(s): {', '.join(unknown)}")
    if not TASK_KEY_RE.fullmatch(metadata["task_key"]):
        raise HandoffError("task_key must be 3-80 lowercase letters, digits, dots, underscores, or hyphens")
    if not SNAPSHOT_RE.fullmatch(metadata["source_snapshot_id"]):
        raise HandoffError("source_snapshot_id must have form git:<12 head>@<16 root>+<clean|dirty>:<16 content>")
    if metadata["role"] not in {"scout", "reviewer"}:
        raise HandoffError("role must be scout or reviewer; cross-session sidecars are read-only")
    _read_path_patterns(metadata["read_paths"])
    if metadata["write_owner"] not in {"target", "none"}:
        raise HandoffError("write_owner must be target or none")
    if metadata["write_paths"] != "none":
        raise HandoffError("write_paths must be none; cross-session sidecars are read-only")
    if metadata["verification_status"] not in {"not_run", "reported"}:
        raise HandoffError("verification_status must be not_run or reported")
    supersedes = metadata["supersedes"]
    if supersedes != "none" and not HANDOFF_ID_RE.fullmatch(supersedes):
        raise HandoffError("supersedes must be none or a handoff id")
    return metadata


def packet_metadata(text: str) -> dict[str, str]:
    return _front_matter(validate_packet(text))


def _output_bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    return str(value or "").encode("utf-8", errors="surrogatepass")


def _git_output(
    root: Path,
    arguments: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess],
    purpose: str,
) -> bytes:
    result = runner(["git", "-C", str(root), *arguments], capture_output=True)
    if result.returncode == 0:
        return _output_bytes(result.stdout)
    diagnostic = _output_bytes(result.stderr or result.stdout).decode("utf-8", errors="replace").strip()
    raise HandoffError(f"cannot {purpose} for {root}: {diagnostic or 'no diagnostic'}")


def _git_relative_paths(raw: bytes, *, label: str) -> tuple[str, ...]:
    paths: list[str] = []
    for encoded in raw.split(b"\x00"):
        if not encoded:
            continue
        value = encoded.decode("utf-8", errors="surrogateescape").replace(chr(92), "/")
        parsed = PurePosixPath(value)
        if parsed.is_absolute() or ".." in parsed.parts or not parsed.parts:
            raise HandoffError(f"Git returned an unsafe {label} path: {value!r}")
        paths.append(parsed.as_posix())
    return tuple(paths)


def _hash_source_path(digest, root: Path, relative: str, *, kind: str) -> None:
    path = root.joinpath(*PurePosixPath(relative).parts)
    digest.update(kind.encode())
    digest.update(b"\x00")
    digest.update(relative.encode("utf-8", errors="surrogatepass"))
    digest.update(b"\x00")
    try:
        stat_result = path.lstat()
    except FileNotFoundError:
        digest.update(b"missing\x00")
        return
    except OSError as exc:
        raise HandoffError(f"cannot fingerprint {kind} path {relative!r}: {exc}") from exc

    digest.update(f"mode:{stat_result.st_mode:o}\x00".encode())
    if path.is_symlink():
        raise HandoffError(f"{kind} path is a symlink and cannot be freshness-bound safely: {relative!r}")
    if path.is_dir():
        raise HandoffError(
            f"{kind} path is a directory, not a bounded file: {relative!r}; use the nested Git root directly"
        )
    if not path.is_file():
        raise HandoffError(f"{kind} path is not a regular file: {relative!r}")
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise HandoffError(f"cannot fingerprint {kind} file {relative!r}: {exc}") from exc
    digest.update(b"\x00")


def _hash_declared_paths(digest, root: Path, patterns: Sequence[str]) -> None:
    matched_count = 0
    for pattern in patterns:
        digest.update(b"pattern\x00")
        digest.update(pattern.encode())
        digest.update(b"\x00")
        try:
            candidates = sorted(root.glob(pattern), key=lambda item: item.as_posix())
        except (OSError, ValueError) as exc:
            raise HandoffError(f"cannot expand read_paths entry {pattern!r}: {exc}") from exc
        files: list[Path] = []
        for candidate in candidates:
            try:
                relative = candidate.relative_to(root).as_posix()
            except ValueError as exc:
                raise HandoffError(f"read_paths entry escaped the source root: {pattern!r}") from exc
            if candidate.is_dir() and not candidate.is_symlink():
                if not glob.has_magic(pattern):
                    raise HandoffError(f"read_paths entry is a directory, not a bounded file: {pattern!r}")
                continue
            files.append(candidate)
            matched_count += 1
            if matched_count > MAX_DECLARED_FILES:
                raise HandoffError(f"read_paths expands beyond {MAX_DECLARED_FILES} files; narrow the sidecar scope")
            _hash_source_path(digest, root, relative, kind="declared")
        if not files:
            digest.update(b"no-match\x00")


def git_snapshot(
    root: Path,
    read_paths: Sequence[str] = (),
    *,
    runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
) -> str:
    """Bind a packet to one Git root plus current relevant file contents."""

    resolved = root.resolve()
    if not resolved.is_dir():
        raise HandoffError(f"source root is not a directory: {resolved}")
    patterns = tuple(pattern for value in read_paths for pattern in _read_path_patterns(value))

    head = _git_output(resolved, ("rev-parse", "HEAD"), runner=runner, purpose="read source Git HEAD")
    head = head.decode("ascii", errors="replace").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{12,64}", head):
        raise HandoffError(f"source Git HEAD is not a hexadecimal object id: {head!r}")

    top_level_raw = _git_output(
        resolved,
        ("rev-parse", "--show-toplevel"),
        runner=runner,
        purpose="read source Git worktree root",
    )
    top_level = Path(top_level_raw.decode("utf-8", errors="surrogateescape").strip()).resolve()
    try:
        same_root = resolved.samefile(top_level)
    except OSError:
        same_root = os.path.normcase(str(resolved)) == os.path.normcase(str(top_level))
    if not same_root:
        raise HandoffError(f"source root must be the Git worktree top level: {top_level}")
    root_identity = os.path.normcase(str(top_level)).replace(chr(92), "/").encode()
    root_hash = hashlib.sha256(root_identity).hexdigest()[:16]

    status = _git_output(
        resolved,
        ("status", "--porcelain=v1", "-z", "--untracked-files=all"),
        runner=runner,
        purpose="fingerprint source Git status",
    )
    tracked_raw = _git_output(
        resolved,
        ("diff", "--name-only", "-z", "HEAD", "--"),
        runner=runner,
        purpose="list changed tracked source files",
    )
    untracked_raw = _git_output(
        resolved,
        ("ls-files", "--others", "--exclude-standard", "-z"),
        runner=runner,
        purpose="list untracked source files",
    )
    tracked = _git_relative_paths(tracked_raw, label="tracked")
    untracked = _git_relative_paths(untracked_raw, label="untracked")
    if len(untracked) > MAX_UNTRACKED_FILES:
        raise HandoffError(f"source root has {len(untracked)} untracked files; maximum is {MAX_UNTRACKED_FILES}")

    digest = hashlib.sha256()
    digest.update(b"status\x00")
    digest.update(status)
    for relative in sorted(set(tracked)):
        _hash_source_path(digest, resolved, relative, kind="tracked")
    for relative in sorted(set(untracked)):
        _hash_source_path(digest, resolved, relative, kind="untracked")
    _hash_declared_paths(digest, resolved, patterns)
    state = "clean" if not status else "dirty"
    return f"git:{head[:12]}@{root_hash}+{state}:{digest.hexdigest()[:16]}"


def _section_body(text: str, heading: str) -> str | None:
    match = re.search(
        rf"(?ims)^##[ \t]+{re.escape(heading)}[ \t]*$\n(?P<body>.*?)(?=^##[ \t]+|\Z)",
        text,
    )
    return match.group("body").strip() if match else None


def validate_packet(text: str) -> str:
    """Return the normalized packet or raise with the exact contract failure."""

    normalised = _normalise_packet(text)
    encoded = normalised.encode("utf-8")
    if len(encoded) > MAX_PACKET_BYTES:
        raise HandoffError(f"packet is {len(encoded)} bytes; maximum is {MAX_PACKET_BYTES}")
    if "\x00" in normalised:
        raise HandoffError("packet contains a NUL byte")
    _front_matter(normalised)

    headings = [item.strip().casefold() for item in re.findall(r"(?m)^##[ \t]+(.+?)[ \t]*$", normalised)]
    expected = [item.casefold() for item in REQUIRED_SECTIONS]
    missing = [name for name in REQUIRED_SECTIONS if name.casefold() not in headings]
    duplicates = [name for name in REQUIRED_SECTIONS if headings.count(name.casefold()) > 1]
    if missing:
        raise HandoffError(f"packet is missing required section(s): {', '.join(missing)}")
    if duplicates:
        raise HandoffError(f"packet repeats required section(s): {', '.join(duplicates)}")
    positions = [headings.index(name) for name in expected]
    if positions != sorted(positions):
        raise HandoffError(f"required sections must appear in this order: {', '.join(REQUIRED_SECTIONS)}")
    empty = [name for name in REQUIRED_SECTIONS if not _section_body(normalised, name)]
    if empty:
        raise HandoffError(f"packet has empty required section(s): {', '.join(empty)}")
    return normalised


def read_packet(path: Path) -> str:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise HandoffError(f"cannot read packet {path}: {exc}") from exc
    if len(raw) > MAX_PACKET_BYTES:
        raise HandoffError(f"packet is {len(raw)} bytes; maximum is {MAX_PACKET_BYTES}")
    try:
        return validate_packet(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise HandoffError(f"packet must be UTF-8: {exc}") from exc


def build_message(*, target_session: str, packet: str, source_session: str | None = None) -> RenderedHandoff:
    target = _canonical_uuid(target_session, "target session")
    source = _canonical_uuid(source_session, "source session") if source_session else "not-recorded"
    normalised = validate_packet(packet)
    metadata = _front_matter(normalised)
    digest = hashlib.sha256(f"{target}\n{normalised}".encode()).hexdigest()
    handoff_id = f"sc-{digest[:16]}"
    marker = f"[sidecar-handoff v{SCHEMA_VERSION} id={handoff_id} sha256={digest}]"
    message = (
        f"{marker}\n"
        f"source_session: {source}\n"
        f"task_key: {metadata['task_key']}\n"
        f"source_snapshot_id: {metadata['source_snapshot_id']}\n"
        f"verification_status: {metadata['verification_status']}\n"
        "integration_rule: Do not respawn completed work. Adjudicate findings against the current tree. "
        "Queue acceptance is not delivery; only the target can integrate and verify.\n\n"
        f"{normalised}"
    )
    if len(message) > MAX_MESSAGE_CHARS:
        raise HandoffError(
            f"rendered message is {len(message)} characters; maximum is {MAX_MESSAGE_CHARS}. "
            "Split unrelated findings, not one evidence chain."
        )
    return {
        "handoff_id": handoff_id,
        "sha256": digest,
        "marker": marker,
        "message": message,
        "message_chars": len(message),
        "target_session": target,
        "source_session": source,
        "source_snapshot_id": metadata["source_snapshot_id"],
        "supersedes": metadata["supersedes"],
        "task_key": metadata["task_key"],
        "verification_status": metadata["verification_status"],
    }


def _rows(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    rows: list[dict] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    for line in lines:
        try:
            row = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _append_row(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n").encode()
    flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        written = os.write(descriptor, payload)
        if written != len(payload):
            raise OSError(f"short ledger append: wrote {written} of {len(payload)} bytes")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _claim_path(claims_dir: Path, target: str, task_key: str) -> Path:
    return claims_dir / f"{target}_{task_key}.claim"


def _claim(claims_dir: Path, target: str, task_key: str, row: dict) -> Path:
    claims_dir.mkdir(parents=True, exist_ok=True)
    path = _claim_path(claims_dir, target, task_key)
    payload = (json.dumps({**row, "state": "queue_in_progress"}, sort_keys=True) + "\n").encode()
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise HandoffError(f"task is already being queued: {task_key} -> {target}") from exc
    try:
        written = os.write(descriptor, payload)
        if written != len(payload):
            raise OSError(f"short claim write: wrote {written} of {len(payload)} bytes")
        os.fsync(descriptor)
    except OSError:
        os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
    os.close(descriptor)
    return path


def _replace_claim(path: Path, row: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    payload = (json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n").encode()
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        try:
            written = os.write(descriptor, payload)
            if written != len(payload):
                raise OSError(f"short claim update: wrote {written} of {len(payload)} bytes")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise
    try:
        os.replace(temporary, path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise


def _release_claim(path: Path, *, resolution: str) -> None:
    try:
        path.unlink()
    except OSError as exc:
        raise HandoffError(
            f"{resolution} state is durable but claim cleanup failed at {path}: {exc}. "
            f"Run recover --resolution {resolution}."
        ) from exc


def _require_external_packet(packet_path: Path, source_root: Path) -> None:
    packet = packet_path.resolve()
    root = source_root.resolve()
    try:
        packet.relative_to(root)
    except ValueError:
        return
    raise HandoffError(
        "packet file must be outside the source root so its snapshot field is not self-referential; use a temp file"
    )


def queue_handoff(
    *,
    target_session: str,
    packet_path: Path,
    source_root: Path,
    source_session: str | None = None,
    ledger: Path = DEFAULT_LEDGER,
    claims_dir: Path = DEFAULT_CLAIMS,
    codex_path: str | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    snapshotter: Callable[[Path, Sequence[str]], str] = git_snapshot,
) -> dict:
    _require_external_packet(packet_path, source_root)
    packet = read_packet(packet_path)
    rendered = build_message(target_session=target_session, packet=packet, source_session=source_session)
    target = str(rendered["target_session"])
    handoff_id = str(rendered["handoff_id"])
    task_key = str(rendered["task_key"])
    supersedes = str(rendered["supersedes"])
    expected_snapshot = str(rendered["source_snapshot_id"])
    metadata = _front_matter(packet)
    read_paths = _read_path_patterns(metadata["read_paths"])
    current_snapshot = snapshotter(source_root, read_paths)
    if expected_snapshot != current_snapshot:
        raise HandoffError(
            f"stale packet snapshot: packet has {expected_snapshot}, current source is {current_snapshot}"
        )

    rows = _rows(ledger)
    same_task = [row for row in rows if row.get("target_session") == target and row.get("task_key") == task_key]
    latest_task = same_task[-1] if same_task else None
    if latest_task and latest_task.get("handoff_id") == handoff_id and latest_task.get("state") in ACTIVE_STATES:
        raise HandoffError(f"identical handoff is already active: {handoff_id} -> {target}")
    if supersedes != "none" and not any(
        row.get("handoff_id") == supersedes and row.get("state") not in {"failed"} for row in same_task
    ):
        raise HandoffError(f"supersedes does not name an existing handoff for task {task_key}: {supersedes}")
    if latest_task and latest_task.get("state") in ACTIVE_STATES:
        previous = str(latest_task.get("handoff_id"))
        if supersedes != previous:
            raise HandoffError(
                f"task {task_key} is already active as {previous}; set supersedes to that id only for a correction"
            )

    executable = codex_path or shutil.which("codex")
    if not executable:
        raise HandoffError("codex executable not found on PATH")
    command = [executable, "queue", "--thread", target, "--message", str(rendered["message"])]
    base_row = {
        "handoff_id": handoff_id,
        "message_chars": rendered["message_chars"],
        "packet": packet_path.name,
        "read_paths": list(read_paths),
        "role": metadata["role"],
        "sha256": rendered["sha256"],
        "source_session": rendered["source_session"],
        "source_root": str(source_root.resolve()),
        "source_snapshot_id": expected_snapshot,
        "supersedes": supersedes,
        "task_key": task_key,
        "target_session": target,
        "ts": _utc_now(),
        "verification_status": rendered["verification_status"],
        "write_owner": metadata["write_owner"],
        "write_paths": metadata["write_paths"],
    }
    try:
        claim = _claim(claims_dir, target, task_key, base_row)
    except OSError as exc:
        raise HandoffError(f"cannot create durable task claim for {task_key}: {exc}") from exc

    try:
        completed = runner(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
        returncode = completed.returncode
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
    except OSError as exc:
        failed = {**base_row, "state": "failed", "error": str(exc)[:500]}
        try:
            _append_row(ledger, failed)
        except OSError as ledger_exc:
            raise HandoffError(
                f"codex queue could not start and the ledger append failed; recovery claim retained at {claim}: "
                f"{ledger_exc}"
            ) from exc
        _release_claim(claim, resolution="failed")
        raise HandoffError(f"codex queue could not start: {exc}") from exc
    except Exception as exc:
        unknown = {
            **base_row,
            "state": "unknown",
            "error": f"queue runner raised {type(exc).__name__}: {str(exc)[:400]}",
        }
        try:
            _append_row(ledger, unknown)
        except OSError as ledger_exc:
            raise HandoffError(
                f"queue outcome and ledger state are unknown; recovery claim retained at {claim}: {ledger_exc}"
            ) from exc
        raise HandoffError(
            f"queue outcome is unknown; recovery claim retained at {claim}. Check status, then use recover."
        ) from exc

    if returncode != 0:
        error = (stderr or stdout or "no diagnostic").strip()[-500:]
        unknown = {
            **base_row,
            "state": "unknown",
            "error": f"codex queue exited {returncode}; outcome requires target inspection: {error}",
        }
        try:
            _append_row(ledger, unknown)
        except OSError as ledger_exc:
            raise HandoffError(
                f"codex queue exited {returncode} and the ledger append failed; recovery claim retained at "
                f"{claim}: {ledger_exc}"
            ) from ledger_exc
        raise HandoffError(
            f"codex queue exited {returncode}; outcome is unknown and recovery claim is retained at {claim}. "
            "Check the target, then use recover."
        )

    match = RECEIPT_RE.search(stdout)
    receipt = None
    receipt_error = "queue returned success without a matching canonical receipt"
    if match and match.group("thread") == target:
        try:
            receipt = _canonical_uuid(match.group("receipt"), "queue receipt")
        except HandoffError as exc:
            receipt_error = str(exc)
    if receipt is None:
        unknown = {**base_row, "state": "unknown", "error": receipt_error}
        try:
            _append_row(ledger, unknown)
        except OSError as ledger_exc:
            raise HandoffError(
                f"queue may have succeeded but the ledger append failed; recovery claim retained at {claim}: "
                f"{ledger_exc}"
            ) from ledger_exc
        raise HandoffError(f"{receipt_error}; recovery claim retained at {claim}. Check status, then use recover.")

    result = {**base_row, "state": "accepted_unconsumed", "receipt": receipt}
    try:
        _replace_claim(
            claim,
            {
                **result,
                "receipt_persisted_ts": _utc_now(),
                "state": "accepted_receipt_pending_ledger",
            },
        )
    except OSError as exc:
        raise HandoffError(
            f"queue returned receipt {receipt}, but the recovery claim update failed at {claim}: {exc}. "
            "Do not retry; inspect the target."
        ) from exc
    try:
        _append_row(ledger, result)
    except OSError as exc:
        raise HandoffError(
            f"queue was accepted but the receipt ledger append failed; recovery claim retained at {claim}: {exc}"
        ) from exc
    _release_claim(claim, resolution="accepted")
    return result


def _claim_record(path: Path) -> dict | None:
    rows = _rows(path)
    return rows[-1] if rows else None


def _find_claim(claims_dir: Path, target: str, handoff_id: str) -> tuple[Path, dict] | None:
    if not claims_dir.is_dir():
        return None
    try:
        candidates = claims_dir.glob(f"{target}_*.claim")
        for path in candidates:
            row = _claim_record(path)
            if row and row.get("target_session") == target and row.get("handoff_id") == handoff_id:
                return path, row
    except OSError:
        return None
    return None


def recover_claim(
    *,
    target_session: str,
    task_key: str,
    handoff_id: str,
    resolution: str,
    ledger: Path = DEFAULT_LEDGER,
    claims_dir: Path = DEFAULT_CLAIMS,
) -> dict:
    """Resolve one retained claim only after the operator checks the target session."""

    target = _canonical_uuid(target_session, "target session")
    if not TASK_KEY_RE.fullmatch(task_key):
        raise HandoffError("task key has an invalid form")
    if not HANDOFF_ID_RE.fullmatch(handoff_id):
        raise HandoffError("handoff id must have form sc- plus 16 lowercase hex characters")
    path = _claim_path(claims_dir, target, task_key)
    row = _claim_record(path)
    if not row:
        raise HandoffError(f"no readable recovery claim exists for {task_key} -> {target}")
    if row.get("target_session") != target or row.get("task_key") != task_key or row.get("handoff_id") != handoff_id:
        raise HandoffError("recovery claim metadata does not match the requested target, task key, and handoff id")

    tracked = [
        item for item in _rows(ledger) if item.get("target_session") == target and item.get("handoff_id") == handoff_id
    ]
    latest = tracked[-1] if tracked else None
    if resolution == "accepted":
        receipt = latest.get("receipt") if latest else row.get("receipt")
        if not isinstance(receipt, str):
            raise HandoffError("accepted recovery requires a canonical receipt in the ledger or retained claim")
        receipt = _canonical_uuid(receipt, "receipt")
        if latest and latest.get("state") in ACCEPTED_STATES:
            claim_receipt = row.get("receipt")
            if claim_receipt is not None and claim_receipt != receipt:
                raise HandoffError("accepted ledger and recovery claim receipts disagree")
        elif row.get("state") == "accepted_receipt_pending_ledger" and row.get("receipt") == receipt:
            recovered_accepted = {
                **row,
                "recovered_ts": _utc_now(),
                "state": "accepted_unconsumed",
            }
            try:
                _append_row(ledger, recovered_accepted)
            except OSError as exc:
                raise HandoffError(f"accepted recovery ledger append failed; claim retained at {path}: {exc}") from exc
        else:
            raise HandoffError("accepted recovery requires an accepted ledger row or a receipt-persisted pending claim")
        _release_claim(path, resolution="accepted")
        return {
            "handoff_id": handoff_id,
            "state": "accepted_claim_released",
            "target_session": target,
            "task_key": task_key,
        }
    if resolution != "failed":
        raise HandoffError("resolution must be accepted or failed")
    if latest and latest.get("state") in ACCEPTED_STATES:
        raise HandoffError("cannot mark an accepted handoff failed; use status and accepted recovery")

    recovered = {
        **row,
        "error": "operator confirmed the queued message was not accepted",
        "recovered_ts": _utc_now(),
        "state": "failed",
    }
    try:
        _append_row(ledger, recovered)
    except OSError as exc:
        raise HandoffError(f"recovery ledger append failed; claim retained at {path}: {exc}") from exc
    _release_claim(path, resolution="failed")
    return {
        "handoff_id": handoff_id,
        "state": "failed_claim_released",
        "target_session": target,
        "task_key": task_key,
    }


def _user_message_matches(path: Path, *, target: str, row: dict) -> bool:
    marker = f"[sidecar-handoff v{SCHEMA_VERSION} id={row.get('handoff_id')} sha256={row.get('sha256')}]"
    source_session = row.get("source_session")
    if source_session == "not-recorded":
        source_argument = None
    elif isinstance(source_session, str):
        source_argument = source_session
    else:
        return False
    try:
        handle = path.open(encoding="utf-8", errors="replace")
    except OSError:
        return False
    with handle:
        for raw in handle:
            if marker not in raw:
                continue
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue
            payload = item.get("payload") or {}
            if item.get("type") != "response_item" or payload.get("type") != "message" or payload.get("role") != "user":
                continue
            text = "".join(str(part.get("text", "")) for part in payload.get("content", []) if isinstance(part, dict))
            if not text.startswith(marker + "\n"):
                continue
            packet_boundary = text.find("\n\n---\n")
            if packet_boundary < 0:
                continue
            packet = text[packet_boundary + 2 :]
            try:
                rendered = build_message(
                    target_session=target,
                    packet=packet,
                    source_session=source_argument,
                )
            except HandoffError:
                continue
            if rendered["message"] != text:
                continue
            if all(
                rendered[key] == row.get(key)
                for key in (
                    "handoff_id",
                    "sha256",
                    "source_snapshot_id",
                    "task_key",
                    "verification_status",
                )
            ):
                return True
    return False


def _session_files(codex_home: Path, target: str) -> list[Path]:
    found: list[Path] = []
    for directory in (codex_home / "sessions", codex_home / "archived_sessions"):
        if not directory.is_dir():
            continue
        try:
            found.extend(directory.rglob(f"*{target}.jsonl"))
        except OSError:
            continue
    return sorted(set(found), key=lambda item: item.stat().st_mtime if item.exists() else 0, reverse=True)


def status_handoff(
    *,
    target_session: str,
    handoff_id: str,
    ledger: Path = DEFAULT_LEDGER,
    claims_dir: Path = DEFAULT_CLAIMS,
    codex_home: Path | None = None,
) -> dict:
    target = _canonical_uuid(target_session, "target session")
    if not HANDOFF_ID_RE.fullmatch(handoff_id):
        raise HandoffError("handoff id must have form sc- plus 16 lowercase hex characters")
    tracked = [
        row for row in _rows(ledger) if row.get("target_session") == target and row.get("handoff_id") == handoff_id
    ]
    latest = tracked[-1] if tracked else None
    retained_claim = _find_claim(claims_dir, target, handoff_id)
    receipt = latest.get("receipt") if latest else None
    accepted = False
    if latest and latest.get("state") in ACCEPTED_STATES and isinstance(receipt, str):
        try:
            accepted = _canonical_uuid(receipt, "receipt") == receipt
        except HandoffError:
            accepted = False

    delivered_file = None
    if accepted and latest:
        codex_home = codex_home or Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
        files = _session_files(codex_home, target)
        delivered_file = next(
            (path for path in files if _user_message_matches(path, target=target, row=latest)),
            None,
        )
    if delivered_file:
        state = "delivered"
    elif accepted:
        state = "accepted_unconsumed"
    elif retained_claim:
        state = "recovery_required"
    elif latest and latest.get("state") not in ACCEPTED_STATES:
        state = str(latest.get("state", "unknown"))
    elif latest:
        state = "unknown"
    else:
        state = "untracked"
    return {
        "claim_cleanup_required": bool(retained_claim and accepted),
        "handoff_id": handoff_id,
        "claim_file": str(retained_claim[0]) if retained_claim else None,
        "receipt": receipt,
        "session_file": str(delivered_file) if delivered_file else None,
        "state": state,
        "target_session": target,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("template", help="print the required Markdown packet template")
    snapshot = sub.add_parser("snapshot", help="print the current Git source snapshot id")
    snapshot.add_argument("--root", required=True, type=Path, help="exact Git worktree used by the sidecar")
    snapshot.add_argument(
        "--read-path",
        required=True,
        action="append",
        help="bounded relative file or glob; repeat for each sidecar read scope",
    )

    validate = sub.add_parser("validate", help="validate and identify a packet without queueing it")
    validate.add_argument("--thread", required=True, help="target Codex session UUID")
    validate.add_argument("--file", required=True, type=Path, help="UTF-8 Markdown packet")
    validate.add_argument("--source-root", required=True, type=Path, help="Git worktree bound by the packet")
    validate.add_argument("--source-session", help="optional source Codex session UUID")

    queue = sub.add_parser("queue", help="queue one validated packet and record its receipt")
    queue.add_argument("--thread", required=True, help="target Codex session UUID")
    queue.add_argument("--file", required=True, type=Path, help="UTF-8 Markdown packet")
    queue.add_argument("--source-root", required=True, type=Path, help="Git worktree bound by the packet")
    queue.add_argument("--source-session", help="optional source Codex session UUID")
    queue.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER, help=argparse.SUPPRESS)
    queue.add_argument("--claims-dir", type=Path, default=DEFAULT_CLAIMS, help=argparse.SUPPRESS)
    queue.add_argument("--codex", dest="codex_path", help=argparse.SUPPRESS)

    status = sub.add_parser("status", help="report accepted_unconsumed versus delivered")
    status.add_argument("--thread", required=True, help="target Codex session UUID")
    status.add_argument("--handoff-id", required=True)
    status.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER, help=argparse.SUPPRESS)
    status.add_argument("--claims-dir", type=Path, default=DEFAULT_CLAIMS, help=argparse.SUPPRESS)
    status.add_argument("--codex-home", type=Path, help=argparse.SUPPRESS)

    recover = sub.add_parser("recover", help="release one retained claim after checking the target session")
    recover.add_argument("--thread", required=True, help="target Codex session UUID")
    recover.add_argument("--task-key", required=True)
    recover.add_argument("--handoff-id", required=True)
    recover.add_argument("--resolution", required=True, choices=("accepted", "failed"))
    recover.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER, help=argparse.SUPPRESS)
    recover.add_argument("--claims-dir", type=Path, default=DEFAULT_CLAIMS, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            with suppress(OSError, TypeError, ValueError):
                reconfigure(errors="backslashreplace")
    args = _parser().parse_args(argv)
    try:
        if args.command == "template":
            print(TEMPLATE, end="")
            return 0
        if args.command == "snapshot":
            print(git_snapshot(args.root, args.read_path))
            return 0
        if args.command == "validate":
            _require_external_packet(args.file, args.source_root)
            packet = read_packet(args.file)
            result = build_message(
                target_session=args.thread,
                packet=packet,
                source_session=args.source_session,
            )
            read_paths = _read_path_patterns(_front_matter(packet)["read_paths"])
            current_snapshot = git_snapshot(args.source_root, read_paths)
            if result["source_snapshot_id"] != current_snapshot:
                raise HandoffError(
                    f"stale packet snapshot: packet has {result['source_snapshot_id']}, "
                    f"current source is {current_snapshot}"
                )
            print(
                json.dumps(
                    {
                        key: result[key]
                        for key in (
                            "handoff_id",
                            "message_chars",
                            "sha256",
                            "source_snapshot_id",
                            "task_key",
                            "target_session",
                        )
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "queue":
            result = queue_handoff(
                target_session=args.thread,
                packet_path=args.file,
                source_root=args.source_root,
                source_session=args.source_session,
                ledger=args.ledger,
                claims_dir=args.claims_dir,
                codex_path=args.codex_path,
            )
            print(json.dumps(result, sort_keys=True))
            return 0
        if args.command == "recover":
            result = recover_claim(
                target_session=args.thread,
                task_key=args.task_key,
                handoff_id=args.handoff_id,
                resolution=args.resolution,
                ledger=args.ledger,
                claims_dir=args.claims_dir,
            )
            print(json.dumps(result, sort_keys=True))
            return 0
        result = status_handoff(
            target_session=args.thread,
            handoff_id=args.handoff_id,
            ledger=args.ledger,
            claims_dir=args.claims_dir,
            codex_home=args.codex_home,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    except HandoffError as exc:
        print(f"sidecar-handoff: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
