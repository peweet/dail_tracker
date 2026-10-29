#!/usr/bin/env python
"""PreToolUse hook — when a tool call touches a DATED doc under planning/product/doc, say
where its state of record is and how far the engine has moved since.

Grounded in 2026-10-21: the first 60 lines of OUTCOME_PANEL_2026_09_28.md (the pre-work
"refinements, ranked" plan) were quoted as the current state, while the 600 lines below
recorded the fixes, an out-of-sample test and a 4,848-appeal wide scan that superseded
every figure — and 67 engine/catalogue commits had landed after the doc's last section.
The habit fix is "read the headings to the END first"; this hook does that mechanically
at the one moment it matters, before the read lands.

Extended 2026-10-22: the first version fired on `Read` only. In auto mode the assistant reads
with `sed`/`cat`/`grep` through Bash, so a whole session quoted June and August performance
figures (PLAN_ENGINE_PARALLELISM_2026_08_24, PERFORMANCE_AUDIT_2026_08_30, the 13 June spatial
benchmark) as current without the hook ever firing. It now also inspects `Grep` paths and
every dated-doc path inside a `Bash`/`PowerShell` command, reads the doc's front matter
(`status:`, `updated:`, `domain:`), and names the current statement of record for
performance documents (the Status block in doc/PERFORMANCE_EXECUTION_LEDGER.md).

Advisory only (additionalContext, never a block), once per session per file. Cost: one
small file read plus one bounded `git log`; fails open to silence on any error.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PRODUCT = os.path.join(_REPO, "planning", "product")
# A dated doc path, absolute or relative, possibly after a shell variable ("$P/doc/...").
_DATED_DOC = re.compile(
    r"(?P<path>(?:[A-Za-z]:/|/)?(?:[\w.$~{}\-]+/)*?doc/(?:[\w.\-]+/)*[\w.\-]*?"
    r"(?P<y>20\d\d)[-_](?P<m>\d\d)[-_](?P<d>\d\d)[\w.\-]*\.md)",
    re.I,
)
_HEADING = re.compile(r"^(#{1,3})\s+(.*)$")
_DATE_IN_TEXT = re.compile(
    r"(20\d\d-\d\d-\d\d|\b\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) 20\d\d)"
)
_STATUS_BLOCK = re.compile(r"^\*\*Status, .*?20\d\d", re.M)
_FRONT_MATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---", re.S)
_ENGINE_PATHS = ("core/evaluation", "rules/issue_catalogue.yaml")
_REPORT_PATHS = ("core", "api", "services", "deploy", "native")
_PERF_NAME = re.compile(
    r"PERF|PARALLEL|OPTIMI|RUST|TYPST|SCAL|BENCH|LIBRAR|RUNTIME|CAPACITY|THROUGHPUT|LATENCY|EFFICIEN", re.I
)
_PERF_DOMAIN = re.compile(r"perf|infra|pdf-delivery", re.I)
_CURRENT_LIKE = {"LIVE", "ACTIVE", "READY", "MEASURED"}
_HISTORY_LIKE = {"HISTORICAL", "SUPERSEDED", "ARCHIVED", "RETIRED"}
_LEDGER = "planning/product/doc/PERFORMANCE_EXECUTION_LEDGER.md"
_MAX_BYTES = 2 * 1024 * 1024
_MAX_FILES_PER_CALL = 3


def _tool_input(payload: dict) -> dict:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    return ti if isinstance(ti, dict) else {}


def _already(session: str, key: str) -> bool:
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_dated_doc_{session[:12]}_{key}")
        if os.path.exists(marker):
            return True
        with open(marker, "w") as fh:
            fh.write("1")
        return False
    except Exception:
        return True


def _commits_since(iso_date: str, paths: tuple[str, ...]) -> int | None:
    try:
        out = subprocess.run(
            ["git", "-C", _PRODUCT, "log", "--format=%h", f"--since={iso_date}", "--", *paths],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        if out.returncode != 0:
            return None
        return sum(1 for line in out.stdout.splitlines() if line.strip())
    except Exception:
        return None


def _front_matter(text: str) -> dict[str, str]:
    m = _FRONT_MATTER.match(text)
    if not m:
        return {}
    fields: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" in line and not line.startswith((" ", "\t", "-")):
            key, _, value = line.partition(":")
            fields[key.strip().lower()] = value.strip()
    return fields


def _resolve(candidate: str) -> str | None:
    """Map a path fragment from a command or tool input onto a real file, or None."""
    cand = candidate.replace("\\", "/")
    tries = [cand]
    if not os.path.isabs(cand):
        tries.append(os.path.join(_REPO, cand))
        tries.append(os.path.join(_PRODUCT, cand))
    idx = cand.lower().find("doc/")
    if idx >= 0:
        tries.append(os.path.join(_PRODUCT, cand[idx:]))
    for path in tries:
        if os.path.isfile(path):
            full = os.path.abspath(path).replace("\\", "/")
            if "/planning/product/doc/" in full.lower():
                return full
    return None


def _summarise(path: str, doc_date: str) -> str | None:
    if os.path.getsize(path) > _MAX_BYTES:
        return None
    with open(path, encoding="utf-8", errors="replace") as fh:
        text = fh.read()
    name = os.path.basename(path)
    fm = _front_matter(text)
    status = fm.get("status", "")
    status_key = re.split(r"[\s;(]", status.upper().strip() or "")[0] if status else ""
    updated = fm.get("updated", "") or doc_date
    headings = [
        (len(m.group(1)), m.group(2).strip()) for m in (_HEADING.match(line) for line in text.splitlines()) if m
    ]
    h2 = [title for level, title in headings if level == 2]
    dated = [title for title in h2 if _DATE_IN_TEXT.search(title)]
    has_status = bool(_STATUS_BLOCK.search(text[:4000]))
    perfish = bool(_PERF_NAME.search(name)) or bool(_PERF_DOMAIN.search(fm.get("domain", "")))
    commits = _commits_since(updated, _REPORT_PATHS if perfish else _ENGINE_PATHS)
    what = "engine/report-path" if perfish else "engine/catalogue"

    bits = [f"[dated-doc] {name}: {len(h2)} H2 sections"]
    if has_status:
        bits.append("a **Status** block at the top supersedes everything above the section it names — read it first")
    if status_key in _HISTORY_LIKE:
        bits.append(f"front matter `status: {status}` — a historical record; every figure is the {updated} engine")
    elif status_key in _CURRENT_LIKE and commits:
        bits.append(
            f"front matter says `status: {status}` but {commits} {what} commit(s) landed after `updated: {updated}` — "
            "the label is not currency"
        )
    if dated:
        bits.append(f'last dated section: "{dated[-1][:70]}"')
    if h2:
        tail = " | ".join(t[:40] for t in h2[-3:])
        bits.append(f"final sections: {tail}")
    if commits is not None and status_key not in _HISTORY_LIKE:
        bits.append(
            f"{commits} {what} commit(s) since {updated} — figures here are "
            + ("current only if a later measurement says so" if commits else "measured on the current engine")
        )
    if perfish and not name.startswith("PERFORMANCE_EXECUTION_LEDGER"):
        bits.append(f"current statement of record for performance figures: the Status block at the top of {_LEDGER}")
    bits.append("read the headings to the END before quoting a figure from the top")
    return "; ".join(bits)


def _candidates(tool: str, ti: dict) -> list[tuple[str, str]]:
    """Return (resolved_path, iso_date) pairs this call touches, deduplicated, bounded."""
    if tool == "Read":
        sources = [str(ti.get("file_path") or "")]
    elif tool == "Grep":
        sources = [str(ti.get("path") or "")]
    elif tool in ("Bash", "PowerShell"):
        sources = [str(ti.get("command") or "")]
    else:
        return []
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for source in sources:
        for m in _DATED_DOC.finditer(source.replace("\\", "/")):
            # An absolute path that exists is the file; only fragments need resolving.
            fragment = m.group("path")
            resolved = fragment if (os.path.isabs(fragment) and os.path.isfile(fragment)) else None
            if resolved and "/planning/product/doc/" not in os.path.abspath(resolved).replace("\\", "/").lower():
                resolved = None
            resolved = resolved or _resolve(fragment)
            if not resolved or resolved.lower() in seen:
                continue
            seen.add(resolved.lower())
            found.append((resolved, f"{m.group('y')}-{m.group('m')}-{m.group('d')}"))
            if len(found) >= _MAX_FILES_PER_CALL:
                return found
    return found


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    tool = payload.get("tool_name") or payload.get("toolName") or ""
    if tool not in ("Read", "Grep", "Bash", "PowerShell"):
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    messages: list[str] = []
    for path, doc_date in _candidates(tool, _tool_input(payload)):
        key = re.sub(r"[^A-Za-z0-9]", "_", os.path.basename(path))[:40]
        if _already(session, key):
            continue
        try:
            message = _summarise(path, doc_date)
        except Exception:
            continue
        if message:
            messages.append(message)
    if not messages:
        return 0
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": "\n".join(messages)}}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
