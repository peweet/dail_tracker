#!/usr/bin/env python
"""SessionStart hook — inject a tiny, fresh project status line.

Gives every new session a few load-bearing facts without the agent spending tool
calls to derive them: current git branch, whether doc/INDEX.md is stale, and the
most recent data-refresh heartbeat. Kept to a couple of lines of context on purpose.

Emits Claude-Code's structured additionalContext (nested + flat for VS Code
compatibility). Always exits 0 and never raises — a status line must not be able
to break a session.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _git_branch() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(REPO), capture_output=True, text=True, timeout=5,
        )
        return out.stdout.strip() or "?"
    except Exception:
        return "?"


def _doc_index_note() -> str:
    """Cheap staleness proxy: is any doc/*.md newer than the generated INDEX.md?"""
    try:
        docdir = REPO / "doc"
        index = docdir / "INDEX.md"
        if not index.exists():
            return "doc/INDEX.md missing (run tools/build_doc_index.py)"
        idx_m = index.stat().st_mtime
        newer = [p.name for p in docdir.glob("*.md") if p.name != "INDEX.md" and p.stat().st_mtime > idx_m]
        if newer:
            return f"doc/INDEX.md STALE ({len(newer)} doc(s) changed since; run tools/build_doc_index.py)"
        return "doc/INDEX.md fresh"
    except Exception:
        return ""


def _heartbeat_note() -> str:
    try:
        hb = REPO / "data" / "_meta" / "heartbeats"
        if not hb.is_dir():
            return ""
        files = sorted(hb.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            return ""
        latest = files[0]
        stamp = ""
        with contextlib_suppress():
            data = json.loads(latest.read_text(encoding="utf-8"))
            stamp = str(data.get("last_success") or data.get("ended_utc") or data.get("timestamp") or "")
        return f"{len(files)} refresh heartbeat(s); latest: {latest.stem}{(' @ ' + stamp) if stamp else ''}"
    except Exception:
        return ""


def _mcp_note() -> str:
    """MCP health: config lint + code compile + a real stdio connect probe.

    Hardened 2026-07-25 after `${workspaceFolder}` in .mcp.json (VS Code syntax Claude
    Code never expands) left the server unable to connect for 8 days while this note
    said "config+code OK" — the old check validated config and code but never a
    connection. Checks in order of determinism:
      1. .mcp.json parses, holds NO unexpanded ${...} placeholder, and each server's
         command/first-arg path resolves to a real file (relative paths resolve
         against the repo root, matching Claude Code's behaviour);
      2. every mcp_server/*.py compiles;
      3. unless DAIL_SKIP_MCP_PROBE=1, a real handshake: spawn the server and exchange
         an MCP `initialize` over stdio — the only proof it can actually start.
    """
    try:
        cfg = REPO / ".mcp.json"
        if not cfg.exists():
            return "MCP: .mcp.json MISSING — dail-tracker tools unavailable"
        raw = cfg.read_text(encoding="utf-8")
        try:
            servers = json.loads(raw).get("mcpServers", {})
        except Exception:
            return "MCP: .mcp.json UNPARSEABLE — fix it, then /mcp"
        if "${" in raw:
            return ("MCP: .mcp.json holds an unexpanded ${...} placeholder — Claude Code "
                    "only expands env vars, so the server CANNOT connect; use relative or "
                    "absolute paths")
        cmd_path = args = None
        for spec in servers.values():
            cmd = str(spec.get("command", ""))
            args = [str(a) for a in spec.get("args", [])]
            cmd_path = Path(cmd) if Path(cmd).is_absolute() else REPO / cmd
            if not cmd_path.is_file():
                return f"MCP: command path NOT FOUND ({cmd}) — server cannot start"
            for a in args:
                if a.endswith(".py"):
                    ap = Path(a) if Path(a).is_absolute() else REPO / a
                    if not ap.is_file():
                        return f"MCP: server script NOT FOUND ({a})"
        import py_compile

        for p in sorted((REPO / "mcp_server").glob("*.py")):
            try:
                py_compile.compile(str(p), doraise=True)
            except Exception as exc:  # noqa: BLE001 — name the file, keep the session alive
                return f"MCP: server code BROKEN ({p.name}: {type(exc).__name__}) — /mcp will fail"
        import os

        if os.environ.get("DAIL_SKIP_MCP_PROBE") == "1" or cmd_path is None:
            return "MCP: config+code OK (probe skipped; if tools are missing, /mcp)"
        return _mcp_connect_probe(cmd_path, args or [])
    except Exception:
        return ""


def _mcp_connect_probe(cmd_path: Path, args: list[str]) -> str:
    """Spawn the configured server and exchange an MCP initialize (newline-delimited
    JSON-RPC over stdio). ~0.5-2 s; 6 s hard timeout; fails open to a WARN note, never
    an exception — a status line must not break a session."""
    try:
        init = json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                       "clientInfo": {"name": "session-context-probe", "version": "0"}},
        }) + "\n"
        proc = subprocess.Popen(
            [str(cmd_path), *args], cwd=str(REPO),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, encoding="utf-8",
        )
        try:
            out, _ = proc.communicate(input=init, timeout=6)
        finally:
            if proc.poll() is None:
                proc.kill()
        if out and '"serverInfo"' in out:
            return "MCP: server handshake OK (connect via /mcp if tools are missing)"
        return "MCP: server spawned but handshake FAILED — run `claude mcp list` to diagnose"
    except subprocess.TimeoutExpired:
        return "MCP: connect probe TIMED OUT — server may hang on start; `claude mcp list`"
    except Exception:
        return "MCP: connect probe errored — `claude mcp list` to diagnose"


def _adoption_tripwire() -> str:
    """Trend guard on the ledger's mcp column (rows exist from 2026-07-25): if the
    last 3 substantive sessions (>=20 turns) all logged ZERO dail-tracker calls, say
    so — one dead session is noise, three is a broken reflex or a broken server."""
    try:
        path = REPO / "logs" / "session_token_ledger.jsonl"
        if not path.exists():
            return ""
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines()[-60:]:
            try:
                o = json.loads(line)
            except Exception:
                continue
            if o.get("turns", 0) >= 20 and "mcp" in o:
                rows.append(o)
        last = rows[-3:]
        if len(last) == 3 and all(not r.get("mcp") for r in last):
            return ("adoption tripwire: last 3 substantive sessions logged 0 MCP calls — "
                    "the server connects now; route data questions via describe_dataset/"
                    "search_project")
        return ""
    except Exception:
        return ""


def _discoveries_note() -> str:
    """Point at the token-saving discovery index — pull, not push.

    Reads only tools/discoveries.jsonl (a few KB, ~0.2 ms) and counts rows; it never
    runs tools/token_ledger.py, which scans every transcript and is far too heavy for a
    hook. One line so the agent knows to fetch a cached one-liner before re-exploring.
    """
    try:
        path = REPO / "tools" / "discoveries.jsonl"
        if not path.exists():
            return ""
        n = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                if not str(json.loads(line).get("id", "")).startswith("_"):
                    n += 1
            except Exception:
                continue
        if not n:
            return ""
        return f"{n} discoveries indexed — `python tools/discoveries.py <topic>` for the one-liner before exploring"
    except Exception:
        return ""


def _mcp_adoption_note(current_id: str = "") -> str:
    """Surface the previous session's steering-tool adoption — feedback at the one
    moment it can change behaviour.

    Reads ONLY the single most recent transcript via check_mcp_adoption.scan_transcript
    (one file, cheap: ~26 ms on a 4.8 MB transcript, ~65 ms worst case). It must never
    call scan_all() — scanning every transcript is far too heavy for a hook, the same
    reason _discoveries_note refuses to run token_ledger.
    Measured 2026-07-20: navigation tools (describe_dataset / search_project) are called
    ~never while Read/Grep/Glob run thousands of times; this makes that gap felt per
    session instead of buried in a memory file.

    current_id is the SessionStart payload's transcript_path or session_id; the current
    transcript is excluded so a *resumed* session (its own transcript is newest and
    already has turns) reports the PRIOR session, not itself. The turns<5 skip alone
    only covers a fresh start.
    """
    try:
        import glob
        import os
        import sys as _sys

        tools = REPO / "tools"
        if str(tools) not in _sys.path:
            _sys.path.insert(0, str(tools))
        from check_mcp_adoption import PROJ, scan_transcript  # type: ignore

        paths = sorted(glob.glob(os.path.join(PROJ, "*.jsonl")), key=os.path.getmtime, reverse=True)
        for p in paths:
            stem = os.path.basename(p).split(".")[0]
            if current_id and stem and stem in current_id:
                continue  # never report the current session as "last session"
            r = scan_transcript(p)
            if r["turns"] < 5:
                continue  # skip a just-started session (near-zero turns)
            nav, mcp, raw = r["steer"], r["mcp_total"], r["raw_total"]
            if raw >= 20 and nav == 0 and mcp == 0:
                return (f"steering: last session {raw} Read/Grep/Glob, 0 MCP — for data "
                        "shape/where-does-X-live, try describe_dataset/search_project first")
            if mcp or nav:
                return f"steering: last session {mcp} MCP call(s) ({nav} navigation), {raw} raw reads"
            return ""  # low-activity session, nothing worth surfacing
        return ""
    except Exception:
        return ""


def _style_digest_note() -> str:
    """Weekly one-liner from the silent style log (style_lint.py demoted its per-reply
    warnings 2026-07-25). Emits at most once per 7 days, then advances the marker."""
    try:
        log = REPO / "logs" / "style_lint_log.jsonl"
        state = REPO / "logs" / "style_lint_digest_state.json"
        if not log.exists():
            return ""
        import time
        now = time.time()
        last = 0.0
        try:
            last = float(json.loads(state.read_text(encoding="utf-8")).get("last", 0))
        except Exception:
            pass
        if now - last < 7 * 86400:
            return ""
        from collections import Counter
        kinds: Counter[str] = Counter()
        rows = 0
        for line in log.read_text(encoding="utf-8").splitlines():
            try:
                o = json.loads(line)
            except Exception:
                continue
            rows += 1
            for w in o.get("warns", []):
                kinds["jargon" if str(w).startswith("jargon") else "long-sentence"] += 1
        if not rows:
            return ""
        state.write_text(json.dumps({"last": now}), encoding="utf-8")
        parts = ", ".join(f"{v} {k}" for k, v in kinds.most_common())
        return f"style digest (weekly): {rows} flagged replies — {parts}; log: logs/style_lint_log.jsonl"
    except Exception:
        return ""


class contextlib_suppress:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return True  # swallow everything


def _current_session_id() -> str:
    """Read the SessionStart payload on stdin for transcript_path / session_id.

    Lets _mcp_adoption_note exclude the current transcript. Same plain-read pattern the
    sibling hooks use (guard_data_reads, style_lint, firewall_check): Claude Code always
    sends the payload and closes stdin, so read() returns. Fails open to "" on an empty
    or unparseable payload (e.g. a manual test run) — the note then falls back to the
    turns<5 heuristic. Never raises; a status line must not break a session.
    """
    try:
        data = sys.stdin.read()
        if not data.strip():
            return ""
        payload = json.loads(data)
        if isinstance(payload, dict):
            return str(payload.get("transcript_path") or payload.get("session_id") or "")
    except Exception:
        pass
    return ""


def main() -> int:
    current_id = _current_session_id()
    parts = [f"branch: {_git_branch()}"]
    for note in (_doc_index_note(), _heartbeat_note(), _mcp_note(), _discoveries_note(),
                 _mcp_adoption_note(current_id), _adoption_tripwire(), _style_digest_note()):
        if note:
            parts.append(note)
    ctx = "Project status — " + " · ".join(parts) + (
        ". Data lives behind the dail-tracker MCP (describe_dataset / list_datasets / "
        "search_project) — don't scan parquet."
    )
    out = {
        "additionalContext": ctx,
        "hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx},
    }
    sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
