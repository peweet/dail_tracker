#!/usr/bin/env python
"""PreToolUse hook — block ad-hoc network-liveness probes the repo already has a tool for.

Written 2026-07-31 after the user flagged, for the second documented time (see MEMORY.md
feedback_use_internal_tools_not_adhoc), that the assistant reaches for curl/nslookup/ping
one-liners to answer "is this source/endpoint up" instead of running the repo's own
checkers: tools/build_source_health.py --check-links (OK/WARNING/FAILED/SKIPPED with
history) and tools/migration/check_source_cadence.py (cadence + broken-since state). A
memory note is Tier 3 (advisory — the assistant reads it and chooses to comply) and that
tier had already failed twice. Per feedback_guardrail_determinism_tiers, the fix is to
move THIS SPECIFIC, mechanically-unambiguous action up a tier, not to harden anything
semantic: "is this command idiom a liveness probe" is a regex match, not a judgment call.

Deliberately narrow: only matches liveness/status-check IDIOMS (curl -I / -o NUL -w
"%{http_code}", nslookup, ping, wget --spider, Test-NetConnection, Resolve-DnsName,
Invoke-WebRequest -Method Head / .StatusCode). A curl that fetches actual content (e.g.
the allow-listed doc-fetch commands in .claude/settings.json) is a different action and
must not be caught here — this hook checks HOW the command asks, not THAT it uses curl.

Cross-tool notes, same as guard_memory.py / guard_data_reads.py:
  * VS Code ignores matcher syntax and fires every hook under an event, so this script
    self-filters on the command text rather than trusting the matcher.
  * Tool-input keys differ across tools (snake_case vs camelCase) — try all.
  * stderr + exit 2 blocks in both tools. Exit 0 allows.

Escape hatch: set DAIL_ALLOW_ADHOC_NET=1 when the check genuinely isn't one the internal
tools cover (e.g. a brand-new source not yet registered).

Side effect: every match is appended to logs/adhoc_network_probe.jsonl, blocked or not,
so recurrence is visible without re-deriving it from a transcript search.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "logs" / "adhoc_network_probe.jsonl"

#: Idioms that mean "check whether a host/URL is alive/reachable", not "fetch its content".
#: Matched case-insensitively against the command text.
PROBE_PATTERNS = (
    (r"\bnslookup\b", "nslookup (DNS resolution probe)"),
    (r"\bResolve-DnsName\b", "Resolve-DnsName (DNS resolution probe)"),
    (r"\bping\s+(-n\s+\d+\s+)?[\w.\-]+", "ping (reachability probe)"),
    (r"\bTest-NetConnection\b", "Test-NetConnection (reachability probe)"),
    (r"\bTest-Connection\b", "Test-Connection (reachability probe)"),
    (r"\bwget\s+.*--spider\b", "wget --spider (liveness-only fetch)"),
    (r"\bcurl\b(?=.*(-I\b|--head\b|-sI\b))", "curl -I/--head (status-only fetch)"),
    (
        r"\bcurl\b(?=.*-o\s+(/dev/null|NUL)\b)(?=.*-w\s+[\"']?%\{http_code\})",
        "curl -o /dev/null -w %{http_code} (status-only fetch)",
    ),
    (r"Invoke-WebRequest.*-Method\s+Head\b", "Invoke-WebRequest -Method Head (status-only fetch)"),
    (r"Invoke-WebRequest.*\.StatusCode\b", "Invoke-WebRequest .StatusCode (status-only fetch)"),
)

#: Internal tools that already answer "is a registered source/endpoint healthy".
INTERNAL_TOOLS = (
    "tools/build_source_health.py --check-links  (or DAIL_CHECK_LINKS=1)",
    "tools/migration/check_source_cadence.py",
)


def _command(payload: dict) -> str:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    if not isinstance(ti, dict):
        return ""
    for key in ("command", "cmd", "script"):
        value = ti.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def classify(command: str) -> tuple[str, str]:
    """Return (pattern, why) for the first liveness-probe idiom the command matches."""
    for pattern, why in PROBE_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            return pattern, why
    return "", ""


def record(entry: dict) -> None:
    """Append one sample. Never raises — a ledger write must not break a tool call."""
    try:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0  # never break the agent on a parse hiccup

    command = _command(payload)
    if not command:
        return 0
    pattern, why = classify(command)
    if not pattern:
        return 0

    allowed = os.environ.get("DAIL_ALLOW_ADHOC_NET") == "1"
    record(
        {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "kind": why,
            "command": command[:300],
            "blocked": not allowed,
        }
    )
    if allowed:
        return 0

    tools = "\n".join(f"  {t}" for t in INTERNAL_TOOLS)
    sys.stderr.write(
        f"Blocked: this command is {why} — a hand-rolled endpoint/DNS liveness check. "
        "This repo already has checkers that answer exactly that question, with the "
        "status semantics and history the ad-hoc version can't reproduce:\n"
        f"{tools}\n"
        "Run one of those instead. Override with DAIL_ALLOW_ADHOC_NET=1 if this genuinely "
        "isn't one they cover (e.g. a source not yet registered).\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
