#!/usr/bin/env python
"""PostToolUse hook — nudge toward a dail-tracker DOMAIN tool when a Bash/PowerShell

command's shape looks like a hand-rolled DuckDB/polars query over a topic one of
the ~60 domain tools (procurement_by_authority, payments_by_year, member_speeches,
list_recent_votes, ...) already answers.

Sibling to grep_routing_hint.py, but for the other half of the adoption gap that
memory left open: 2026-07-31 fixed navigation-tool friction (search_project,
code_outline, py_refs, py_deps, view_deps, column_deps — alwaysLoad, no ToolSearch
tax) and it measurably narrowed the raw:nav ratio. It never touched the ~60
domain-data tools, which stayed deferred as "genuinely situational" — and a
2026-08-08 measurement (tools/token_week_review.py) found only 28 domain-tool
calls across 142 sessions in a week. Built, not used — this hook is the same
recognition-nudge shipped for Grep, aimed at the surface nobody nudged yet.

Deliberately soft (advisory additionalContext, never a block): whether a specific
DuckDB query "should have" been a domain-tool call is a semantic judgment the tool
itself can't always make (custom joins, one-off aggregates, and grain-sensitive
money queries are legitimate reasons to hand-write SQL — see CLAUDE.md's never-sum
rule). Confidence-gated the same way grep_routing_hint.py is: BOTH a data-query
signal (duckdb/polars/parquet-path mention) AND a topic-keyword match are required,
not either alone — a bare word like "votes" in a commit message or doc edit must
not fire this. Same one-nudge-per-session-per-category rate limit; same
best-effort trial log (logs/domain_tool_routing_trial.jsonl) so "does usage rise"
is measurable later instead of re-derived from a transcript audit again.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from datetime import UTC, datetime

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_TRIAL_LOG = os.path.join(_REPO, "logs", "domain_tool_routing_trial.jsonl")

#: Any of these appearing in the command text is evidence it queries real data,
#: not just mentions a topic word in passing (a commit message, a doc edit).
_DATA_QUERY_SIGNAL = re.compile(
    r"duckdb|\bpolars\b|\.parquet\b|read_parquet|scan_parquet|data[/\\](?:gold|silver)[/\\]parquet",
    re.IGNORECASE,
)

#: category -> (topic regex, the domain tool(s) to suggest instead).
#: Keywords are real table/module names from dail_tracker_core/queries/, not
#: guesses — add a category only with a real query module or dataset behind it.
CATEGORY_HINTS: dict[str, tuple[re.Pattern[str], str]] = {
    "procurement": (
        re.compile(r"procurement|ted_ie_awards|\btender", re.IGNORECASE),
        "procurement_by_authority / procurement_by_cpv / procurement_notice / open_tenders",
    ),
    "payments": (
        re.compile(r"\bpayments?\b|public_body_payment", re.IGNORECASE),
        "payments_by_year / top_payments / public_body_payments",
    ),
    "lobbying": (
        re.compile(r"lobbying|\bdpo_", re.IGNORECASE),
        "lobbying_organisations / company_influence / dpo_lobbying_profile",
    ),
    "votes": (
        re.compile(r"\bvotes?\b|\bdivisions?\b", re.IGNORECASE),
        "list_recent_votes / search_votes_by_topic / get_division / division_interest_breakdown",
    ),
    "speeches_questions": (
        re.compile(r"\bspeeches?\b|member_question", re.IGNORECASE),
        "member_speeches / search_questions / member_question_count_by_year",
    ),
    "committees": (
        re.compile(r"\bcommittees?\b", re.IGNORECASE),
        "list_committees / get_committee",
    ),
    "corporate": (
        re.compile(r"corporate_distress|revolving_door|\bsipo\b", re.IGNORECASE),
        "corporate_distress_notices / corporate_repeat_distress / revolving_door",
    ),
    "council_minutes": (
        re.compile(r"council_minutes", re.IGNORECASE),
        "search_council_minutes",
    ),
    "planning": (
        re.compile(r"planning_precedent|\bsiting\b", re.IGNORECASE),
        "search_planning_precedents / siting_check",
    ),
    "legislation": (
        re.compile(r"legislation|statutory_instrument|circular_si", re.IGNORECASE),
        "search_legislation / search_statutory_instruments / circular_si_crosswalk",
    ),
    "judiciary": (
        re.compile(r"judicial|judiciary|courts_health", re.IGNORECASE),
        "judicial_appointments / courts_health",
    ),
    "ministerial": (
        re.compile(r"ministerial_diary|who_ministers_meet|\bcabinet\b", re.IGNORECASE),
        "who_ministers_meet / ministerial_diary_organisation / current_cabinet / who_was_minister",
    ),
}


def _command(payload: dict) -> str:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    if not isinstance(ti, dict):
        return ""
    for key in ("command", "cmd", "script"):
        value = ti.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _classify(command: str) -> str | None:
    if not command or not _DATA_QUERY_SIGNAL.search(command):
        return None
    for category, (pattern, _hint) in CATEGORY_HINTS.items():
        if pattern.search(command):
            return category
    return None


def _log_trial(session: str, category: str, nudged: bool, command: str) -> None:
    """Best-effort: never let logging break the hook."""
    try:
        row = {
            "ts": datetime.now(UTC).isoformat(),
            "session": session[:12],
            "category": category,
            "nudged": nudged,
            "command": command[:80],
        }
        os.makedirs(os.path.dirname(_TRIAL_LOG), exist_ok=True)
        with open(_TRIAL_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
    except Exception:
        pass


def _already_nudged(session: str, category: str) -> bool:
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_domain_route_{session[:12]}_{category}")
        if os.path.exists(marker):
            return True
        with open(marker, "w") as fh:
            fh.write("1")
        return False
    except Exception:
        return True  # can't track -> stay silent rather than risk nagging every call


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    tool = payload.get("tool_name") or payload.get("toolName") or ""
    if tool not in ("Bash", "PowerShell"):
        return 0
    try:
        command = _command(payload)
        category = _classify(command)
    except Exception:
        return 0
    if not category:
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    already = _already_nudged(session, category)
    _log_trial(session, category, nudged=not already, command=command)
    if already:
        return 0
    _, hint = CATEGORY_HINTS[category]
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "additionalContext": (
                        f"[tool-routing] That query looks like it covers the '{category}' "
                        f"topic by hand. Check whether {hint} already answers it before more "
                        "hand-rolled SQL — grain-sensitive money queries and custom joins are "
                        "still a legitimate reason to keep the query you just ran."
                    ),
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
