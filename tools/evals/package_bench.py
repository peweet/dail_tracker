"""A/B bench: do the 2026-07-25 tools (search_speeches/search_questions/column_deps)
beat the baseline toolset on the SAME tasks?

Design (PaddleOCR-style): identical prompts, identical model, two variants —
`baseline` and `newtools` both wire the MCP server explicitly. The baseline uses a
static public-safe legacy catalog: every registered public tool except the three
new tools and the three project-disabled private siting tools below. It is static
by design so a future catalog addition cannot silently widen the benchmark arm.
Ground truth was established INDEPENDENTLY of the new tools (plain ILIKE
reference queries; manual read of the payments view chain) so the test is not
circular. Metrics per run: correctness score vs ground truth, tool-call count,
turns, cost. The final answer text is kept for qualitative comparison.

Run: .venv/Scripts/python tools/evals/package_bench.py [baseline|newtools] [task...]
Lesson from routing_probe baked in: the SDK loads no .mcp.json — mcp_servers is
wired explicitly or every probe measures tool-absence.
"""

import json
import re
import sys
from pathlib import Path

import anyio

PROJ_PATH = Path(__file__).resolve().parents[2]
if str(PROJ_PATH) not in sys.path:
    sys.path.insert(0, str(PROJ_PATH))

from tools.evals.provider_adapter import EvalRequest, dail_tracker_mcp, run_eval  # noqa: E402

PROJ = str(PROJ_PATH)

NEW_TOOLS = [
    "mcp__dail-tracker__search_speeches",
    "mcp__dail-tracker__search_questions",
    "mcp__dail-tracker__column_deps",
]

BASELINE_TOOLS = (
    "search_members",
    "get_member_record",
    "list_recent_votes",
    "get_division",
    "division_interest_breakdown",
    "voting_vs_interests",
    "search_legislation",
    "get_bill",
    "search_statutory_instruments",
    "circular_si_crosswalk",
    "top_payments",
    "lobbying_organisations",
    "revolving_door",
    "ministerial_diary_top_organisations",
    "ministerial_diary_organisation",
    "who_ministers_meet",
    "company_influence",
    "access_to_contracts",
    "procurement_lobbying_overlap",
    "search_suppliers",
    "get_supplier",
    "procurement_competition",
    "list_committees",
    "get_committee",
    "get_member_interests",
    "who_was_minister",
    "get_member_questions",
    "member_question_count_by_year",
    "payments_by_year",
    "member_speeches",
    "party_donations",
    "party_election_spend",
    "judicial_appointments",
    "courts_health",
    "public_appointments",
    "charity_financials",
    "corporate_distress_notices",
    "corporate_repeat_distress",
    "nphdb_bam_disclosures",
    "public_body_payments",
    "procurement_by_authority",
    "procurement_by_cpv",
    "open_tenders",
    "current_cabinet",
    "dpo_lobbying_profile",
    "search_votes_by_topic",
    "join_map",
    "data_coverage",
    "list_datasets",
    "describe_dataset",
    "search_project",
    "code_outline",
    "py_deps",
    "py_refs",
    "json_peek",
    "view_deps",
    "search_council_minutes",
    "source_fetch_failures",
    "procurement_notice",
    "project_value_estimate",
    "cross_register_watchlist",
    "organisation_dossier",
    "derelict_levy_compliance",
    "council_scorecard",
    "afs_coverage",
    "housing_money",
    "attendance_ranking",
    "gov_finance_annual",
)

TASKS = {
    "column-lineage": {
        "prompt": (
            "In this repo's sql_views layer: if the total_paid column exposed by the view "
            "v_payments_yearly_evolution were renamed, which OTHER registered views would "
            "break, directly or transitively? Reply ONLY with a JSON array of view names."
        ),
        "gt": {"v_payments_alltime_ranking", "v_payments_alltime_summary"},
        "kind": "exact_set",
    },
    "topic-speeches": {
        "prompt": (
            "Using the dail-tracker data: which TDs have spoken most on the Dáil floor about "
            "defective concrete blocks / mica redress? Reply ONLY with a JSON array of up to "
            "5 speaker names, most prominent first."
        ),
        # ILIKE reference top-8 (bench_gt.py 2026-07-25) — generous superset for precision
        "gt": {
            "taoiseach",
            "thomas pringle",
            "pearse doherty",
            "richard boyd barrett",
            "charles ward",
            "peadar tóibín",
            "pádraig mac lochlainn",
            "eoin ó broin",
        },
        "kind": "name_overlap",
    },
    "topic-questions": {
        "prompt": (
            "Using the dail-tracker data: which TWO departments/ministries receive the most "
            "parliamentary questions about direct provision? Reply ONLY with a JSON array of "
            "the two ministry names."
        ),
        "gt": {"children", "justice"},
        "kind": "name_overlap",
    },
}


def _norm(s: str) -> str:
    s = s.lower().replace("deputy ", "").replace("the ", "").replace("minister for ", "")
    return re.sub(r"[^a-zá-ú ]", "", s).strip()


def score(kind: str, gt: set, answer: list) -> float:
    ans = {_norm(str(a)) for a in answer if str(a).strip()}
    if not ans:
        return 0.0
    gtn = {_norm(g) for g in gt}
    if kind == "exact_set":
        inter = len(ans & gtn)
        return round(2 * inter / (len(ans) + len(gtn)), 3)  # F1 on sets
    hits = sum(1 for a in ans if any(g in a or a in g for g in gtn))
    return round(hits / len(ans), 3)  # precision vs reference superset


def parse_answer(text: str) -> list:
    m = re.search(r"\[.*?\]", text or "", re.S)
    if not m:
        return []
    try:
        v = json.loads(m.group(0))
        return v if isinstance(v, list) else []
    except Exception:
        return []


async def run_task(task: str, variant: str) -> dict:
    spec = TASKS[task]
    calls: list[str] = []
    cost = None
    text = ""
    err = None
    provider = None
    model = None
    try:
        result = await run_eval(
            EvalRequest(
                prompt=spec["prompt"],
                cwd=PROJ,
                claude_model="claude-sonnet-5",
                max_turns=12,
                sandbox="read-only",
                project_settings=True,
                allowed_tools=(
                    NEW_TOOLS if variant == "newtools" else [f"mcp__dail-tracker__{tool}" for tool in BASELINE_TOOLS]
                ),
                disallowed_tools=NEW_TOOLS if variant == "baseline" else [],
                mcp_servers=dail_tracker_mcp(PROJ),
            )
        )
        calls = result.tool_names
        cost = result.cost_usd
        text = result.final_text
        provider = result.provider
        model = result.model
        if result.is_error:
            err = result.error
    except Exception as e:  # keep partials — they are the finding
        err = f"{type(e).__name__}: {e}"
    answer = parse_answer(text)
    out = {
        "task": task,
        "variant": variant,
        "score": score(spec["kind"], spec["gt"], answer),
        "tool_calls": len(calls),
        "mcp_calls": sum(c.startswith("mcp__") for c in calls),
        "new_tool_calls": sum(c in NEW_TOOLS for c in calls),
        "cost_usd": round(cost, 4) if cost else None,
        "answer": answer,
        "sequence": calls,
        "provider": provider,
        "model": model,
    }
    if err:
        out["error"] = err
    return out


async def main():
    import sys

    # --require-perfect is the fail-closed gate flag: any score below 1.0 exits
    # nonzero (test/tools/evals/test_promptfoo_probe_contracts.py pins this).
    require_perfect = "--require-perfect" in sys.argv[1:]
    args = [a.lower() for a in sys.argv[1:] if a != "--require-perfect"]
    variants = [v for v in ("baseline", "newtools") if not args or v in args] or ["baseline", "newtools"]
    tasks = [t for t in TASKS if t in args] or list(TASKS)
    imperfect: list[str] = []
    for variant in variants:
        for task in tasks:
            r = await run_task(task, variant)
            print(json.dumps(r, ensure_ascii=False))
            if float(r.get("score") or 0.0) < 1.0:
                imperfect.append(f"{task}/{variant}={r.get('score')}")
    if require_perfect and imperfect:
        raise SystemExit(f"require-perfect: score(s) below 1.0: {', '.join(imperfect)}")


if __name__ == "__main__":
    anyio.run(main)
