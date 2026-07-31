"""Convention ratchet — keeps the shared-utility consolidation from regrowing.

The 2026-07 refactor (memory: refactor-utility-portfolio) collapsed the big
duplication clusters into shared utilities:

    services/http_engine.fetch_bytes + polite_headers   (was: 91 hand-rolled HTTP files)
    services/coverage_io.save_coverage                  (was: ~43 raw write_text(json.dumps..))
    services/extract_runner.run_extractor               (was: 94 ad-hoc __main__ blocks)
    utility/ui/format.py                                (was: 8+ divergent _eur/_fmt clones)
    ui.components.dt_page                               (was: 12 pages without an error boundary)

`save_parquet` proved that a utility only reaches 100% adoption when a check
enforces it. This tool is that check, as a RATCHET: files that already carried
a pattern before the refactor are grandfathered in the baselines below and stay
allowed; any NEW file (or a rule with an empty baseline) fails. When you fix a
grandfathered file, the tool tells you to delete it from the baseline so the
ratchet only ever tightens. Do NOT add new files to a baseline — use the
shared utility instead.

Run:  ./.venv/Scripts/python tools/check_conventions.py
Also runs in the fast pytest suite via test/tools/test_conventions.py.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXTRACTORS = ROOT / "extractors"
PAGES = ROOT / "utility" / "pages_code"

# ── Rules over extractors/ ────────────────────────────────────────────────────

# R1: hand-rolled HTTP. New scrapers must use services.http_engine
#     (fetch_bytes / fetch_json / fetch_text / polite_headers).
RE_RAW_HTTP = re.compile(r"requests\.(get|post|Session)\(|urllib\.request")
BASELINE_RAW_HTTP = {
    "_gnews_resolve.py",
    "afs_amalgamated_extract.py",
    "cbi_registers_extract.py",
    "cro_financial_statements_extract.py",
    "cso_pxstat_extract.py",
    "derelict_sites_levy_extract.py",
    "housing_construction_pipeline_extract.py",
    "la_budgets_extract.py",
    "la_councillor_payments_extract.py",
    "lgas_audit_reports_extract.py",
    "member_contact_extract.py",
    "ministerial_diaries_extract.py",
    "news_mentions_extract.py",
    "opr_plan_directions_extract.py",
    "planning_acp_precedents.py",
    "planning_appeal_outcomes.py",
    "planning_applications_ingest.py",
    "planning_decision_profiles.py",
    "planning_layers_freshness.py",
    "planning_layers_ingest.py",
    "procurement_etenders_extract.py",
    "procurement_la_payments_extract.py",
    "sample_extract_procurement_pdf.py",
    "si_legislation_directory_extract.py",
    "si_lrc_classlist_extract.py",
    "sipo_candidate_expenses_crawl.py",
    "stateboards_roster_extract.py",
    "ted_ireland_tenders_extract.py",
}

# R2: User-Agent literals. Use polite_headers(browser=...) instead.
RE_UA_LITERAL = re.compile(r"Mozilla/5\.0")
BASELINE_UA_LITERAL = {
    "afs_amalgamated_extract.py",
    "cbi_registers_extract.py",
    "cso_pxstat_extract.py",
    "derelict_sites_levy_extract.py",
    "etenders_live_tenders_extract.py",
    "_gnews_resolve.py",
    "housing_construction_pipeline_extract.py",
    "la_budgets_extract.py",
    "la_councillor_payments_extract.py",
    "lgas_audit_reports_extract.py",
    "ministerial_diaries_extract.py",
    "news_mentions_extract.py",
    "opr_plan_directions_extract.py",
    "procurement_etenders_extract.py",
    "procurement_la_payments_extract.py",
    "procurement_la_seed.py",
    "sample_extract_procurement_pdf.py",
    "sipo_candidate_expenses_crawl.py",
    "stateboards_roster_extract.py",
}

# R3: direct parquet writes. save_parquet is at 100% adoption — keep it there.
RE_RAW_PARQUET = re.compile(r"\.write_parquet\(|\.to_parquet\(")
BASELINE_RAW_PARQUET: set[str] = set()

# R4: ad-hoc logging config. run_extractor / setup_standalone_logging own this.
RE_BASICCONFIG = re.compile(r"logging\.basicConfig")
BASELINE_BASICCONFIG = {"participation_extract.py"}

# R5: raw coverage-JSON emits. Use services.coverage_io.save_coverage (atomic).
RE_RAW_COVERAGE = re.compile(r"write_text\(json\.dumps")
BASELINE_RAW_COVERAGE = {
    "cbi_registers_extract.py",
    "diary_ocr.py",
    "disclosed_bq_po_extract.py",
    "disclosed_bq_po_newbodies_extract.py",
    "judiciary_bench_extract.py",
    "judiciary_diary_link.py",
    "la_afs_camelot_capital_ie.py",
    "la_afs_camelot_ie.py",
    "la_afs_capital_extract.py",
    "legal_diary_extract.py",
    "legal_diary_openview_extract.py",
    "ministerial_briefs_extract.py",
    "noac_scorecard_history_extract.py",
    "planning_appeal_outcomes.py",
    "planning_cpo_compensation.py",
    "planning_decision_profiles.py",
    "planning_layers_ingest.py",
    "procurement_award_spend_link.py",
    "procurement_dept_readingorder_parser.py",
    "procurement_etenders_extract.py",
    "procurement_hse_tusla_materialize.py",
    "procurement_hse_tusla_parser.py",
    "procurement_la_payments_extract.py",
    "procurement_la_seed.py",
    "procurement_nphdb_parser.py",
    "procurement_nta_parser.py",
    "procurement_payments_consolidate.py",
    "procurement_seai_parser.py",
    "sample_extract_procurement_pdf.py",
    "si_legislation_directory_extract.py",
    "si_lrc_classlist_extract.py",
    "si_lrc_enrichment_build.py",
    "sipo_candidate_expenses_crawl.py",
    "sipo_candidate_ocr.py",
    "sipo_donations_paddle_etl.py",
    "sipo_expense_items_paddle_etl.py",
    "sipo_expenses_paddle_etl.py",
    "stateboards_roster_extract.py",
    "ted_ireland_extract.py",
    "ted_ireland_buyer_history_extract.py",
    "ted_ireland_tenders_extract.py",
    "ted_ireland_winner_history_extract.py",
}

# ── Rules over utility/pages_code/ ────────────────────────────────────────────

# R6: retired page-local formatter clones. The canonical versions live in
#     ui/format.py — redefining one of these names re-opens the drift the
#     consolidation closed (divergent rounding/NA handling per page).
RE_RETIRED_FORMATTER = re.compile(
    r"^def (_eur|_eur_scale|_eur_full|_fine_eur|_esc|_truthy|_coalesce|_fmt_period|_fmt_meeting_date|fmt_civic_date|fmt_month)\(",
    re.MULTILINE,
)

EXTRACTOR_RULES = [
    ("raw-http", RE_RAW_HTTP, BASELINE_RAW_HTTP, "use services.http_engine (fetch_bytes/fetch_json/polite_headers)"),
    ("ua-literal", RE_UA_LITERAL, BASELINE_UA_LITERAL, "use services.http_engine.polite_headers(browser=...)"),
    ("raw-parquet-write", RE_RAW_PARQUET, BASELINE_RAW_PARQUET, "use services.parquet_io.save_parquet (atomic, zstd)"),
    ("logging-basicconfig", RE_BASICCONFIG, BASELINE_BASICCONFIG, "use services.extract_runner.run_extractor(main)"),
    ("raw-coverage-json", RE_RAW_COVERAGE, BASELINE_RAW_COVERAGE, "use services.coverage_io.save_coverage (atomic)"),
]


def _page_entry_missing_dt_page(source: str, path: Path) -> list[str]:
    """R7: every top-level ``<name>_page`` entry function in pages_code must be
    decorated with @dt_page (CSS + sidebar + error boundary in one path)."""
    out: list[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:  # a syntax error should fail loudly, not pass silently
        return [f"{path.name}: syntax error prevented dt_page check ({exc})"]
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        if not node.name.endswith("_page") or node.name.startswith("_"):
            continue
        deco_names = {d.id if isinstance(d, ast.Name) else getattr(d, "attr", "") for d in node.decorator_list}
        if "dt_page" not in deco_names:
            out.append(f"{path.name}:{node.lineno}: entry `{node.name}` lacks @dt_page")
    return out


# ── R9: largest-file ratchet ─────────────────────────────────────────────────
# doc/REFACTORING_TOKEN_ECONOMICS.md: an agent's cost to change a file scales
# with the largest file it must read, and the saving from splitting only lands
# when the big file physically shrinks. These are the seven candidates from
# doc/REFACTORING_CANDIDATES.md at their 2026-07-30 baseline line counts.
# Growth past the cap fails: either extract the addition into a module (see the
# candidate plan) or, if the growth is deliberate, raise the cap in this table —
# visibly, in a diff. When a candidate is refactored, ratchet its cap DOWN.
LARGEST_FILE_CAPS = {
    "utility/shared_css.py": 6572,
    # procurement.py (4,665) split into pages_code/procurement/ 2026-07-31 (C2,
    # doc/REFACTORING_CANDIDATES.md) — cap its largest resulting module instead.
    "utility/pages_code/procurement/patterns.py": 737,
    "utility/pages_code/member_overview.py": 2498,
    "utility/ui/components.py": 2155,
    # server.py: a parallel session is actively growing this file (2744→2847 across
    # 2026-07-30/31); capped with headroom until that work lands, then ratchet down.
    "mcp_server/server.py": 2900,
    "test/sql_views/test_sql_views.py": 3579,
    "extractors/procurement_public_body_extract.py": 2948,
}


def _largest_file_ratchet() -> list[str]:
    out: list[str] = []
    for rel, cap in LARGEST_FILE_CAPS.items():
        path = ROOT / rel
        if not path.exists():  # split/renamed away — the ratchet's win condition
            continue
        with path.open(encoding="utf-8", errors="replace") as fh:
            loc = sum(1 for _ in fh)
        if loc > cap:
            out.append(
                f"{rel}: {loc} lines > cap {cap} — extract the addition into a module "
                f"(doc/REFACTORING_CANDIDATES.md) or raise the cap here deliberately"
            )
    return out


# ── R10: split-candidate gate watch ──────────────────────────────────────────
# doc/REFACTORING_CANDIDATES.md §Scope (2026-07-31): a file qualifies for the
# split-for-tokens program only at >=2,000 lines AND >=30 changes/6mo AND a
# localizing common change — and it gets a cost-of-change bench baseline
# (tools/evals/cost_of_change_bench.py) BEFORE anyone splits it. This watch
# NOTES (advisory, never fails) any tracked source file that crosses the size
# threshold and was not part of the 2026-07-31 assessment, so new crossers get
# measured instead of refactored on faith. Churn is deliberately not computed
# here (git-dependent, non-deterministic in CI) — the NOTE tells you to check.
SPLIT_GATE_LINES = 2000
# Assessed 2026-07-31 (already in LARGEST_FILE_CAPS, or excluded with cause in
# the doc's Scope section). Directories whose whole class is excluded:
SPLIT_GATE_EXCLUDED_DIRS = ("extractors/", "iris/", "pipeline_sandbox/", "test/")
SPLIT_GATE_ASSESSED = {
    "utility/pages_code/corporate.py",  # excluded: churn <15/6mo
    "utility/pages_code/lobbying_3.py",  # excluded: churn <15/6mo
}


def _split_gate_watch() -> list[str]:
    out: list[str] = []
    import subprocess

    try:
        tracked = subprocess.run(
            ["git", "ls-files", "*.py"], cwd=str(ROOT), capture_output=True, text=True, check=True
        ).stdout.splitlines()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return out  # no git (e.g. a bare CI checkout step) — the watch is advisory anyway
    for rel in tracked:
        if rel.startswith(SPLIT_GATE_EXCLUDED_DIRS) or rel in SPLIT_GATE_ASSESSED:
            continue
        if rel in LARGEST_FILE_CAPS:
            continue
        path = ROOT / rel
        if not path.exists():
            continue
        with path.open(encoding="utf-8", errors="replace") as fh:
            loc = sum(1 for _ in fh)
        if loc >= SPLIT_GATE_LINES:
            out.append(
                f"{rel}: {loc} lines — crossed the split-gate size threshold since the "
                f"2026-07-31 assessment. Check its churn; if >=30 changes/6mo, freeze a bench "
                f"prompt and record a baseline BEFORE splitting (doc/REFACTORING_CANDIDATES.md §Scope)"
            )
    return out


# ── R8: rule-file self-hygiene ────────────────────────────────────────────────
# The always-loaded instruction files must not use the borrowed-abstraction
# vocabulary that .claude/rules/communication.md bans — "token multipliers" sat in
# CLAUDE.md for months and shipped the counter-example alongside the rule until a
# manual audit caught it (2026-07-20). Zero-tolerance (empty baseline): these files
# are small and hand-edited, so there is nothing to grandfather. communication.md
# and evidence.md are exempt — they QUOTE the banned phrases in order to ban them.
RE_RULE_JARGON = re.compile(
    r"token multiplier|has implications for|the tension here is|\butilize\b|\bleverage\b"
    r"|\boperationali[sz]e\b|\bcommence\b|it's worth noting|it is worth noting",
    re.IGNORECASE,
)
RULE_JARGON_EXEMPT = {"communication.md", "evidence.md"}


def _rule_file_jargon() -> list[str]:
    out: list[str] = []
    targets = [ROOT / "CLAUDE.md"] + sorted((ROOT / ".claude" / "rules").glob("*.md"))
    for md in targets:
        if not md.exists() or md.name in RULE_JARGON_EXEMPT:
            continue
        in_fence = False
        for lineno, line in enumerate(md.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence or line.lstrip().startswith("|"):  # code + tables may quote
                continue
            m = RE_RULE_JARGON.search(line)
            if m:
                rel = md.relative_to(ROOT).as_posix()
                out.append(f"{rel}:{lineno}: “{m.group(0)}” — use the plain word (communication.md rule 3)")
    return out


def main() -> int:
    violations: list[str] = []
    stale_baseline: list[str] = []

    for rule, regex, baseline, fix in EXTRACTOR_RULES:
        offenders = set()
        for py in sorted(EXTRACTORS.glob("*.py")):
            if regex.search(py.read_text(encoding="utf-8", errors="replace")):
                offenders.add(py.name)
        for name in sorted(offenders - baseline):
            violations.append(f"[{rule}] extractors/{name} — {fix}")
        for name in sorted(baseline - offenders):
            stale_baseline.append(f"[{rule}] {name} no longer offends — remove it from the baseline (ratchet down)")

    # rglob: page packages (e.g. pages_code/procurement/) keep their entry function
    # and formatter hygiene in submodules — a flat glob would silently stop checking them.
    for py in sorted(PAGES.rglob("*.py")):
        source = py.read_text(encoding="utf-8", errors="replace")
        for m in RE_RETIRED_FORMATTER.finditer(source):
            line = source[: m.start()].count("\n") + 1
            violations.append(
                f"[retired-formatter] utility/pages_code/{py.name}:{line} — "
                f"`{m.group(0).strip()}` re-defines a ui.format canonical; import it instead"
            )
        violations.extend(f"[missing-dt-page] utility/pages_code/{v}" for v in _page_entry_missing_dt_page(source, py))

    violations.extend(f"[rule-file-jargon] {v}" for v in _rule_file_jargon())
    violations.extend(f"[largest-file-ratchet] {v}" for v in _largest_file_ratchet())

    for note in stale_baseline:
        print(f"NOTE  {note}")
    for note in _split_gate_watch():
        print(f"NOTE  [split-gate] {note}")
    if violations:
        for v in violations:
            print(f"FAIL  {v}")
        print(
            f"\nFAIL — {len(violations)} convention violation(s). See tools/check_conventions.py header for the fixes."
        )
        return 1
    print("OK — conventions hold (ratchet baselines unchanged).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
