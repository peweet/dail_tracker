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
# Two extractor roots since the civic planning lane moved under planning/ (consolidation plan §6):
# a file leaving extractors/ must NOT silently leave the ratchet.
EXTRACTOR_DIRS = (ROOT / "extractors", ROOT / "planning" / "civic" / "extractors")
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
    "planning_appeal_outcomes.py",
    "planning_applications_ingest.py",
    "planning_decision_profiles.py",
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

# R11: pandas in the ETL layer. CLAUDE.md never-break rule: "Polars for ETL,
#     pandas only in the UI layer." Baseline is EMPTY (verified zero offenders
#     across both extractor roots 2026-07-31) — this is the R3 pattern: lock in
#     100% adoption before the first offender exists. The concrete threat is
#     model library-familiarity bias (Twist & Harman et al., Findings of ACL
#     2026, arXiv:2503.17181): under pressure a code model reaches for the
#     familiar library, and pandas/polars look interchangeable at a glance.
RE_PANDAS_IMPORT = re.compile(r"^\s*(?:import pandas\b|from pandas\b)", re.MULTILINE)
BASELINE_PANDAS_IMPORT: set[str] = set()

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
    ("pandas-in-etl", RE_PANDAS_IMPORT, BASELINE_PANDAS_IMPORT, "Polars for ETL; pandas only in the UI layer (CLAUDE.md never-break rule)"),
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
    # Raised 6572→6656 2026-08-01: the 07-31/08-01 feature work landed at 6656, and
    # the C1/C3/C4 splits were MEASURED as token-cost regressions and reverted
    # (commit c42e78a) — so growth is absorbed by cap raises, not extraction, until
    # a bench run says otherwise (doc/REFACTORING_CANDIDATES.md).
    "utility/shared_css.py": 6656,
    # procurement.py (4,665) split into pages_code/procurement/ 2026-07-31 (C2,
    # doc/REFACTORING_CANDIDATES.md) — cap its largest resulting module instead.
    "utility/pages_code/procurement/patterns.py": 737,
    "utility/pages_code/member_overview.py": 2498,
    # Raised 2155→2191 2026-08-01, same reasoning as shared_css.py above.
    "utility/ui/components.py": 2191,
    # The parallel session growing this file (2744→2847→2923, 2026-07-30/31) has
    # landed; headroom removed 2026-08-01 — cap ratcheted to the landed size.
    "mcp_server/server.py": 2923,
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


# ── R12: extraction quality report (completeness + recall sections) ──────────
# Correctness gates say nothing about what an extraction MISSED (memory:
# feedback_source_fidelity_audit_method). Any extraction OUTPUT directory — one
# holding a corpus/ subdir or a *_clean.jsonl — must ship a quality report (.md)
# containing BOTH "## Completeness" and "## Recall" section markers, per
# doc/EXTRACTION_QUALITY_CHECKLIST.md. Ratchet: dirs predating the rule are
# grandfathered; new extractions fail until the report exists. Deliberately a
# marker check, not a content check — the sections' quality is judgement
# (guardrail-determinism tiers), their PRESENCE is deterministic.
EXTRACTION_QUALITY_SECTIONS = ("## Completeness", "## Recall")
BASELINE_EXTRACTION_QUALITY = {
    "pipeline_sandbox/committee_evidence",
    "pipeline_sandbox/council_minutes",
    "pipeline_sandbox/semistate_minutes",
}


def _extraction_quality_reports() -> list[str]:
    out: list[str] = []
    roots = [ROOT / "pipeline_sandbox", *EXTRACTOR_DIRS]
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for marker in list(root.glob("*/corpus")) + list(root.glob("*/*_clean.jsonl")):
            d = marker.parent
            if d in seen:
                continue
            seen.add(d)
            rel = d.relative_to(ROOT).as_posix()
            if rel in BASELINE_EXTRACTION_QUALITY:
                continue
            mds = list(d.glob("*.md"))
            ok = any(
                all(s in md.read_text(encoding="utf-8", errors="replace") for s in EXTRACTION_QUALITY_SECTIONS)
                for md in mds
            )
            if not ok:
                out.append(
                    f"{rel}: extraction output with no quality report carrying "
                    f"{' + '.join(EXTRACTION_QUALITY_SECTIONS)} sections — "
                    f"see doc/EXTRACTION_QUALITY_CHECKLIST.md"
                )
    return out


# ── R13: raw parquet writes OUTSIDE the extractor dirs ───────────────────────
# R3 locked save_parquet at 100% in the two extractor roots, but the never-break
# rule ("parquet writes are atomic, zstd, row-floor guard") is repo-wide. The
# 2026-08-01 prose-rule audit swept services/, dail_tracker_core/, votes/,
# attendance/, payments/, iris/, tools/, planning/ and found ONE production
# bypass (planning/product/core/dem.py, fixed same day) — so this locks the
# whole production tree at zero, the R3/R11 move. Sandbox trees and tests stay
# out of scope; services/parquet_io.py IS the canonical writer.
PARQUET_SCAN_DIRS = (
    "services", "dail_tracker_core", "votes", "attendance", "payments",
    "iris", "tools", "planning", "wikidata", "committees", "reference",
)
PARQUET_SCAN_EXEMPT = {"services/parquet_io.py"}
BASELINE_RAW_PARQUET_REPO: set[str] = set()


def _raw_parquet_outside_extractors() -> list[str]:
    out: list[str] = []
    extractor_rels = {d.relative_to(ROOT).as_posix() for d in EXTRACTOR_DIRS}
    for dirname in PARQUET_SCAN_DIRS:
        root = ROOT / dirname
        if not root.exists():  # planning/product is overlay-tracked; absent in CI checkouts
            continue
        for py in sorted(root.rglob("*.py")):
            rel = py.relative_to(ROOT).as_posix()
            parts = set(rel.split("/"))
            if parts & {"sandbox", "pipeline_sandbox", "test", "__pycache__"}:
                continue
            if rel in PARQUET_SCAN_EXEMPT or rel in BASELINE_RAW_PARQUET_REPO:
                continue
            if any(rel.startswith(ex + "/") for ex in extractor_rels):
                continue  # R3's ground
            if RE_RAW_PARQUET.search(py.read_text(encoding="utf-8", errors="replace")):
                out.append(f"{rel} — use services.parquet_io.save_parquet (atomic, zstd; min_rows opt-in)")
    return out


# ── R14: hand-rolled accent-fold normalisation ───────────────────────────────
# CLAUDE.md never-break rule: the members join key is the normalised TD name —
# "reuse the existing normaliser; don't invent matching." The canonicals are
# shared/normalise_join_key.py and shared/name_norm.py. 25 files predating this
# rule carry their own unicodedata.normalize("NFKD"/"NFD") call (many fold ORG
# or LA names, which is legitimate — but nothing distinguishes them mechanically,
# so they are grandfathered rather than judged here). The ratchet stops call-site
# #26: a NEW hand-rolled fold either belongs in a shared/ helper or should import
# one. Threat model = library-familiarity bias, same as R11.
RE_HAND_NFKD = re.compile(r"""unicodedata\.normalize\(\s*["']NFK?D["']""")
# The two canonical homes, plus this file (its comments QUOTE the banned call —
# same exemption logic as R8's RULE_JARGON_EXEMPT).
NFKD_CANONICAL = {"shared/normalise_join_key.py", "shared/name_norm.py", "tools/check_conventions.py"}
BASELINE_HAND_NFKD = {
    # council_votes_extract graduated from the sandbox 2026-08-01 carrying _fold():
    # an initial-PRESERVING person-name fold for RosterResolver.initials ("O. Moran"
    # → "Orla Moran"). Neither canonical fits: normalise_join_key SORTS characters
    # (destroys initial+surname structure), name_norm is the ORG-name variant.
    # Documented semantic difference, not drift.
    "extractors/council_votes_extract.py",
    "committees/committees_long_format_etl.py",
    "dail_tracker_core/buyer_xref.py",
    "extractors/_diary_minister.py",
    "extractors/cbi_registers_extract.py",
    "extractors/diary_company_influence.py",
    "extractors/diary_lobbying_overlap.py",
    "extractors/diary_org_match.py",
    "extractors/hsa_comah_extract.py",
    "extractors/judiciary_bench_extract.py",
    "extractors/la_budgets_extract.py",
    "extractors/lgas_audit_reports_extract.py",
    "extractors/lpt_laf_extract.py",
    "extractors/news_mentions_extract.py",
    "extractors/noac_indicators_long_extract.py",
    "extractors/noac_scorecard_extract.py",
    "extractors/noac_scorecard_history_extract.py",
    "extractors/persist_judiciary_data.py",
    "extractors/procurement_payments_consolidate.py",
    "planning/civic/extractors/opr_plan_directions_extract.py",
    "planning/civic/extractors/planning_appeal_outcomes.py",
    "reference/ec_constituency_crosswalk_extract.py",
    "utility/ui/avatars.py",
    "utility/ui/entity_links.py",
    "wikidata/ministerial_tenure_build.py",
    "wikidata/stateboards_wikidata_enrich.py",
}


def _hand_rolled_nfkd() -> tuple[list[str], list[str]]:
    """Returns (violations, stale_baseline_notes)."""
    import subprocess

    try:
        tracked = subprocess.run(
            ["git", "ls-files", "*.py"], cwd=str(ROOT), capture_output=True, text=True, check=True
        ).stdout.splitlines()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return [], []
    offenders: set[str] = set()
    for rel in tracked:
        rel = rel.replace("\\", "/")
        if rel.startswith(("test/", "pipeline_sandbox/")) or rel in NFKD_CANONICAL:
            continue
        path = ROOT / rel
        if not path.exists():
            continue
        if RE_HAND_NFKD.search(path.read_text(encoding="utf-8", errors="replace")):
            offenders.add(rel)
    violations = [
        f"{rel} — import shared.normalise_join_key / shared.name_norm instead of hand-rolling the fold"
        for rel in sorted(offenders - BASELINE_HAND_NFKD)
    ]
    stale = [
        f"[hand-rolled-nfkd] {rel} no longer offends — remove it from the baseline (ratchet down)"
        for rel in sorted(BASELINE_HAND_NFKD - offenders)
    ]
    return violations, stale


# ── Streamlit API compliance (was civic-ui-review "Pass B") ───────────────────
# Moved out of the review skill 2026-08-01. These are decidable by reading the
# source, so a model re-deciding them each review was both slower and less
# reliable: the skill's `html.escape` check was a false NEGATIVE on
# lobbying_3.py, which escapes via `from html import escape as _h` (line 95).
# Keeping them here frees the skill to spend its attention on what only a human
# or a rendered page can judge.
RE_API_RULES = (
    (
        "unsafe-allow-html",
        re.compile(r"unsafe_allow_html\s*=\s*True"),
        "use st.html(...) — st.markdown(unsafe_allow_html=True) is the deprecated path",
    ),
    (
        "use-container-width",
        re.compile(r"use_container_width\s*=\s*True"),
        'use width="stretch"',
    ),
    (
        "radio-horizontal",
        re.compile(r"st\.radio\((?:[^()]|\([^()]*\))*horizontal\s*=\s*True", re.S),
        "use st.segmented_control(...) for a 2-5 option toggle",
    ),
    (
        "blank-write-spacer",
        re.compile(r"st\.write\(\s*['\"]\s*['\"]\s*\)"),
        "use st.space(n) for vertical spacing",
    ),
    (
        "page-local-css",
        re.compile(r"<style"),
        "move the rules into utility/shared_css.py — page-local CSS forks the design system",
    ),
)

# Grandfathered at introduction (2026-08-01). Counts then: unsafe-allow-html 20
# occurrences across 9 files, page-local-css 15 across 8. The other three rules
# start EMPTY — they had zero offenders, so they are a pure ratchet.
BASELINE_API: dict[str, set[str]] = {
    "unsafe-allow-html": {
        "committees.py", "corporate.py", "election_2024.py", "judiciary.py", "lobbying_3.py",
        "public_appointments.py", "statutory_instruments.py", "support.py", "what_they_own.py",
    },
    "page-local-css": {
        "accommodation_spend.py", "corporate.py", "election_2024.py", "judiciary.py",
        "patterns.py", "public_appointments.py", "statutory_instruments.py", "your_council.py",
    },
    "use-container-width": set(),
    "radio-horizontal": set(),
    "blank-write-spacer": set(),
}

# Both escapers count. `ui.format.esc` (format.py:115) is the repo's canonical one —
# it wraps html.escape and renders NA as "". Recognising only the stdlib spelling
# reproduced the exact false positive this rule exists to prevent: 13 of the 14
# first-run hits were files escaping correctly through `from ui.format import esc`.
RE_ESCAPE_IMPORT = re.compile(
    r"^\s*(?:from\s+html\s+import\s+escape|import\s+html\b|from\s+ui\.format\s+import\s+[^\n]*\besc\b)",
    re.M,
)
RE_HTML_FSTRING = re.compile(r"st\.html\(\s*f['\"]")


def _streamlit_api_compliance() -> tuple[list[str], list[str]]:
    """Returns (violations, stale_baseline_notes)."""
    violations: list[str] = []
    found: dict[str, set[str]] = {rule: set() for rule, _, _ in RE_API_RULES}
    for py in sorted(PAGES.rglob("*.py")):
        source = py.read_text(encoding="utf-8", errors="replace")
        rel = py.relative_to(ROOT).as_posix()
        for rule, regex, fix in RE_API_RULES:
            match = regex.search(source)
            if not match:
                continue
            found[rule].add(py.name)
            if py.name in BASELINE_API[rule]:
                continue
            line = source[: match.start()].count("\n") + 1
            violations.append(f"[{rule}] {rel}:{line} — {fix}")
        # Escaping is checked at FILE level and alias-aware on purpose: matching
        # the literal string "html.escape(" misses every aliased import, which is
        # exactly how the old skill checklist cleared a file it should have read.
        if RE_HTML_FSTRING.search(source) and not RE_ESCAPE_IMPORT.search(source):
            violations.append(
                f"[unescaped-html] {rel} — interpolates into st.html(f\"...\") but imports no "
                f"escaper; add `from html import escape`"
            )
    stale = [
        f"[{rule}] {name} no longer offends — remove it from BASELINE_API (ratchet down)"
        for rule in sorted(BASELINE_API)
        for name in sorted(BASELINE_API[rule] - found[rule])
    ]
    return violations, stale


def main() -> int:
    violations: list[str] = []
    stale_baseline: list[str] = []

    for rule, regex, baseline, fix in EXTRACTOR_RULES:
        offenders = set()
        offender_paths: dict[str, str] = {}
        for exdir in EXTRACTOR_DIRS:
            for py in sorted(exdir.glob("*.py")):
                if py.name == "__init__.py":
                    continue
                if regex.search(py.read_text(encoding="utf-8", errors="replace")):
                    offenders.add(py.name)
                    offender_paths[py.name] = py.relative_to(ROOT).as_posix()
        for name in sorted(offenders - baseline):
            violations.append(f"[{rule}] {offender_paths[name]} — {fix}")
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
    violations.extend(f"[extraction-quality] {v}" for v in _extraction_quality_reports())
    violations.extend(f"[raw-parquet-write] {v}" for v in _raw_parquet_outside_extractors())
    nfkd_violations, nfkd_stale = _hand_rolled_nfkd()
    violations.extend(f"[hand-rolled-nfkd] {v}" for v in nfkd_violations)
    stale_baseline.extend(nfkd_stale)
    api_violations, api_stale = _streamlit_api_compliance()
    violations.extend(api_violations)
    stale_baseline.extend(api_stale)

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
