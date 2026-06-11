"""Repo-wide null-sentinel / dead-column sweep over EVERY registered SQL view.

Born from the 2026-06-11 null/empty-string audit, which found three real bugs
this class of test would have caught: literal 'Null'/'n/a' supplier strings
surviving the eTenders coercion, a '—' display sentinel papering over 557
recoverable sponsors, and sibling views drifting on dirty-value filters.

This is the broad complement to the per-view regression guards in
test_sql_views.py: those lock CONFIRMED bug semantics on synthetic fixtures and
run in CI; this one runs against REAL pipeline output (DAIL_INTEGRATION_TESTS=1
only) and catches what fixtures structurally cannot — a NEW sentinel spelling
arriving in a future source export, or a schema drift that hollows a column out
to all-empty strings.

Two checks per VARCHAR column of every view:
  1. sentinel literals — whole-value, case-insensitive after trim
     ('null', 'none', 'n/a', '—', 'undefined', ...)
  2. fully-empty columns — every row '' (the payments-rankings P0 class, where
     parquet schema drift silently blanked party_name/constituency)

Legitimate hits are ALLOWLISTED with a reason — as-filed source text protected
by the no-inference rule, real enum values, documented placeholders. A failure
therefore means: either fix the pipeline, or add a (view, column) entry HERE
with a reason, consciously. Never silence by deleting the check.

Run:  DAIL_INTEGRATION_TESTS=1 pytest test/sql_views/test_dq_sentinel_sweep.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

pytestmark = [
    pytest.mark.sql,
    pytest.mark.skipif(
        os.environ.get("DAIL_INTEGRATION_TESTS") != "1",
        reason="data-quality sweep needs real pipeline output (set DAIL_INTEGRATION_TESTS=1)",
    ),
]

# Whole-value sentinels, compared case-insensitively after TRIM. Adding here
# widens the net for every view at once.
SENTINELS = ("null", "none", "nan", "n/a", "na", "-", "--", "—", "undefined", "nil", "#n/a")

# (view, column) -> reason. Seeded from the 2026-06-11 audit: every entry was
# inspected and is either as-filed source data (no-inference rule: present it,
# don't clean it), a real enum value, or a documented placeholder.
SENTINEL_ALLOWLIST: dict[tuple[str, str], str] = {
    # Charities Regulator register bands: 'NONE' is a real band meaning zero staff.
    ("v_charity_financials_by_year", "employees_band"): "as-filed CRA band ('NONE' = zero employees)",
    ("v_charity_financials_by_year", "volunteers_band"): "as-filed CRA band ('NONE' = zero volunteers)",
    ("v_experimental_lobbying_org_index_enriched", "volunteers_band_latest"): "as-filed CRA band",
    # Courts Service source marks non-circuit venues 'NA'.
    ("v_courthouses", "circuit"): "as-filed Courts Service value for non-circuit venues",
    # lobbying.ie register free-text: filers themselves type 'N/A'/'None'/'NIL'.
    ("v_lobbying_contact_detail", "member_name"): "as-filed register text",
    ("v_lobbying_contact_detail", "intended_results"): "as-filed register text",
    ("v_lobbying_contact_detail", "person_primarily_responsible"): "as-filed register text",
    ("v_lobbying_contact_detail_with_dpo", "member_name"): "as-filed register text",
    ("v_lobbying_index", "member_name"): "as-filed register text",
    ("v_lobbying_policy_exposure", "member_name"): "as-filed register text",
    ("v_lobbying_sources", "member_name"): "as-filed register text",
    ("v_lobbying_topic_search", "specific_details"): "as-filed register text",
    ("v_lobbying_topic_search", "intended_results"): "as-filed register text",
    ("v_lobbying_topic_search", "person_primarily_responsible"): "as-filed register text",
    ("v_lobbying_org_index", "website"): "as-filed register text",
    ("v_lobbying_org_index", "company_registration_number"): "as-filed register text",
    ("v_lobbying_org_index", "company_registered_name"): "as-filed register text",
    ("v_experimental_lobbying_org_index_enriched", "website"): "as-filed register text",
    ("v_experimental_lobbying_org_index_enriched", "register_company_registration_number"): "as-filed register text",
    ("v_experimental_lobbying_org_index_enriched", "register_company_name"): "as-filed register text",
    # Oireachtas API ships topic='Undefined' on a handful of questions.
    ("v_member_questions", "topic"): "as-filed Oireachtas API value",
    ("v_member_question_top_topics", "topic"): "as-filed Oireachtas API value",
    ("v_member_debate_sections", "topic"): "as-filed Oireachtas API value",
    # 'none' is a real enum value of the CRO match method, not a stringified null.
    ("v_procurement_ted_awards", "cro_match_method"): "real enum value 'none'",
    ("v_procurement_ted_winner_history", "cro_match_method"): "real enum value 'none'",
    # TED source data carries a buyer literally named 'None' on one notice.
    ("v_procurement_competition", "buyer_name"): "as-filed TED source value",
    ("v_procurement_ted_awards", "buyer_name"): "as-filed TED source value",
    ("v_procurement_ted_winner_history", "buyer_name"): "as-filed TED source value",
    # SIPO GE2024 OCR'd disclosures: candidates wrote 'None'/'NIL' on the forms.
    ("v_sipo_candidate_expense_items", "detail"): "as-filed SIPO form text",
    ("v_sipo_candidate_top_details", "detail"): "as-filed SIPO form text",
    ("v_sipo_candidate_expenses", "party_declared"): "as-filed SIPO form text",
}

# (view, column) -> reason, for columns where EVERY row is '' by design.
ALL_EMPTY_ALLOWLIST: dict[tuple[str, str], str] = {
    # Documented placeholders: payments_member_enrichment.py not yet built.
    ("v_payments_alltime_ranking", "party_name"): "documented placeholder until member enrichment lands",
    ("v_payments_alltime_ranking", "constituency"): "documented placeholder until member enrichment lands",
    # Register supplies no sector taxonomy; documented '' AS sector.
    ("v_lobbying_org_index", "sector"): "documented: register has no clean sector source",
}


def _all_views_conn():
    """One connection with every registerable view: the API set (correct order +
    substitutions) plus a two-pass catch-all for views outside the API globs."""
    from dail_tracker_core.connections import api_conn
    from dail_tracker_core.db import register_views

    conn = api_conn()
    for _ in range(2):  # pass 2 catches deps that sort later alphabetically
        register_views(conn, ["*.sql"], swallow_errors=True)
    return conn


def test_no_unexplained_sentinels_or_dead_columns():
    conn = _all_views_conn()
    views = [
        r[0]
        for r in conn.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY view_name").fetchall()
    ]
    assert len(views) > 100, f"only {len(views)} views registered — registration is broken, sweep would be hollow"

    sent_list = ", ".join(f"'{s}'" for s in SENTINELS)
    violations: list[str] = []
    seen_allowlisted: set[tuple[str, str]] = set()

    for view in views:
        cols = [
            (name, dtype)
            for name, dtype, *_ in conn.execute(f'DESCRIBE "{view}"').fetchall()
            if dtype.upper() == "VARCHAR"  # exact: VARCHAR[] list columns can't TRIM
        ]
        if not cols:
            continue
        aggs = ["COUNT(*) AS _total"]
        for name, _ in cols:
            q = name.replace('"', '""')
            aggs.append(f'COUNT(*) FILTER (LOWER(TRIM("{q}")) IN ({sent_list})) AS "s__{q}"')
            aggs.append(f'COUNT(*) FILTER (TRIM("{q}") = \'\' OR "{q}" IS NULL) AS "e__{q}"')
        row = conn.execute(f'SELECT {", ".join(aggs)} FROM "{view}"').fetchone()
        vals = dict(zip([d[0] for d in conn.description], row, strict=True))
        total = vals["_total"]
        if total == 0:
            continue

        for name, _ in cols:
            key = (view, name)
            n_sent = vals[f"s__{name}"]
            if n_sent:
                if key in SENTINEL_ALLOWLIST:
                    seen_allowlisted.add(key)
                else:
                    sample = conn.execute(
                        f'SELECT DISTINCT "{name}" FROM "{view}" WHERE LOWER(TRIM("{name}")) IN ({sent_list}) LIMIT 5'
                    ).fetchall()
                    violations.append(f"SENTINEL  {view}.{name}: {n_sent}/{total} rows hold {[s[0] for s in sample]!r}")
            # dead column: every single row is ''/NULL with at least one literal ''
            if vals[f"e__{name}"] == total and key not in ALL_EMPTY_ALLOWLIST:
                n_literal_empty = conn.execute(
                    f'SELECT COUNT(*) FILTER (TRIM("{name}") = \'\') FROM "{view}"'
                ).fetchone()[0]
                if n_literal_empty:
                    violations.append(f"ALL-EMPTY {view}.{name}: all {total} rows are ''/NULL (schema drift?)")

    assert not violations, (
        f"{len(violations)} unexplained data-quality finding(s) — fix the pipeline, "
        "or allowlist with a reason in this file:\n  " + "\n  ".join(violations)
    )


def test_allowlist_carries_no_stale_entries():
    """An allowlist entry whose view/column no longer exists is dead weight that
    would silently mask a future regression under a renamed column."""
    conn = _all_views_conn()
    views = {r[0] for r in conn.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()}
    stale = []
    for view, col in {**SENTINEL_ALLOWLIST, **ALL_EMPTY_ALLOWLIST}:
        if view not in views:
            stale.append(f"{view} (view gone)")
            continue
        names = {r[0] for r in conn.execute(f'DESCRIBE "{view}"').fetchall()}
        if col not in names:
            stale.append(f"{view}.{col} (column gone)")
    assert not stale, f"stale allowlist entries: {stale}"
