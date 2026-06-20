"""Parity tests for the interests diff/summary views.

v_member_interests_declarations and v_member_interests_member_year_summary
replaced the in-Streamlit business logic that used to live in
utility/ui/interests_panel.py (_real_descriptions de-dup, the set-difference
year-on-year diff, and the category/new/removed counting). These tests pin the
views to *exactly* that former Python behaviour so the move stays faithful.

Integration-only: skips if the interests silver parquet is not built.
"""

from __future__ import annotations

import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import interests as q

_DECL_COLUMNS = {"declaration_year", "interest_category", "interest_text", "change_status"}
_SUMMARY_COLUMNS = {
    "declaration_year",
    "party_name",
    "constituency",
    "total_declarations",
    "category_count",
    "new_count",
    "removed_count",
    "has_prior_year",
    "is_landlord",
    "is_property_owner",
    "property_count",
    "share_count",
}


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["member_interests_*.sql", "member_zz_interests_*.sql"], swallow_errors=False)
    yield c
    c.close()


def _detail_or_skip(conn):
    r = q.detail(conn, "Dáil")
    if not r.ok:
        pytest.skip(f"interests views not available: {r.unavailable_reason}")
    # full detail (not the LIMIT 1000 browse query) straight off the view
    try:
        return conn.execute(
            "SELECT member_name, declaration_year, interest_category, interest_text,"
            " landlord_flag, property_flag FROM v_member_interests_detail WHERE house = 'Dáil'"
        ).df()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"detail view unavailable: {exc}")


# ── former Python logic, verbatim from interests_panel.py ─────────────────────


def _real_descriptions(rows) -> list[str]:
    seen: dict[str, None] = {}
    for d in rows["interest_text"].tolist():
        s = str(d).strip()
        if s and s.lower() not in ("no interests declared", "", "nan"):
            seen[s] = None
    return list(seen)


# ── Column contracts ──────────────────────────────────────────────────────────


def test_declarations_columns(conn):
    if not q.detail(conn, "Dáil").ok:
        pytest.skip("interests views not available")
    r = q.member_declarations(conn, "Dáil", "—does-not-exist—")
    assert _DECL_COLUMNS.issubset(set(r.data.columns))


def test_summary_columns(conn):
    if not q.detail(conn, "Dáil").ok:
        pytest.skip("interests views not available")
    r = q.member_year_summary(conn, "Dáil", "—does-not-exist—")
    assert _SUMMARY_COLUMNS.issubset(set(r.data.columns))


def test_change_status_domain(conn):
    if not q.detail(conn, "Dáil").ok:
        pytest.skip("interests views not available")
    vals = conn.execute("SELECT DISTINCT change_status FROM v_member_interests_declarations").df()
    assert set(vals["change_status"]).issubset({"new", "unchanged", "removed"})


# ── Parity: summary counts match the old Python, member-by-member ─────────────


def test_summary_matches_python(conn):
    detail = _detail_or_skip(conn)
    summ = conn.execute("SELECT * FROM v_member_interests_member_year_summary WHERE house = 'Dáil'").df()
    s_idx = {(r.member_name, int(r.declaration_year)): r for r in summ.itertuples()}

    mism = 0
    for name, td in detail.groupby("member_name"):
        years = sorted(td["declaration_year"].dropna().astype(int).unique())
        for y in years:
            ydf = td[td["declaration_year"] == y]
            pdf = td[td["declaration_year"] == y - 1]
            has_prior = not pdf.empty
            descs = _real_descriptions(ydf)
            n_entries = len(descs)
            n_cats = len(ydf["interest_category"].dropna().unique())
            prior_all, cur = set(_real_descriptions(pdf)), set(descs)
            n_new = len(cur - prior_all) if has_prior else 0
            n_removed = len(prior_all - cur) if has_prior else 0
            prop = len(_real_descriptions(ydf[ydf["interest_category"] == "Land (including property)"]))
            shar = len(_real_descriptions(ydf[ydf["interest_category"] == "Shares"]))
            row = s_idx.get((name, y))
            assert row is not None, f"missing summary row for {name} {y}"
            got = (
                int(row.total_declarations),
                int(row.category_count),
                int(row.new_count),
                int(row.removed_count),
                bool(row.has_prior_year),
                int(row.property_count),
                int(row.share_count),
                bool(row.is_landlord),
                bool(row.is_property_owner),
            )
            exp = (
                n_entries,
                n_cats,
                n_new,
                n_removed,
                has_prior,
                prop,
                shar,
                bool(ydf["landlord_flag"].any()),
                bool(ydf["property_flag"].any()),
            )
            if got != exp:
                mism += 1
    assert mism == 0, f"{mism} member-year summary rows diverged from the former Python logic"


def test_declarations_per_category_diff_matches_python(conn):
    detail = _detail_or_skip(conn)
    decl = conn.execute(
        "SELECT member_name, declaration_year, interest_category, interest_text, change_status"
        " FROM v_member_interests_declarations WHERE house = 'Dáil'"
    ).df()

    mism = 0
    for name, td in detail.groupby("member_name"):
        years = sorted(td["declaration_year"].dropna().astype(int).unique())
        for y in years:
            ydf = td[td["declaration_year"] == y]
            pdf = td[td["declaration_year"] == y - 1]
            if pdf.empty:  # the panel only renders the diff when a prior year exists
                continue
            dv_y = decl[(decl.member_name == name) & (decl.declaration_year == y)]
            for cat in ydf["interest_category"].dropna().unique():
                cur_c = set(_real_descriptions(ydf[ydf["interest_category"] == cat]))
                pri_c = set(_real_descriptions(pdf[pdf["interest_category"] == cat]))
                exp_new = {d for d in cur_c if d not in pri_c}
                exp_unch = {d for d in cur_c if d in pri_c}
                exp_rem = pri_c - cur_c
                dvc = dv_y[dv_y.interest_category == cat]
                got_new = set(dvc[dvc.change_status == "new"].interest_text)
                got_unch = set(dvc[dvc.change_status == "unchanged"].interest_text)
                got_rem = set(dvc[dvc.change_status == "removed"].interest_text)
                if (exp_new, exp_unch, exp_rem) != (got_new, got_unch, got_rem):
                    mism += 1
    assert mism == 0, f"{mism} (member, year, category) diff groups diverged from the former Python logic"
