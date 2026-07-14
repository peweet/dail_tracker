"""Tests for the 2026-07 MCP domain tool additions.

Covers: cross_register_watchlist / council_scorecard / housing_money /
attendance_ranking / gov_finance_annual.

Registry tests run with no data (the server connection is lazy). The live tests
exercise each tool against the real gold parquet through the server's own union
connection and skip cleanly when the gold/view isn't present on the machine
(same convention as test/dail_tracker_core/test_core_entity.py).

organisation_dossier: the PR3 gate in doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md
was lifted by owner sign-off 2026-07-10 — the tool ships the stable name/company_num
PR3 interface on the INTERIM v_supplier_entity_xref spine (the old pinning test that
asserted it stays unregistered is replaced by the tests below).
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

from mcp_server import server  # noqa: E402

NEW_TOOLS = {
    "cross_register_watchlist",
    "council_scorecard",
    "housing_money",
    "attendance_ranking",
    "gov_finance_annual",
    "organisation_dossier",
}

# A sane per-response ceiling (~25k tokens) — these are summary tools, not dumps.
_MAX_CHARS = 100_000


def _tools() -> dict:
    return {t.name: t for t in asyncio.run(server.mcp.list_tools())}


def test_new_tools_registered_read_only():
    tools = _tools()
    assert set(tools) >= NEW_TOOLS
    for name in NEW_TOOLS:
        assert tools[name].annotations is not None, name
        assert tools[name].annotations.readOnlyHint is True, name
        assert tools[name].description, name  # the docstring is the LLM contract


def test_organisation_dossier_interface_is_pr3_shape():
    # Gate lifted (owner sign-off 2026-07-10): the tool is registered with the STABLE
    # PR3 interface — plain name + optional CRO company_num. Callers must NEVER need
    # the internal spine key, so supplier_norm is not an input.
    tool = _tools()["organisation_dossier"]
    assert tool.annotations is not None and tool.annotations.readOnlyHint is True
    props = tool.inputSchema["properties"]
    assert "name" in props and "company_num" in props
    assert "name" in tool.inputSchema.get("required", [])
    assert "supplier_norm" not in props
    # the docstring is the LLM contract: interim-spine undercount + co-occurrence framing
    desc = tool.description.lower()
    assert "co-occurrence" in desc
    assert "undercount" in desc


def test_new_tools_advertise_bounding_params():
    tools = _tools()
    assert "limit" in tools["cross_register_watchlist"].inputSchema["properties"]
    assert "min_registers" in tools["cross_register_watchlist"].inputSchema["properties"]
    assert "limit" in tools["attendance_ranking"].inputSchema["properties"]
    assert "grain" in tools["housing_money"].inputSchema["properties"]


# ── Live-data tests (skip cleanly when gold isn't on this machine) ────────────────


@pytest.fixture(scope="module")
def live():
    try:
        server._cur().execute("SELECT 1").fetchone()
    except Exception:
        pytest.skip("could not build the MCP union connection on this machine")
    return server


def _assert_sized(result) -> None:
    s = json.dumps(result, default=str)
    assert len(s) < _MAX_CHARS, f"tool response too large: {len(s)} chars"


def _skip_if_unavailable(result) -> None:
    if isinstance(result, dict) and set(result) == {"error"}:
        pytest.skip(f"source unavailable: {result['error']}")


def test_cross_register_watchlist_live(live):
    out = live.cross_register_watchlist(min_registers=1, limit=5)
    _skip_if_unavailable(out)
    assert out["count"] >= 1
    assert 1 <= len(out["entities"]) <= 5
    row = out["entities"][0]
    assert "supplier_norm" in row and "cross_register_count" in row
    assert "co-occurrence" in out["caveat"].lower()
    assert "no individuals" in out["caveat"].lower()
    _assert_sized(out)


def test_council_scorecard_live(live):
    idx = live.council_scorecard()
    _skip_if_unavailable(idx)
    assert idx["councils"], "expected the 31-council CE index"
    label = str(idx["councils"][0]["local_authority"])
    one = live.council_scorecard(label)
    assert one["local_authority"] == label
    assert "chief_executive" in one and "noac_scorecard" in one
    assert "never summed across measures" in one["caveat"]
    _assert_sized(one)
    # a nonsense name returns the label list, not a silent empty
    miss = live.council_scorecard("__no_such_council__")
    assert "error" in miss and miss["councils"]


def test_housing_money_live(live):
    out = live.housing_money()
    _skip_if_unavailable(out)
    assert {"waiting_list", "supply", "hap", "completions_by_year", "accommodation_spend_by_year"} <= set(out)
    spend = out["accommodation_spend_by_year"]
    if isinstance(spend, list) and spend:
        # aggregate rows only — the person-privacy rule: no provider/supplier names here
        assert not ({"provider", "supplier", "supplier_normalised"} & set(spend[0]))
    cavs = out["caveats"]
    assert "never added to procurement award ceilings" in cavs["accommodation_spend"]
    assert "NEVER sum across them" in cavs["money_grains"]
    _assert_sized(out)


def test_attendance_ranking_live(live):
    out = live.attendance_ranking(limit=10)  # year=0 resolves to the latest year
    _skip_if_unavailable(out)
    assert out["year"] >= 2020 and out["house"] == "Dáil"
    turnout = out["turnout_worst_first"]
    _skip_if_unavailable(turnout)
    assert isinstance(turnout, list) and 1 <= len(turnout) <= 10
    row = turnout[0]
    # the corrected participation model's fields pass through VERBATIM — the wrapper
    # must never recompute the denominator (the recurring attendance-denominator bug)
    assert {"turnout_pct", "voted_in", "total_divisions", "is_minister", "is_chair"} <= set(row)
    assert "divisions voted in" in out["caveat"]
    _assert_sized(out)


def test_gov_finance_annual_live(live):
    out = live.gov_finance_annual()
    _skip_if_unavailable(out)
    years = out["by_year"]
    assert isinstance(years, list) and years
    assert {"year", "revenue_eur", "expenditure_eur", "surplus_deficit_eur"} <= set(years[0])
    # newest first, per the view's ORDER BY
    assert int(years[0]["year"]) >= int(years[-1]["year"])
    assert "never summed" in out["caveat"]
    _assert_sized(out)


# ── 2026-07-13 DQ fixes (MCP association sweep 2026-07-11) ────────────────────────


def test_charity_financials_sector_caveat_and_dq_flags_live(live):
    # DQ #8: the sector aggregates served implausible magnitudes uncaveated
    out = live.charity_financials(0)
    _skip_if_unavailable(out)
    totals = out["sector_totals_by_year"]
    _skip_if_unavailable(totals)
    # hard caveat on the response: as-filed/unvalidated + never-compare rails
    cav = out["caveat"]
    assert "AS FILED" in cav and "UNVALIDATED" in cav
    assert "coverage" in cav.lower() and "national accounts" in cav.lower()
    dq = out["data_quality"]
    flags = dq["flags"]
    assert all(f["implausible"] is True and f["reason"] for f in flags)
    # the known-implausible aggregates fire: 2023 gross income €302.9bn (> €100bn ceiling)
    assert any(
        f["period_year"] == 2023 and f["measure"] == "total_gross_income" and f["value"] > 100e9 for f in flags
    ), flags
    # ... and the govt/LA coverage-artifact jump (€4.8m 2014 → €26.6bn 2019) trips the >10× YoY rail
    assert any(f["measure"] == "total_income_govt_or_la" for f in flags), flags
    # numbers pass through UNTOUCHED — we caveat filings, we never correct them
    direct = (
        live._cur()
        .execute("SELECT period_year, total_gross_income FROM v_charity_sector_totals_by_year ORDER BY period_year")
        .fetchall()
    )
    served = [(r["period_year"], float(r["total_gross_income"])) for r in totals]
    assert [(y, float(v)) for y, v in direct] == served
    _assert_sized(out)


def test_charity_financials_single_charity_carries_caveat_live(live):
    out = live.charity_financials(1)  # unknown RCN is fine — the caveat rides regardless
    assert "AS FILED" in out["caveat"]


def test_access_to_contracts_two_lobbying_measures_live(live):
    # DQ #2 (the ROADSTONE contradiction): the diary-grain total_lobbying_returns read 0 while
    # cross_register_watchlist showed returns for the same entity. ROOT CAUSE, fixed 2026-07-14 in
    # extractors/diary_lobbying_overlap.py: the diary joined the register on the REGISTRANT name
    # only, so an org that lobbies through a PR firm (Roadstone via Drury) was invisible. The join
    # now matches registrant OR named client. The tool also carries the register-wide spine fields
    # (canonical key, same spine as the watchlist), so the two tools cannot contradict each other.
    out = live.access_to_contracts(limit=12)
    _skip_if_unavailable(out)
    rows = out["companies"]
    assert rows
    assert {"on_lobbying_register", "register_lobby_returns", "total_lobbying_returns"} <= set(rows[0])
    # a positive register flag always carries a positive floor count
    for r in rows:
        if r["on_lobbying_register"]:
            assert r["register_lobby_returns"] >= 1, r["organisation"]
    # the caveat still warns 0 ≠ 'never lobbied' (the widened join is exact-key, so it undercounts)
    assert "never lobbied" in out["caveat"]
    # the sweep's reproduction, pinned while Roadstone is in the top rows on this gold. Roadstone
    # files NO returns of its own — all 4 are Drury's, naming it as client — so this is exactly the
    # value the registrant-only join could never see, and it must now agree with the spine.
    road = [r for r in rows if "roadstone" in str(r.get("organisation", "")).lower()]
    if road:
        r = road[0]
        assert r["total_lobbying_returns"] >= 1  # client-side returns now count (was 0)
        assert r["on_lobbying_register"] is True and r["register_lobby_returns"] >= 2
    _assert_sized(out)


def test_diary_top_organisations_outside_excludes_supplement_state_bodies_live(live):
    # DQ #3: LDA / NCSE / National Concert Hall / Heritage Council / Dublin Port / Arts Council
    # were flagged is_state_body=False and contaminated the outside-interest ranking. The
    # curated data/_meta/diary_state_bodies_supplement.csv now overrides them at the view.
    rows = live.ministerial_diary_top_organisations(limit=50, outside_only=True)
    _skip_if_unavailable(rows)
    names = {str(r["organisation"]).lower().strip() for r in rows}
    for org in (
        "land development agency",
        "national council for special education",
        "national concert hall",
        "heritage council",
        "dublin port company",
        "arts council",
    ):
        assert org not in names, f"{org} still ranked as an outside interest"


# ── organisation_dossier (PR3 interface on the interim spine) ─────────────────────


def _dossier_probe(live) -> dict:
    """A spine row whose display_name folds EXACTLY back to its supplier_norm (true for
    virtually every anchor row — the display name is a raw supplier spelling of the key),
    so exact-name resolution is deterministic in the tests below."""
    top = live.cross_register_watchlist(min_registers=1, limit=5)
    _skip_if_unavailable(top)
    for row in top["entities"]:
        if row.get("display_name") and live._org_name_key(str(row["display_name"])) == row["supplier_norm"]:
            return row
    pytest.skip("no watchlist row round-trips display_name -> supplier_norm on this gold")


def test_organisation_dossier_exact_name_live(live):
    probe = _dossier_probe(live)
    out = live.organisation_dossier(str(probe["display_name"]))
    _skip_if_unavailable(out)
    assert {"identity", "procurement", "cross_register", "caveat", "matched"} <= set(out)
    assert out["identity"]["supplier_norm"] == probe["supplier_norm"]
    assert out["matched"]["via"] == "exact_name"
    # the ENTITY_COOCCURRENCE caveat rides on the dossier verbatim
    assert "co-occurrence" in out["caveat"].lower()
    assert "no individuals" in out["caveat"].lower()
    # the composer's sum-safe award figure passes through untouched — no invented totals
    assert isinstance(out["procurement"]["awarded_value_safe_eur"], float)
    _assert_sized(out)


def test_organisation_dossier_accented_variant_resolves_same_entity_live(live):
    probe = _dossier_probe(live)
    name = str(probe["display_name"])
    # lower-case, add a fada, and append a legal suffix — the shared NFKD accent-fold +
    # suffix-strip normaliser must land the variant on the SAME canonical key
    variant = name.lower().replace("e", "é", 1) + " limited"
    out = live.organisation_dossier(variant)
    _skip_if_unavailable(out)
    assert "identity" in out, f"variant did not resolve: {out}"
    assert out["identity"]["supplier_norm"] == probe["supplier_norm"]


def test_organisation_dossier_ambiguous_returns_disambiguation_live(live):
    cur = live._cur()
    # a token that substring-matches many suppliers but is no entity's exact canonical key
    tok = "CONSULT"
    n = cur.execute("SELECT count(*) FROM v_supplier_entity_xref WHERE supplier_norm LIKE ?", [f"%{tok}%"]).fetchone()[
        0
    ]
    exact = cur.execute("SELECT count(*) FROM v_supplier_entity_xref WHERE supplier_norm = ?", [tok]).fetchone()[0]
    if n < 2 or exact:
        pytest.skip("gold on this machine lacks an ambiguous probe token")
    out = live.organisation_dossier("consult")
    assert out.get("match") == "ambiguous"
    assert "identity" not in out  # a guess is never returned
    cands = out["disambiguation"]
    assert 2 <= len(cands) <= 8
    assert {"name", "company_num", "registers"} <= set(cands[0])
    assert "procurement" in cands[0]["registers"]  # the spine is procurement-anchored
    _assert_sized(out)


def test_organisation_dossier_no_match_hint_live(live):
    out = live.organisation_dossier("__zz_no_such_organisation_zz__")
    _skip_if_unavailable(out)
    assert out.get("match") == "none"
    assert "identity" not in out
    assert "search_suppliers" in out["hint"]
    _assert_sized(out)
