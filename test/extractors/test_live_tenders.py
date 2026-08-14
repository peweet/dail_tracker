"""Contract for v_procurement_live_tenders — the live national tender pipeline (open opportunities)
over the SANDBOX etenders.gov.ie snapshot. Skips if the snapshot isn't present.

Guards the honesty rails: it is the PLANNED (pre-award) lifecycle tier — a buyer ESTIMATE that is
NEVER summed with awards or payments — and only genuinely-open opportunities are surfaced.
"""

from __future__ import annotations

import duckdb
import polars as pl
import pytest

from dail_tracker_core.db import connect_with_views


@pytest.fixture(scope="module")
def con():
    c = connect_with_views(["procurement_live_tenders.sql"])
    try:
        c.execute("SELECT 1 FROM v_procurement_live_tenders LIMIT 1")
    except duckdb.Error:
        pytest.skip("live-tenders sandbox snapshot not available")
    yield c
    c.close()


def _q(con, sql):
    return con.execute(sql).fetchone()[0]


def test_tier_is_planned_only(con):
    # The live pipeline is a NEW lifecycle stage BEFORE awarded — never AWARDED/COMMITTED/SPENT.
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE realisation_tier <> 'PLANNED'")
    assert bad == 0


def test_value_kind_is_estimate(con):
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE value_kind <> 'estimate_advertised'")
    assert bad == 0


def test_only_open_opportunities(con):
    # Every surfaced tender closes in the future (genuinely open), excluding closed + far-future DPS windows.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE submission_deadline < CURRENT_DATE OR submission_deadline >= CURRENT_DATE + INTERVAL 3 YEAR",
    )
    assert bad == 0


def test_exact_deadline_preserves_source_clock_and_timezone(con):
    columns = {row[0]: row[1] for row in con.execute("DESCRIBE v_procurement_live_tenders").fetchall()}
    assert columns["submission_deadline_at"] == "TIMESTAMP WITH TIME ZONE"
    assert {"deadline_raw", "deadline_timezone", "deadline_timezone_abbreviation"} <= set(columns)

    # Every portal deadline carrying its explicit Irish GMT/IST marker must parse to an instant.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE regexp_matches(deadline_raw, ' (IST|GMT) [0-9]{4}$') "
        "AND submission_deadline_at IS NULL",
    )
    assert bad == 0

    # The local clock printed by the source must survive a UTC round trip. This guards against
    # treating an IST noon deadline as noon UTC (one hour late).
    bad_clock = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE submission_deadline_at IS NOT NULL AND "
        "strftime(timezone('Europe/Dublin', submission_deadline_at), '%H:%M:%S') "
        "<> regexp_extract(deadline_raw, ' ([0-9]{2}:[0-9]{2}:[0-9]{2}) ', 1)",
    )
    assert bad_clock == 0


def test_has_detail_link(con):
    # Each opportunity links back to its eTenders detail page (verifiability / drill-through).
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE detail_url IS NULL OR detail_url = ''")
    assert bad == 0


def test_summary_reconciles_to_open_count(con):
    detail = _q(con, "SELECT COUNT(*) FROM v_procurement_live_tenders")
    summ = _q(con, "SELECT COALESCE(SUM(n_open_tenders), 0) FROM v_procurement_live_tenders_summary")
    assert detail == summ


def test_summary_never_aggregates_planned_estimates(con):
    columns = {row[0] for row in con.execute("DESCRIBE v_procurement_live_tenders_summary").fetchall()}
    assert "est_value_floor_eur" not in columns
    assert "n_with_estimate" in columns
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders_summary WHERE n_with_estimate > n_open_tenders",
    )
    assert bad == 0


def test_buyer_name_is_clean(con):
    # The eTenders grid appends an internal org id ("Cork County Council_424") and school roll
    # numbers ("Scoil Ailbhe - (18030I)") to the buyer name; the extractor strips both. A real
    # acronym/place-name in parens ("…(HIQA)", "…(Navan)") must survive — so we only forbid the
    # identifier forms (trailing _<digits> and " - (<digit-led code>)"), never all parentheses.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        r"WHERE regexp_matches(buyer, '_[0-9]+$') OR regexp_matches(buyer, '[-–]\s*\([0-9]')",
    )
    assert bad == 0


def test_buyer_org_id_is_preserved(con):
    # The stripped org id is not discarded — it is lifted into its own column as a stable join key,
    # and where present it is digits-only.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_live_tenders "
        "WHERE buyer_org_id IS NOT NULL AND NOT regexp_matches(buyer_org_id, '^[0-9]+$')",
    )
    assert bad == 0


# ── CPV detail-page parser (pure unit; no snapshot / no browser needed) ─────────────
@pytest.mark.parametrize(
    ("text", "code", "division"),
    [
        ("CPV Codes: 45000000 - Construction work", "45000000", "Construction"),
        ("Common Procurement Vocabulary (CPV): 72000000 IT", "72000000", "IT services"),
        ("CPV Codes:\n  48000000 - Software package", "48000000", "Software"),
        ("no cpv anywhere here", None, None),
        ("CPV 03000000 farm products", "03000000", "Other/Unknown"),  # unknown division → labelled, not dropped
    ],
)
def test_cpv_parser(text, code, division):
    from extractors.etenders_live_tenders_extract import _cpv_from_text

    assert _cpv_from_text(text) == (code, division)


class _FakeNext:
    def __init__(self, page_number):
        self.page_number = page_number

    def get_attribute(self, _name):
        return f"/grid?-p={self.page_number}&size=25"


class _FakeGridPage:
    def __init__(self, terminal_page):
        self.current_page = 1
        self.terminal_page = terminal_page
        self.url = "https://www.etenders.gov.ie/grid"

    def query_selector(self, _selector):
        if self.current_page < self.terminal_page:
            return _FakeNext(self.current_page + 1)
        return None


def test_scrape_feed_marks_page_cap_as_incomplete(monkeypatch):
    import extractors.etenders_live_tenders_extract as extractor

    page = _FakeGridPage(terminal_page=3)

    def goto(fake_page, url, _settle_ms):
        match = extractor.re.search(r"-p=(\d+)&", url)
        fake_page.current_page = int(match.group(1)) if match else 1
        fake_page.url = url

    monkeypatch.setattr(extractor, "_grid_goto", goto)
    monkeypatch.setattr(extractor, "_header_index", lambda _page: {"title": 0, "resource_id": 1})
    monkeypatch.setattr(
        extractor,
        "_rows",
        lambda fake_page: [{"cells": [f"Tender {fake_page.current_page}", str(fake_page.current_page)], "href": "/x"}],
    )

    result = extractor._scrape_feed(page, "cft", max_pages=2, delay_ms=0)

    assert result.pages_visited == 2
    assert result.terminal_reached is False
    assert result.termination_reason == "page_cap_reached"
    assert len(result.rows) == 2


def test_scrape_feed_records_natural_terminal(monkeypatch):
    import extractors.etenders_live_tenders_extract as extractor

    page = _FakeGridPage(terminal_page=2)

    def goto(fake_page, url, _settle_ms):
        match = extractor.re.search(r"-p=(\d+)&", url)
        fake_page.current_page = int(match.group(1)) if match else 1
        fake_page.url = url

    monkeypatch.setattr(extractor, "_grid_goto", goto)
    monkeypatch.setattr(extractor, "_header_index", lambda _page: {"title": 0, "resource_id": 1})
    monkeypatch.setattr(
        extractor,
        "_rows",
        lambda fake_page: [{"cells": [f"Tender {fake_page.current_page}", str(fake_page.current_page)], "href": "/x"}],
    )

    result = extractor._scrape_feed(page, "notice", max_pages=5, delay_ms=0)

    assert result.pages_visited == 2
    assert result.terminal_reached is True
    assert result.termination_reason == "no_next_page"


def test_fresh_grid_snapshot_retains_prior_cpv_by_portal_id():
    from extractors.etenders_live_tenders_extract import _merge_prior_cpv

    current = pl.DataFrame(
        {
            "feed": ["cft", "cft"],
            "resource_id": ["1", "2"],
            "cpv_code": [None, None],
            "cpv_division": [None, None],
        },
        schema_overrides={"cpv_code": pl.String, "cpv_division": pl.String},
    )
    previous = pl.DataFrame(
        {
            "feed": ["cft"],
            "resource_id": ["1"],
            "cpv_code": ["45000000"],
            "cpv_division": ["Construction"],
        }
    )

    merged = _merge_prior_cpv(current, previous).sort("resource_id")

    assert merged["cpv_code"].to_list() == ["45000000", None]
    assert merged["cpv_division"].to_list() == ["Construction", None]


def test_detail_enrichment_preserves_the_complete_grid(monkeypatch):
    import extractors.etenders_live_tenders_extract as extractor

    class Browser:
        @staticmethod
        def is_connected():
            return True

        @staticmethod
        def close():
            return None

    class Page:
        @staticmethod
        def is_closed():
            return False

        @staticmethod
        def wait_for_timeout(_delay):
            return None

    frame = pl.DataFrame(
        {
            "feed": ["cft", "notice"],
            "resource_id": ["1", "2"],
            "detail_url": ["https://example.test/1", "https://example.test/2"],
            "cpv_code": [None, None],
            "cpv_division": [None, None],
        },
        schema_overrides={"cpv_code": pl.String, "cpv_division": pl.String},
    )
    monkeypatch.setattr(extractor, "_launch", lambda _pw: (Browser(), Page()))
    monkeypatch.setattr(extractor, "_detail_cpv", lambda _page, _url, _delay: ("45000000", "Construction"))

    enriched, attempted, found = extractor._enrich_cpv_rows(object(), frame, max_details=10, delay_ms=0)

    assert enriched.height == frame.height
    assert attempted == found == 1
    assert enriched.sort("resource_id")["cpv_code"].to_list() == ["45000000", None]


def test_change_events_do_not_infer_withdrawal_from_absence():
    from extractors.etenders_live_tenders_extract import _detect_events

    previous = pl.DataFrame(
        [
            {"feed": "cft", "resource_id": "1", "title": "Road", "deadline_raw": "old", "detail_url": "/1"},
            {"feed": "cft", "resource_id": "2", "title": "Bridge", "deadline_raw": "same", "detail_url": "/2"},
        ]
    )
    current = pl.DataFrame(
        [
            {"feed": "cft", "resource_id": "1", "title": "Road", "deadline_raw": "new", "detail_url": "/1"},
            {"feed": "cft", "resource_id": "3", "title": "School", "deadline_raw": "date", "detail_url": "/3"},
        ]
    )

    events = _detect_events(previous, current, "2026-08-14T12:00:00+00:00")

    assert set(events["event_type"]) == {"deadline_raw_changed", "new_opportunity"}
    assert set(events["resource_id"]) == {"1", "3"}
    assert "2" not in events["resource_id"].to_list()


def test_observation_ids_bind_time_and_source_content():
    from extractors.etenders_live_tenders_extract import _build_observations

    frame = pl.DataFrame([{"feed": "cft", "resource_id": "1", "title": "Road", "deadline_raw": "old"}])
    first = _build_observations(frame, "2026-08-14T12:00:00+00:00")
    repeat = _build_observations(frame, "2026-08-14T12:00:00+00:00")
    changed = _build_observations(
        frame.with_columns(pl.lit("new").alias("deadline_raw")),
        "2026-08-14T12:00:00+00:00",
    )

    assert first["observation_id"][0] == repeat["observation_id"][0]
    assert first["content_hash"][0] != changed["content_hash"][0]
    assert first["observation_id"][0] != changed["observation_id"][0]


def test_live_tenders_cpv_division_valid_when_present(con):
    """Once the snapshot is CPV-enriched, every non-null cpv_division must be a real label (never
    an empty string); skips cleanly on an un-enriched snapshot that has no such column/values."""
    try:
        bad = _q(
            con,
            "SELECT COUNT(*) FROM v_procurement_live_tenders WHERE cpv_division IS NOT NULL AND cpv_division = ''",
        )
    except duckdb.Error:
        pytest.skip("snapshot not CPV-enriched yet (no cpv_division column)")
    assert bad == 0
