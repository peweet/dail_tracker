from __future__ import annotations

import duckdb

from dail_tracker_core.queries.procurement.pre_tender import (
    pre_tender_areas,
    pre_tender_lead_by_id,
    pre_tender_lead_count,
    pre_tender_leads,
    pre_tender_sectors,
    pre_tender_stages,
    pre_tender_summary,
    pre_tender_work_package_count,
    pre_tender_work_packages,
)


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE v_procurement_pre_tender_leads (
            lead_id VARCHAR, source_corpus VARCHAR, source_record_id VARCHAR,
            source_date DATE, date_precision VARCHAR, buyer_or_sponsor VARCHAR,
            reporting_area VARCHAR, area_basis VARCHAR, project_name VARCHAR,
            sector VARCHAR, likely_work_package VARCHAR, published_stage VARCHAR,
            normalized_stage VARCHAR, stage_display_order BIGINT, amount_text VARCHAR,
            amount_is_not_aggregable BOOLEAN, source_url VARCHAR,
            source_review_required BOOLEAN, current_status_verified BOOLEAN,
            tender_notice_status VARCHAR, report_as_of VARCHAR, classification_schema VARCHAR,
            school_roll_number VARCHAR, snapshot_freshness VARCHAR,
            stage_1_start_date VARCHAR, stage_2a_start_date VARCHAR,
            stage_2b_start_date VARCHAR, stage_3_start_date VARCHAR,
            stage_4_start_date VARCHAR, start_on_site_date VARCHAR,
            start_on_site_note VARCHAR
        )
        """
    )
    rows = [
        (
            "lead-1",
            "semi_state_minutes",
            "record-1",
            "2026-03-31",
            "day",
            "Uisce Eireann",
            "Navan",
            "explicit",
            "Navan wastewater upgrade",
            "Water",
            "Civil works",
            "approved",
            "approved_to_procure",
            2,
            None,
            True,
            "https://example.ie/1",
            True,
            False,
            "not_checked_against_live_tenders",
            "2026-08-07",
            "pre-tender-lead/1",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "lead-2",
            "pq_attachment_project_table",
            "record-2",
            "2025-06-17",
            "day",
            "Department",
            "Cork",
            "explicit",
            "School extension",
            "Education",
            "Building works",
            "design",
            "detailed_design",
            5,
            "EUR 2m",
            True,
            "https://example.ie/2",
            True,
            False,
            "not_checked_against_live_tenders",
            "2026-08-07",
            "pre-tender-lead/1",
            "12345A",
            "recent_180_days",
            "2020-04-15",
            None,
            None,
            None,
            "2025-05-29",
            "2025-05-29",
            None,
        ),
    ]
    conn.executemany(
        "INSERT INTO v_procurement_pre_tender_leads VALUES (" + ", ".join(["?"] * 31) + ")",
        rows,
    )
    conn.execute(
        """
        CREATE TABLE v_procurement_pre_tender_work_packages (
            package_row_id VARCHAR, lead_id VARCHAR, source_corpus VARCHAR,
            source_record_id VARCHAR, source_date DATE, reporting_area VARCHAR,
            project_name VARCHAR, package_code VARCHAR, package_label VARCHAR,
            package_group VARCHAR, evidence_phrase VARCHAR, matched_field VARCHAR,
            classification_basis VARCHAR, source_url VARCHAR,
            source_review_required BOOLEAN, current_status_verified BOOLEAN,
            amount_is_not_aggregable BOOLEAN, classification_schema VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO v_procurement_pre_tender_work_packages VALUES "
        "('lead-2:electrical_lighting_bems', 'lead-2', 'pq_school_project_table', 'record-2',"
        " '2025-06-17', 'Cork', 'School extension', 'electrical_lighting_bems',"
        " 'Electrical, lighting and BEMS', 'Building services', 'electrical works',"
        " 'evidence_text', 'source_literal_rule', 'https://example.ie/2', TRUE, FALSE, TRUE,"
        " 'pre-tender-work-package/1')"
    )
    return conn


def test_pre_tender_query_filters_without_changing_grain():
    conn = _connection()
    try:
        result = pre_tender_leads(conn, area="nav", sector="Water", stage="approved_to_procure")
        assert result.ok is True
        assert result.data["lead_id"].tolist() == ["lead-1"]
        assert result.data.iloc[0]["amount_is_not_aggregable"]
        assert not result.data.iloc[0]["current_status_verified"]
        assert pre_tender_lead_count(conn, area="nav").data.iloc[0]["total"] == 1
    finally:
        conn.close()


def test_pre_tender_detail_and_unavailable_state():
    conn = _connection()
    try:
        result = pre_tender_lead_by_id(conn, "lead-2")
        assert result.ok is True
        assert result.data.iloc[0]["source_url"] == "https://example.ie/2"
        assert result.data.iloc[0]["school_roll_number"] == "12345A"
        assert result.data.iloc[0]["snapshot_freshness"] == "recent_180_days"
        # Without these the app cannot tell an already-let contract from a lead.
        assert result.data.iloc[0]["stage_1_start_date"] == "2020-04-15"
        assert result.data.iloc[0]["start_on_site_date"] == "2025-05-29"
    finally:
        conn.close()

    empty_conn = duckdb.connect()
    try:
        assert pre_tender_leads(empty_conn).ok is False
    finally:
        empty_conn.close()


def test_pre_tender_work_packages_query_preserves_package_grain():
    conn = _connection()
    try:
        result = pre_tender_work_packages(conn, package_code="electrical_lighting_bems")
        assert result.ok is True
        assert result.data["lead_id"].tolist() == ["lead-2"]
        assert result.data.iloc[0]["evidence_phrase"] == "electrical works"
        assert pre_tender_work_package_count(conn, package_group="Building services").data.iloc[0]["total"] == 1
    finally:
        conn.close()


def test_pre_tender_list_offset_uses_the_same_filtered_result_set():
    conn = _connection()
    try:
        first_page = pre_tender_leads(conn, limit=1, offset=0)
        second_page = pre_tender_leads(conn, limit=1, offset=1)
        assert first_page.data["lead_id"].tolist() == ["lead-1"]
        assert second_page.data["lead_id"].tolist() == ["lead-2"]
    finally:
        conn.close()


def test_facet_counts_move_with_the_other_active_filters():
    """A facet must count under the OTHER filters, never the whole corpus — otherwise the page
    offers an option that returns nothing, which is the bug the live-tender sector facet hit."""
    conn = _connection()
    try:
        all_sectors = pre_tender_sectors(conn)
        assert dict(zip(all_sectors.data["sector"], all_sectors.data["n"], strict=True)) == {
            "Water": 1,
            "Education": 1,
        }
        # Narrowed by an area that only lead-1 reports from, Education must disappear entirely.
        narrowed = pre_tender_sectors(conn, area="nav")
        assert narrowed.data["sector"].tolist() == ["Water"]
        assert pre_tender_areas(conn, sector="Education").data["area"].tolist() == ["Cork"]
        assert pre_tender_stages(conn, area="cork").data["stage"].tolist() == ["detailed_design"]
    finally:
        conn.close()


def test_stage_facet_orders_closest_to_tender_first():
    """stage_display_order is the corpus's own ordering; the facet must honour it rather than
    re-sorting alphabetically, so 'approved to procure' (2) precedes 'detailed design' (5)."""
    conn = _connection()
    try:
        stages = pre_tender_stages(conn)
        assert stages.data["stage"].tolist() == ["approved_to_procure", "detailed_design"]
        assert stages.data["stage_display_order"].tolist() == [2, 5]
    finally:
        conn.close()


def test_summary_reports_verification_and_staleness_counts():
    """The section headline is built from these counts rather than asserting them in prose, so
    the 'nothing has been checked' sentence stays true if the upstream flag ever flips."""
    conn = _connection()
    try:
        s = pre_tender_summary(conn).data.iloc[0]
        assert s["n_leads"] == 2
        assert s["n_areas"] == 2
        assert s["n_sources"] == 2
        assert s["n_status_verified"] == 0
        assert s["n_recent"] == 1
        assert s["n_stale"] == 0
        assert str(s["newest_observation"])[:10] == "2026-03-31"
    finally:
        conn.close()


def test_summary_counts_rows_the_section_title_does_not_describe():
    """The lane is called "before the tender", but some rows record a project that had ALREADY
    reached construction when it was reported — 9 in-construction rows and 4 with a past
    start-on-site date were sitting under a blanket "have not been advertised" headline.

    The count has to come from the summary so the page can state it instead of asserting a claim
    the data contradicts. Both routes to "past tender" are checked: the reported stage, and a
    start-on-site date already gone by.
    """
    conn = _connection()
    try:
        # lead-2 alone qualifies, via a start_on_site_date in the past.
        assert pre_tender_summary(conn).data.iloc[0]["n_past_tender"] == 1

        # A row reported as in construction qualifies on the stage alone, with no dates at all.
        conn.execute(
            "INSERT INTO v_procurement_pre_tender_leads VALUES ("
            "'lead-3', 'pq_attachment_project_table', 'record-3', '2025-09-08', 'day', 'Department',"
            " 'Offaly', 'explicit', 'Colaiste Chilliain', 'Education', 'Building works', 'Stage 4',"
            " 'active_delivery', 60, NULL, TRUE, 'https://example.ie/3', TRUE, FALSE,"
            " 'not_checked_against_live_tenders', '2026-08-07', 'pre-tender-lead/1', NULL,"
            " 'ageing_181_to_365_days', NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        assert pre_tender_summary(conn).data.iloc[0]["n_past_tender"] == 2
    finally:
        conn.close()


def test_summary_counts_rows_no_age_band_can_cover():
    """ "n reported over a year ago" must not be read as an age statement about the whole corpus.
    A row with no source_date can carry no honest band, so the page needs the count of rows its
    staleness sentence does not speak for."""
    conn = _connection()
    try:
        assert pre_tender_summary(conn).data.iloc[0]["n_undated"] == 0
        conn.execute(
            "INSERT INTO v_procurement_pre_tender_leads VALUES ("
            "'lead-4', 'semi_state_minutes', 'record-4', NULL, 'unknown', 'Body', 'Sligo', 'explicit',"
            " 'Undated project', 'Water', 'Civil works', 'planning', 'planning', 8, NULL, TRUE,"
            " 'https://example.ie/4', TRUE, FALSE, 'not_checked_against_live_tenders', '2026-08-07',"
            " 'pre-tender-lead/1', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
        )
        s = pre_tender_summary(conn).data.iloc[0]
        assert s["n_undated"] == 1
        assert s["n_leads"] == 3
    finally:
        conn.close()


def test_facets_report_unavailable_when_the_view_is_missing():
    """Same failure contract as the listing: a missing view is 'unavailable', not 'no options'."""
    empty_conn = duckdb.connect()
    try:
        assert pre_tender_sectors(empty_conn).ok is False
        assert pre_tender_stages(empty_conn).ok is False
        assert pre_tender_areas(empty_conn).ok is False
        assert pre_tender_summary(empty_conn).ok is False
    finally:
        empty_conn.close()
