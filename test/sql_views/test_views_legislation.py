"""
SQL view contract tests — legislation and statutory-instrument views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""


from pathlib import Path

import pytest

from ._view_test_helpers import (
    _USE_REAL_PATHS,
    _DATA_BASE,
    GOLD_PARQUET_DIR,
    SILVER_PARQUET_DIR,
    _con,
    _view_path,
    _load,
    _skip_missing,
    _result,
    _src,
    _assert_cols,
)


# ---------------------------------------------------------------------------
# LEGISLATION VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_legislation_index_executes():
    _skip_missing(SILVER_PARQUET_DIR / "sponsors.parquet")
    con = _con()
    con.execute(_load("legislation_index.sql"))
    result = _result(con, "v_legislation_index")
    assert "bill_title" in result.columns
    assert "introduced_date" in result.columns
    assert "stage_number" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_debates_executes():
    _skip_missing(
        SILVER_PARQUET_DIR / "debates.parquet",
        SILVER_PARQUET_DIR / "sponsors.parquet",
    )
    con = _con()
    con.execute(_load("legislation_debates.sql"))
    result = _result(con, "v_legislation_debates")
    assert "debate_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_debate_listings_executes():
    _skip_missing(SILVER_PARQUET_DIR / "debate_listings.parquet")
    con = _con()
    con.execute(_load("v_debate_listings.sql"))
    result = _result(con, "v_debate_listings")
    for col in (
        "debate_section_id",
        "debate_date",
        "chamber",
        "debate_type",
        "speaker_count",
        "speech_count",
        "debate_url_web",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_debate_listings"
    assert len(result) > 0


# ---------------------------------------------------------------------------
# STATUTORY INSTRUMENT VIEWS
# ---------------------------------------------------------------------------


_SI_STATE_ENUM = {
    "in_force_as_made",
    "amended",
    "partially_revoked",
    "amended_and_partially_revoked",
    "revoked",
    "other_affected",
}


@pytest.mark.sql
def test_v_statutory_instruments_executes():
    """The SI-as-entity view. Locks the signatory contract the SI detail panel
    reads — si_responsible_actor (printed signing office), si_signatory_name
    (printed signer name), and the tenure-inferred si_minister_name/member_code.
    A schema drift on any of these silently breaks 'who signed the SI'. Also
    locks the legal-state columns LEFT-JOINed from v_si_current_state."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    # v_statutory_instruments LEFT-JOINs v_si_current_state, so register that
    # view first (production's register_views does this via alphabetical order).
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    result = _result(con, "v_statutory_instruments")
    for col in (
        "si_id",
        "si_signed_date",
        "si_responsible_actor",
        "si_signatory_name",
        "si_minister_name",
        "si_minister_member_code",
        # legal-state columns from the LEFT JOIN
        "current_state",
        "affecting_sis",
        "state_source_url",
        "directory_updated_to",
        "state_confidence",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_statutory_instruments"
    assert len(result) > 0


@pytest.mark.sql
def test_v_statutory_instruments_left_join_no_inflation():
    """The legal-state LEFT JOIN must be one-row-per-SI: the view row count must
    equal the base parquet row count (no fan-out), and SIs absent from the
    directory crawl must keep a NULL current_state ('status not checked'), never
    drop out."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    base = con.execute(
        f"SELECT count(*) FROM read_parquet('{(_DATA_BASE / 'data/gold/parquet/statutory_instruments.parquet').as_posix()}')"
    ).fetchone()[0]
    view = con.execute("SELECT count(*) FROM v_statutory_instruments").fetchone()[0]
    assert view == base, f"LEFT JOIN inflated rows: base={base} view={view}"


@pytest.mark.sql
def test_v_si_current_state_executes():
    """The SI legal-state view (eISB Legislation Directory). Locks the enum and
    the provenance invariant the detail-panel chip + caveat depend on: every
    revoked / partially_revoked row must carry a confirm link (state_source_url),
    and current_state must stay within the agreed enum."""
    _skip_missing(GOLD_PARQUET_DIR / "si_current_state.parquet")
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    result = _result(con, "v_si_current_state")
    _assert_cols(
        result,
        "si_id",
        "current_state",
        "affecting_sis",
        "this_si_eli_url",
        "how_affected_raw",
        "state_source",
        "state_source_url",
        "directory_updated_to",
        "confidence",
    )
    assert len(result) > 0

    # Enum check across the whole view.
    states = con.execute("SELECT DISTINCT current_state FROM v_si_current_state").fetchall()
    for (s,) in states:
        assert s in _SI_STATE_ENUM, f"current_state '{s}' outside the agreed enum"

    # Provenance invariant: a negative legal state must always be sourced.
    unsourced = con.execute(
        "SELECT count(*) FROM v_si_current_state "
        "WHERE current_state IN ('revoked', 'partially_revoked') AND state_source_url IS NULL"
    ).fetchone()[0]
    assert unsourced == 0, f"{unsourced} revoked/partially_revoked rows missing state_source_url"


@pytest.mark.sql
def test_v_si_current_state_coverage_gate():
    """Join coverage vs gold ≥ 95% (the extractor measured 99.5%). Guards against
    an eISB layout change silently dropping the directory crawl to a stub. Runs
    only against real pipeline output — the CI fixture is a 2/3 stub by design."""
    if not _USE_REAL_PATHS:
        pytest.skip("coverage gate needs real pipeline output (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
    )
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    total, matched = con.execute("SELECT count(*), count(current_state) FROM v_statutory_instruments").fetchone()
    cov = matched / total if total else 0
    assert cov >= 0.95, f"SI legal-state coverage {cov:.1%} < 95% — directory crawl may be broken"


@pytest.mark.sql
def test_v_bill_statutory_instruments_executes():
    """The bill-gated SI view (SIs joined to their enabling Act)."""
    _skip_missing(GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet")
    con = _con()
    con.execute(_load("legislation_statutory_instruments.sql"))
    result = _result(con, "v_bill_statutory_instruments")
    for col in ("bill_id", "si_id", "si_minister", "si_minister_named"):
        assert col in result.columns, f"Expected column '{col}' in v_bill_statutory_instruments"
    assert len(result) > 0


# --- v_si_amendments — the SI→SI amendment/revocation graph (edge inversion) ---

_SI_AMEND_EFFECTS = {"revokes", "amends", "partially revokes", "amends and partially revokes"}


def _write_si_amendments_fixture(root: Path) -> None:
    """Build a minimal si_current_state + statutory_instruments parquet pair under
    root/data/gold/parquet/ that exercises every derivation rule of v_si_amendments."""
    import polars as pl

    pdir = root / "data" / "gold" / "parquet"
    pdir.mkdir(parents=True, exist_ok=True)

    # affected-side rows. Lists are the eISB "affecting" instruments.
    state = pl.DataFrame(
        {
            "si_year": [2020, 2020, 2020, 2020, 2020],
            "si_number": [100, 101, 102, 103, 104],
            "current_state": [
                "revoked",  # -> 1 edge, effect 'revokes'
                "amended",  # -> 1 edge, effect 'amends', provision parsed
                "other_affected",  # EXCLUDED (indirect refs)
                "in_force_as_made",  # EXCLUDED (no affecting edge)
                "partially_revoked",  # -> 1 edge, effect 'partially revokes', amender out-of-gold
            ],
            "this_si_eli_url": ["eli100", "eli101", "eli102", "eli103", "eli104"],
            "how_affected_raw": [
                "Revoked || S.I. No. 200 of 2021 , reg. 5",
                "Reg. 3 amended || S.I. No. 201 of 2022 , reg. 2",
                "Rendered obsolete by revocation of S.I. No. 90 of 2019 || S.I. No. 202 of 2021 , reg. 1",
                None,
                "Reg. 4 revoked || S.I. No. 204 of 2023 , reg. 9",
            ],
            "confidence": [0.90, 0.88, 0.70, 0.95, 0.85],
            "affecting_sis": [
                ["200/2021"],
                ["201/2022"],
                ["90/2019", "202/2021"],  # would inflate if not excluded
                [],
                ["204/2023"],
            ],
            "affecting_si_urls": [
                ["u200"],
                ["u201"],
                ["u90", "u202"],
                [],
                ["u204"],
            ],
        }
    )
    state.write_parquet(pdir / "si_current_state.parquet")

    # titles: include both bases and the in-gold amenders; OMIT 204/2023 so its
    # amender_title must come back NULL (LEFT JOIN, not an inner-join drop).
    sis = pl.DataFrame(
        {
            "si_year": [2020, 2020, 2020, 2020, 2021, 2022],
            "si_number": [100, 101, 102, 104, 200, 201],
            "si_title": [
                "Base A Regs 2020",
                "Base B Regs 2020",
                "Base C Regs 2020",
                "Base E Regs 2020",
                "Revoker Regs 2021",
                "Amender Regs 2022",
            ],
        }
    )
    sis.write_parquet(pdir / "statutory_instruments.parquet")


def test_v_si_amendments_inversion_contract(tmp_path):
    """Precise derivation contract on a synthetic fixture (no real data needed):
    edge inversion, effect mapping, other_affected exclusion, no row inflation
    from multi-element lists, number/year parse, LEFT-JOIN title fill."""
    _write_si_amendments_fixture(tmp_path)
    sql = _view_path("legislation_si_amendments.sql").read_text(encoding="utf-8")
    sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
    con = _con()
    con.execute(sql)
    df = con.execute("SELECT * FROM v_si_amendments ORDER BY affected_number").pl()

    # exactly 3 edges: revoked(100), amended(101), partially_revoked(104).
    # other_affected(102) excluded -> its 2-element list does NOT inflate; 103 has no edge.
    assert df.height == 3, f"expected 3 clean edges, got {df.height}"
    assert set(df["current_state"]) == {"revoked", "amended", "partially_revoked"}
    assert set(df["effect"]).issubset(_SI_AMEND_EFFECTS)
    assert 102 not in set(df["affected_number"]), "other_affected must be excluded"
    assert 103 not in set(df["affected_number"]), "no-edge row must be excluded"

    # effect mapping + number/year parse + provision extraction (the amended row)
    amend = con.execute("SELECT * FROM v_si_amendments WHERE affected_number=101").pl().to_dicts()[0]
    assert amend["effect"] == "amends"
    assert (amend["amender_number"], amend["amender_year"]) == (201, 2022)
    assert amend["amender_title"] == "Amender Regs 2022"
    assert amend["provision_note"] == "Reg. 3 amended"

    # DIR2 inversion: the revoker 200/2021 points at the affected base 100/2020
    rev = con.execute("SELECT * FROM v_si_amendments WHERE amender_number=200 AND amender_year=2021").pl().to_dicts()[0]
    assert rev["effect"] == "revokes"
    assert (rev["affected_number"], rev["affected_year"]) == (100, 2020)

    # LEFT JOIN: amender 204/2023 is absent from gold -> title NULL, row still present
    part = con.execute("SELECT * FROM v_si_amendments WHERE effect='partially revokes'").pl().to_dicts()[0]
    assert (part["amender_number"], part["amender_year"]) == (204, 2023)
    assert part["amender_title"] is None


# --- v_si_lrc_enrichment + v_statutory_instruments_classified (LRC subject) ---

_LRC_STATUS_ENUM = {"matched_classified_list", "not_matched"}
_LRC_FORBIDDEN = {"in_force", "valid", "invalid", "official_status", "legally_current"}


@pytest.mark.sql
def test_v_si_lrc_enrichment_executes():
    """LRC subject-classification view. Locks the column contract the SI subject
    chip + topic facet read, the SAFE-status enum (never 'in force'), and one row
    per SI (no fan-out from the source summary)."""
    _skip_missing(GOLD_PARQUET_DIR / "si_lrc_enrichment_summary.parquet")
    con = _con()
    con.execute(_load("legislation_si_lrc_enrichment.sql"))
    result = _result(con, "v_si_lrc_enrichment")
    for col in (
        "si_year",
        "si_number",
        "has_lrc_classified_list_match",
        "lrc_primary_subject",
        "lrc_primary_leaf",
        "lrc_enrichment_status",
        "lrc_caveat",
        "lrc_list_updated_to",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_si_lrc_enrichment"

    # safe status vocabulary — the dangerous failure is a legal-status assertion
    states = {s for (s,) in con.execute("SELECT DISTINCT lrc_enrichment_status FROM v_si_lrc_enrichment").fetchall()}
    assert states <= _LRC_STATUS_ENUM, f"status outside safe enum: {states - _LRC_STATUS_ENUM}"
    joined = " ".join(states)
    for bad in _LRC_FORBIDDEN:
        assert bad not in joined, f"forbidden legal-status token {bad!r} in lrc_enrichment_status"

    # one row per SI
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (si_year, si_number)) FROM v_si_lrc_enrichment"
    ).fetchone()
    assert n == distinct, f"v_si_lrc_enrichment not one-row-per-SI: {n} rows, {distinct} distinct"
    # unmatched rows must carry no subject (never a fabricated classification)
    bad = con.execute(
        "SELECT count(*) FROM v_si_lrc_enrichment "
        "WHERE lrc_enrichment_status='not_matched' AND lrc_primary_subject IS NOT NULL"
    ).fetchone()[0]
    assert bad == 0, f"{bad} not_matched rows carry a subject"


@pytest.mark.sql
def test_v_statutory_instruments_classified_no_inflation():
    """The page's browse surface = v_statutory_instruments LEFT JOIN the LRC
    enrichment. Must stay one-row-per-SI (no fan-out) and expose the subject
    columns the facet/chip read."""
    _skip_missing(
        GOLD_PARQUET_DIR / "statutory_instruments.parquet",
        GOLD_PARQUET_DIR / "si_current_state.parquet",
        GOLD_PARQUET_DIR / "si_lrc_enrichment_summary.parquet",
    )
    con = _con()
    # dependency order: current_state -> index (v_statutory_instruments) ->
    # lrc_enrichment -> zz_classified
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    con.execute(_load("legislation_si_lrc_enrichment.sql"))
    con.execute(_load("legislation_si_zz_classified.sql"))
    base = con.execute("SELECT count(*) FROM v_statutory_instruments").fetchone()[0]
    clf = con.execute("SELECT count(*) FROM v_statutory_instruments_classified").fetchone()[0]
    assert clf == base, f"LRC LEFT JOIN inflated rows: base={base} classified={clf}"
    result = _result(con, "v_statutory_instruments_classified")
    for col in ("si_id", "lrc_primary_subject", "lrc_primary_leaf", "lrc_enrichment_status"):
        assert col in result.columns, f"Expected column '{col}' in v_statutory_instruments_classified"


@pytest.mark.sql
def test_v_si_amendments_executes():
    """Real-data execute + contract: column shape, effect enum, other_affected
    excluded, and row count equals the clean-state edge count in the source
    parquet (guards the inversion against silent fan-out or scope drift)."""
    _skip_missing(GOLD_PARQUET_DIR / "si_current_state.parquet", GOLD_PARQUET_DIR / "statutory_instruments.parquet")
    con = _con()
    con.execute(_load("legislation_si_amendments.sql"))
    result = _result(con, "v_si_amendments")
    for col in (
        "amender_number",
        "amender_year",
        "amender_title",
        "amender_eli_url",
        "effect",
        "current_state",
        "provision_note",
        "confidence",
        "affected_number",
        "affected_year",
        "affected_title",
        "affected_eli_url",
    ):
        assert col in result.columns, f"Expected column '{col}' in v_si_amendments"

    effects = con.execute("SELECT DISTINCT effect FROM v_si_amendments").fetchall()
    for (e,) in effects:
        assert e in _SI_AMEND_EFFECTS, f"effect '{e}' outside the agreed set"
    assert con.execute("SELECT count(*) FROM v_si_amendments WHERE current_state='other_affected'").fetchone()[0] == 0

    # row count must equal sum(len(affecting_sis)) over clean states — no inflation
    src = (_DATA_BASE / "data/gold/parquet/si_current_state.parquet").as_posix()
    expected = con.execute(
        f"SELECT coalesce(sum(len(affecting_sis)),0) FROM read_parquet('{src}') "
        "WHERE current_state IN ('revoked','partially_revoked','amended','amended_and_partially_revoked') "
        "AND affecting_sis IS NOT NULL"
    ).fetchone()[0]
    view_n = con.execute("SELECT count(*) FROM v_si_amendments").fetchone()[0]
    assert view_n == expected, f"edge count {view_n} != clean-state affecting count {expected}"


# ---------------------------------------------------------------------------
# LEGISLATION VIEWS (gap backfill)
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_legislation_detail_executes():
    _skip_missing(*_src("data/silver/parquet/sponsors.parquet"))
    con = _con()
    con.execute(_load("legislation_detail.sql"))
    result = _result(con, "v_legislation_detail")
    _assert_cols(
        result, "bill_id", "bill_title", "bill_status", "sponsor", "introduced_date", "current_stage", "oireachtas_url"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_pdfs_executes():
    _skip_missing(*_src("data/silver/parquet/versions.parquet"))
    con = _con()
    con.execute(_load("legislation_pdfs.sql"))
    result = _result(con, "v_legislation_pdfs")
    _assert_cols(result, "bill_id", "pdf_category", "pdf_label", "pdf_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_sources_executes():
    _skip_missing(*_src("data/silver/parquet/sponsors.parquet"))
    con = _con()
    con.execute(_load("legislation_sources.sql"))
    result = _result(con, "v_legislation_sources")
    _assert_cols(result, "bill_id", "oireachtas_url", "source_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_timeline_executes():
    _skip_missing(*_src("data/silver/parquet/stages.parquet"))
    con = _con()
    con.execute(_load("legislation_timeline.sql"))
    result = _result(con, "v_legislation_timeline")
    _assert_cols(result, "bill_id", "stage_name", "stage_date", "stage_number", "chamber")
    assert len(result) > 0


@pytest.mark.sql
def test_v_legislation_pre2014_acts_executes():
    """Curated pre-2014 Acts crosswalk (data/_meta/pre2014_acts.csv). The meta CSV
    is hand-maintained and may be absent on a fresh checkout."""
    _skip_missing(*_src("data/_meta/pre2014_acts.csv"))
    con = _con()
    con.execute(_load("legislation_pre2014_acts.sql"))
    result = _result(con, "v_legislation_pre2014_acts")
    _assert_cols(result, "canonical_bill_id", "act_short_title", "act_year", "policy_domain")
    assert len(result) > 0


@pytest.mark.sql
def test_v_bill_si_operation_mix_executes():
    """Reads v_bill_statutory_instruments — load that first."""
    _skip_missing(*_src("data/gold/parquet/bill_statutory_instruments.parquet"))
    con = _con()
    con.execute(_load("legislation_statutory_instruments.sql"))
    con.execute(_load("legislation_zz_bill_si_operation_mix.sql"))
    result = _result(con, "v_bill_si_operation_mix")
    _assert_cols(result, "bill_id", "si_operation", "n")
    assert len(result) > 0


@pytest.mark.sql
def test_v_act_commencement_executes():
    """Commencement-order timeline per Act. Composes v_bill_statutory_instruments
    + v_statutory_instruments (which itself LEFT-JOINs v_si_current_state) — load
    the dependency views first, in directory order."""
    _skip_missing(
        *_src(
            "data/gold/parquet/bill_statutory_instruments.parquet",
            "data/gold/parquet/statutory_instruments.parquet",
            "data/gold/parquet/si_current_state.parquet",
        )
    )
    con = _con()
    con.execute(_load("legislation_si_current_state.sql"))
    con.execute(_load("legislation_si_index.sql"))
    con.execute(_load("legislation_statutory_instruments.sql"))
    con.execute(_load("legislation_zz_act_commencement.sql"))
    result = _result(con, "v_act_commencement")
    _assert_cols(
        result,
        "bill_id",
        "si_id",
        "si_signed_date",
        "si_commenced_sections",
        "si_minister_name",
        "order_current_state",
    )
    assert len(result) > 0


# --- NULL/EMPTY-STRING regression guard (2026-06-11 audit): legislation
# sponsor fallback — see test_views_procurement.py for the sibling guards
# against the same audit. ---


def _write_sponsors_fixture(tmp_path):
    """Minimal sponsors.parquet feeding BOTH v_legislation_index and v_legislation_detail."""
    import polars as pl

    pdir = tmp_path / "data" / "silver" / "parquet"
    pdir.mkdir(parents=True)
    n = 3
    df = pl.DataFrame(
        {
            "bill_year": ["2024", "2024", "2023"],
            "bill_no": ["10", "11", "12"],
            "short_title_en": ["PMB Bill 2024", "Govt Bill 2024", "Orphan Bill 2023"],
            "long_title_en": ["An Act A", "An Act B", "An Act C"],
            "status": ["Current", "Enacted", "Lapsed"],
            "bill_type": ["Private Member", "Government", "Government"],
            "source": ["Private Member"] * n,
            "origin_house": ["Dáil Éireann"] * n,
            # The three sponsor shapes in the silver data:
            #   PMB    -> sponsor_by_show_as set
            #   Govt   -> ONLY sponsor_as_show_as set (the 557-bill em-dash regression)
            #   Orphan -> both NULL (must be excluded by the WHERE, not shown as '—')
            "sponsor_by_show_as": ["Jane Doe", None, None],
            "sponsor_as_show_as": [None, "Minister for Health", None],
            "sponsor_is_primary": [True, None, None],
            "unique_member_code": ["JaneDoe.D.2020", None, None],
            "context_date": ["2024-01-15", "2024-02-20", "2023-03-01"],
            "last_updated": ["2024-06-01"] * n,
            "method": ["api"] * n,
            "most_recent_stage_event_show_as": ["Second Stage"] * n,
            "most_recent_stage_event_progress_stage": ["3", "11", "2"],
            "most_recent_stage_event_house_show_as": ["Dáil Éireann"] * n,
            "most_recent_stage_event_stage_completed": ["false", "true", "false"],
            "bill_url": ["u10", "u11", "u12"],
        }
    )
    df.write_parquet(pdir / "sponsors.parquet")


@pytest.mark.sql
def test_legislation_index_sponsor_falls_back_like_detail(tmp_path):
    """Government bills carry the sponsor ONLY in sponsor_as_show_as. The index
    once coalesced sponsor_by_show_as straight to '—', so 557 bills (34%)
    rendered an em-dash the detail panel resolved fine. Lock the fallback AND
    index↔detail parity so the two COALESCE chains can't drift again."""
    _write_sponsors_fixture(tmp_path)
    con = _con()
    for fname in ("legislation_index.sql", "legislation_detail.sql"):
        sql = _view_path(fname).read_text(encoding="utf-8")
        sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
        con.execute(sql)

    idx = con.execute("SELECT bill_id, sponsor FROM v_legislation_index").pl()
    # Orphan (both sponsor fields NULL) is excluded by the WHERE — so a '—'
    # sponsor can only mean a skipped fallback, never genuinely-missing data.
    assert idx.height == 2
    assert "2023_12" not in set(idx["bill_id"])
    assert "—" not in set(idx["sponsor"]), "index dropped to '—' despite sponsor_as_show_as being populated"
    by_id = dict(idx.iter_rows())
    assert by_id["2024_10"] == "Jane Doe"
    assert by_id["2024_11"] == "Minister for Health"

    # Parity: every bill must show the SAME sponsor on index and detail.
    mismatch = con.execute(
        """
        SELECT i.bill_id, i.sponsor, d.sponsor
        FROM v_legislation_index i JOIN v_legislation_detail d USING (bill_id)
        WHERE i.sponsor IS DISTINCT FROM d.sponsor
        """
    ).fetchall()
    assert mismatch == [], f"index/detail sponsor drift: {mismatch}"
