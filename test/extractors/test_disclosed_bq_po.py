"""Tests for the disclosed BigQuery PO/payments lanes
(extractors/disclosed_bq_po_extract.py + disclosed_bq_po_newbodies_extract.py).

These two lanes shipped 2026-07-17 with no dedicated tests. What they guard is worth pinning:

  1. Pure-function units — the `_clean` DQ expression and raw-drop discovery (CI, no data files).
  2. The HSE-history lane's SCOPE invariants — only net-new periods (never a period the
     hse_tusla PDF parse already holds: the cross-lane double-count trap), rollup bodies
     excluded, semantics INHERITED from gold (never guessed), privacy leak refuses to write.
  3. The new-bodies lane's registry allow-list (unregistered body silently excluded — that is
     the design), entity-prefix stripping, exact SCHEMA_COLS order (the consolidator concats
     with no reorder), and `_assert_disjoint` — the fail-closed guard that HALTS when a
     publisher_id appears in another lane or gold, EXCEPT gold rows this lane itself wrote.

Everything runs on synthetic fixtures in tmp_path via monkeypatched module constants —
no raw_bq drop, no real gold, no network.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
import disclosed_bq_po_extract as hse_lane  # noqa: E402
import disclosed_bq_po_newbodies_extract as new_lane  # noqa: E402

RAW_COLS = ["PO", "Supplier", "Total", "Description", "QTR", "Year", "entity", "year_quarter"]


def _write_raw(directory: Path, rows: list[dict], name: str = "bq-results-test.csv") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    pl.DataFrame([{c: r.get(c) for c in RAW_COLS} for r in rows]).write_csv(path)
    return path


def _raw_row(entity: str, period_q: str, year: int, supplier: str = "Acme Ltd", total: float = 25_000.0) -> dict:
    return {
        "PO": "PO123",
        "Supplier": supplier,
        "Total": total,
        "Description": "Works",
        "QTR": period_q,  # lowercase "q1" — the lanes' quarter parse strips a lowercase q only
        "Year": year,
        "entity": entity,
        "year_quarter": f"{year}-{period_q}",
    }


def _write_gold(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return path


def _gold_hse_row(period: str, parser_name: str = "hse_tusla_pdf") -> dict:
    return {
        "publisher_id": "ie_hse",
        "publisher_name": "Health Service Executive",
        "publisher_type": "state_body",
        "sector": "health",
        "amount_semantics": "payment_actual",
        "period": period,
        "parser_name": parser_name,
    }


# ---- 1. pure-function units -------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  Murphy \n Contractors\t Ltd  ", "Murphy Contractors Ltd"),
        ("NULL", None),
        ("n/a", None),
        ("None", None),
        ("", None),
        ("plain", "plain"),
    ],
)
def test_clean_expression(raw, expected):
    df = pl.DataFrame({"x": [raw]}).select(hse_lane._clean("x").alias("out"))
    assert df["out"][0] == expected


def test_find_raw_picks_latest_drop(tmp_path, monkeypatch):
    raw_dir = tmp_path / "raw_bq"
    _write_raw(raw_dir, [_raw_row("X", "q1", 2020)], name="bq-results-20260101.csv")
    newest = _write_raw(raw_dir, [_raw_row("X", "q1", 2020)], name="bq-results-20260619.csv")
    monkeypatch.setattr(hse_lane, "RAW_DIR", raw_dir)
    assert hse_lane._find_raw() == newest


def test_find_raw_none_when_dir_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(hse_lane, "RAW_DIR", tmp_path / "nonexistent")
    assert hse_lane._find_raw() is None


def test_lane_schemas_carry_the_same_columns():
    """The consolidator folds both lanes into one fact; a column-SET drift breaks _load_facts."""
    assert set(hse_lane.SCHEMA_COLS) == set(new_lane.SCHEMA_COLS)


# ---- 2. HSE-history lane ----------------------------------------------------


@pytest.fixture
def hse_env(tmp_path, monkeypatch):
    """Synthetic raw drop + gold anchor, all module paths redirected into tmp_path."""
    monkeypatch.setenv("DAIL_SKIP_ROW_FLOOR", "1")  # fixtures are tiny; the 40k floor is not under test
    raw_dir = tmp_path / "raw_bq"
    gold = tmp_path / "gold" / "procurement_payments_fact.parquet"
    out = tmp_path / "silver" / "disclosed_bq_po_payments_fact.parquet"
    coverage = tmp_path / "meta" / "disclosed_bq_po_coverage.json"
    out.parent.mkdir(parents=True)
    coverage.parent.mkdir(parents=True)
    monkeypatch.setattr(hse_lane, "RAW_DIR", raw_dir)
    monkeypatch.setattr(hse_lane, "GOLD", gold)
    monkeypatch.setattr(hse_lane, "OUT", out)
    monkeypatch.setattr(hse_lane, "COVERAGE", coverage)
    # the success print does OUT.relative_to(ROOT); with OUT in tmp, ROOT must be too
    monkeypatch.setattr(hse_lane, "ROOT", tmp_path)
    return raw_dir, gold, out, coverage


def test_hse_no_raw_drop_is_a_noop(hse_env):
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    assert hse_lane.build() == 0
    assert not out.exists()


def test_hse_missing_gold_anchor_is_a_noop(hse_env):
    raw_dir, gold, out, _ = hse_env
    _write_raw(raw_dir, [_raw_row("Health Service Executive", "q1", 2018)])
    assert hse_lane.build() == 0
    assert not out.exists()


def test_hse_emits_only_net_new_periods(hse_env):
    """A period the hse_tusla PDF parse already holds must NOT be re-emitted — that is the
    cross-lane double-count the consolidator's per-source reconcile cannot catch."""
    raw_dir, gold, out, coverage = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(
        raw_dir,
        [
            _raw_row("Health Service Executive", "q1", 2018),  # net-new -> emitted
            _raw_row("Health Service Executive", "q4", 2021),  # held by the PDF parse -> excluded
            _raw_row("Irish Water", "q1", 2018),  # rollup body -> excluded
            _raw_row("Legal Aid Board", "q1", 2018),  # not HSE -> excluded
        ],
    )
    assert hse_lane.build() == 0
    lane = pl.read_parquet(out)
    assert lane["period"].unique().to_list() == ["2018-Q1"]
    assert lane["publisher_id"].unique().to_list() == ["ie_hse"]
    assert lane.height == 1
    cov = json.loads(coverage.read_text(encoding="utf-8"))
    assert cov["periods"] == ["2018-Q1"]
    assert cov["rows"] == 1


def test_hse_all_periods_already_held_is_a_noop(hse_env):
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(raw_dir, [_raw_row("Health Service Executive", "q4", 2021)])
    assert hse_lane.build() == 0
    assert not out.exists()


def test_hse_semantics_inherited_from_gold_not_guessed(hse_env):
    """amount_semantics comes from gold's ie_hse rows — the blank-PO heuristic mislabels HSE,
    which is exactly why the module docstring forbids guessing."""
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(raw_dir, [_raw_row("Health Service Executive", "q1", 2018)])
    hse_lane.build()
    lane = pl.read_parquet(out)
    assert lane["amount_semantics"].unique().to_list() == ["payment_actual"]
    assert lane["publisher_type"].unique().to_list() == ["state_body"]


def test_hse_output_matches_schema_cols_exactly(hse_env):
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(raw_dir, [_raw_row("Health Service Executive", "q1", 2018)])
    hse_lane.build()
    assert pl.read_parquet(out).columns == hse_lane.SCHEMA_COLS


def test_hse_individual_suppliers_are_privacy_gated_not_dropped(hse_env):
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(
        raw_dir,
        [
            _raw_row("Health Service Executive", "q1", 2018, supplier="Acme Ltd"),
            _raw_row("Health Service Executive", "q1", 2018, supplier="Mary Murphy"),
        ],
    )
    hse_lane.build()
    lane = pl.read_parquet(out)
    person = lane.filter(pl.col("supplier_raw") == "Mary Murphy")
    assert person.height == 1, "the row is retained (analysis/coverage), only display is gated"
    assert person["privacy_status"][0] == "review_personal_data"
    assert person["public_display"][0] is False
    company = lane.filter(pl.col("supplier_raw") == "Acme Ltd")
    assert company["public_display"][0] is True


def test_hse_privacy_leak_refuses_to_write(hse_env, monkeypatch):
    raw_dir, gold, out, _ = hse_env
    _write_gold(gold, [_gold_hse_row("2021-Q4")])
    _write_raw(raw_dir, [_raw_row("Health Service Executive", "q1", 2018, supplier="Mary Murphy")])

    real = hse_lane.pbe.classify_and_flag

    def leaky(df):
        # Simulate a classifier regression: an individual marked displayable.
        return real(df).with_columns(pl.lit(True).alias("public_display"))

    monkeypatch.setattr(hse_lane.pbe, "classify_and_flag", leaky)
    with pytest.raises(SystemExit, match="privacy quarantine breached"):
        hse_lane.build()
    assert not out.exists(), "a leak must leave silver unwritten"


# ---- 3. new-bodies lane -----------------------------------------------------

REGISTRY_COLS = [
    "entity_clean",
    "publisher_id",
    "publisher_name",
    "publisher_type",
    "sector",
    "amount_semantics",
    "source_landing_url",
]


def _write_registry(path: Path, bodies: list[tuple[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "entity_clean": entity,
            "publisher_id": pid,
            "publisher_name": entity,
            "publisher_type": "local_authority",
            "sector": "local_government",
            "amount_semantics": "po_committed",
            "source_landing_url": "https://example.gov.ie/",
        }
        for entity, pid in bodies
    ]
    pl.DataFrame([{c: r[c] for c in REGISTRY_COLS} for r in rows]).write_csv(path)
    return path


@pytest.fixture
def newbodies_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DAIL_SKIP_ROW_FLOOR", "1")
    raw_dir = tmp_path / "raw_bq"
    silver = tmp_path / "silver"
    gold = tmp_path / "gold" / "procurement_payments_fact.parquet"
    registry = tmp_path / "meta" / "procurement_disclosed_bodies.csv"
    out = silver / "disclosed_bq_po_newbodies_fact.parquet"
    coverage = tmp_path / "meta" / "disclosed_bq_po_newbodies_coverage.json"
    silver.mkdir(parents=True)
    coverage.parent.mkdir(parents=True)
    monkeypatch.setattr(new_lane, "RAW_DIR", raw_dir)
    monkeypatch.setattr(new_lane, "SILVER", silver)
    monkeypatch.setattr(new_lane, "GOLD", gold)
    monkeypatch.setattr(new_lane, "REGISTRY", registry)
    monkeypatch.setattr(new_lane, "OUT", out)
    monkeypatch.setattr(new_lane, "COVERAGE", coverage)
    monkeypatch.setattr(new_lane, "ROOT", tmp_path)
    return raw_dir, silver, gold, registry, out


def test_newbodies_registry_is_the_allow_list(newbodies_env):
    """A body in the raw extract but NOT in the registry is silently excluded — by design,
    only bodies with a confirmed identity + regime ship."""
    raw_dir, _, _, registry, out = newbodies_env
    _write_registry(registry, [("Louth County Council", "ie_la_louth")])
    _write_raw(
        raw_dir,
        [
            _raw_row("Louth County Council", "q1", 2020),
            _raw_row("Mystery Unregistered Body", "q1", 2020),
        ],
    )
    assert new_lane.build() == 0
    lane = pl.read_parquet(out)
    assert lane["publisher_id"].unique().to_list() == ["ie_la_louth"]
    assert lane.height == 1


def test_newbodies_entity_prefixes_are_stripped_before_the_registry_join(newbodies_env):
    raw_dir, _, _, registry, out = newbodies_env
    _write_registry(registry, [("An Garda Síochána", "ie_garda")])
    # The raw feed carries "Agency :  <name>" (double space confirmed in the real CSV).
    _write_raw(raw_dir, [_raw_row("Agency :  An Garda Síochána", "q1", 2020)])
    new_lane.build()
    lane = pl.read_parquet(out)
    assert lane["publisher_id"].to_list() == ["ie_garda"]


def test_newbodies_warns_for_registry_body_absent_from_raw(newbodies_env, capsys):
    raw_dir, _, _, registry, _ = newbodies_env
    _write_registry(
        registry,
        [("Louth County Council", "ie_la_louth"), ("Ghost Body", "ie_ghost")],
    )
    _write_raw(raw_dir, [_raw_row("Louth County Council", "q1", 2020)])
    new_lane.build()
    assert "WARN registry body absent from raw extract: 'Ghost Body'" in capsys.readouterr().out


def test_newbodies_output_column_order_is_exact(newbodies_env):
    """_load_facts concats with NO reorder — the order must match the base lanes byte-for-byte."""
    raw_dir, _, _, registry, out = newbodies_env
    _write_registry(registry, [("Louth County Council", "ie_la_louth")])
    _write_raw(raw_dir, [_raw_row("Louth County Council", "q1", 2020)])
    new_lane.build()
    assert pl.read_parquet(out).columns == new_lane.SCHEMA_COLS


def test_newbodies_missing_registry_is_a_noop(newbodies_env):
    raw_dir, _, _, _, out = newbodies_env
    _write_raw(raw_dir, [_raw_row("Louth County Council", "q1", 2020)])
    assert new_lane.build() == 0
    assert not out.exists()


# ---- 4. the cross-lane double-count guard -----------------------------------


def test_disjoint_guard_trips_on_another_silver_lane(newbodies_env):
    _, silver, _, _, _ = newbodies_env
    pl.DataFrame({"publisher_id": ["ie_la_louth"]}).write_parquet(silver / "la_payments_fact.parquet")
    with pytest.raises(SystemExit, match="DOUBLE-COUNT GUARD TRIPPED"):
        new_lane._assert_disjoint({"ie_la_louth"})


def test_disjoint_guard_trips_on_gold_rows_from_a_foreign_lane(newbodies_env):
    _, _, gold, _, _ = newbodies_env
    _write_gold(gold, [{"publisher_id": "ie_la_louth", "parser_name": "la_payments"}])
    with pytest.raises(SystemExit, match="DOUBLE-COUNT GUARD TRIPPED"):
        new_lane._assert_disjoint({"ie_la_louth"})


def test_disjoint_guard_ignores_golds_own_prior_rows(newbodies_env):
    """Gold holds this lane's own rows from the prior build — tripping on them would make
    every re-run after the first fail. Only FOREIGN parser rows count."""
    _, _, gold, _, _ = newbodies_env
    _write_gold(gold, [{"publisher_id": "ie_la_louth", "parser_name": new_lane.PARSER_NAME}])
    new_lane._assert_disjoint({"ie_la_louth"})  # must not raise


def test_disjoint_guard_passes_on_genuinely_new_ids(newbodies_env):
    _, silver, gold, _, _ = newbodies_env
    pl.DataFrame({"publisher_id": ["ie_other"]}).write_parquet(silver / "la_payments_fact.parquet")
    _write_gold(gold, [{"publisher_id": "ie_else", "parser_name": "la_payments"}])
    new_lane._assert_disjoint({"ie_la_louth"})  # must not raise


def test_newbodies_build_halts_when_guard_trips(newbodies_env):
    """The guard is wired into build(), not just importable — a conflicting lane halts the write."""
    raw_dir, silver, _, registry, out = newbodies_env
    _write_registry(registry, [("Louth County Council", "ie_la_louth")])
    _write_raw(raw_dir, [_raw_row("Louth County Council", "q1", 2020)])
    pl.DataFrame({"publisher_id": ["ie_la_louth"]}).write_parquet(silver / "la_payments_fact.parquet")
    with pytest.raises(SystemExit, match="DOUBLE-COUNT GUARD TRIPPED"):
        new_lane.build()
    assert not out.exists()
