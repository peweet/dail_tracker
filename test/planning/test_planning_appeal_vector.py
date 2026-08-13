from __future__ import annotations

import datetime as dt

import polars as pl

from planning.civic.extractors import planning_appeal_outcomes as extractor
from planning.civic.extractors.planning_appeal_outcomes import _auth_key, _case_status, _council_decision, _ms_to_date
from planning.civic.extractors.planning_appeal_vector import (
    appeal_case_expr,
    authority_key_expr,
    case_status_expr,
    council_decision_expr,
    epoch_ms_date_expr,
)


def test_native_scalar_expressions_preserve_existing_contracts():
    rows = pl.DataFrame(
        {
            "authority": ["Cork County Council - West Cork Section", "Dún Laoghaire Council", None, "Test"],
            "decision": ["Granted-Conditional", "Refused", "Other", "Zed"],
            "appeal": ["ABP-312345-22", "no case", None, "123456"],
            "ms": [0, 1_709_164_800_000, None, 86_400_000],
        }
    )
    out = rows.select(
        authority_key_expr("authority").alias("authority"),
        council_decision_expr("decision").alias("decision"),
        appeal_case_expr("appeal").alias("appeal"),
        epoch_ms_date_expr("ms").alias("date"),
    )
    assert out["authority"].to_list() == [_auth_key(v) for v in rows["authority"]]
    assert out["decision"].to_list() == [_council_decision(v) for v in rows["decision"]]
    assert out["appeal"].to_list() == ["312345", None, None, "123456"]
    assert out["date"].to_list() == [_ms_to_date(v) for v in rows["ms"]]


def test_case_status_expression_preserves_stale_target_semantics():
    frame = pl.DataFrame(
        {
            "decision": ["Case is due to be decided by 01/01/2020", "Permission granted", None],
            "decided": [1_577_836_800_000, 1_577_836_800_000, None],
        }
    )
    got = frame.select(case_status_expr("decision", "decided").alias("status"))["status"].to_list()
    expected = [_case_status(row["decision"], row["decided"]) for row in frame.iter_rows(named=True)]
    assert got == expected == ["live", "decided", "live"]


def test_epoch_conversion_is_utc_and_null_preserving():
    frame = pl.DataFrame({"ms": [-1, 0, 86_400_000, None]}, schema={"ms": pl.Int64})
    assert frame.select(epoch_ms_date_expr("ms"))["ms"].to_list() == [
        dt.date(1969, 12, 31),
        dt.date(1970, 1, 1),
        dt.date(1970, 1, 2),
        None,
    ]


def test_applications_read_projects_exact_match_contract(monkeypatch, tmp_path):
    seen = {}

    def read(path, *, columns):
        seen.update(path=path, columns=columns)
        return pl.DataFrame({name: [] for name in columns})

    monkeypatch.setattr(extractor.pl, "read_parquet", read)
    result = extractor._load_applications(tmp_path / "apps.parquet")
    assert result.columns == list(extractor._APPLICATION_COLUMNS)
    assert seen == {
        "path": tmp_path / "apps.parquet",
        "columns": list(extractor._APPLICATION_COLUMNS),
    }
