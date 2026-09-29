"""Contract for merging council-published rows into the national planning register."""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from planning.civic import register_supplement as rs  # noqa: E402


def _national(rows, *, source=None):
    df = pl.DataFrame(rows, schema={"PlanningAuthority": pl.String, "ApplicationNumber": pl.String}, orient="row")
    return df if source is None else df.with_columns(pl.Series(rs.SOURCE_COL, source))


def _supplement(rows):
    return pl.DataFrame(
        rows, schema={"PlanningAuthority": pl.String, "ApplicationNumber": pl.String, "lon": pl.Float64}, orient="row"
    )


CORK, DUBLIN = "Cork City Council", "Dublin City Council"


def _merge(register, supplement):
    merged, stats = rs.merged_register(register.lazy(), supplement)
    return merged.collect(), stats


def test_national_row_wins_across_reference_spellings():
    # Cork City's Agile '25/44149' is the national feed's '2544149' — the national row is kept.
    merged, stats = _merge(
        _national([(CORK, "2544149")]), _supplement([(CORK, "25/44149", -8.4), (CORK, "26/45001", -8.5)])
    )
    assert merged.height == 2
    assert merged.filter(pl.col(rs.SOURCE_COL) == rs.COUNCIL)["ApplicationNumber"].to_list() == ["26/45001"]
    assert stats["authorities"][CORK] == {"supplement_rows": 2, "already_national": 1, "added": 1}
    assert stats["national_rows"] == 1


def test_every_row_is_labelled_with_its_source():
    merged, _ = _merge(_national([(DUBLIN, "WEB1/26")]), _supplement([(DUBLIN, "WEB2/26", -6.2)]))
    assert dict(zip(merged["ApplicationNumber"], merged[rs.SOURCE_COL], strict=True)) == {
        "WEB1/26": rs.NATIONAL,
        "WEB2/26": rs.COUNCIL,
    }


def test_merge_is_idempotent_on_a_register_that_already_holds_council_rows():
    supplement = _supplement([(DUBLIN, "WEB2/26", -6.2)])
    once, _ = _merge(_national([(DUBLIN, "WEB1/26")]), supplement)
    twice, _ = _merge(once, supplement)
    assert twice.sort("ApplicationNumber").equals(once.sort("ApplicationNumber"))


def test_null_source_is_national_not_dropped():
    register = _national([(DUBLIN, "WEB1/26"), (DUBLIN, "WEB3/26")], source=[None, rs.NATIONAL])
    merged, stats = _merge(register, _supplement([(DUBLIN, "WEB2/26", -6.2)]))
    assert merged.height == 3
    assert stats["national_rows"] == 2
    assert merged[rs.SOURCE_COL].null_count() == 0


def test_duplicate_supplement_keys_refuse_the_merge():
    with pytest.raises(rs.SupplementError, match="duplicate"):
        _merge(_national([(CORK, "1")]), _supplement([(CORK, "26/1", -8.4), (CORK, "261", -8.4)]))


def test_merge_into_refuses_to_overwrite_the_register_it_reads(tmp_path):
    reg = tmp_path / "reg.parquet"
    _national([(DUBLIN, "WEB1/26")]).write_parquet(reg)
    with pytest.raises(rs.SupplementError, match="dest"):
        rs.merge_into(reg, tmp_path / "supp.parquet", reg)


def test_merge_into_without_a_supplement_writes_nothing(tmp_path):
    reg = tmp_path / "reg.parquet"
    _national([(DUBLIN, "WEB1/26")]).write_parquet(reg)
    assert rs.merge_into(reg, tmp_path / "absent.parquet", tmp_path / "out.parquet") is None
    assert not (tmp_path / "out.parquet").exists()


def test_merge_into_writes_the_merged_register(tmp_path):
    reg, supp, out = tmp_path / "reg.parquet", tmp_path / "supp.parquet", tmp_path / "out.parquet"
    _national([(DUBLIN, "WEB1/26")]).write_parquet(reg)
    _supplement([(DUBLIN, "WEB2/26", -6.2)]).write_parquet(supp)
    stats = rs.merge_into(reg, supp, out)
    assert stats["authorities"][DUBLIN]["added"] == 1
    assert pl.read_parquet(out).height == 2
