"""cadastre_parcels_fetch: per-county parts, resume, and a one-county run that cannot clobber.

Network is stubbed. What is pinned is the write discipline that the 2026-08-09 national run
lacked: a county that is fetched stays fetched; a run can resume; a short total never replaces
the national file; and `--county X` never touches it at all.
"""

from __future__ import annotations

import pyarrow.parquet as pq
import pytest
import shapely
from shapely.geometry import box

from extractors import cadastre_parcels_fetch as mod
from services.geoparquet_io import validate_geoparquet


def _rows(n: int, x0: float):
    return [
        (shapely.to_wkb(box(x0 + i * 0.001, 53.0, x0 + i * 0.001 + 0.0005, 53.0005)), f"SP-{x0}-{i}", "X", 1.0)
        for i in range(n)
    ]


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    out = tmp_path / "cadastre"
    monkeypatch.setattr(mod, "OUT_DIR", out)
    monkeypatch.setattr(mod, "DEST", out / "parcels_freehold.parquet")
    monkeypatch.setattr(mod, "PARTS_DIR", out / "parts")
    monkeypatch.setattr(mod, "ROW_FLOOR", 9)
    calls: list[str | None] = []

    def fake_fetch(county):
        calls.append(county)
        return {"Galway": _rows(4, -9.0), "Mayo": _rows(3, -9.5), "Sligo": _rows(2, -8.5), None: _rows(1, -7.0)}[county]

    monkeypatch.setattr(mod, "_fetch_county", fake_fetch)
    monkeypatch.setattr(mod, "_counties", lambda: ["Galway", "Mayo", "Sligo", None])
    return out, calls


def test_one_county_run_writes_a_part_and_never_the_national_file(sandbox):
    out, calls = sandbox
    part = mod.build(only="Galway")
    assert part == out / "parts" / "galway.parquet" and part.is_file()
    assert not (out / "parcels_freehold.parquet").exists()
    assert pq.read_table(part).num_rows == 4
    summary = validate_geoparquet(part, deep=True)
    assert summary.row_count == 4
    assert summary.geometry_types == ("Polygon",)
    assert calls == ["Galway"]


def test_full_build_is_resumable_and_assembles_once_every_part_exists(sandbox):
    out, calls = sandbox
    mod.build(only="Galway")
    dest = mod.build()
    assert dest == out / "parcels_freehold.parquet" and dest.is_file()
    assert pq.read_table(dest).num_rows == 10
    summary = validate_geoparquet(dest, deep=True)
    assert summary.row_count == 10
    assert summary.geometry_types == ("Polygon",)
    # Galway was not fetched twice: its part was kept.
    assert calls.count("Galway") == 1
    assert (out / "parts" / "_null.parquet").is_file()
    # A second full run fetches nothing and re-assembles from parts.
    before = len(calls)
    mod.build()
    assert len(calls) == before


def test_assemble_refuses_on_missing_parts_and_on_a_short_total(sandbox, monkeypatch):
    out, _ = sandbox
    mod.build(only="Galway")
    with pytest.raises(SystemExit, match="missing"):
        mod.build(assemble_only=True)
    assert not (out / "parcels_freehold.parquet").exists()
    monkeypatch.setattr(mod, "ROW_FLOOR", 50)
    with pytest.raises(SystemExit, match="row floor"):
        mod.build()
    assert not (out / "parcels_freehold.parquet").exists()
    assert not (out / "parcels_freehold.parquet.part").exists()


def test_stale_partial_write_from_an_earlier_run_is_removed(sandbox):
    out, _ = sandbox
    out.mkdir(parents=True)
    stale = out / "parcels_freehold.parquet.part"
    stale.write_bytes(b"\x00" * 10)
    mod.build(only="Mayo")
    assert not stale.exists()


def test_refresh_refetches_an_existing_part(sandbox):
    _, calls = sandbox
    mod.build(only="Sligo")
    mod.build(only="Sligo")
    assert calls.count("Sligo") == 1
    mod.build(only="Sligo", refresh=True)
    assert calls.count("Sligo") == 2
