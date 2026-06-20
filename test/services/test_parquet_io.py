"""Tests for services/parquet_io.save_parquet — the atomic shared writer.

Proves the two guarantees the helper exists for:
  - A successful write produces a readable parquet with the zstd convention and
    leaves no '.part' temp behind.
  - A FAILED write leaves the previous good file untouched and cleans up the
    temp — i.e. a crash mid-write can never corrupt canonical gold/silver.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.parquet_io import RowFloorViolation, save_parquet


def test_polars_roundtrip_and_no_temp(tmp_path):
    dest = tmp_path / "out.parquet"
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    returned = save_parquet(df, dest)

    assert returned == dest
    assert dest.exists()
    assert not (tmp_path / "out.parquet.part").exists()
    assert pl.read_parquet(dest).equals(df)


def test_pandas_roundtrip_and_no_temp(tmp_path):
    dest = tmp_path / "out_pd.parquet"
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    save_parquet(df, dest)

    assert dest.exists()
    assert not (tmp_path / "out_pd.parquet.part").exists()
    pd.testing.assert_frame_equal(pd.read_parquet(dest), df)


def test_empty_polars_frame_is_writable(tmp_path):
    """A truly schemaless empty frame must still produce a scannable file."""
    dest = tmp_path / "empty.parquet"
    save_parquet(pl.DataFrame(), dest)
    assert dest.exists()
    assert pl.read_parquet(dest).height == 0


def test_failed_write_preserves_previous_file(tmp_path, monkeypatch):
    """The core atomicity guarantee: if the write raises mid-way, the previous
    good file is intact and no '.part' temp is left behind.
    """
    dest = tmp_path / "gold.parquet"
    good = pl.DataFrame({"a": [1, 2, 3]})
    save_parquet(good, dest)  # establish a previous good version
    assert pl.read_parquet(dest).equals(good)

    # Force the next write to blow up partway through writing the temp.
    real_replace = Path.replace

    def boom(self, target):  # noqa: ARG001
        raise OSError("simulated disk failure mid-write")

    monkeypatch.setattr(Path, "replace", boom)

    with pytest.raises(OSError, match="simulated disk failure"):
        save_parquet(pl.DataFrame({"a": [99]}), dest)

    monkeypatch.setattr(Path, "replace", real_replace)

    # Previous good file is untouched; no temp litter remains.
    assert dest.exists()
    assert pl.read_parquet(dest).equals(good), "previous good gold was corrupted!"
    assert not (tmp_path / "gold.parquet.part").exists()


# ---------------------------------------------------------------- min_rows floor
def test_min_rows_floor_blocks_truncated_overwrite(tmp_path):
    """A below-floor frame must NOT replace the previous good fact: this is the
    truncated-/wiped-harvest guard (a partial `--only` slice overwriting ~85k rows).
    """
    dest = tmp_path / "fact.parquet"
    good = pl.DataFrame({"a": list(range(100))})
    save_parquet(good, dest)  # full fact in place

    tiny = pl.DataFrame({"a": [1, 2, 3]})  # botched harvest
    with pytest.raises(RowFloorViolation, match="refusing to overwrite"):
        save_parquet(tiny, dest, min_rows=50)

    # Previous good fact survives intact; no temp litter.
    assert pl.read_parquet(dest).equals(good), "good fact was clobbered by a tiny harvest!"
    assert not (tmp_path / "fact.parquet.part").exists()


def test_min_rows_floor_refuses_first_write(tmp_path):
    """The floor also blocks a below-floor *first* write — no dest is created, so
    a downstream consolidate cannot silently build on a tiny fact."""
    dest = tmp_path / "fresh.parquet"
    with pytest.raises(RowFloorViolation):
        save_parquet(pl.DataFrame({"a": [1, 2, 3]}), dest, min_rows=50)
    assert not dest.exists()


def test_min_rows_floor_passes_at_or_above_floor(tmp_path):
    """A frame meeting the floor writes normally."""
    dest = tmp_path / "ok.parquet"
    df = pl.DataFrame({"a": list(range(50))})
    save_parquet(df, dest, min_rows=50)  # exactly at floor
    assert pl.read_parquet(dest).height == 50


def test_min_rows_floor_bypass_env(tmp_path, monkeypatch):
    """DAIL_SKIP_ROW_FLOOR=1 forces a deliberate below-floor write (bootstrap /
    scoped rebuild)."""
    monkeypatch.setenv("DAIL_SKIP_ROW_FLOOR", "1")
    dest = tmp_path / "forced.parquet"
    save_parquet(pl.DataFrame({"a": [1, 2, 3]}), dest, min_rows=50)
    assert pl.read_parquet(dest).height == 3


def test_min_rows_none_is_unguarded_default(tmp_path):
    """Omitting min_rows keeps the pre-existing behavior: any frame writes."""
    dest = tmp_path / "small.parquet"
    save_parquet(pl.DataFrame({"a": [1]}), dest)
    assert pl.read_parquet(dest).height == 1
