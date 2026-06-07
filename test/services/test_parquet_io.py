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
from services.parquet_io import save_parquet


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
