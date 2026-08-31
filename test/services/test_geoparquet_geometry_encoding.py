"""The GeoParquet writer must never dictionary-encode the geometry column.

Regression for 2026-08-29. ``convert_parquet_file`` left pyarrow's ``use_dictionary`` at its
default, which is ``True`` for *every* column, so each GeoParquet this repo published carried a
dictionary-attempted ``wkb`` column (encodings PLAIN + RLE + RLE_DICTIONARY). WKB values are
unique and large: the dictionary page fills, the writer falls back to PLAIN mid-row-group, and a
reader then has to decode that group's dictionary pages whole before it can skip a single row.

The same defect was found and fixed in the private siting tree on 2026-08-28, where it was
measured on real layers with identical rows and row groups: a 2 km window on ``osm_roads`` cost
8.0 ms against a PLAIN sibling, 18.0 ms against the default writer's output, and 13.9 ms once
rewritten without a dictionary; ``la_boundaries`` 5.6 / 12.8 / 5.5 ms; and on a one-row-group
file the loss of page skipping turned a 7 ms read into 228 ms. The public writer never received
that fix, and four live extractors publish through it (``cadastre_parcels_fetch``,
``epa_licensed_facilities_extract``, ``hsa_comah_extract``).

Two layers of check here:

* file-level — what the writer actually emits, read back from the Parquet footer;
* property-level — CrossHair against :func:`dictionary_columns`, which is the allow-list the
  writer is driven by. The file tests pin the cases we thought of; CrossHair searches for the
  input we did not, specifically an input where a non-geometry column gets dropped.

Fast tier: no engine, no layer store, a few small Parquet files in a temp dir.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
import pytest
import shapely

from services.geoparquet_io import (
    GEOMETRY_ENCODING_POLICY,
    GeoParquetSafetyError,
    convert_parquet_file,
    dictionary_columns,
    geometry_encodings,
    require_plain_geometry,
    validate_geoparquet,
)
from services.parquet_io import save_parquet

DICTIONARY = "RLE_DICTIONARY"


def _source(tmp_path: Path, rows: int = 400) -> Path:
    """Distinct polygons so a dictionary cannot help, plus a low-cardinality attribute so we
    can tell "dictionaries are off for geometry" from "dictionaries are off entirely"."""
    rng = np.random.default_rng(2026)
    xs = rng.uniform(-10.0, -6.0, rows)
    ys = rng.uniform(51.5, 55.0, rows)
    geoms = [
        shapely.Point(x, y).buffer(0.01 + 0.001 * (i % 7), 16) for i, (x, y) in enumerate(zip(xs, ys, strict=True))
    ]
    source = tmp_path / "source.parquet"
    pl.DataFrame(
        {
            "county": [("Cork", "Galway", "Mayo")[i % 3] for i in range(rows)],
            "wkb": [shapely.to_wkb(g) for g in geoms],
        }
    ).write_parquet(source)
    return source


# ── What the writer emits ────────────────────────────────────────────────────


def test_geometry_column_is_written_plain_and_attributes_keep_the_dictionary(tmp_path):
    source = _source(tmp_path)
    output = tmp_path / "out.parquet"
    convert_parquet_file(source, output, source_crs="OGC:CRS84", geometry_column="wkb")

    assert DICTIONARY not in geometry_encodings(output, "wkb")
    # The fix is scoped to geometry: a repeated short string still earns its dictionary.
    assert DICTIONARY in _encodings_of(output, "county")


def test_pyarrow_default_would_dictionary_encode_the_geometry(tmp_path):
    """Prove the gate can fail.

    The same rows through pyarrow's default writer settings carry exactly the encoding the fix
    removes. If pyarrow ever changes that default, this test fails and tells the next reader
    that the regression can no longer recur by this route — rather than leaving a green suite
    that silently checks nothing.
    """
    source = _source(tmp_path)
    default_out = tmp_path / "default.parquet"
    pq.write_table(pq.read_table(source), default_out, compression="zstd", compression_level=3)

    assert DICTIONARY in _encodings_of(default_out, "wkb")


def test_geometry_encoding_map_is_exact(tmp_path):
    """Pin every column's encoding, not just geometry's.

    ``use_dictionary`` takes top-level field names while pyarrow keys its decision on leaf
    paths, so the bbox struct's leaves match nothing in the allow-list and are written PLAIN.
    That is what we want for near-unique float32 coverings, but it would also quietly disable
    dictionaries on a future low-cardinality struct attribute — so the whole map is pinned here
    rather than left as an undocumented side effect.
    """
    source = _source(tmp_path)
    output = tmp_path / "out.parquet"
    convert_parquet_file(source, output, source_crs="OGC:CRS84", geometry_column="wkb")

    dictionaried = {name for name in _column_paths(output) if DICTIONARY in _encodings_of(output, name)}
    assert dictionaried == {"county"}


def test_save_parquet_publishes_plain_geometry_end_to_end(tmp_path):
    """The real public entry point, not just the converter underneath it."""
    dest = tmp_path / "layer.parquet"
    rng = np.random.default_rng(7)
    frame = pl.DataFrame(
        {
            "county": [("Cork", "Galway")[i % 2] for i in range(300)],
            "wkb": [
                shapely.to_wkb(shapely.Point(float(x), float(y)).buffer(0.01, 16))
                for x, y in zip(rng.uniform(-10, -6, 300), rng.uniform(51.5, 55, 300), strict=True)
            ],
        }
    )
    save_parquet(frame, dest, geoparquet=True, source_crs="OGC:CRS84")

    assert DICTIONARY not in geometry_encodings(dest, "wkb")


# ── The published-artifact gate ──────────────────────────────────────────────


def test_require_plain_geometry_rejects_a_dictionary_encoded_file(tmp_path):
    source = _source(tmp_path)
    offender = tmp_path / "offender.parquet"
    pq.write_table(pq.read_table(source), offender, compression="zstd")

    with pytest.raises(GeoParquetSafetyError, match=GEOMETRY_ENCODING_POLICY):
        require_plain_geometry(offender, geometry_column="wkb")


def test_require_plain_geometry_accepts_the_writers_own_output(tmp_path):
    source = _source(tmp_path)
    output = tmp_path / "out.parquet"
    convert_parquet_file(source, output, source_crs="OGC:CRS84", geometry_column="wkb")

    require_plain_geometry(output, geometry_column="wkb")  # must not raise


def test_validate_geoparquet_gates_an_externally_supplied_dictionary_file(tmp_path):
    """Covers the branch that skips conversion.

    ``save_parquet_stream`` only converts a part that is not already GeoParquet; an input that
    arrives with valid ``geo`` metadata goes straight to validation. Without the gate living in
    ``validate_geoparquet`` too, such a file would publish unexamined.
    """
    source = _source(tmp_path)
    converted = tmp_path / "converted.parquet"
    convert_parquet_file(source, converted, source_crs="OGC:CRS84", geometry_column="wkb")

    # Rewrite the valid GeoParquet through pyarrow's defaults: metadata intact, encoding wrong.
    table = pq.read_table(converted)
    reencoded = tmp_path / "reencoded.parquet"
    pq.write_table(table, reencoded, compression="zstd")

    assert DICTIONARY in geometry_encodings(reencoded, "wkb")
    with pytest.raises(GeoParquetSafetyError, match=GEOMETRY_ENCODING_POLICY):
        validate_geoparquet(reencoded, geometry_column="wkb", deep=True)


def test_geometry_encodings_reads_every_row_group(tmp_path):
    """A single offending row group is enough; the check must not stop at the first."""
    source = _source(tmp_path, rows=600)
    output = tmp_path / "multi.parquet"
    convert_parquet_file(source, output, source_crs="OGC:CRS84", geometry_column="wkb", batch_rows=100)

    assert pq.ParquetFile(output).metadata.num_row_groups > 1
    assert DICTIONARY not in geometry_encodings(output, "wkb")


# ── The allow-list property, searched symbolically ───────────────────────────


def _lossy_dictionary_columns(names: list[str], geometry_column: str) -> list[str]:
    """A deliberately wrong allow-list, used only to prove the CrossHair check can fail.

    It drops every column after the first, which is exactly the "reduction" shape the real
    postcondition forbids. If CrossHair cannot refute this, it is not searching, and the
    passing result on the real function means nothing.

    pre: len(names) <= 4
    post: all((name in __return__) == (name != geometry_column) for name in names)
    """
    return [name for name in names if name != geometry_column][:1]


def _crosshair_messages(func, *, iterations: int = 30, timeout: float = 30.0):
    from crosshair.core_and_libs import analyze_function, run_checkables
    from crosshair.options import AnalysisOptionSet

    options = AnalysisOptionSet(max_uninteresting_iterations=iterations, per_condition_timeout=timeout)
    return run_checkables(analyze_function(func, options))


def _counterexamples(messages) -> list[str]:
    """Messages that are an actual refutation, not an exhausted search budget.

    CrossHair reports CANNOT_CONFIRM when it ran out of budget without finding anything. That
    is a weaker result than a proof and is treated as a pass here; a POST_FAIL or EXEC_ERR is a
    real counterexample and is not.
    """
    return [f"{m.state}: {m.message}" for m in messages if "CANNOT_CONFIRM" not in str(m.state)]


@pytest.mark.crosshair
def test_crosshair_finds_no_input_where_a_column_is_dropped():
    """The no-reduction property: membership in the allow-list is exactly "not the geometry
    column", so no attribute column can be silently excluded from dictionary encoding."""
    assert _counterexamples(_crosshair_messages(dictionary_columns)) == []


@pytest.mark.crosshair
def test_crosshair_refutes_a_lossy_allow_list():
    """Prove the symbolic check can fail — the companion to the test above."""
    assert _counterexamples(_crosshair_messages(_lossy_dictionary_columns)) != []


# ── Helpers ──────────────────────────────────────────────────────────────────


def _column_paths(path: Path) -> list[str]:
    first = pq.ParquetFile(path).metadata.row_group(0)
    return [first.column(i).path_in_schema for i in range(first.num_columns)]


def _encodings_of(path: Path, column: str) -> set[str]:
    """Footer encodings for any column, including a struct leaf.

    :func:`geometry_encodings` deliberately refuses a missing column and is the production
    entry point; this is the test-side equivalent that can also reach ``bbox.xmin``.
    """
    metadata = pq.ParquetFile(path).metadata
    index = _column_paths(path).index(column)
    found: set[str] = set()
    for group in range(metadata.num_row_groups):
        found.update(str(encoding) for encoding in metadata.row_group(group).column(index).encodings)
    return found
