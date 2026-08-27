"""Atomic parquet writes — the single shared writer for all pipeline ETL.

Every gold/silver parquet writer should route through ``save_parquet`` instead of
calling ``df.write_parquet(final_path)`` / ``df.to_parquet(final_path)`` directly,
for two reasons:

1. **Atomicity.** A direct write to the final path truncates the previous good
   file the instant the writer opens it; a crash, kill, or bad frame mid-write
   then leaves a corrupt parquet at the canonical name that the next read silently
   mis-parses (or that a human has to hand-restore from a ``.bak`` — which is
   exactly why ``statutory_instruments.parquet.bak`` existed). We write to a
   sibling ``<dest>.part`` then ``os.replace()`` it onto ``dest``. ``replace()``
   is atomic within a filesystem, so any reader ever sees either the complete
   previous file or the complete new one — never a half-written one. On failure
   the previous ``dest`` is left untouched and the partial temp is removed.

2. **One place for the compression convention** (feedback_parquet_write_convention):
   zstd / level 3 / statistics / row-group sizing on every writer, applied here
   instead of being re-typed (and occasionally forgotten) at 70+ call sites.

Auto-detects Polars vs pandas so callers can't pick the wrong writer. Pass kwargs
to override any default (e.g. ``save_parquet(df, p, compression_level=9)``).

3. **Optional row-count floor** (``min_rows=``). The self-fetching scraper ETLs
   (procurement public-body / local-authority / consolidated gold fact) overwrite
   a large canonical fact every run. A botched harvest — a plain ``--only`` that
   forgets ``--merge`` (wipes the fact to one publisher), a source that quietly
   started serving a bot-challenge, a parser that dropped every row — produces a
   *valid but tiny* frame that would otherwise atomically replace ~85k good rows.
   Passing ``min_rows`` refuses the write below that floor (``RowFloorViolation``,
   dest untouched), generalizing cro_poller's ``MIN_ROWS`` guard from bronze to the
   silver/gold facts. Off by default; opt-in per call site. Set env
   ``DAIL_SKIP_ROW_FLOOR=1`` to bypass for a deliberate small/bootstrap write.
"""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Project parquet convention, split by engine because the kwarg names differ.
# row_group_size caps rows per group so large facts land as several groups instead
# of one: a single row group forces single-threaded scans and makes the per-group
# min/max statistics unskippable (there is only one range). Both engines accept the
# kwarg (pandas forwards it to pyarrow). Frames under the cap yield one group as
# before, so small files are unchanged. 128k rows ≈ a few groups on the large facts
# (speeches ~575k → 5, questions ~277k → 3), enough to feed the cores without the
# metadata/compression cost of many tiny groups. Measured 2.2–2.9× on the real
# member-speech-summary aggregation via duckdb; file size unchanged.
_ROW_GROUP_SIZE = 128_000
_POLARS_DEFAULTS = {
    "compression": "zstd",
    "compression_level": 3,
    "statistics": True,
    "row_group_size": _ROW_GROUP_SIZE,
}
_PANDAS_DEFAULTS = {"index": False, "compression": "zstd", "compression_level": 3, "row_group_size": _ROW_GROUP_SIZE}

# Escape hatch for the min_rows floor: a genuine bootstrap / intentionally scoped
# small write sets DAIL_SKIP_ROW_FLOOR=1, mirroring cro_poller's --force.
_FLOOR_BYPASS_ENV = "DAIL_SKIP_ROW_FLOOR"
_THREAD_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


class RowFloorViolation(ValueError):
    """A frame fell below its declared ``min_rows`` floor.

    Raised *before* any write, so the previous good ``dest`` is left untouched —
    the same contract cro_poller's ``SourceDrift`` gives bronze: a truncated /
    wiped harvest never clobbers the healthy file already on disk.
    """


def _is_polars(df) -> bool:
    """True for a Polars eager or lazy frame without importing at module load."""
    return type(df).__module__.split(".")[0] == "polars"


def _is_polars_lazy(df) -> bool:
    return _is_polars(df) and type(df).__name__ == "LazyFrame"


def _row_count(df) -> int:
    """Row count for a Polars or pandas frame (no polars import at module load)."""
    if _is_polars_lazy(df):
        import polars as pl

        return int(df.select(pl.len()).collect().item())
    return int(df.height) if _is_polars(df) else int(len(df))


def save_parquet(
    df,
    dest,
    *,
    min_rows: int | None = None,
    geoparquet: bool = False,
    geometry_column: str = "wkb",
    source_crs: str | None = None,
    **kwargs,
) -> Path:
    """Atomically write ``df`` (Polars or pandas) to ``dest`` with zstd defaults.

    Writes to ``<dest>.part`` then ``os.replace()``s it onto ``dest``. If the
    write raises, ``dest`` is left as it was and the partial temp is cleaned up.
    Returns the final path. Override any default via kwargs.

    ``min_rows`` (opt-in): refuse to write — leaving the previous ``dest``
    untouched — when ``df`` has fewer than ``min_rows`` rows, guarding a canonical
    fact against a truncated/wiped harvest. Bypass with env ``DAIL_SKIP_ROW_FLOOR=1``.
    """
    dest = Path(dest)
    if min_rows is not None:
        n = _row_count(df)
        if n < min_rows:
            if os.environ.get(_FLOOR_BYPASS_ENV) == "1":
                logger.warning(
                    "row floor bypassed (%s=1): %s has %d rows < floor %d", _FLOOR_BYPASS_ENV, dest.name, n, min_rows
                )
            else:
                raise RowFloorViolation(
                    f"{dest.name}: {n} rows < floor {min_rows}; refusing to overwrite "
                    f"(set {_FLOOR_BYPASS_ENV}=1 to force)"
                )

    def write_part(tmp: Path) -> None:
        if _is_polars(df):
            import polars as pl

            opts = {**_POLARS_DEFAULTS, **kwargs}
            if _is_polars_lazy(df):
                if len(df.collect_schema()) == 0:
                    pl.DataFrame({"_empty": pl.Series([], dtype=pl.Int64)}).write_parquet(tmp, **opts)
                else:
                    df.sink_parquet(tmp, **opts)
            elif df.is_empty() and len(df.columns) == 0:
                # Polars cannot round-trip a truly schemaless empty frame; write a
                # zero-row sentinel column so scan_parquet still works downstream
                # (consumers filter on row count, not on this column).
                pl.DataFrame({"_empty": pl.Series([], dtype=pl.Int64)}).write_parquet(tmp, **opts)
            else:
                df.write_parquet(tmp, **opts)
        else:
            opts = {**_PANDAS_DEFAULTS, **kwargs}
            df.to_parquet(tmp, **opts)

    save_parquet_stream(
        write_part,
        dest,
        geoparquet=geoparquet,
        geometry_column=geometry_column,
        source_crs=source_crs,
        geoparquet_compression_level=int(kwargs.get("compression_level", 3)),
    )
    return dest


def save_parquet_stream(
    write_part,
    dest,
    *,
    geoparquet: bool = False,
    geometry_column: str = "wkb",
    source_crs: str | None = None,
    geoparquet_compression_level: int = 3,
) -> tuple[Path, object]:
    """Atomically publish a bounded-memory parquet produced by the callback.

    With geoparquet enabled the raw part is converted, deeply validated, and
    payload-compared before promotion. Any failure leaves the previous canonical
    file untouched and removes both temporary files.
    """
    dest = Path(dest)
    if geoparquet and source_crs is None:
        raise ValueError("GeoParquet writes require explicit source_crs axis semantics")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with _destination_write_lock(dest):
        raw = _unique_part(dest)
        converted: Path | None = None
        result = None
        try:
            result = write_part(raw)
            if not raw.is_file():
                raise RuntimeError(f"stream writer did not create {raw}")
            publish = raw
            if geoparquet:
                from .geoparquet_io import (
                    compare_source_payload,
                    convert_parquet_file,
                    is_geoparquet,
                    validate_geoparquet,
                )

                if is_geoparquet(raw):
                    validate_geoparquet(raw, geometry_column=geometry_column, deep=True)
                else:
                    assert source_crs is not None
                    converted = _unique_part(dest)
                    convert_parquet_file(
                        raw,
                        converted,
                        source_crs=source_crs,
                        geometry_column=geometry_column,
                        compression_level=geoparquet_compression_level,
                    )
                    validate_geoparquet(converted, geometry_column=geometry_column, deep=True)
                    compare_source_payload(raw, converted)
                    publish = converted
            publish.replace(dest)
        finally:
            with contextlib.suppress(OSError):
                raw.unlink()
            if converted is not None:
                with contextlib.suppress(OSError):
                    converted.unlink()
    return dest, result


def _thread_lock(dest: Path) -> threading.Lock:
    key = str(dest.resolve()).casefold() if os.name == "nt" else str(dest.resolve())
    with _THREAD_LOCKS_GUARD:
        return _THREAD_LOCKS.setdefault(key, threading.Lock())


@contextlib.contextmanager
def _destination_write_lock(dest: Path):
    """Serialize writers to one destination across threads and processes."""
    with _thread_lock(dest):
        lock_path = dest.parent / f".{dest.name}.write.lock"
        handle = lock_path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:  # pragma: no cover - Linux CI
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:  # pragma: no cover - Linux CI
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


# Public migration seam: in-place conversion and ordinary producers must hold
# the exact same destination lock.  Keep the private name as a compatibility
# alias for the existing writer call above.
destination_write_lock = _destination_write_lock


def _unique_part(dest: Path) -> Path:
    descriptor, value = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".part", dir=dest.parent)
    os.close(descriptor)
    part = Path(value)
    part.unlink()
    return part
