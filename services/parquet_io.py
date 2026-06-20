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
   zstd / level 3 / statistics on every writer, applied here instead of being
   re-typed (and occasionally forgotten) at 70+ call sites.

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
from pathlib import Path

logger = logging.getLogger(__name__)

# Project parquet convention, split by engine because the kwarg names differ.
_POLARS_DEFAULTS = {"compression": "zstd", "compression_level": 3, "statistics": True}
_PANDAS_DEFAULTS = {"index": False, "compression": "zstd", "compression_level": 3}

# Escape hatch for the min_rows floor: a genuine bootstrap / intentionally scoped
# small write sets DAIL_SKIP_ROW_FLOOR=1, mirroring cro_poller's --force.
_FLOOR_BYPASS_ENV = "DAIL_SKIP_ROW_FLOOR"


class RowFloorViolation(ValueError):
    """A frame fell below its declared ``min_rows`` floor.

    Raised *before* any write, so the previous good ``dest`` is left untouched —
    the same contract cro_poller's ``SourceDrift`` gives bronze: a truncated /
    wiped harvest never clobbers the healthy file already on disk.
    """


def _is_polars(df) -> bool:
    """True for a Polars DataFrame without importing polars at module load."""
    return type(df).__module__.split(".")[0] == "polars"


def _row_count(df) -> int:
    """Row count for a Polars or pandas frame (no polars import at module load)."""
    return int(df.height) if _is_polars(df) else int(len(df))


def save_parquet(df, dest, *, min_rows: int | None = None, **kwargs) -> Path:
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
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / (dest.name + ".part")
    try:
        if _is_polars(df):
            import polars as pl

            opts = {**_POLARS_DEFAULTS, **kwargs}
            if df.is_empty() and len(df.columns) == 0:
                # Polars cannot round-trip a truly schemaless empty frame; write a
                # zero-row sentinel column so scan_parquet still works downstream
                # (consumers filter on row count, not on this column).
                pl.DataFrame({"_empty": pl.Series([], dtype=pl.Int64)}).write_parquet(tmp, **opts)
            else:
                df.write_parquet(tmp, **opts)
        else:
            opts = {**_PANDAS_DEFAULTS, **kwargs}
            df.to_parquet(tmp, **opts)
        tmp.replace(dest)
    except BaseException:
        # Never leave a half-written temp behind, and never touch the good dest.
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    return dest
