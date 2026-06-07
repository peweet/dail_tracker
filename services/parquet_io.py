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
"""

from __future__ import annotations

import contextlib
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Project parquet convention, split by engine because the kwarg names differ.
_POLARS_DEFAULTS = {"compression": "zstd", "compression_level": 3, "statistics": True}
_PANDAS_DEFAULTS = {"index": False, "compression": "zstd", "compression_level": 3}


def _is_polars(df) -> bool:
    """True for a Polars DataFrame without importing polars at module load."""
    return type(df).__module__.split(".")[0] == "polars"


def save_parquet(df, dest, **kwargs) -> Path:
    """Atomically write ``df`` (Polars or pandas) to ``dest`` with zstd defaults.

    Writes to ``<dest>.part`` then ``os.replace()``s it onto ``dest``. If the
    write raises, ``dest`` is left as it was and the partial temp is cleaned up.
    Returns the final path. Override any default via kwargs.
    """
    dest = Path(dest)
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
