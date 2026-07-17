"""Atomic coverage/provenance JSON writes — the sidecar twin of parquet_io.

Every extractor that ships a fact also ships a small coverage/provenance JSON
next to it (row counts, per-source tallies, quarantine stats, fetch failures).
~30+ call sites hand-rolled the same idiom::

    OUT_COV.write_text(json.dumps(cov, indent=2, default=str), encoding="utf-8")

That write is NOT atomic — ``write_text`` truncates the previous good sidecar
the instant it opens, so a crash mid-write leaves a corrupt/empty coverage file
beside a healthy parquet (the exact failure ``save_parquet`` was built to
prevent for the facts themselves). ``save_coverage`` gives sidecars the same
contract: write ``<dest>.part``, then ``os.replace()`` onto ``dest`` — readers
see either the complete previous file or the complete new one, never a
half-written one.

Deliberately schema-free: coverage payloads legitimately differ per domain
(publisher tallies vs. year matrices vs. OCR stats). The shared part is the
serialisation convention (indent=2, UTF-8, ``default=str`` so dates/Paths
never crash the emit) and the atomic replace — not the shape.
"""

from __future__ import annotations

import contextlib
import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def save_coverage(
    payload: Any,
    dest: str | Path,
    *,
    indent: int = 2,
    ensure_ascii: bool = False,
    default=str,
    **kwargs,
) -> Path:
    """Atomically write ``payload`` as JSON to ``dest`` (UTF-8, indent=2).

    Mirrors ``save_parquet``: writes ``<dest>.part`` then ``os.replace()``s it
    onto ``dest``; on failure the previous ``dest`` is untouched and the partial
    temp is removed. ``default=str`` by default so date/Path values serialise
    instead of raising mid-run. Extra kwargs pass through to ``json.dumps``.
    Returns the final path.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / (dest.name + ".part")
    try:
        tmp.write_text(
            json.dumps(payload, indent=indent, ensure_ascii=ensure_ascii, default=default, **kwargs),
            encoding="utf-8",
        )
        tmp.replace(dest)
    except BaseException:
        # Never leave a half-written temp behind, and never touch the good dest.
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    return dest
