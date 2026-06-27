"""Lightweight data-fidelity gate for the bronze/silver layers.

ONE job: stop a single stray / fat-fingered / mis-OCR'd figure (a €999,999,999 donation,
a spend total off by 1000×) from propagating into the published app — WITHOUT taking the
whole run down for one bad cell. This app's value is trust; a ridiculous number reaching a
total is the failure that loses users.

This is a thin, reusable wrapper over the project's OWN pure-Polars contract engine
(``services.data_contracts``) — deliberately NOT a new third-party validation library:
no new dependency, nothing extra shipped to Streamlit Cloud, and the SAME quarantine
convention as the payment / SIPO guards (offending rows + a tracked JSON summary land in
``data/_meta/quarantine/``). Drop it at the edge of any bronze/silver loader::

    from tools.data_fidelity import fidelity_gate

    df = fidelity_gate(
        df,
        name="sipo_donations",
        bounds={"value_eur": (0, 1_000_000_000)},   # absurd-only: > €1bn is impossible
    )
    # df now carries ONLY plausible rows; the rest are quarantined + logged for review.

Design rules (the pushback that keeps this from becoming a straitjacket):
  * Bounds are ABSURD-ONLY, not business rules. Set them so wide that only genuine garbage
    trips them — a tight bound couples you to the source and will cry wolf on real data.
  * It QUARANTINES, it does not halt — except on a SPIKE (> ``max_offending_frac`` of a
    column out of bounds), which means the source itself changed shape, not a fat-finger.
  * Quarantine's own failure mode is silent erosion (rows vanish, nobody looks), so every
    gated run logs how many rows / which resource were held — wire that into a review queue.

For drift relative to *history* ("6× last quarter", "value past the 99.9th percentile we've
ever seen") — genuinely higher fidelity but heavier, needing a baseline store — see the
planned baseline check. THIS gate is the cheap, dependency-free fat-finger catcher.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from services.data_contracts import (
    QUARANTINE_DIR,
    BoundRule,
    ContractReport,
    partition_implausible,
)

logger = logging.getLogger(__name__)

# column -> (min, max); use None for an open side, e.g. {"value_eur": (0, None)}.
Bounds = dict[str, "tuple[float | None, float | None]"]


def fidelity_gate(
    df: pl.DataFrame,
    *,
    name: str,
    bounds: Bounds,
    max_offending_frac: float = 0.02,
    quarantine_dir: Path = QUARANTINE_DIR,
    write_quarantine: bool = True,
    return_report: bool = False,
) -> pl.DataFrame | tuple[pl.DataFrame, ContractReport]:
    """Return only the plausible rows of ``df``; quarantine + log the implausible ones.

    ``bounds`` maps a numeric column to ``(min, max)`` absurd-only limits (``None`` = open
    side). A row with any bounded column outside its limits is held back, written to the
    quarantine dir, and excluded from the returned frame — so it cannot reach the app.

    Raises :class:`services.data_contracts.ImplausibleValueSpike` if more than
    ``max_offending_frac`` of any column is out of bounds (a structural change, not a stray
    cell). Set ``return_report=True`` to also get the :class:`ContractReport` (counts,
    samples, quarantine paths) for a review queue / dashboard.
    """
    rules = tuple(
        BoundRule(column, min_value=lo, max_value=hi, max_offending_frac=max_offending_frac)
        for column, (lo, hi) in bounds.items()
    )
    plausible, implausible, report = partition_implausible(
        df,
        name=name,
        bounds=rules,
        quarantine_dir=quarantine_dir,
        write_quarantine=write_quarantine,
    )

    if implausible.height:
        logger.warning(
            "fidelity_gate[%s]: quarantined %d of %d rows (%.2f%%) as implausible — held for review%s",
            name,
            implausible.height,
            df.height,
            100 * implausible.height / max(df.height, 1),
            f" at {report.quarantine_parquet}" if report.quarantine_parquet else "",
        )

    return (plausible, report) if return_report else plausible
