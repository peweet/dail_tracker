"""A/B probes for tools/row_iteration_runbook.yaml — one per runbook candidate id.

Each probe returns the INCUMBENT (baseline) and the MIGRATED (candidate) implementation of one
site, both closed over the same fixture, so tools/row_iteration_ab.py can prove byte-identical
output before it reports a single timing.

⚠ THE BASELINE MUST REPLICATE THE INCUMBENT FAITHFULLY, expensive steps included. A baseline that
does less work than production once inverted a conclusion in this repo — a validation A/B looked
like a 0.43x regression because the reference skipped the two costliest predicates, when the
honest number was 1.18x in the other direction. Where a probe's baseline is the PRE-migration code
it is copied verbatim from git history, not paraphrased.

Fixtures are drawn from real repo parquet where one exists, because synthetic geometry and
synthetic strings have both produced false readings here: a 5-vertex fixture reported 2.7x for a
change that was 0.78x on real parcels.
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[1]

_MIN_TITLE = 12
_MIN_TOKEN = 6


@dataclass(frozen=True)
class Probe:
    """One A/B: two callables that must return byte-identical results."""

    baseline: Callable[[], Any]
    candidate: Callable[[], Any]
    rows: int
    describes: str


def _payment_descriptions(limit: int) -> pl.Series:
    path = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fixture missing: {path}")
    return pl.read_parquet(path, columns=["description"]).head(limit)["description"]


def procurement_contains_dynamic(limit: int = 120_000) -> Probe:
    """Row-wise substring containment -> native str.contains(expr, literal=True)."""
    from extractors.procurement_award_payment_candidates import _contains_dynamic

    text = _payment_descriptions(limit).fill_null("").str.to_lowercase().str.replace_all(r"[^a-z0-9]", "")
    haystack = text.to_list()
    # Half the needles are genuine substrings so BOTH branches are exercised; a fixture where every
    # row is False would pass while proving nothing.
    needles = [
        h[2 : 2 + max(_MIN_TITLE, len(h) // 3)] if (i % 2 == 0 and len(h) >= _MIN_TITLE + 4) else f"zzz{i}"
        for i, h in enumerate(haystack)
    ]
    frame = pl.DataFrame({"h": haystack, "n": needles})

    def baseline() -> pl.Series:
        # Verbatim pre-migration body.
        return frame.with_columns(
            pl.struct("h", "n")
            .map_elements(
                lambda row: bool(row["n"]) and len(row["n"]) >= _MIN_TITLE and row["n"] in row["h"],
                return_dtype=pl.Boolean,
            )
            .alias("v")
        )["v"]

    def candidate() -> pl.Series:
        return frame.with_columns(_contains_dynamic("h", "n", minimum_needle_length=_MIN_TITLE).alias("v"))["v"]

    return Probe(baseline, candidate, frame.height, "substring containment on real payment descriptions")


def procurement_has_dynamic_token(limit: int = 120_000) -> Probe:
    """Row-wise token membership -> native list.contains(expr)."""
    from extractors.procurement_award_payment_candidates import _has_dynamic_token

    tokens = _payment_descriptions(limit).fill_null("").str.to_lowercase().str.extract_all(r"[a-z0-9]+").to_list()
    needles = [(t[0] if (i % 2 == 0 and t and len(t[0]) >= _MIN_TOKEN) else f"zzzzzz{i}") for i, t in enumerate(tokens)]
    frame = pl.DataFrame({"t": tokens, "n": needles})

    def baseline() -> pl.Series:
        return frame.with_columns(
            pl.struct("t", "n")
            .map_elements(
                lambda row: bool(row["n"]) and len(row["n"]) >= _MIN_TOKEN and row["n"] in row["t"],
                return_dtype=pl.Boolean,
            )
            .alias("v")
        )["v"]

    def candidate() -> pl.Series:
        return frame.with_columns(_has_dynamic_token("t", "n", minimum_needle_length=_MIN_TOKEN).alias("v"))["v"]

    return Probe(baseline, candidate, frame.height, "token membership on real payment descriptions")


def legal_diary_parties_unique(limit: int = 4_000, repetition: int = 30) -> Probe:
    """parties() per row -> parties() per DISTINCT raw_case, joined back.

    Repetition is set BELOW the 63.95x observed in the real gold file, so the probe understates
    rather than flatters the gain.
    """
    from extractors.legal_diary_extract import parties

    struct_dtype = pl.Struct(
        [
            pl.Field("case_anonymised", pl.Utf8),
            pl.Field("plaintiff", pl.Utf8),
            pl.Field("defendant", pl.Utf8),
            pl.Field("plaintiff_kind", pl.Utf8),
        ]
    )
    path = ROOT / "data/gold/parquet/judicial_legal_diary_openview_cases.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fixture missing: {path}")
    distinct = pl.read_parquet(path, columns=["case_anonymised"])["case_anonymised"].unique().to_list()[:limit]
    rng = np.random.default_rng(0)
    raw = [distinct[i] for i in rng.integers(0, len(distinct), len(distinct) * repetition)]
    frame = pl.DataFrame({"raw_case": raw})

    def baseline() -> pl.DataFrame:
        return (
            frame.with_columns(pl.col("raw_case").map_elements(parties, return_dtype=struct_dtype).alias("_p"))
            .unnest("_p")
            .sort("raw_case", "case_anonymised")
        )

    def candidate() -> pl.DataFrame:
        anonymised = (
            frame.select("raw_case")
            .unique()
            .with_columns(pl.col("raw_case").map_elements(parties, return_dtype=struct_dtype).alias("_p"))
            .unnest("_p")
        )
        return frame.join(anonymised, on="raw_case", how="left").sort("raw_case", "case_anonymised")

    return Probe(baseline, candidate, frame.height, "anonymise per distinct case line, join back")


def object_construction_idioms(rows: int = 200_000) -> Probe:
    """The tier-2 ceiling: the best idiom when N Python objects genuinely must exist.

    Not a migration. Re-measured on every harness run so the 1.78x ceiling stays a measurement on
    THIS box rather than a remembered number — if it ever rises materially, tier 2 becomes worth
    revisiting and the runbook's advice changes.
    """
    rng = np.random.default_rng(0)
    frame = pl.DataFrame(
        {
            "ref": [f"REF-{i}" for i in range(rows)],
            "authority": rng.choice(["Cork", "Galway", "Mayo", "Sligo"], rows),
            "decision": rng.choice(["Granted", "Refused"], rows),
            "dist": rng.uniform(0, 5000, rows),
            "year": rng.integers(2015, 2026, rows),
        }
    )
    columns = ["ref", "authority", "decision", "dist", "year"]

    def baseline() -> list[tuple]:
        return [tuple(row.values()) for row in frame.iter_rows(named=True)]

    def candidate() -> list[tuple]:
        return list(zip(*(frame[column].to_list() for column in columns), strict=True))

    return Probe(baseline, candidate, rows, "building N python objects (tier-2 ceiling)")


def _payments_column(column: str, limit: int) -> pl.DataFrame:
    path = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
    if not path.exists():
        raise FileNotFoundError(f"fixture missing: {path}")
    return pl.read_parquet(path, columns=[column]).head(limit)


def _unique_join_probe(column: str, function, limit: int, describes: str) -> Probe:
    """Shared shape: a pure per-row function over a REPEATING column.

    The function is deterministic in one column, so computing it per DISTINCT value and joining
    back is semantically identical and does the work once per distinct input instead of once per
    row. This is the only lever when the function itself cannot be expressed columnar.
    """
    frame = _payments_column(column, limit)

    def baseline() -> pl.Series:
        return frame.with_columns(pl.col(column).map_elements(function, return_dtype=pl.Utf8).alias("v"))["v"]

    def candidate() -> pl.Series:
        distinct = (
            frame.select(column)
            .unique()
            .with_columns(pl.col(column).map_elements(function, return_dtype=pl.Utf8).alias("v"))
        )
        return frame.join(distinct, on=column, how="left")["v"]

    return Probe(baseline, candidate, frame.height, describes)


def procurement_strip_leading_ref(limit: int = 200_000) -> Probe:
    """_strip_leading_ref over supplier_raw — 9.9x repetition on the real payments fact."""
    from extractors.procurement_payments_consolidate import _strip_leading_ref

    return _unique_join_probe("supplier_raw", _strip_leading_ref, limit, "strip bled-in refs per distinct supplier")


def procurement_canon_spend_category(limit: int = 200_000) -> Probe:
    """canon_spend_category over description — 12.0x repetition on the real payments fact."""
    from extractors.procurement_payments_consolidate import canon_spend_category

    return _unique_join_probe(
        "description", canon_spend_category, limit, "canonicalise spend category per distinct description"
    )


PROBES: dict[str, Callable[[], Probe]] = {
    "procurement_contains_dynamic": procurement_contains_dynamic,
    "procurement_has_dynamic_token": procurement_has_dynamic_token,
    "legal_diary_parties_unique": legal_diary_parties_unique,
    "object_construction_idioms": object_construction_idioms,
    "procurement_strip_leading_ref": procurement_strip_leading_ref,
    "procurement_canon_spend_category": procurement_canon_spend_category,
}
