"""Quantity-surveyor benchmark valuation — INFERENCE (experimental/analyst surface only).

Applies published Irish €/m² construction-cost benchmarks to a deliverable to produce
an INDICATIVE value RANGE, then year-adjusts it to a contract's award half-year via the
SCSI Tender Price Index — the QS "bring costs to the tender date" method. It does NOT
report a disclosed figure: it is an estimate, the way a QS sizes a project before bills
of quantities exist.

Per the project rule, inference is forbidden in the citizen-facing app; this is surfaced
ONLY behind an explicit experimental/indicative label, and every output carries its
method, the €/m² range used, the TPI adjustment, sources, and caveats so the estimate is
auditable and never asserted as fact.

Data (hand-curated source-of-truth, ingested 2026-06-11):
  - data/_meta/qs_cost_benchmarks.csv     — €/m² (and per-unit/key/space/m) by building type
  - data/_meta/scsi_tender_price_index.csv — SCSI TPI half-yearly 1998H1..2025H1 (base 1998H1=100)

Pure logic, no Streamlit. The UI passes user input and renders the returned dict verbatim.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

_META = Path(__file__).resolve().parent.parent / "data" / "_meta"
_BENCHMARKS_CSV = _META / "qs_cost_benchmarks.csv"
_TPI_CSV = _META / "scsi_tender_price_index.csv"


@dataclass(frozen=True)
class Benchmark:
    source: str
    category: str
    subtype: str
    region: str
    unit: str  # per_m2 | per_unit | per_key | per_space | per_m
    lo_eur: float
    hi_eur: float
    basis_period: str  # e.g. "2025H1"
    vat: str  # incl | excl
    excludes: str
    notes: str


def _rows(path: Path) -> list[dict]:
    """Read a curated _meta CSV, skipping the leading '#' comment lines but keeping
    the header (the first line)."""
    with path.open(encoding="utf-8") as fh:
        lines = [ln for ln in fh if not ln.lstrip().startswith("#")]
    return list(csv.DictReader(lines))


@lru_cache(maxsize=1)
def load_benchmarks() -> tuple[Benchmark, ...]:
    out: list[Benchmark] = []
    for r in _rows(_BENCHMARKS_CSV):
        try:
            out.append(
                Benchmark(
                    source=r["source"].strip(),
                    category=r["category"].strip(),
                    subtype=r["subtype"].strip(),
                    region=r["region"].strip(),
                    unit=r["unit"].strip(),
                    lo_eur=float(r["lo_eur"]),
                    hi_eur=float(r["hi_eur"]),
                    basis_period=r["basis_period"].strip(),
                    vat=r["vat"].strip(),
                    excludes=r["excludes"].strip(),
                    notes=r.get("notes", "").strip(),
                )
            )
        except (KeyError, ValueError):
            continue
    return tuple(out)


@lru_cache(maxsize=1)
def load_tpi() -> dict[str, float]:
    """period (e.g. '2019H1') -> index value (base 1998H1 = 100)."""
    return {r["period"].strip(): float(r["index_base_1998h1"]) for r in _rows(_TPI_CSV)}


def latest_tpi_period() -> str:
    return sorted(load_tpi())[-1]


def tpi_for_year(year: int) -> float | None:
    """Index for a calendar year — the mean of its published halves (H1/H2)."""
    tpi = load_tpi()
    halves = [tpi[p] for p in (f"{year}H1", f"{year}H2") if p in tpi]
    if halves:
        return sum(halves) / len(halves)
    return None


def _tpi_for_period(period: str) -> float | None:
    """Index for a 'YYYYH1'/'YYYYH2' basis period, falling back to the year mean."""
    tpi = load_tpi()
    if period in tpi:
        return tpi[period]
    if len(period) >= 4 and period[:4].isdigit():
        return tpi_for_year(int(period[:4]))
    return None


@dataclass
class Estimate:
    ok: bool
    subtype: str = ""
    message: str = ""
    payload: dict = field(default_factory=dict)


def list_subtypes() -> list[dict]:
    """The selectable building types, for a UI dropdown (per-m² ones first — they
    take a floor area; per-unit/key/space ones price by count)."""
    seen, out = set(), []
    for b in load_benchmarks():
        key = (b.subtype, b.unit)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "subtype": b.subtype,
                "category": b.category,
                "unit": b.unit,
                "source": b.source,
                "label": f"{b.subtype.replace('_', ' ')} ({b.category}, {b.source})",
            }
        )
    out.sort(key=lambda d: (d["unit"] != "per_m2", d["category"], d["subtype"]))
    return out


def estimate(
    subtype: str,
    *,
    units: int = 1,
    area_m2: float | None = None,
    award_year: int | None = None,
    framework_ceiling_eur: float | None = None,
) -> Estimate:
    """Indicative, year-adjusted benchmark valuation of a construction deliverable.

    ``subtype`` selects a row from the benchmark table. ``area_m2`` is required for
    per-m² building types (a per-unit/key/space type ignores it). ``award_year`` adjusts
    the cost to that year via the TPI (defaults to the benchmark's own basis period, i.e.
    no adjustment). ``framework_ceiling_eur`` adds the "ceiling vs estimate" gap.

    Returns an Estimate whose ``payload`` is a fully self-describing, auditable dict.
    """
    matches = [b for b in load_benchmarks() if b.subtype == subtype]
    if not matches:
        return Estimate(ok=False, message=f"No benchmark for '{subtype}'.")
    b = matches[0]

    if b.unit == "per_m2":
        if not area_m2 or area_m2 <= 0:
            return Estimate(ok=False, subtype=subtype, message="A floor area (m²) is required for this building type.")
        base_lo, base_hi = b.lo_eur * area_m2, b.hi_eur * area_m2
        unit_desc = f"{area_m2:,.0f} m² × €{b.lo_eur:,.0f}–€{b.hi_eur:,.0f}/m²"
    else:
        base_lo, base_hi = b.lo_eur, b.hi_eur
        per = b.unit.replace("per_", "per ")
        unit_desc = f"€{b.lo_eur:,.0f}–€{b.hi_eur:,.0f} {per}"

    # Year adjustment via TPI (QS tender-date method).
    basis_idx = _tpi_for_period(b.basis_period)
    factor = 1.0
    tpi_note = f"no year adjustment (benchmark basis {b.basis_period})"
    if award_year is not None and basis_idx:
        award_idx = tpi_for_year(award_year)
        if award_idx:
            factor = award_idx / basis_idx
            tpi_note = (
                f"adjusted to {award_year} via SCSI TPI "
                f"(index {award_idx:.1f} / {basis_idx:.1f} = ×{factor:.3f})"
            )
        else:
            tpi_note = f"no TPI for {award_year}; left at benchmark basis {b.basis_period}"

    lo = base_lo * factor * max(units, 1)
    hi = base_hi * factor * max(units, 1)
    mid = (lo + hi) / 2

    payload: dict = {
        "estimate_kind": "INDICATIVE benchmark valuation — inference, NOT a disclosed figure",
        "read_as": {
            "building_type": b.subtype.replace("_", " "),
            "category": b.category,
            "units": max(units, 1),
            "per_unit_basis": unit_desc,
        },
        "method": "RICS NRM elemental order-of-cost: units × floor area × €/m² (or per-unit rate), "
        "then year-adjusted by the SCSI Tender Price Index",
        "year_adjustment": tpi_note,
        "value_eur": {"low": round(lo), "mid": round(mid), "high": round(hi)},
        "basis": {
            "source": b.source,
            "basis_period": b.basis_period,
            "vat": "includes VAT" if b.vat == "incl" else "excludes VAT",
            "excludes": b.excludes,
        },
        "caveat": "Indicative range only. " + (
            "Excludes VAT, site works, professional fees, abnormals and inflation to award date — "
            "so it is a hard-construction FLOOR, materially below a full contract value. "
            if b.vat == "excl"
            else "Insurance-reinstatement / VAT-inclusive basis — a different basis from new-build hard cost. "
        )
        + "Use as a sizing sanity-check, never as the contract's actual value.",
        "sources": sorted({bm.source for bm in load_benchmarks()})
        + ["SCSI Tender Price Index (data/_meta/scsi_tender_price_index.csv)"],
    }
    if framework_ceiling_eur and framework_ceiling_eur > 0 and mid > 0:
        mult = framework_ceiling_eur / mid
        payload["framework_ceiling_eur"] = round(framework_ceiling_eur)
        payload["ceiling_vs_estimate_multiple"] = round(mult, 1)
        payload["ceiling_reading"] = (
            f"The framework / DPS ceiling (€{framework_ceiling_eur:,.0f}) is ~{mult:.1f}× the indicative "
            "build value of this deliverable — i.e. the ceiling is legal headroom, not a measure of this contract."
        )
    return Estimate(ok=True, subtype=subtype, payload=payload)
