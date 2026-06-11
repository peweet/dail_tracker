"""Quantity-surveyor benchmark valuation (MCP analyst helper).

⚠️ THIS IS INFERENCE. It does NOT report a disclosed figure — it applies published
cost-per-m² benchmarks to a deliverable description to produce an INDICATIVE RANGE,
the way a QS sizes a project before bills of quantities exist. Per the project rule
(inference welcome in the analyst/planning conduit, FORBIDDEN in the citizen-facing
app), this belongs in the MCP/chat surface only, and every output carries its method
and assumptions so the estimate is auditable, never asserted as fact.

Method (RICS NRM order-of-cost-estimating, elemental): value ≈ units × floor_area_m²
× €/m². Benchmarks are Irish 2025–26 figures from the SCSI tender-price indices and
public Irish cost guides (sources in `BENCHMARKS`). The output is a RANGE plus the
gap to any framework ceiling, so a reader sees how far a headline ceiling sits from a
defensible project value.
"""

from __future__ import annotations

import re
from typing import Any

# Irish new-build cost ranges, € per m² (gross internal area), 2025–26.
# Sources: Society of Chartered Surveyors Ireland (SCSI) Tender Price Index &
# House Rebuild / Housing Development calculators; Buildcost Construction Cost Guide
# (H2 2025); SelfBuild build-costs 2026. Ranges are deliberately wide — regional and
# specification spread in Ireland is large (~€1,500–€6,000/m²).
BENCHMARKS: dict[str, dict[str, Any]] = {
    "social_housing": {"lo": 1900, "hi": 2600, "area_m2": 95, "label": "social / standard housing (semi-d / terrace)"},
    "house": {"lo": 2000, "hi": 2800, "area_m2": 110, "label": "private house (typical)"},
    "apartment": {"lo": 3000, "hi": 4500, "area_m2": 75, "label": "apartment (2-bed GIA)"},
    "school": {"lo": 2500, "hi": 3600, "area_m2": 1200, "label": "school / education building"},
}
SOURCES = [
    "SCSI Tender Price Index & House Rebuild Calculator (scsi.ie)",
    "Buildcost Construction Cost Guide H2 2025 (buildcost.ie)",
    "SelfBuild Build Costs 2026 (selfbuild.ie)",
]

_COUNT_RE = re.compile(
    r"(\d+)\s+(?:no\.?\s+)?([\w\-/. ]*?(?:dwelling|home|hous|unit|apartment|flat|school|classroom)\w*)", re.I
)


def _category(text: str) -> str:
    t = (text or "").lower()
    if any(w in t for w in ("apartment", "flat", "duplex")):
        return "apartment"
    if any(w in t for w in ("school", "classroom", "education")):
        return "school"
    if any(w in t for w in ("social", "council", "affordable", "local authority")):
        return "social_housing"
    return "house"


def estimate(
    deliverable: str = "",
    *,
    units: int = 0,
    category: str = "",
    area_m2: float = 0.0,
    framework_ceiling_eur: float | None = None,
) -> dict:
    """Indicative benchmark valuation of a construction deliverable.

    Either pass a free-text `deliverable` (e.g. '12 semi-detached dwellings') and let it
    parse units + category, or pass `units`/`category`/`area_m2` explicitly. Returns a
    low/high € range with every assumption shown, plus — if a `framework_ceiling_eur` is
    given — how many times larger that ceiling is than the estimated build (the gap that
    makes a headline ceiling misleading).
    """
    cat = category or _category(deliverable)
    if cat not in BENCHMARKS:
        cat = "house"
    bench = BENCHMARKS[cat]

    if not units:
        m = _COUNT_RE.search(deliverable or "")
        units = int(m.group(1)) if m else 1
    area = area_m2 or bench["area_m2"]

    lo = units * area * bench["lo"]
    hi = units * area * bench["hi"]
    mid = (lo + hi) / 2

    out: dict[str, Any] = {
        "estimate_kind": "INDICATIVE benchmark valuation — NOT a disclosed figure (inference)",
        "deliverable_read_as": {"units": units, "category": bench["label"], "area_m2_each": area},
        "rate_eur_per_m2": {"low": bench["lo"], "high": bench["hi"]},
        "estimated_construction_value_eur": {"low": round(lo), "mid": round(mid), "high": round(hi)},
        "method": "RICS NRM elemental order-of-cost: units × floor_area_m² × €/m²",
        "assumptions": [
            f"{units} unit(s) of '{bench['label']}'",
            f"{area} m² gross internal area each (benchmark default)" if not area_m2 else f"{area} m² each (supplied)",
            f"€{bench['lo']}–€{bench['hi']}/m² Irish 2025–26 range",
        ],
        "sources": SOURCES,
        "caveat": "Regional/spec spread is wide; excludes site works, fees, VAT, inflation to award date. "
        "Use as a sizing sanity-check, never as the project's actual cost.",
    }
    if framework_ceiling_eur:
        out["framework_ceiling_eur"] = framework_ceiling_eur
        out["ceiling_vs_estimate_multiple"] = round(framework_ceiling_eur / mid, 1) if mid else None
        out["reading"] = (
            f"The framework ceiling (€{framework_ceiling_eur:,.0f}) is ~{out['ceiling_vs_estimate_multiple']}× "
            f"the indicative build value of this seed deliverable — i.e. the ceiling is legal headroom, "
            f"not a measure of this contract."
        )
    return out
