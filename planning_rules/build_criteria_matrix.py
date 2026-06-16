"""Consolidate the 31 per-council v2 criteria extracts into a cross-council comparison matrix.

Pure deterministic reshape of `_criteria_map/v2/<slug>.json` (no inference on the verbatim layer).
Emits three artifacts:

  1. _criteria_map/criteria_matrix.csv          — WIDE, one row/council, every field VERBATIM
  2. _criteria_map/criteria_matrix_numeric.csv  — DERIVED numeric layer for rankable discretion
                                                   fields (parsed conservatively; each value keeps
                                                   its verbatim `basis` + a `multi` flag when the
                                                   source cell held >1 number of that unit)
  3. CRITERIA_MAP.md                            — human-readable comparison of the council-DISCRETION
                                                   fields (the "postcode lottery") + a national-
                                                   deferral callout listing the fields that are
                                                   uniform because councils defer to national guidance

The verbatim layer is the source of truth and involves zero inference. The numeric layer is clearly
DERIVED: it only parses a number where the pattern is unambiguous, always keeps the original string,
and flags compound cells — so it is auditable, not authoritative.
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
V2_DIR = HERE / "_criteria_map" / "v2"
OUT_DIR = HERE / "_criteria_map"
MD_PATH = HERE / "CRITERIA_MAP.md"

# ---- field layout (mirrors SCHEMA_v2.md) ----------------------------------------------------------
TOP_FIELDS = [
    "rural_min_site_area", "rural_site_scaling", "ribbon_development_rule",
    "road_setback_national_m", "road_setback_regional_m", "road_setback_local_m",
    "sightline_x_m", "sightline_y", "wastewater_standard",
    "residential_density_dph", "plot_ratio", "site_coverage_pct", "separation_distance_m",
    "private_open_space", "public_open_space_pct", "car_parking_dwelling", "building_height",
    "trigger_AA", "trigger_EIA", "trigger_FRA", "trigger_AHIA", "trigger_EcIA", "trigger_VIA", "trigger_TTA",
]
GROUPS = {
    "residential": ["housing_mix", "dwelling_min_floor_areas", "apartment_min_floor_areas",
                    "apartment_dual_aspect_pct", "apartment_storage", "apartment_floor_to_ceiling",
                    "extensions", "family_flat_max_gfa", "domestic_garage_pod", "dormer_standard",
                    "boundary_treatment_height", "naming", "taking_in_charge", "build_to_rent",
                    "co_living", "student_accommodation", "bonds"],
    "open_space": ["public_open_space_pct", "public_open_space_qual", "private_open_space_house",
                   "balcony_min_areas", "play_space"],
    "community": ["childcare_provision", "part_v_pct", "social_infrastructure_audit", "education", "health"],
    "transport": ["car_parking_standards", "cycle_parking", "ev_charging_pct", "building_line",
                  "stopping_distances_sightlines", "universal_access", "loading_coach_taxi"],
    "water_environment": ["suds_requirement", "surface_water", "bin_refuse_storage", "daylight_sunlight", "noise"],
    "non_residential": ["retail_sequential_test", "retail_floorspace_caps", "shopfront_signage", "advertising",
                        "employment_industry", "business_tech_parks", "home_based_economic",
                        "agricultural_separation", "seveso", "petrol_stations", "amusement_arcades",
                        "hot_food_takeaway", "vape_betting"],
    "energy_infrastructure": ["wind_energy_setback", "solar", "telecommunications", "overhead_lines", "data_centre"],
    "heritage_natural": ["archaeology_zones", "protected_structures_curtilage", "biodiversity_green_infra"],
}


def field_paths() -> list[str]:
    paths = list(TOP_FIELDS)
    for g, ks in GROUPS.items():
        paths += [f"{g}.{k}" for k in ks]
    return paths


def get(d: dict, path: str):
    if "." in path:
        g, k = path.split(".", 1)
        return (d.get(g) or {}).get(k)
    return d.get(path)


# ---- conservative numeric parsers -----------------------------------------------------------------
# Each returns (value, multi) where multi=True if the source string held >1 number of that unit
# (so the parsed value is "first of several" — flagged, never silently collapsed).

def _all(pattern: str, s: str) -> list[str]:
    return re.findall(pattern, s, flags=re.IGNORECASE)


def parse_metres(s: str):
    # plain metres, NOT m² / sq.m / mm
    ms = _all(r"(\d+(?:\.\d+)?)\s*m(?![²2mm]|\w)", s)
    if not ms:
        return None, False
    return float(ms[0]), len(set(ms)) > 1


def parse_area_m2(s: str):
    # hectares -> m²; else m²/sq.m/sqm
    ha = _all(r"(\d+(?:\.\d+)?)\s*(?:hectare|ha\b)", s)
    if ha:
        return round(float(ha[0]) * 10000), len(set(ha)) > 1
    sqm = _all(r"(\d+(?:\.\d+)?)\s*(?:sq\.?\s*m|sqm|m²|square\s*met)", s)
    if sqm:
        return float(sqm[0]), len(set(sqm)) > 1
    return None, False


def parse_pct(s: str):
    ps = _all(r"(\d+(?:\.\d+)?)\s*%", s)
    if not ps:
        return None, False
    return float(ps[0]), len(set(ps)) > 1


def parse_dph(s: str):
    ds = _all(r"(\d+)\s*(?:dph|uph|dwellings?\s*per\s*hectare|units?\s*per\s*hectare)", s)
    if not ds:
        return None, False
    return int(ds[0]), len(set(ds)) > 1


def parse_ribbon(s: str):
    # "5+ houses per 250 m" -> (5, 250)
    m = re.search(r"(\d+)\D{1,40}?(\d+)\s*m\b", s, flags=re.IGNORECASE)
    if not m:
        return None, None
    return int(m.group(1)), int(m.group(2))


# derived numeric columns: out_col -> (source_path, parser) ; fallback handled inline
NUMERIC = [
    ("rural_min_site_m2", "rural_min_site_area", parse_area_m2),
    ("road_setback_national_m_num", "road_setback_national_m", parse_metres),
    ("road_setback_regional_m_num", "road_setback_regional_m", parse_metres),
    ("road_setback_local_m_num", "road_setback_local_m", parse_metres),
    ("separation_distance_m_num", "separation_distance_m", parse_metres),
    ("sightline_x_m_num", "sightline_x_m", parse_metres),
    ("ev_charging_pct_num", "transport.ev_charging_pct", parse_pct),
    ("part_v_pct_num", "community.part_v_pct", parse_pct),
    ("public_open_space_pct_num", "open_space.public_open_space_pct", parse_pct),
    ("site_coverage_pct_num", "site_coverage_pct", parse_pct),
    ("residential_density_dph_num", "residential_density_dph", parse_dph),
    ("family_flat_max_gfa_m2", "residential.family_flat_max_gfa", parse_area_m2),
]

# ---- MD field selections --------------------------------------------------------------------------
DISCRETION = [
    ("rural_min_site_area", "Rural one-off — min site area"),
    ("ribbon_development_rule", "Ribbon / linear-development rule"),
    ("road_setback_national_m", "Building setback — national road"),
    ("road_setback_regional_m", "Building setback — regional road"),
    ("road_setback_local_m", "Building setback — local road"),
    ("sightline_y", "Sightline Y (visibility distance)"),
    ("residential_density_dph", "Residential density"),
    ("site_coverage_pct", "Site coverage"),
    ("separation_distance_m", "Separation (overlooking)"),
    ("car_parking_dwelling", "Car parking per dwelling"),
    ("open_space.public_open_space_pct", "Public open space %"),
    ("transport.cycle_parking", "Cycle parking"),
    ("transport.ev_charging_pct", "EV charging %"),
    ("community.childcare_provision", "Childcare provision"),
    ("residential.family_flat_max_gfa", "Family/granny flat max GFA"),
    ("residential.bonds", "Completion bond"),
]
DEFERRAL = [
    ("residential.apartment_min_floor_areas", "Apartment min floor areas"),
    ("residential.apartment_dual_aspect_pct", "Apartment dual-aspect %"),
    ("residential.apartment_floor_to_ceiling", "Apartment floor-to-ceiling"),
    ("residential.dwelling_min_floor_areas", "Dwelling min floor areas"),
    ("building_height", "Building height"),
    ("energy_infrastructure.wind_energy_setback", "Wind-energy setback"),
]
_DEFERRAL_RX = re.compile(r"guidelines|sppr|2018|2020|2009|national", re.IGNORECASE)


def load() -> list[dict]:
    out = []
    for p in sorted(V2_DIR.glob("*.json")):
        out.append(json.loads(p.read_text(encoding="utf-8")))
    return out


def md_cell(v) -> str:
    if not v:
        return "—"
    s = " ".join(str(v).split())
    return (s[:157] + "…") if len(s) > 158 else s


def main() -> None:
    councils = sorted(load(), key=lambda d: d.get("council", ""))
    paths = field_paths()

    # 1) WIDE verbatim CSV --------------------------------------------------------------------------
    with (OUT_DIR / "criteria_matrix.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["council", "slug", "authority_type", "plan_name", "extract_status"] + paths)
        for d in councils:
            w.writerow([d.get("council"), d.get("slug"), d.get("authority_type"),
                        d.get("plan_name"), d.get("extract_status")]
                       + [get(d, p) for p in paths])

    # 2) DERIVED numeric CSV ------------------------------------------------------------------------
    with (OUT_DIR / "criteria_matrix_numeric.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        hdr = ["council", "slug"]
        for col, _, _ in NUMERIC:
            hdr += [col, col + "__multi", col + "__basis"]
        hdr += ["ribbon_houses", "ribbon_per_m", "ribbon__basis"]
        w.writerow(hdr)
        for d in councils:
            row = [d.get("council"), d.get("slug")]
            for col, src, parser in NUMERIC:
                raw = get(d, src)
                if col == "public_open_space_pct_num" and not raw:
                    raw = get(d, "public_open_space_pct")  # fall back to top-level
                if raw:
                    val, multi = parser(str(raw))
                    row += [val, ("Y" if multi else ""), " ".join(str(raw).split())[:200]]
                else:
                    row += ["", "", ""]
            rb = get(d, "ribbon_development_rule")
            if rb:
                h, m = parse_ribbon(str(rb))
                row += [h if h is not None else "", m if m is not None else "", " ".join(str(rb).split())[:200]]
            else:
                row += ["", "", ""]
            w.writerow(row)

    # 3) CRITERIA_MAP.md ----------------------------------------------------------------------------
    lines: list[str] = []
    lines.append("# Cross-Council Development-Management Standards — Comparison Matrix\n")
    lines.append("_Generated by `build_criteria_matrix.py` from the 31 per-council verbatim extracts "
                 "in `_criteria_map/v2/`. Every cell is quoted verbatim from that council's adopted "
                 "Development Plan; `—` means the council sets **no standard** for that item (genuine, "
                 "not a gap). Cells truncated to ~158 chars for readability — full text + section "
                 "provenance in the per-council JSON and `criteria_matrix.csv`._\n")
    lines.append(f"**Councils: {len(councils)}** · partials: "
                 f"{sum(1 for d in councils if d.get('extract_status') != 'complete')} · "
                 "machine-readable: `criteria_matrix.csv` (verbatim) + `criteria_matrix_numeric.csv` (derived).\n")

    lines.append("\n## 1. Council-discretion standards — where the rule actually differs by county\n")
    lines.append("_These are the standards each council sets at its own discretion; the variation here "
                 "is the genuine cross-council difference (the 'postcode lottery')._\n")
    for path, label in DISCRETION:
        lines.append(f"\n### {label}\n")
        lines.append("| Council | Standard |")
        lines.append("|---|---|")
        for d in councils:
            lines.append(f"| {d.get('council')} | {md_cell(get(d, path))} |")

    # deferral callout
    lines.append("\n## 2. National-deferral standards — uniform across councils (no local 'lottery')\n")
    lines.append("_For these items most councils do not set their own figure — they apply a national "
                 "Specific Planning Policy Requirement / guideline. Shown: how many of the 31 councils "
                 "state anything, and how many of those simply defer to national guidance._\n")
    lines.append("| Standard | Councils stating it | …of which defer to national guidance |")
    lines.append("|---|---|---|")
    for path, label in DEFERRAL:
        vals = [get(d, path) for d in councils]
        nonnull = [v for v in vals if v]
        defer = [v for v in nonnull if _DEFERRAL_RX.search(str(v))]
        lines.append(f"| {label} | {len(nonnull)}/31 | {len(defer)}/{len(nonnull) if nonnull else 0} |")

    # statutory obligation — Part V (NOT council discretion)
    lines.append("\n## 2b. Statutory obligation — Part V social/affordable housing\n")
    lines.append("_Part V of the Planning and Development Act 2000 (as amended) is set by the **Oireachtas, "
                 "not the council** — so it is NOT a postcode lottery. The obligation arises for developments "
                 "of **9 or more units, or any housing on a site >0.1 ha**; developments of ≤4 units (or ≤0.1 ha) "
                 "may seek an exemption certificate. The reservation was raised from **10% to 20%** "
                 "(social + affordable/cost-rental) by the Affordable Housing Act 2021: 20% applies to land "
                 "bought on/after 1 Aug 2021, while land bought 2015–2021 stayed at 10% until **2026**, after "
                 "which 20% applies to all land. The per-council values below are just RESTATEMENTS of the "
                 "national rule — variation reflects plan vintage (older plans say 10%, newer 20%) or whether "
                 "the council documents it in its separate Housing Strategy, not local discretion._\n")
    lines.append("| Council | Plan | Part V as restated in the plan (verbatim) |")
    lines.append("|---|---|---|")
    for d in councils:
        v = get(d, "community.part_v_pct")
        if v:
            lines.append(f"| {d.get('council')} | {md_cell(d.get('plan_name'))} | {md_cell(v)} |")
    n_state = sum(1 for d in councils if get(d, "community.part_v_pct"))
    lines.append(f"\n_{n_state}/31 councils restate Part V in the DM chapter; the other {31 - n_state} carry it "
                 "in their Housing Strategy (a separate appendix) — absence here is documentation placement, "
                 "not a missing obligation. The threshold itself (units / site area) is **computable directly "
                 "from the applications feed** (`NumResidentialUnits`, `AreaofSite`) — see the Part V liability probe._\n")

    # numeric ranking appendix
    lines.append("\n## 3. Derived numeric ranking (auditable; parsed from the verbatim cells)\n")
    lines.append("_Conservative parse of the cleanly-numeric discretion fields, for ranking only. "
                 "`*` flags a cell that held more than one number (value = first match). "
                 "Always cross-check against the verbatim column._\n")
    rank_specs = [
        ("Rural min site area (m²)", "rural_min_site_area", parse_area_m2, False),
        ("EV charging (%)", "transport.ev_charging_pct", parse_pct, True),
        ("Part V social housing (%)", "community.part_v_pct", parse_pct, True),
        ("Public open space (%)", "open_space.public_open_space_pct", parse_pct, True),
    ]
    for label, path, parser, hi in rank_specs:
        rows = []
        for d in councils:
            raw = get(d, path)
            if not raw and path == "open_space.public_open_space_pct":
                raw = get(d, "public_open_space_pct")
            if raw:
                v, multi = parser(str(raw))
                if v is not None:
                    rows.append((v, multi, d.get("council")))
        rows.sort(key=lambda r: -r[0] if hi else r[0])
        lines.append(f"\n**{label}** ({'high→low' if hi else 'low→high'}, {len(rows)} councils with a parseable value):\n")
        lines.append("| Council | Value |")
        lines.append("|---|---|")
        for v, multi, c in rows:
            disp = f"{v:g}{'*' if multi else ''}"
            lines.append(f"| {c} | {disp} |")

    MD_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"wrote {OUT_DIR/'criteria_matrix.csv'}")
    print(f"wrote {OUT_DIR/'criteria_matrix_numeric.csv'}")
    print(f"wrote {MD_PATH}")
    print(f"councils={len(councils)} fields={len(paths)}")


if __name__ == "__main__":
    main()
