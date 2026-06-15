# Cross-council criteria map — schema v2 (full-chapter extraction)

Pilot output → `_criteria_map/v2/<council_slug>.json`. Superset of v1 (all v1 keys preserved
at top level) plus grouped sections capturing **everything quantitative or rule-like in the DM
chapter**, not just the slim v1 subset.

## No-inference rules (unchanged, load-bearing)
- Quote the figure/wording **verbatim** from the council's own DM chapter (or the named
  appendix/section it cross-refers). Keep units as written (`"2000 m²"`, `"22 m"`, `"15%"`).
- If a standard is **not stated**, use `null`. **Never** borrow another council's value or infer.
- If stated as prose/range, capture the prose verbatim (trimmed).
- Every non-null value carries a provenance section ref in the parallel `_prov` object
  (e.g. `"public_open_space_pct": "15%"` → `_prov.public_open_space_pct: "15.6.6"`).
- Many councils cite **national guidelines** rather than set their own number (e.g. apartment
  floor areas → "per Sustainable Urban Housing: Design Standards for New Apartments 2020").
  Capture that reference verbatim — it is the rule they apply.

## Structure
```jsonc
{
  "schema_version": 2,
  "council": "...", "slug": "...", "authority_type": "...",
  "plan_name": "...", "dm_location": "...", "extract_status": "complete|partial",

  // --- v1 fields preserved verbatim (copy from _criteria_map/<slug>.json) ---
  "rural_min_site_area": null, "rural_site_scaling": null, "ribbon_development_rule": null,
  "road_setback_national_m": null, "road_setback_regional_m": null, "road_setback_local_m": null,
  "sightline_x_m": null, "sightline_y": null, "wastewater_standard": null,
  "residential_density_dph": null, "plot_ratio": null, "site_coverage_pct": null,
  "separation_distance_m": null, "private_open_space": null, "public_open_space_pct": null,
  "car_parking_dwelling": null, "building_height": null,
  "trigger_AA": null, "trigger_EIA": null, "trigger_FRA": null, "trigger_AHIA": null,
  "trigger_EcIA": null, "trigger_VIA": null, "trigger_TTA": null,

  // --- v2 expansion ---
  "residential": {
    "housing_mix": null, "dwelling_min_floor_areas": null,
    "apartment_min_floor_areas": null, "apartment_dual_aspect_pct": null,
    "apartment_storage": null, "apartment_floor_to_ceiling": null,
    "extensions": null, "family_flat_max_gfa": null, "domestic_garage_pod": null,
    "dormer_standard": null, "boundary_treatment_height": null, "naming": null,
    "taking_in_charge": null, "build_to_rent": null, "co_living": null,
    "student_accommodation": null, "bonds": null
  },
  "open_space": {
    "public_open_space_pct": null, "public_open_space_qual": null,
    "private_open_space_house": null, "balcony_min_areas": null, "play_space": null
  },
  "community": {
    "childcare_provision": null, "part_v_pct": null,
    "social_infrastructure_audit": null, "education": null, "health": null
  },
  "transport": {
    "car_parking_standards": null, "cycle_parking": null, "ev_charging_pct": null,
    "building_line": null, "stopping_distances_sightlines": null,
    "universal_access": null, "loading_coach_taxi": null
  },
  "water_environment": {
    "suds_requirement": null, "surface_water": null, "bin_refuse_storage": null,
    "daylight_sunlight": null, "noise": null
  },
  "non_residential": {
    "retail_sequential_test": null, "retail_floorspace_caps": null,
    "shopfront_signage": null, "advertising": null, "employment_industry": null,
    "business_tech_parks": null, "home_based_economic": null,
    "agricultural_separation": null, "seveso": null, "petrol_stations": null,
    "amusement_arcades": null, "hot_food_takeaway": null, "vape_betting": null
  },
  "energy_infrastructure": {
    "wind_energy_setback": null, "solar": null, "telecommunications": null,
    "overhead_lines": null, "data_centre": null
  },
  "heritage_natural": {
    "archaeology_zones": null, "protected_structures_curtilage": null,
    "biodiversity_green_infra": null
  },

  "_prov": { /* field_name -> section/appendix ref, only for non-null values */ },
  "notes": "cross-chapter caveats, draft-vs-adopted, extraction limits"
}
```

## Process
- v1 fields: copy verbatim from the existing `_criteria_map/<slug>.json` (don't re-derive).
- v2 fields: read the DM chapter (+ named cross-ref appendices) in `raw/` and fill verbatim.
- Source = the council's own `raw/` PDF/HTML, extracted locally (`pdftotext -layout` / `fitz` /
  HTML parse). Web search locates docs but cannot extract numbers reliably (§23.11).
- A field genuinely absent from the chapter stays `null` — that is data, not a gap to paper over.
