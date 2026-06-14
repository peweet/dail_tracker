# Cross-council criteria map — schema

Each council → one `<council_slug>.json` here, extracted from that council's
`dm_standards.md` + `required_assessments.md`. **No-inference rules:**
- Quote the figure **verbatim** from the extract (e.g. `"2000 m²"`, `"22 m"`, `"35 dph"`).
- If a criterion is **not stated** in that council's extract, use `null` (never guess/borrow another council's value).
- If it's stated but as prose/range, capture the prose (e.g. `"35-50 dph; 35 default"`).

## Fields (flat JSON object)

```json
{
  "council": "Galway County Council",
  "slug": "galway_county_council",
  "authority_type": "county_council",
  "plan_name": "Galway County Development Plan 2022-2028",
  "dm_location": "Chapter 15",
  "extract_status": "complete|partial",

  "rural_min_site_area": "2000 m²",
  "rural_site_scaling": "+10 m² per 1 m² of house >200 m²",
  "ribbon_development_rule": "5+ houses per 250 m of road frontage",
  "road_setback_national_m": "90 m",
  "road_setback_regional_m": "25 m (35 m former national)",
  "road_setback_local_m": "15 m",
  "sightline_x_m": "2.4 m",
  "sightline_y": "215/160/120/90/70/50/35 m by design speed",
  "wastewater_standard": "EPA CoP single houses; IS EN 12566",

  "residential_density_dph": "35 dph default; higher at transit nodes",
  "plot_ratio": null,
  "site_coverage_pct": "75% single-storey / 60% two-storey",
  "separation_distance_m": "22 m opposing first-floor windows",
  "private_open_space": null,
  "public_open_space_pct": null,
  "car_parking_dwelling": null,
  "building_height": "context; landmark height at urban nodes",

  "trigger_AA": "screened on all; NIS where effect not excludable",
  "trigger_EIA": "Sch.5 thresholds",
  "trigger_FRA": "Justification Test in Flood Zone A/B",
  "trigger_AHIA": "works to a Protected Structure",
  "trigger_EcIA": "within/near SAC/SPA/NHA",
  "trigger_VIA": "Class 3/4 landscape / protected views",
  "trigger_TTA": "above traffic thresholds",

  "notes": "anything notable / cross-chapter caveats"
}
```

Consolidated outputs (built after the fan-out): `criteria_matrix.csv` (one row/council) +
`../CRITERIA_MAP.md` (comparison tables). Source of truth = the per-council `dm_standards.md`.
