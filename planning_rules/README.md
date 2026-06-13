# Planning Rules — per-authority Development Management Standards

The **"rule book"** Irish planning officers apply when deciding applications is the
**Development Management Standards (DM Standards)** chapter of each local authority's
**Development Plan** — the quantitative standards (residential density, car-parking ratios,
rural one-off-house site minimums, separation distances, sightlines/visibility splays,
setbacks, plot ratios, etc.) plus the **assessment triggers** (when Appropriate Assessment,
EIA, heritage/conservation, flood-risk assessments become mandatory).

> ⚠️ **This rule book is NOT universal and NOT static.**
> - **Per-authority:** each of Ireland's **31 local authorities** adopts its own Development
>   Plan; the DM-standards chapter differs in both content *and* chapter number
>   (Galway Co. = Ch.15, Fingal = Ch.14, …). The same proposal can pass in one county and
>   fail next door.
> - **Time-versioned:** plans ran on a **6-year cycle** under the Planning & Development Act
>   2000; the **Planning & Development Act 2024** moves this to a **10-year lifespan with a
>   mandatory 5-year review** (commencing in phases 2025+). Plans are also amended mid-cycle
>   by **variations**. A national layer above them (NPF, RSES, and the new **National Planning
>   Statements** replacing **Section 28 Guidelines** / SPPRs) can **override** local standards.
>
> Every extract here is therefore stamped with its **plan name, years, chapter, and fetch
> date** in `_source.json`, and must be re-pulled when a council adopts a new plan or variation.

## Layout

```
planning_rules/
  city_councils/                  # Dublin City, Cork City, Galway City
    <council_slug>/
      dm_standards.md             # extracted quantitative standards (tracked)
      required_assessments.md     # assessment triggers: AA / EIA / heritage / flood (tracked)
      _source.json                # provenance: plan name, years, chapter, URL, fetch date, status
      raw/                        # original HTML/PDF download (gitignored — regenerable source)
  city_and_county_councils/       # Limerick, Waterford (merged authorities)
    <council_slug>/ ...
  county_councils/                # the remaining 26 county councils
    <council_slug>/ ...
```

## Conventions

- **Source of truth = the markdown extracts.** The `raw/` downloads are gitignored
  (regenerable), matching the repo's `*.pdf`/`*.html` convention. `_source.json` is negated
  back in (it would otherwise be swallowed by the blanket `*.json` rule).
- **No-inference:** extracts record only what the plan states. Where a standard is absent or
  the plan is PDF-only/not-found, `_source.json.status` says so honestly — never fabricated.
- Most councils share the **`consult.<council>.ie`** consultation portal (uniform HTML scrape);
  **Cork City** runs its own portal, and a few are PDF-only.

See [MANIFEST.md](MANIFEST.md) for per-authority status. Scoping context:
[../doc/PLANNING_PERMISSION_SCOPING.md](../doc/PLANNING_PERMISSION_SCOPING.md) §12.
