# Suggested Agent Setup

Use small agents so Claude does not burn tokens or cross architectural boundaries.

## Explore agent

Find the minimum relevant files. Do not edit.

## Shape agent

Plan the UX before code. Existing pages are functional references, not design references.

## Contract agent

Turns a page idea into YAML. Missing data shapes become `TODO_PIPELINE_VIEW_REQUIRED`.

## Data-view agent

Implements pipeline-owned registered analytical views. This is where joins, aggregations, flags, rankings, fuzzy matching, raw Parquet scans, and view registration belong.

## Streamlit frontend agent

Builds only the contract-approved Streamlit page. It may be bold on UI, but strict on data semantics.

## Reviewer agent

Checks:
- logic firewall
- contract compliance
- visual boldness
- UX/accessibility
- design drift
- SQL safety/performance
- TODO pipeline wiring

Recommended sequence:

```text
Explore -> Shape -> Contract -> Data-view if needed -> Frontend -> Reviewer -> Boldness pass if still too similar
```
