# Suggested Agent Setup

Use small agents so Claude does not burn tokens or cross architectural boundaries.

## Explore agent

Find the minimum relevant files. Do not edit.

Use when locating:
- page contract
- existing page file
- shared CSS
- data-access helper
- navigation file

## Contract agent

Turns a page idea into YAML.

It must not implement UI or invent data logic. Missing data shapes become `TODO_PIPELINE_VIEW_REQUIRED`.

## Data-view agent

Implements pipeline-owned DuckDB views.

This is where joins, aggregations, flags, rankings, fuzzy matching, and raw Parquet scans belong.

## Streamlit frontend agent

Builds only the contract-approved Streamlit page.

It may use retrieval SQL only. It must reuse shared CSS. It must not add features outside the contract.

## Reviewer agent

Checks:
- logic firewall
- contract compliance
- UX/accessibility
- design drift
- SQL safety/performance

Recommended sequence:

```text
Explore -> Contract -> Data-view -> Frontend -> Reviewer
```
