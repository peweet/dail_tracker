---
name: data-question
description: Answer a question about the tracker's data the cheap, correct way
argument-hint: "[question]"
agent: agent
tools: ['dail-tracker/*']
---

## Objective

Answer this question about the Dáil Tracker data: ${input:question}

## Scope

Work in this order, using the dail-tracker MCP tools only; never read a parquet:

1. `search_project('<topic>')` to locate the relevant datasets, views, or docs.
2. `list_datasets` or `describe_dataset` to confirm grain, year span, and columns.
3. Call the specific domain tools for the requested facts.
4. If money is involved, obey `never_sum_with`; never add procurement-awarded, payments,
   budgets, donations, or allowances across grains.

## Invariants

Present only what the data shows. Do not infer missing facts. Cite source URLs returned by the
tools.

## Acceptance

- Dataset grain, coverage, and columns are confirmed before a numerical answer.
- Every material claim is bounded by returned evidence and money-grain boundaries are kept.
- Unknown or unavailable facts are reported as such rather than inferred.

## Result contract

Lead with the answer, then give supporting facts, caveats, and source links. Name the MCP tools
used; do not imply that a local parquet or unreturned source was inspected.
