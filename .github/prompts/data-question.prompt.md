---
name: data-question
description: Answer a question about the tracker's data the cheap, correct way
argument-hint: "[question]"
agent: agent
tools: ['dail-tracker/*']
---

Answer this question about the Dáil Tracker data: ${input:question}

Work in this order, using the dail-tracker MCP tools only — never read a parquet:
1. `search_project('<topic>')` to locate the dataset(s)/view(s)/doc(s) that cover it.
2. `list_datasets` / `describe_dataset` to confirm the exact fact, its grain, year span and
   columns before querying.
3. Call the specific domain tool(s) for the numbers.
4. If money is involved, obey each fact's `never_sum_with` class — never add figures across
   procurement-awarded, payments, budgets, donations or allowances.

Present only what the data shows (no inference) and cite the source URLs the tools return.
