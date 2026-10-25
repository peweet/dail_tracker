---
name: Agent task
about: One agent-sized change with a test, in the five-section task packet
title: ""
labels: agent-ready
assignees: ""
---

<!-- One issue = one change a single session can land with one test. Split anything larger. -->

## Objective

One observable outcome. What is true after this lands that is not true now.

## Scope

Files, tools and systems the change may touch. Name the ones it must not.

## Invariants

Repo rules this change must keep: logic firewall, never-sum money grains, provenance, join
keys, parquet via `save_parquet`, assertions and baselines untouched.

## Acceptance

Binary checks that produce evidence. A command and its expected result, or a test node.

## Result contract

What the closing report must contain: changed files, commands run with observed output,
unresolved questions, residual risk.
