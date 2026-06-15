"""Citizen siting-check engine (Streamlit-free core).

Given a point (and a development type), evaluate which planning *issues* the site
triggers, quote the governing rule verbatim from the per-council rulebook, and attach
ACP precedent — never a grant/refuse verdict or a design prescription (see
doc/PLANNING_SITING_DECISION_TREE.md §1 + memory feedback_no_inference_in_app).

Layout (built incrementally):
  catalogue.py — load planning_rules/issue_catalogue.yaml into typed nodes
  rulebook.py  — resolve a node's rule_ref to verbatim text for the council in force
  layers.py    — load the ingested GeoParquet designation layers (Phase 0)
  council.py   — which local authority a point falls in
  predicates.py / engine.py — evaluate triggers and assemble the result

All logic lives here; the Streamlit page is a thin renderer over engine.evaluate().
"""
