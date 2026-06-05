"""dail_tracker_core — Streamlit-free query/business logic.

Pilot package (see doc/fastapi_query_core_uncoupling_plan.md). Modules here must
NOT import streamlit: the same logic backs the Streamlit UI today and can back a
FastAPI/React interface later. Pure derivation lives here; rendering stays in the
interface layer.
"""
