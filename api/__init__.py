"""Dáil Tracker read-only JSON API (FastAPI over dail_tracker_core).

Optional component — install with the ``api`` extra (`uv sync --extra api`). The
deployed Streamlit app does NOT import this package. Run with:

    uvicorn api.main:app --reload

See doc/archive/API_LAYER_PLAN.md for the architecture + real-world precedents.
"""

__version__ = "1.0.0"
