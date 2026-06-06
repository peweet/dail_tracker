"""
Shared SQL-view registration — now a thin re-export shim over dail_tracker_core.db.

History: this module owned the project-root / sql_views path, the
``'data/...'`` → absolute-path rewrite, and the glob→substitute→absolutize→execute
registration loop. During the Streamlit-uncoupling work that exact logic was
moved into the Streamlit-free core (``dail_tracker_core/db.py``) so a future
FastAPI/React interface can register views without importing anything under
``utility/``. The two copies were byte-identical.

Now that every data-access module builds its connection via
``dail_tracker_core.db`` directly, this module re-exports the core symbols so the
remaining importers (the SQL-view test suite) keep working unchanged. There is a
single implementation — the core one.
"""

from __future__ import annotations

from dail_tracker_core.db import (  # noqa: F401 — re-exported for backwards compatibility
    PROJECT_ROOT,
    SQL_VIEWS_DIR,
    absolutize_data_paths,
    connect_with_views,
    register_views,
)

__all__ = [
    "PROJECT_ROOT",
    "SQL_VIEWS_DIR",
    "absolutize_data_paths",
    "connect_with_views",
    "register_views",
]
