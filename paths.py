"""Canonical project-root resolution — the single source of truth for "where is root".

Deliberately side-effect-free: importing this module does NOT touch the filesystem
(no dir creation, no logging, no IO). That is the whole point — modules that only
need the project root (refresh chains, the Iris ETLs, the UI's SQL-view registry)
can import `PROJECT_ROOT` from here without dragging in `config.init_dirs()` or any
other import-time work.

Historically the project root was re-derived from `Path(__file__).resolve().parent`
in ~12 places (each `*_refresh.py`, the Iris ETLs, `config.py`, the SQL registry).
That made every file move silently shift paths. This module collapses those to one
definition; `config.py` and everything else now import from here.

When the repo moves to a `src/dail_tracker/` layout this file becomes
`src/dail_tracker/infra/paths.py` and `PROJECT_ROOT` is computed relative to the
package root instead of the file's parent — a one-line change, in one place.
"""

from __future__ import annotations

from pathlib import Path

# This file lives at the repo root today, so its parent IS the project root.
PROJECT_ROOT: Path = Path(__file__).resolve().parent
