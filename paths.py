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

import os
import tempfile
from pathlib import Path

# This file lives at the repo root today, so its parent IS the project root.
PROJECT_ROOT: Path = Path(__file__).resolve().parent


def absolute_path(value: str | Path, *, base: Path = PROJECT_ROOT) -> Path:
    """Resolve ``value`` without depending on the process working directory.

    Relative configuration values are anchored to ``base``; absolute values are
    preserved. The function only resolves a path and never creates it.
    """
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


def configured_path(name: str, default: str | Path, *, base: Path = PROJECT_ROOT) -> Path:
    """Resolve an environment-configurable path to an absolute path."""
    return absolute_path(os.environ.get(name, default), base=base)


def runtime_path(*parts: str, env_var: str = "DAIL_RUNTIME_DIR") -> Path:
    """Return an absolute, contained path in the process runtime/cache area.

    ``DAIL_RUNTIME_DIR`` can relocate all transient artefacts. When unset, the
    OS-native temporary directory is used instead of a platform-specific path.
    Child components may not escape the selected root.
    """
    default_root = Path(tempfile.gettempdir()) / "dail_extractor"
    root = configured_path(env_var, default_root)
    candidate = root.joinpath(*parts).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"runtime path escapes {env_var}: {parts!r}") from exc
    return candidate
