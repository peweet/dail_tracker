"""Import-first bootstrap for the Streamlit entry point.

``app.py`` imports this on line 1, before ``streamlit``. Two jobs, both of which have
to happen before any other import runs:

1. Put the repo root on ``sys.path`` so ``services`` / ``dail_tracker_core`` resolve
   when streamlit execs ``utility/app.py`` (it adds ``utility/`` to the path, not the root).
2. Import ``services.runtime_env``, which caps the BLAS thread count. Streamlit pulls in
   pandas, and an uncapped OpenBLAS reserves ~650 MB of commit per process on this
   20-core box versus ~142 MB at four threads.

It exists as its own module so ``app.py`` keeps a clean block of imports — inlining the
``sys.path`` code would make every import below it an E402.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import services.runtime_env  # noqa: E402,F401
