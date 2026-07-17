"""Standard ``__main__`` harness for standalone extractor runs.

94 of 103 extractors carry a ``__main__`` block and ~30 of them hand-configure
logging (``logging.basicConfig`` variants) while most re-type the same UTF-8
stdout dance. This harness collapses that boilerplate to one line::

    if __name__ == "__main__":
        run_extractor(main)

What it does, in order:
  1. Reconfigures stdout/stderr to UTF-8 (Windows consoles otherwise break on
     Irish names/accents — the reason PYTHONIOENCODING is required everywhere).
  2. Routes logging through ``services.logging_setup.setup_standalone_logging``
     so the file log lands at ``logs/standalone/<name>.log`` (rotated, capped)
     instead of an ad-hoc basicConfig / repo-root file.
  3. Runs ``main_fn`` with top-level error handling: an unhandled exception is
     logged WITH traceback and exits 1 (so pipeline.py / schedulers see the
     failure), Ctrl+C exits 130 quietly, and an int return becomes the exit
     code (argparse-style mains that return a status keep working).

The harness deliberately does NOT own argparse: extractors' flags are too
varied to template (--only/--merge/--years/--rebuild…), and ``main_fn`` parsing
its own ``sys.argv`` inside the harness works unchanged.
"""

from __future__ import annotations

import contextlib
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from services.logging_setup import setup_standalone_logging


def run_extractor(main_fn: Callable[[], object], *, name: str | None = None) -> None:
    """Run an extractor's ``main()`` under the standard standalone harness.

    ``name`` labels the log file (``logs/standalone/<name>.log``); it defaults
    to the invoked script's stem, which is right for the normal
    ``python extractors/<x>.py`` case.
    """
    name = name or Path(sys.argv[0]).stem or "extractor"
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
        sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    setup_standalone_logging(name)
    try:
        rv = main_fn()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception:
        logging.getLogger(name).exception("extractor failed: %s", name)
        raise SystemExit(1) from None
    raise SystemExit(rv if isinstance(rv, int) else 0)
