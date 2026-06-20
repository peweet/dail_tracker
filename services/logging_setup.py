"""Root-logger configuration.

Single rule for the whole project: **every log file lives under `logs/`**.

    logs/
    ├── runs/<run_id>/…          # pipeline runs (orchestrator, per-step capture)
    ├── standalone/<name>.log    # standalone script runs (rotated, capped)
    ├── pipeline.log             # legacy fallback (rotated, capped)
    └── latest_run_id.txt

Pipeline orchestrator passes the run_id so its log lives at
logs/runs/<run_id>/pipeline.log — keeping each run self-contained and
zip-friendly for CI artifact upload.

Step subprocesses spawned by pipeline.py see the env var DAIL_PIPELINE_RUN_ID;
they skip adding a file handler because the orchestrator already captures
their stdout+stderr into the per-step log file.

Standalone scripts (refresh/ETL entrypoints) call `setup_standalone_logging`
so their file output lands at logs/standalone/<name>.log — never the repo root.
The rotating handlers cap each file at 5 MB × 3 backups, so no log can grow into
the unbounded 89 MB monster the old plain-FileHandler fallback produced.
"""

from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler

from config import LOG_DIR
from services.run_paths import ENV_RUN_ID, run_dir

STANDALONE_DIR = LOG_DIR / "standalone"

# Cap any single log file so a long-running or repeatedly-appended script can't
# recreate the old unbounded logs/pipeline.log.
_MAX_BYTES = 5 * 1024 * 1024
_BACKUP_COUNT = 3
_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def setup_logging(run_id: str | None = None) -> None:
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    under_orchestrator = run_id is None and os.environ.get(ENV_RUN_ID) is not None
    if under_orchestrator:
        # Parent process is already tee-ing our stdout/stderr to the per-step
        # log; adding a second FileHandler here would just duplicate writes
        # into the legacy logs/pipeline.log path.
        return

    if run_id is not None:
        # One run = one bounded file; a plain handler is fine and keeps the
        # per-run log a single contiguous artifact for CI upload.
        log_path = run_dir(run_id) / "pipeline.log"
        file_handler: logging.Handler = logging.FileHandler(log_path, encoding="utf-8")
    else:
        # Legacy fallback for standalone scripts that call setup_logging() with
        # no run_id. Rotate it so it can never grow unbounded again.
        log_path = LOG_DIR / "pipeline.log"
        file_handler = RotatingFileHandler(log_path, maxBytes=_MAX_BYTES, backupCount=_BACKUP_COUNT, encoding="utf-8")

    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def setup_standalone_logging(name: str, level: int = logging.INFO) -> None:
    """Console + rotated file logging for a standalone script run.

    File lands at ``logs/standalone/<name>.log`` (capped, rotated) so standalone
    entrypoints stop scattering logs across the repo root via manual
    ``> foo.log`` redirects. Pass a stable ``name`` per script (its stem).

    When the script is invoked as a pipeline step (DAIL_PIPELINE_RUN_ID set),
    the file handler is skipped — the orchestrator already captures stdout/stderr
    into logs/runs/<run_id>/steps/. Idempotent: a second call is a no-op.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    root_logger.setLevel(level)
    formatter = logging.Formatter(_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    if os.environ.get(ENV_RUN_ID) is not None:
        return

    STANDALONE_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        STANDALONE_DIR / f"{name}.log",
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


if __name__ == "__main__":
    setup_logging()
    logging.info("Logging setup complete. This is a test log entry.")
