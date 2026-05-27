"""Root-logger configuration.

Pipeline orchestrator passes the run_id so its log lives at
logs/runs/<run_id>/pipeline.log — keeping each run self-contained and
zip-friendly for CI artifact upload.

Step subprocesses spawned by pipeline.py see the env var DAIL_PIPELINE_RUN_ID;
they skip adding a file handler because the orchestrator already captures
their stdout+stderr into the per-step log file.

Scripts run standalone (no env var, no run_id) fall back to logs/pipeline.log.
"""

from __future__ import annotations

import logging
import os

from config import LOG_DIR
from services.run_paths import ENV_RUN_ID, run_dir


def setup_logging(run_id: str | None = None) -> None:
    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

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

    log_path = run_dir(run_id) / "pipeline.log" if run_id is not None else LOG_DIR / "pipeline.log"

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


if __name__ == "__main__":
    setup_logging()
    logging.info("Logging setup complete. This is a test log entry.")
