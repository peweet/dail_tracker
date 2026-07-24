"""logging_setup delegates to cloud JSON when env opts in — else keeps files.

This is the change that makes the ETL cloud-followable: run_id stops being a
directory name and becomes a log field, so a collector can group every discrete
process's lines by it. Verified here without writing to the real logs/ tree.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from services import logging_cloud as lc
from services import logging_setup
from services.run_paths import ENV_RUN_ID


@pytest.fixture(autouse=True)
def _isolate_logging():
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    # A real process starts with no root handlers; pytest leaves its own. Clear
    # them so the `if root.handlers: return` guard in the LOCAL path behaves as it
    # would on a fresh interpreter.
    root.handlers[:] = []
    lc.clear_context()
    yield
    lc.clear_context()
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def _buf_from_root() -> io.StringIO:
    buf = io.StringIO()
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = buf
    return buf


def test_setup_logging_cloud_mode_emits_json_with_run_id(monkeypatch):
    monkeypatch.setenv("DAIL_LOG_CLOUD", "1")
    logging_setup.setup_logging(run_id="2026-07-20T09-00-00Z-deadbeef")
    buf = _buf_from_root()
    logging.getLogger("etl.step").info("processing rows")
    payload = json.loads(buf.getvalue().strip())
    assert payload["run_id"] == "2026-07-20T09-00-00Z-deadbeef"
    assert payload["service"] == "pipeline"
    assert payload["message"] == "processing rows"


def test_setup_logging_cloud_mode_falls_back_to_env_run_id(monkeypatch):
    monkeypatch.setenv("DAIL_LOG_CLOUD", "1")
    monkeypatch.setenv(ENV_RUN_ID, "env-run-77")
    logging_setup.setup_logging(run_id=None)
    buf = _buf_from_root()
    logging.getLogger("x").info("hi")
    assert json.loads(buf.getvalue().strip())["run_id"] == "env-run-77"


def test_standalone_cloud_mode_stamps_step_name(monkeypatch):
    monkeypatch.setenv("DAIL_LOG_CLOUD", "1")
    monkeypatch.setenv(ENV_RUN_ID, "run-abc")
    logging_setup.setup_standalone_logging("lobbying_refresh")
    buf = _buf_from_root()
    logging.getLogger("lobbying").info("fetched")
    payload = json.loads(buf.getvalue().strip())
    assert payload["service"] == "lobbying_refresh"
    assert payload["step"] == "lobbying_refresh"
    assert payload["run_id"] == "run-abc"


def _all_env_local(monkeypatch):
    monkeypatch.delenv("DAIL_LOG_CLOUD", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    monkeypatch.delenv(ENV_RUN_ID, raising=False)


def test_local_standalone_writes_a_text_file_not_json(monkeypatch, tmp_path):
    """Default (no cloud env) keeps the existing rotated-file + text behaviour.

    Asserted on the FILE, not a stream buffer: RotatingFileHandler subclasses
    StreamHandler, so swapping ".stream" would also hijack the file handler.
    """
    _all_env_local(monkeypatch)
    monkeypatch.setattr(logging_setup, "STANDALONE_DIR", tmp_path / "standalone")
    # The local path early-returns if root already has handlers (its idempotency
    # guard). pytest's logging plugin attaches one, so clear at the call site to
    # emulate a fresh process.
    logging.getLogger().handlers[:] = []
    logging_setup.setup_standalone_logging("probe_local")

    assert logging_cloud_mode_is_off()
    logging.getLogger("probe").info("plain line")
    for h in logging.getLogger().handlers:
        h.flush()

    log_file = tmp_path / "standalone" / "probe_local.log"
    assert log_file.exists(), "local mode must still write a rotated file"
    content = log_file.read_text(encoding="utf-8")
    assert "plain line" in content
    # Text format, so a line is NOT a JSON object.
    with pytest.raises(json.JSONDecodeError):
        json.loads(content.strip().splitlines()[0])


def logging_cloud_mode_is_off() -> bool:
    return not lc.cloud_mode()
