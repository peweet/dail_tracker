"""Tests for services/logging_cloud.py — the serving-layer logging config."""

from __future__ import annotations

import io
import json
import logging
import logging.config

import pytest

from services import logging_cloud as lc


@pytest.fixture(autouse=True)
def _restore_root_logging():
    """Each test reconfigures root logging; put it back afterwards."""
    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    lc.clear_context()
    yield
    lc.clear_context()
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def _capture(fmt: str = "json", level: str = "INFO") -> tuple[logging.Logger, io.StringIO]:
    """Configure logging, then redirect the installed handler to a buffer."""
    lc.configure_logging(fmt=fmt, level=level, service="test-svc")
    buf = io.StringIO()
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = buf
    return logging.getLogger("test.logger"), buf


# ---------------------------------------------------------------------------
# JSON shape
# ---------------------------------------------------------------------------


def test_json_format_emits_one_object_per_line():
    log, buf = _capture()
    log.info("hello")
    payload = json.loads(buf.getvalue().strip())
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "test.logger"
    assert payload["service"] == "test-svc"
    assert "ts" in payload


def test_extra_fields_become_top_level_json_keys():
    log, buf = _capture()
    log.info("req done", extra={"event": "request", "status": 200, "duration_ms": 12.5})
    payload = json.loads(buf.getvalue().strip())
    assert payload["event"] == "request"
    assert payload["status"] == 200
    assert payload["duration_ms"] == 12.5


def test_unserialisable_extra_falls_back_to_repr():
    log, buf = _capture()
    log.info("odd", extra={"thing": object()})
    payload = json.loads(buf.getvalue().strip())
    assert payload["thing"].startswith("<object object")


def test_exception_is_captured_as_a_field():
    log, buf = _capture()
    try:
        raise ValueError("boom")
    except ValueError:
        log.exception("failed")
    payload = json.loads(buf.getvalue().strip())
    assert "ValueError: boom" in payload["exception"]


def test_text_format_includes_request_id_slot():
    log, buf = _capture(fmt="text")
    log.info("plain")
    assert "[-]" in buf.getvalue()
    assert "plain" in buf.getvalue()


# ---------------------------------------------------------------------------
# Request correlation
# ---------------------------------------------------------------------------


def test_request_id_defaults_to_dash():
    log, buf = _capture()
    log.info("no request")
    assert json.loads(buf.getvalue().strip())["request_id"] == "-"


def test_bound_request_id_appears_on_every_record():
    log, buf = _capture()
    token = lc.set_request_id("abc123")
    try:
        log.info("first")
        log.info("second")
    finally:
        lc.reset_request_id(token)
    ids = [json.loads(line)["request_id"] for line in buf.getvalue().strip().splitlines()]
    assert ids == ["abc123", "abc123"]


def test_request_id_resets_after_token_reset():
    log, buf = _capture()
    token = lc.set_request_id("xyz")
    lc.reset_request_id(token)
    log.info("after")
    assert json.loads(buf.getvalue().strip())["request_id"] == "-"


def test_new_request_id_is_short_and_unique():
    a, b = lc.new_request_id(), lc.new_request_id()
    assert a != b
    assert len(a) == 12


# ---------------------------------------------------------------------------
# Redaction — must land BEFORE the first real secret exists
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw, must_not_contain",
    [
        ("Authorization: Bearer abcdef123456789", "abcdef123456789"),
        ("key sk-ant-api03-SECRETVALUE123 used", "SECRETVALUE123"),
        ("aws AKIAIOSFODNN7EXAMPLE here", "AKIAIOSFODNN7EXAMPLE"),
        ("api_key=supersecret123", "supersecret123"),
        ('token: "tok_live_9988776655"', "tok_live_9988776655"),
        ("password=hunter2xx", "hunter2xx"),
        ("https://user:p4ssw0rd@example.com/x", "p4ssw0rd"),
    ],
)
def test_secrets_are_redacted(raw: str, must_not_contain: str):
    assert must_not_contain not in lc.redact(raw)
    assert "REDACTED" in lc.redact(raw)


def test_redaction_applies_through_the_handler():
    log, buf = _capture()
    log.info("calling with api_key=leakedvalue99")
    out = buf.getvalue()
    assert "leakedvalue99" not in out
    assert "REDACTED" in out


def test_redaction_applies_to_args_not_just_message():
    log, buf = _capture()
    log.info("auth header was %s", "Bearer zzzzzzzzzzzz")
    out = buf.getvalue()
    assert "zzzzzzzzzzzz" not in out


def test_sha256_digests_are_NOT_redacted():
    """The repo logs sha256 digests legitimately (tools/data_manifest.py).

    A generic long-hex redaction rule would corrupt them, so the patterns key off
    an explicit label or vendor prefix instead. This pins that decision.
    """
    digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    assert lc.redact(f"sha256 {digest}") == f"sha256 {digest}"


def test_ordinary_text_is_untouched():
    msg = "Loaded 1,083 rows from procurement_payments_fact for Mayo County Council"
    assert lc.redact(msg) == msg


# ---------------------------------------------------------------------------
# The uvicorn hazard — the reason this module exists separately
# ---------------------------------------------------------------------------


def test_configure_logging_applies_even_when_handlers_already_exist():
    """logging_setup.setup_logging early-returns if root already has handlers.

    Under uvicorn that is a silent no-op. logging_cloud must NOT do that, or the
    served process runs with someone else's logging configuration.
    """
    root = logging.getLogger()
    sentinel = logging.StreamHandler(io.StringIO())
    root.addHandler(sentinel)
    assert root.handlers

    lc.configure_logging(fmt="json", level="INFO")

    assert sentinel not in root.handlers, "pre-existing handler should have been replaced"
    assert len(root.handlers) == 1


def test_configure_logging_is_idempotent():
    lc.configure_logging(fmt="json")
    lc.configure_logging(fmt="json")
    assert len(logging.getLogger().handlers) == 1


def test_uvicorn_access_logger_is_silenced():
    """Our middleware emits the access line with a request id; uvicorn's is a dupe."""
    lc.configure_logging(fmt="json")
    access = logging.getLogger("uvicorn.access")
    assert access.propagate is False
    assert access.handlers == []


def test_uvicorn_error_logger_propagates_to_our_handler():
    lc.configure_logging(fmt="json")
    err = logging.getLogger("uvicorn.error")
    assert err.propagate is True
    assert err.handlers == []


# ---------------------------------------------------------------------------
# Environment resolution
# ---------------------------------------------------------------------------


def test_level_read_from_environment(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    lc.configure_logging()
    assert logging.getLogger().level == logging.WARNING


def test_invalid_level_falls_back_to_info(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "LOUD")
    lc.configure_logging()
    assert logging.getLogger().level == logging.INFO


def test_format_read_from_environment(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    log, buf = _capture(fmt=None)  # type: ignore[arg-type]
    log.info("env driven")
    json.loads(buf.getvalue().strip())  # parses => json formatter selected


def test_service_name_read_from_environment(monkeypatch):
    monkeypatch.setenv("LOG_SERVICE", "api-prod")
    lc.configure_logging(fmt="json")
    buf = io.StringIO()
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = buf
    logging.getLogger("x").info("hi")
    assert json.loads(buf.getvalue().strip())["service"] == "api-prod"


def test_noisy_third_party_loggers_are_quieted():
    """httpx logs one INFO line per outbound request — observed in a live smoke run.

    Left at INFO those bury our own records and cost money per GB ingested.
    """
    lc.configure_logging(fmt="json", level="INFO")
    for name in ("httpx", "urllib3", "botocore"):
        assert logging.getLogger(name).level == logging.WARNING


def test_quieted_library_record_is_dropped():
    _, buf = _capture()
    logging.getLogger("httpx").info("HTTP Request: GET http://example/x 200 OK")
    assert buf.getvalue() == ""


def test_quieted_library_still_reports_warnings():
    _, buf = _capture()
    logging.getLogger("httpx").warning("connection pool full")
    assert "connection pool full" in buf.getvalue()


def test_no_file_handler_is_installed():
    """Containers collect stdout; a file handler would vanish on restart."""
    lc.configure_logging(fmt="json")
    assert not any(isinstance(h, logging.FileHandler) for h in logging.getLogger().handlers)


# ---------------------------------------------------------------------------
# Cross-process correlation — bound static context
# ---------------------------------------------------------------------------


def test_bound_context_appears_as_json_fields():
    lc.bind_context(run_id="2026-07-20T10-00-00Z-abcd1234", step="poll_lobbying")
    log, buf = _capture()
    log.info("working")
    payload = json.loads(buf.getvalue().strip())
    assert payload["run_id"] == "2026-07-20T10-00-00Z-abcd1234"
    assert payload["step"] == "poll_lobbying"


def test_bind_context_ignores_none():
    lc.bind_context(run_id=None, step="x")
    assert lc.get_context() == {"step": "x"}


def test_bind_context_is_additive():
    lc.bind_context(run_id="r1")
    lc.bind_context(step="s1")
    assert lc.get_context() == {"run_id": "r1", "step": "s1"}


def test_explicit_record_field_wins_over_bound_context():
    lc.bind_context(run_id="process-level")
    log, buf = _capture()
    log.info("per-line override", extra={"run_id": "line-level"})
    assert json.loads(buf.getvalue().strip())["run_id"] == "line-level"


def test_context_survives_reconfigure():
    """run_id is bound once at startup; changing format later must not drop it."""
    lc.bind_context(run_id="persist")
    lc.configure_logging(fmt="text")
    lc.configure_logging(fmt="json")
    log = logging.getLogger("x")
    buf = io.StringIO()
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler):
            h.stream = buf
    log.info("still here")
    assert json.loads(buf.getvalue().strip())["run_id"] == "persist"


# ---------------------------------------------------------------------------
# cloud_mode() env resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("val", ["1", "true", "TRUE", "yes"])
def test_cloud_mode_on_via_flag(monkeypatch, val):
    monkeypatch.setenv("DAIL_LOG_CLOUD", val)
    assert lc.cloud_mode() is True


def test_cloud_mode_on_via_log_format(monkeypatch):
    monkeypatch.delenv("DAIL_LOG_CLOUD", raising=False)
    monkeypatch.setenv("LOG_FORMAT", "json")
    assert lc.cloud_mode() is True


def test_cloud_mode_off_by_default(monkeypatch):
    monkeypatch.delenv("DAIL_LOG_CLOUD", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    assert lc.cloud_mode() is False
