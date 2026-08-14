"""Logging for the SERVING layer — the API, and anything running in a container.

Sibling to `services/logging_setup.py`, not a replacement. The two exist because
their requirements genuinely differ:

    logging_setup.py   ETL / pipeline / standalone scripts
                       -> rotated FILES under logs/, human-readable, run-scoped,
                          and guarded by `if root_logger.handlers: return` so a
                          script invoked as a pipeline step doesn't double-log.

    logging_cloud.py   FastAPI / containers
                       -> STDOUT only, optionally JSON, request-correlated, and
                          deliberately NOT guarded — see below.

**Why the missing guard is the whole point.** `logging_setup.setup_logging` early-returns
when the root logger already has handlers (logging_setup.py:46). Under uvicorn that is a
silent failure: uvicorn installs its own root handlers at startup, so a later call would
no-op and the app's configuration would never apply. `dictConfig` here replaces root
handlers unconditionally, which is correct for a process we own end to end.

Environment
-----------
    LOG_LEVEL    DEBUG|INFO|WARNING|ERROR       (default INFO)
    LOG_FORMAT   json|text                      (default text locally, json in container)
    LOG_SERVICE  name stamped on every record    (default "dail-tracker")

Nothing here writes files. In a container the platform collects stdout; writing to
disk would vanish on restart and consume the container filesystem meanwhile.
"""

from __future__ import annotations

import json
import logging
import logging.config
import os
import re
import sys
import uuid
from collections.abc import Mapping
from contextvars import ContextVar
from itertools import islice
from typing import Any

# ---------------------------------------------------------------------------
# Request correlation
# ---------------------------------------------------------------------------
# One id per request, readable from anywhere in the call stack without threading
# it through every function. A log line with no request context gets "-".

_request_id: ContextVar[str] = ContextVar("request_id", default="-")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_EVENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*$")
OBSERVABILITY_SCHEMA = "dail-siting-observability/1"
_MAX_EVENT_NAME_CHARS = 96
_MAX_FIELD_CHARS = 2048
_MAX_MESSAGE_CHARS = 4096
_MAX_EXCEPTION_CHARS = 16384
_MAX_TEXT_RECORD_CHARS = _MAX_MESSAGE_CHARS + _MAX_EXCEPTION_CHARS
_MAX_COLLECTION_ITEMS = 40
_MAX_NESTING_DEPTH = 4
_CONTEXT_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
_EVENT_RESERVED_FIELDS = {
    "event",
    "event_id",
    "exception",
    "level",
    "logger",
    "message",
    "request_id",
    "schema",
    "service",
    "stack",
    "ts",
}


def new_request_id() -> str:
    """A short id. Full uuid4 is noise in a log line; 12 hex chars is plenty."""
    return uuid.uuid4().hex[:12]


def normalize_request_id(value: str | None) -> str:
    """Return a safe bounded support reference, replacing invalid input.

    Request ids may cross an external proxy boundary. Keeping the accepted
    alphabet and length deliberately small prevents control-character log
    injection and unbounded client-supplied log lines.
    """
    candidate = (value or "").strip()
    return candidate if _REQUEST_ID_RE.fullmatch(candidate) else new_request_id()


def set_request_id(value: str) -> object:
    """Bind an id for the current context. Returns the token for reset()."""
    return _request_id.set(normalize_request_id(value))


def reset_request_id(token: object) -> None:
    _request_id.reset(token)  # type: ignore[arg-type]


def get_request_id() -> str:
    return _request_id.get()


class RequestIdFilter(logging.Filter):
    """Stamp every record with the current request id."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        return True


# ---------------------------------------------------------------------------
# Process-wide static context — cross-process correlation
# ---------------------------------------------------------------------------
# request_id is per-request (async-scoped). This is the other half: fields that
# identify the WHOLE process and belong on every line it emits — run_id, step,
# git sha. On one machine you correlate discrete processes by reading files under
# logs/runs/<run_id>/. On cloud there is one stdout stream, so the correlation
# key has to live IN the record as a field the collector can group by. That is
# what this binds.

_static_context: dict[str, Any] = {}


def bind_context(**fields: Any) -> None:
    """Attach fields to every subsequent log record from this process.

    Idempotent and additive. Pass run_id/step/sha once at startup; None values
    are ignored so callers can pass optionals without guarding.
    """
    conflicts = sorted(_EVENT_RESERVED_FIELDS.intersection(fields))
    if conflicts:
        raise ValueError(f"reserved context fields: {', '.join(conflicts)}")
    invalid = sorted(key for key in fields if not _CONTEXT_KEY_RE.fullmatch(key))
    if invalid:
        raise ValueError(f"invalid context fields: {', '.join(invalid)}")
    for key, value in fields.items():
        if value is not None:
            _static_context[key] = value


def clear_context() -> None:
    _static_context.clear()


def get_context() -> dict[str, Any]:
    return dict(_static_context)


class StaticContextFilter(logging.Filter):
    """Inject the bound process context onto each record (without clobbering)."""

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _static_context.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


# ---------------------------------------------------------------------------
# Structured events — the tracing primitive
# ---------------------------------------------------------------------------
# One helper both a pipeline step-span and a source-health transition go through,
# so a trace line looks the same wherever it comes from: an `event` name plus
# typed fields, carried on the bound run_id. This is the "no useless data" seam —
# callers emit an event only when something worth tracing happened, not per tick.


def log_event(
    event: str,
    *,
    logger: str = "trace",
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit one structured line: {event: <name>, ...fields} at INFO by default.

    In cloud mode this is a JSON object a collector can filter by `event`. In
    local text mode it renders as a readable line — same call, no branching at
    the call site. None-valued fields are dropped so optionals need no guard.
    """
    if len(event) > _MAX_EVENT_NAME_CHARS or not _EVENT_NAME_RE.fullmatch(event):
        raise ValueError(f"invalid observability event name: {event!r}")
    conflicts = sorted(_EVENT_RESERVED_FIELDS.intersection(fields))
    if conflicts:
        raise ValueError(f"reserved observability fields: {', '.join(conflicts)}")
    clean = {k: v for k, v in fields.items() if v is not None}
    logging.getLogger(logger).log(level, event, extra={"event": event, **clean})


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------
# There are no runtime secrets today, which is exactly why this lands now: the
# filter must exist BEFORE the first key does, or the first leak is already in a
# traceback somewhere.
#
# Deliberately NOT redacted: bare long hex strings. This repo logs sha256 digests
# legitimately (tools/data_manifest.py writes them to backup_manifest.tsv), and a
# generic hex rule would corrupt them. Patterns here key off an explicit label or
# a known vendor prefix instead.

_REDACTIONS: list[tuple[re.Pattern[str], str]] = [
    # Authorization: Bearer <token>
    (re.compile(r"(?i)\b(bearer)\s+[A-Za-z0-9\-._~+/]{8,}=*"), r"\1 [REDACTED]"),
    # Vendor-prefixed API keys (Anthropic/OpenAI/OpenRouter style)
    (re.compile(r"\b(sk-(?:ant-|or-)?[A-Za-z0-9\-_]{8,})"), "[REDACTED_KEY]"),
    # AWS / R2 access key ids
    (re.compile(r"\b(AKIA[0-9A-Z]{16})\b"), "[REDACTED_KEY]"),
    # labelled secrets:  api_key=xyz  token: xyz  password="xyz"
    (
        re.compile(
            r"(?i)\b(api[_-]?key|apikey|access[_-]?key|secret[_-]?key|secret|token|password|passwd|pwd)"
            r"(\s*[=:]\s*)([\"']?)([^\s\"',;&]{4,})\3"
        ),
        r"\1\2\3[REDACTED]\3",
    ),
    # url credentials:  https://user:pass@host
    (re.compile(r"://([^:/\s]+):([^@/\s]+)@"), r"://\1:[REDACTED]@"),
    # Email addresses are identities and never belong in operational logs.
    (
        re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"),
        "[REDACTED_EMAIL]",
    ),
]

# Structured fields are an allowlisted operational surface, not a second path
# around message redaction. These names are always suppressed even if a future
# caller accidentally attaches them through extra fields or log_event.
_SENSITIVE_FIELD_NAMES = {
    "actor_id",
    "api_key",
    "apikey",
    "authorization",
    "body",
    "boundary",
    "boundary_fingerprint_sha256",
    "case_id",
    "cookie",
    "coordinates",
    "email",
    "evidence_manifest_id",
    "html",
    "job_id",
    "latitude",
    "longitude",
    "markdown",
    "password",
    "payload",
    "pdf_base64",
    "project_description",
    "prompt",
    "request_body",
    "request_sha256",
    "report_id",
    "response",
    "response_body",
    "secret",
    "session_token",
    "token",
}
_SENSITIVE_FIELD_RE = re.compile(
    r"(^|_)(actor_id|api_key|authorization|body|boundary|boundary_fingerprint_sha256|"
    r"case_id|cookie|coordinates?|email|evidence_manifest_id|html|idempotency_key|job_id|"
    r"latitude|longitude|markdown|password|payload|pdf_base64|prompt|recipient|report_id|"
    r"request_sha256|response|secret|session_token|token)(_|$)"
)


def redact(text: str) -> str:
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def _safe_key(value: Any) -> str:
    return re.sub(r"[\x00-\x1f\x7f]", " ", redact(str(value)))[:80]


def _safe_field(
    key: str,
    value: Any,
    *,
    depth: int = 0,
    seen: frozenset[int] = frozenset(),
) -> Any:
    normalised = key.lower().replace("-", "_")
    if normalised in _SENSITIVE_FIELD_NAMES or _SENSITIVE_FIELD_RE.search(normalised):
        return "[REDACTED_FIELD]"
    return _safe_value(value, depth=depth, seen=seen)


def _safe_value(
    value: Any,
    *,
    depth: int = 0,
    seen: frozenset[int] = frozenset(),
) -> Any:
    if isinstance(value, str):
        return redact(value)[:_MAX_FIELD_CHARS]
    if isinstance(value, Mapping):
        if depth >= _MAX_NESTING_DEPTH:
            return "[TRUNCATED]"
        identity = id(value)
        if identity in seen:
            return "[CYCLE]"
        nested_seen = seen | {identity}
        return {
            _safe_key(key): _safe_field(str(key), item, depth=depth + 1, seen=nested_seen)
            for key, item in islice(value.items(), _MAX_COLLECTION_ITEMS)
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        if depth >= _MAX_NESTING_DEPTH:
            return "[TRUNCATED]"
        identity = id(value)
        if identity in seen:
            return "[CYCLE]"
        nested_seen = seen | {identity}
        return [_safe_value(item, depth=depth + 1, seen=nested_seen) for item in islice(value, _MAX_COLLECTION_ITEMS)]
    return value


class RedactionFilter(logging.Filter):
    """Scrub credential-shaped substrings from the message and its args."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = redact(record.msg)
        if record.args:
            if isinstance(record.args, dict):
                record.args = _safe_value(record.args)
            elif isinstance(record.args, tuple):
                record.args = tuple(_safe_value(a) for a in record.args)
        return True


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------

# LogRecord attributes that are structural, not payload. Anything else the caller
# attached via `extra=` becomes a top-level JSON field.
_RESERVED = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
    "request_id",
    "service",
    "event_id",
    "exception",
    "level",
    "logger",
    "schema",
    "stack",
    "ts",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per line — what log aggregators can filter and alert on."""

    def __init__(self, service: str = "dail-tracker") -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "schema": OBSERVABILITY_SCHEMA,
            "event_id": getattr(record, "event_id", uuid.uuid4().hex),
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", self.service),
            "request_id": getattr(record, "request_id", "-"),
            "message": redact(record.getMessage())[:_MAX_MESSAGE_CHARS],
        }
        if record.exc_info:
            payload["exception"] = redact(self.formatException(record.exc_info))[:_MAX_EXCEPTION_CHARS]
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)[:_MAX_EXCEPTION_CHARS]

        for key, value in record.__dict__.items():
            if key in _RESERVED or key.startswith("_"):
                continue
            value = _safe_field(key, value)
            try:
                json.dumps(value)
                payload[key] = value
            except (TypeError, ValueError):
                payload[key] = f"<{type(value).__name__}>"

        return json.dumps(payload, ensure_ascii=False, default=str)


class SafeTextFormatter(logging.Formatter):
    """Apply the same redaction and bounds when local text mode renders tracebacks."""

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))[:_MAX_TEXT_RECORD_CHARS]

    def formatException(self, ei) -> str:  # noqa: N802 - logging API (base names the param "ei")
        return redact(super().formatException(ei))[:_MAX_EXCEPTION_CHARS]

    def formatStack(self, stack_info: str) -> str:  # noqa: N802 - logging API
        return redact(super().formatStack(stack_info))[:_MAX_EXCEPTION_CHARS]


_TEXT_FORMAT = "%(asctime)s %(levelname)-7s [%(request_id)s] %(name)s: %(message)s"

# Third-party loggers that emit a line per operation at INFO. In aggregation they
# bury our own records and cost real money per GB ingested. Observed concretely:
# httpx logs one INFO line for every outbound request.
_NOISY_LIBRARIES = (
    "httpx",
    "httpcore",
    "urllib3",
    "botocore",
    "boto3",
    "s3transfer",
    "asyncio",
    "matplotlib",
    "PIL",
    "fsspec",
    "numexpr",
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def cloud_mode() -> bool:
    """True when this process should log cloud-style (JSON to stdout).

    Opt-in, so a laptop keeps the file-based text logs by default. A container
    sets one of these and every process — API and ETL alike — emits the same
    structured stream a collector can parse.
    """
    if os.environ.get("DAIL_LOG_CLOUD", "").lower() in {"1", "true", "yes"}:
        return True
    return os.environ.get("LOG_FORMAT", "").lower() == "json"


def _resolve(level: str | None, fmt: str | None, service: str | None) -> tuple[str, str, str]:
    level_s = (level or os.environ.get("LOG_LEVEL") or "INFO").upper()
    if level_s not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        level_s = "INFO"

    fmt_s = (fmt or os.environ.get("LOG_FORMAT") or "").lower()
    if fmt_s not in {"json", "text"}:
        # Cloud mode implies JSON; a bare local run gets the readable format.
        # (LOG_FORMAT=text stays respected — it wins on the line above.)
        fmt_s = "json" if cloud_mode() else "text"

    service_s = service or os.environ.get("LOG_SERVICE") or "dail-tracker"
    return level_s, fmt_s, service_s


def configure_logging(
    *,
    level: str | None = None,
    fmt: str | None = None,
    service: str | None = None,
) -> None:
    """Install stdout logging for a served process. Safe to call more than once.

    Unlike `logging_setup.setup_logging`, this does NOT bail out when handlers
    already exist — that guard is what makes the ETL helper wrong under uvicorn.
    """
    level_s, fmt_s, service_s = _resolve(level, fmt, service)

    formatter: dict[str, Any] = (
        {"()": JsonFormatter, "service": service_s}
        if fmt_s == "json"
        else {"()": SafeTextFormatter, "fmt": _TEXT_FORMAT, "datefmt": "%Y-%m-%d %H:%M:%S"}
    )

    logging.config.dictConfig(
        {
            "version": 1,
            # Leave module-level loggers created at import time alone; only the
            # handler wiring below changes.
            "disable_existing_loggers": False,
            "filters": {
                "request_id": {"()": RequestIdFilter},
                "static_context": {"()": StaticContextFilter},
                "redaction": {"()": RedactionFilter},
            },
            "formatters": {"default": formatter},
            "handlers": {
                "stdout": {
                    "class": "logging.StreamHandler",
                    "stream": sys.stdout,
                    "formatter": "default",
                    "filters": ["request_id", "static_context", "redaction"],
                    "level": level_s,
                }
            },
            "root": {"handlers": ["stdout"], "level": level_s},
            "loggers": {
                # Uvicorn ships its own handlers; strip them so everything goes
                # through one pipeline and one format.
                "uvicorn": {"handlers": [], "propagate": True, "level": level_s},
                "uvicorn.error": {"handlers": [], "propagate": True, "level": level_s},
                # Access logs are re-emitted by our own middleware with the
                # request id attached; uvicorn's duplicate is noise.
                "uvicorn.access": {"handlers": [], "propagate": False, "level": "WARNING"},
                **{name: {"level": "WARNING"} for name in _NOISY_LIBRARIES},
            },
        }
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
