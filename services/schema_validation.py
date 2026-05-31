"""Validate-at-fetch for Oireachtas API responses (DAIL-019 / DAIL-020).

Every Oireachtas JSON fetch funnels through services/http_engine.fetch_json.
This module lets that single choke point assert the response still has the
shape the pipeline depends on *before* any downstream flattener reads it. The
bug class this guards against is silent upstream drift: a renamed envelope key
(``results`` -> ``data``), a dropped ``head.counts.resultCount`` (which every
paginator reads to detect truncation), or a renamed per-item wrapper
(``member`` -> something else). Any of those would otherwise surface much later
as empty silver tables or mis-counted activity, with no obvious cause.

Design notes:
  - Schemas live in services/schemas/<endpoint>.json and are deliberately
    *loose*: they pin the top-level envelope and the one wrapper key per result
    item, and stay silent on everything nested. That catches a renamed key but
    does not break when the API adds a new optional field.
  - Validation is keyed off the request URL. Only api.oireachtas.ie endpoints
    we recognise are validated; any other host (test doubles, jsonplaceholder,
    a future new endpoint) is skipped, so generic fetch_json usage is unaffected.
  - On drift we raise SchemaValidationError. The fetch path lets it propagate so
    a systematic break fails loudly rather than silently shrinking the dataset.
"""

from __future__ import annotations

import json
import logging
from functools import cache
from pathlib import Path
from urllib.parse import urlsplit

import jsonschema

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"

# Host whose responses we validate. Other hosts (test doubles, unrelated APIs)
# are passed through untouched.
_OIREACHTAS_HOST = "api.oireachtas.ie"

# Map the first path segment after /v1/ to a schema file stem. Endpoints the
# pipeline does not (yet) have a schema for are intentionally absent — an
# unknown endpoint is skipped, not failed, so adding a new fetch never breaks
# before its schema is written.
_ENDPOINT_BY_PATH_SEGMENT = {
    "members": "members",
    "legislation": "legislation",
    "questions": "questions",
    "votes": "votes",
    "debates": "debates",
}


class SchemaValidationError(Exception):
    """Raised when an API response no longer matches its registered schema.

    Carries the endpoint name so the fetch-path log makes the source obvious.
    """

    def __init__(self, endpoint: str, message: str) -> None:
        self.endpoint = endpoint
        super().__init__(f"[{endpoint}] {message}")


@cache
def _load_schema(endpoint: str) -> dict:
    """Load and cache one endpoint's JSON Schema from services/schemas/."""
    schema_path = SCHEMA_DIR / f"{endpoint}.json"
    with schema_path.open(encoding="utf-8") as fh:
        return json.load(fh)


def endpoint_from_url(url: str) -> str | None:
    """Return the registered endpoint name for an Oireachtas API URL, else None.

    Returns None for any host other than api.oireachtas.ie and for any
    api.oireachtas.ie path whose first /v1/ segment has no registered schema.
    None means "do not validate" — the caller treats it as a pass-through.
    """
    parts = urlsplit(url)
    if parts.hostname != _OIREACHTAS_HOST:
        return None

    segments = [seg for seg in parts.path.split("/") if seg]
    # Expect .../v1/<endpoint>/...  -> take the segment after "v1".
    if "v1" in segments:
        idx = segments.index("v1")
        if idx + 1 < len(segments):
            return _ENDPOINT_BY_PATH_SEGMENT.get(segments[idx + 1])
    return None


def validate_response(endpoint: str, payload: object) -> None:
    """Validate one parsed API page against its registered schema.

    Raises SchemaValidationError if the payload drifts from the schema, or if
    ``endpoint`` has no schema file (a programming error — callers resolve the
    endpoint via endpoint_from_url, which only returns registered names).
    """
    if endpoint not in _ENDPOINT_BY_PATH_SEGMENT.values():
        raise SchemaValidationError(endpoint, "no schema registered for this endpoint")

    try:
        jsonschema.validate(instance=payload, schema=_load_schema(endpoint))
    except jsonschema.ValidationError as exc:
        # exc.message is the human-readable failure; exc.json_path locates it.
        raise SchemaValidationError(
            endpoint,
            f"response failed schema validation at {exc.json_path}: {exc.message}",
        ) from exc


def validate_if_known(url: str, payload: object) -> None:
    """Validate the payload iff the URL maps to a registered endpoint.

    The fetch-path entry point: resolves the endpoint from the URL and validates
    only when it is one we recognise. Unknown hosts/endpoints are a no-op.
    """
    endpoint = endpoint_from_url(url)
    if endpoint is None:
        return
    validate_response(endpoint, payload)
