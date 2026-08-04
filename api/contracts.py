"""Shared OpenAPI error contracts for all versioned API routers."""

from __future__ import annotations

from typing import Any

from dail_tracker_core.models.responses import (
    BadRequestErrorResponse,
    NotFoundErrorResponse,
    UnavailableErrorResponse,
)

# Router-level defaults keep the machine-readable error envelope visible in the
# generated API contract.  Individual routes may never emit every status, but a
# client can safely implement these four responses once for the whole v1 API.
ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": BadRequestErrorResponse, "description": "Invalid request"},
    404: {"model": NotFoundErrorResponse, "description": "Resource not found"},
    422: {"model": BadRequestErrorResponse, "description": "Request validation failed"},
    503: {"model": UnavailableErrorResponse, "description": "Required data source unavailable"},
}
