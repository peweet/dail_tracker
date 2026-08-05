"""Private PublicSignal procurement feed boundary tests."""

from __future__ import annotations

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from api.routers.procurement import require_public_signal_feed_token


def _request(authorization: str | None = None) -> Request:
    headers = [] if authorization is None else [(b"authorization", authorization.encode())]
    return Request({"type": "http", "method": "GET", "path": "/v1/procurement/opportunities", "headers": headers})


def test_private_feed_token_fails_closed_when_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PUBLIC_SIGNAL_FEED_TOKEN", raising=False)
    with pytest.raises(HTTPException) as exc:
        require_public_signal_feed_token(_request("Bearer anything"))
    assert exc.value.status_code == 503


def test_private_feed_token_requires_exact_bearer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBLIC_SIGNAL_FEED_TOKEN", "private-token")
    with pytest.raises(HTTPException) as missing:
        require_public_signal_feed_token(_request())
    assert missing.value.status_code == 401
    with pytest.raises(HTTPException) as wrong:
        require_public_signal_feed_token(_request("Bearer wrong"))
    assert wrong.value.status_code == 401
    require_public_signal_feed_token(_request("Bearer private-token"))
