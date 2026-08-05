"""Contract tests for public-body payment selector semantics."""

from __future__ import annotations

import pytest

from dail_tracker_core import dossiers


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"side": "suplier"}, "side must be"),
        ({"order_by": "largest"}, "order_by must be"),
    ],
)
def test_public_body_payments_rejects_unknown_selectors_before_querying(kwargs: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        dossiers.public_body_payments(object(), **kwargs)
