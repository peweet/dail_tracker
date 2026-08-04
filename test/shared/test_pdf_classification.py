from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from shared.pdf_classification import (
    DIARY_TEXT_CHAR_THRESHOLD,
    PAYMENTS_TEXT_CHAR_THRESHOLD,
    PdfClassificationObservation,
    decide_text_layer,
    legacy_has_text_layer,
    normalize_pdf_inspector_result,
    run_shadow_pdf_inspection,
)


@pytest.mark.parametrize(
    ("count", "threshold", "expected"),
    [
        (100, DIARY_TEXT_CHAR_THRESHOLD, False),
        (101, DIARY_TEXT_CHAR_THRESHOLD, True),
        (200, PAYMENTS_TEXT_CHAR_THRESHOLD, False),
        (201, PAYMENTS_TEXT_CHAR_THRESHOLD, True),
    ],
)
def test_legacy_text_layer_boundaries_are_strict(count: int, threshold: int, expected: bool) -> None:
    assert legacy_has_text_layer(count, threshold=threshold) is expected


@pytest.mark.parametrize(
    ("count", "threshold", "shadow", "expected", "disagrees"),
    [
        (
            100,
            DIARY_TEXT_CHAR_THRESHOLD,
            PdfClassificationObservation(
                pdf_type="text_based",
                confidence_raw=1.0,
                page_count=1,
                ocr_pages_0based=(),
                has_encoding_issues=False,
            ),
            False,
            True,
        ),
        (
            201,
            PAYMENTS_TEXT_CHAR_THRESHOLD,
            PdfClassificationObservation(
                pdf_type="scanned",
                confidence_raw=0.95,
                page_count=1,
                ocr_pages_0based=(0,),
                has_encoding_issues=False,
            ),
            True,
            True,
        ),
    ],
)
def test_shadow_observation_cannot_change_legacy_routing(
    count: int,
    threshold: int,
    shadow: PdfClassificationObservation,
    expected: bool,
    disagrees: bool,
) -> None:
    decision = decide_text_layer(count, threshold=threshold, shadow=shadow)

    assert decision.has_text_layer is expected
    assert decision.legacy_has_text_layer is expected
    assert decision.shadow_disagrees is disagrees


def test_encoding_issues_prevent_shadow_native_text_endorsement() -> None:
    shadow = PdfClassificationObservation(
        pdf_type="text_based",
        confidence_raw=1.0,
        page_count=1,
        ocr_pages_0based=(),
        has_encoding_issues=True,
    )

    decision = decide_text_layer(101, threshold=DIARY_TEXT_CHAR_THRESHOLD, shadow=shadow)

    assert decision.has_text_layer is True
    assert decision.shadow_has_usable_native_text is False


def test_normalizes_one_based_detect_result_once() -> None:
    result = SimpleNamespace(
        pdf_type="mixed",
        confidence=0.7,
        page_count=3,
        pages_needing_ocr=[3, 1, 3],
        ocr_reasons_by_page=[
            SimpleNamespace(page=3, reasons=["no_text", "scanned"]),
            SimpleNamespace(page=1, reasons=["scanned"]),
        ],
        has_encoding_issues=False,
        processing_time_ms=12,
    )

    observation = normalize_pdf_inspector_result(
        result,
        page_index_base=1,
        provider_version="fixed-build",
    )

    assert observation.ocr_pages_0based == (0, 2)
    assert [(row.page_0based, row.reasons) for row in observation.ocr_reasons_by_page_0based] == [
        (0, ("scanned",)),
        (2, ("no_text", "scanned")),
    ]
    assert observation.provider_version == "fixed-build"


def test_preserves_zero_based_classification_pages() -> None:
    observation = normalize_pdf_inspector_result(
        {
            "pdf_type": "mixed",
            "confidence": 0.7,
            "page_count": 3,
            "pages_needing_ocr": [0, 2],
        },
        page_index_base=0,
    )

    assert observation.ocr_pages_0based == (0, 2)


@pytest.mark.parametrize(
    ("pages", "base"),
    [
        ([0], 1),
        ([3], 0),
        ([4], 1),
    ],
)
def test_rejects_out_of_range_pages(pages: list[int], base: int) -> None:
    with pytest.raises(ValueError, match="out-of-range"):
        normalize_pdf_inspector_result(
            {
                "pdf_type": "mixed",
                "confidence": 0.7,
                "page_count": 3,
                "pages_needing_ocr": pages,
            },
            page_index_base=base,  # type: ignore[arg-type]
        )


def test_absent_provider_is_unavailable_without_importing_native_module(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "pdf_inspector", raising=False)

    probe = run_shadow_pdf_inspection(b"%PDF", None, page_index_base=1)

    assert probe.status == "unavailable"
    assert probe.observation is None
    assert "pdf_inspector" not in sys.modules


def test_provider_error_falls_back_without_escaping() -> None:
    def failing_provider(_pdf_bytes: bytes) -> object:
        raise RuntimeError("broken document")

    probe = run_shadow_pdf_inspection(b"%PDF", failing_provider, page_index_base=1)

    assert probe.status == "error"
    assert probe.observation is None
    assert probe.error == "RuntimeError: broken document"


def test_invalid_provider_result_falls_back_without_escaping() -> None:
    probe = run_shadow_pdf_inspection(
        b"%PDF",
        lambda _pdf_bytes: {
            "pdf_type": "mixed",
            "confidence": float("nan"),
            "page_count": 1,
            "pages_needing_ocr": [1],
        },
        page_index_base=1,
    )

    assert probe.status == "invalid_result"
    assert probe.observation is None
    assert probe.error == "ValueError: confidence must be between 0 and 1"


def test_unexpected_normalization_error_falls_back_without_escaping() -> None:
    probe = run_shadow_pdf_inspection(
        b"%PDF",
        lambda _pdf_bytes: {
            "pdf_type": "mixed",
            "confidence": 10**10000,
            "page_count": 1,
            "pages_needing_ocr": [1],
        },
        page_index_base=1,
    )

    assert probe.status == "invalid_result"
    assert probe.observation is None
    assert probe.error is not None
    assert probe.error.startswith("OverflowError:")
