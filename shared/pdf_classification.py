"""Conservative boundary for optional PDF-classification observations.

The extractors' existing text-layer decisions remain authoritative.  An
external classifier can be sampled through this module, but its result is
normalised as shadow telemetry and cannot change routing.

This module deliberately does not import ``pdf_inspector``.  A caller may
inject a provider after its native dependency has been qualified and isolated.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from typing import Any, Literal

PdfType = Literal["text_based", "scanned", "image_based", "mixed"]
ProbeStatus = Literal["ok", "unavailable", "error", "invalid_result"]
PageIndexBase = Literal[0, 1]

DIARY_TEXT_CHAR_THRESHOLD = 100
PAYMENTS_TEXT_CHAR_THRESHOLD = 200

_PDF_TYPES: frozenset[str] = frozenset({"text_based", "scanned", "image_based", "mixed"})
_MISSING = object()


@dataclass(frozen=True, slots=True)
class OcrPageReasons:
    """OCR reasons for one page, normalised to a zero-based page index."""

    page_0based: int
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PdfClassificationObservation:
    """Validated, provider-neutral shadow observation for a PDF."""

    pdf_type: PdfType
    confidence_raw: float
    page_count: int
    ocr_pages_0based: tuple[int, ...]
    ocr_reasons_by_page_0based: tuple[OcrPageReasons, ...] = ()
    has_encoding_issues: bool | None = None
    processing_time_ms: int | None = None
    provider: str = "pdf-inspector"
    provider_version: str | None = None


@dataclass(frozen=True, slots=True)
class PdfClassificationProbe:
    """Outcome of a best-effort shadow-classifier call."""

    status: ProbeStatus
    observation: PdfClassificationObservation | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class TextLayerDecision:
    """Legacy decision plus a non-authoritative classifier comparison."""

    has_text_layer: bool
    legacy_has_text_layer: bool
    shadow_has_usable_native_text: bool | None

    @property
    def shadow_disagrees(self) -> bool | None:
        """Whether a conclusive shadow observation disagrees with legacy logic."""

        if self.shadow_has_usable_native_text is None:
            return None
        return self.shadow_has_usable_native_text != self.legacy_has_text_layer


def legacy_has_text_layer(text_char_count: int, *, threshold: int) -> bool:
    """Return the existing strict ``count > threshold`` classification."""

    _validate_non_negative_int(text_char_count, field="text_char_count")
    _validate_non_negative_int(threshold, field="threshold")
    return text_char_count > threshold


def decide_text_layer(
    text_char_count: int,
    *,
    threshold: int,
    shadow: PdfClassificationObservation | None = None,
) -> TextLayerDecision:
    """Apply legacy routing and attach an optional shadow comparison.

    ``has_text_layer`` intentionally always equals ``legacy_has_text_layer``.
    This invariant prevents a classifier observation from skipping an existing
    OCR route or bypassing a source-specific parser.
    """

    legacy = legacy_has_text_layer(text_char_count, threshold=threshold)
    shadow_native = _shadow_has_usable_native_text(shadow)
    return TextLayerDecision(
        has_text_layer=legacy,
        legacy_has_text_layer=legacy,
        shadow_has_usable_native_text=shadow_native,
    )


def normalize_pdf_inspector_result(
    result: object,
    *,
    page_index_base: PageIndexBase,
    provider: str = "pdf-inspector",
    provider_version: str | None = None,
) -> PdfClassificationObservation:
    """Validate and normalise a ``detect_pdf_bytes``-style result.

    ``detect_pdf_bytes`` currently emits one-based page numbers, whereas the
    lightweight classification API emits zero-based numbers.  Requiring the
    caller to state the input base prevents an implicit or double conversion.
    """

    if isinstance(page_index_base, bool) or page_index_base not in (0, 1):
        raise ValueError("page_index_base must be 0 or 1")
    if not isinstance(provider, str) or not provider.strip():
        raise ValueError("provider must be a non-empty string")
    if provider_version is not None and not isinstance(provider_version, str):
        raise TypeError("provider_version must be a string or None")

    pdf_type_value = _read_field(result, "pdf_type")
    if not isinstance(pdf_type_value, str) or pdf_type_value not in _PDF_TYPES:
        raise ValueError(f"unsupported pdf_type: {pdf_type_value!r}")
    pdf_type: PdfType = pdf_type_value  # type: ignore[assignment]

    confidence_value = _read_field(result, "confidence")
    if isinstance(confidence_value, bool) or not isinstance(confidence_value, (int, float)):
        raise TypeError("confidence must be a finite number")
    confidence = float(confidence_value)
    if not isfinite(confidence) or not 0.0 <= confidence <= 1.0:
        raise ValueError("confidence must be between 0 and 1")

    page_count = _read_field(result, "page_count")
    _validate_positive_int(page_count, field="page_count")

    raw_pages = _read_field(result, "pages_needing_ocr")
    pages = _normalise_pages(
        raw_pages,
        page_count=page_count,
        page_index_base=page_index_base,
        field="pages_needing_ocr",
    )

    encoding_value = _read_field(result, "has_encoding_issues", default=None)
    if encoding_value is not None and not isinstance(encoding_value, bool):
        raise TypeError("has_encoding_issues must be a bool or None")

    processing_time = _read_field(result, "processing_time_ms", default=None)
    if processing_time is not None:
        _validate_non_negative_int(processing_time, field="processing_time_ms")

    raw_reasons = _read_field(result, "ocr_reasons_by_page", default=())
    reason_rows = _normalise_reason_rows(
        raw_reasons,
        ocr_pages_0based=pages,
        page_count=page_count,
        page_index_base=page_index_base,
    )

    return PdfClassificationObservation(
        pdf_type=pdf_type,
        confidence_raw=confidence,
        page_count=page_count,
        ocr_pages_0based=pages,
        ocr_reasons_by_page_0based=reason_rows,
        has_encoding_issues=encoding_value,
        processing_time_ms=processing_time,
        provider=provider.strip(),
        provider_version=provider_version,
    )


def run_shadow_pdf_inspection(
    pdf_bytes: bytes,
    provider: Callable[[bytes], object] | None,
    *,
    page_index_base: PageIndexBase,
    provider_name: str = "pdf-inspector",
    provider_version: str | None = None,
) -> PdfClassificationProbe:
    """Run an injected classifier without allowing failures to escape.

    This is an adapter seam, not a production isolation boundary.  A native
    provider must still run in a bounded subprocess before it is enabled for
    untrusted PDFs.
    """

    if provider is None:
        return PdfClassificationProbe(status="unavailable")

    try:
        raw_result = provider(pdf_bytes)
    except Exception as exc:  # noqa: BLE001 - shadow failures must fall back
        return PdfClassificationProbe(status="error", error=_error_summary(exc))

    try:
        observation = normalize_pdf_inspector_result(
            raw_result,
            page_index_base=page_index_base,
            provider=provider_name,
            provider_version=provider_version,
        )
    except Exception as exc:  # noqa: BLE001 - malformed shadow output must fall back
        return PdfClassificationProbe(status="invalid_result", error=_error_summary(exc))
    return PdfClassificationProbe(status="ok", observation=observation)


def _shadow_has_usable_native_text(
    observation: PdfClassificationObservation | None,
) -> bool | None:
    if observation is None:
        return None
    if observation.pdf_type != "text_based" or observation.ocr_pages_0based:
        return False
    if observation.has_encoding_issues is None:
        return None
    return not observation.has_encoding_issues


def _read_field(value: object, field: str, *, default: Any = _MISSING) -> Any:
    if isinstance(value, Mapping):
        if field in value:
            return value[field]
    else:
        try:
            return getattr(value, field)
        except AttributeError:
            pass
    if default is not _MISSING:
        return default
    raise AttributeError(f"classifier result is missing {field!r}")


def _normalise_pages(
    raw_pages: object,
    *,
    page_count: int,
    page_index_base: PageIndexBase,
    field: str,
) -> tuple[int, ...]:
    if isinstance(raw_pages, (str, bytes)) or not isinstance(raw_pages, Sequence):
        raise TypeError(f"{field} must be a sequence of page numbers")
    pages: set[int] = set()
    for raw_page in raw_pages:
        if isinstance(raw_page, bool) or not isinstance(raw_page, int):
            raise TypeError(f"{field} must contain only integers")
        page_0based = raw_page - page_index_base
        if page_0based < 0 or page_0based >= page_count:
            raise ValueError(f"{field} contains out-of-range page {raw_page}")
        pages.add(page_0based)
    return tuple(sorted(pages))


def _normalise_reason_rows(
    raw_rows: object,
    *,
    ocr_pages_0based: tuple[int, ...],
    page_count: int,
    page_index_base: PageIndexBase,
) -> tuple[OcrPageReasons, ...]:
    if isinstance(raw_rows, (str, bytes)) or not isinstance(raw_rows, Sequence):
        raise TypeError("ocr_reasons_by_page must be a sequence")

    reasons_by_page: dict[int, set[str]] = {}
    valid_ocr_pages = set(ocr_pages_0based)
    for row in raw_rows:
        page = _normalise_pages(
            [_read_field(row, "page")],
            page_count=page_count,
            page_index_base=page_index_base,
            field="ocr_reasons_by_page.page",
        )[0]
        if page not in valid_ocr_pages:
            raise ValueError("OCR reasons reference a page not marked as needing OCR")
        raw_reasons = _read_field(row, "reasons")
        if isinstance(raw_reasons, (str, bytes)) or not isinstance(raw_reasons, Sequence):
            raise TypeError("OCR reasons must be a sequence of strings")
        reasons = reasons_by_page.setdefault(page, set())
        for reason in raw_reasons:
            if not isinstance(reason, str) or not reason:
                raise TypeError("OCR reasons must contain non-empty strings")
            reasons.add(reason)

    return tuple(
        OcrPageReasons(page_0based=page, reasons=tuple(sorted(reasons)))
        for page, reasons in sorted(reasons_by_page.items())
    )


def _validate_non_negative_int(value: object, *, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value < 0:
        raise ValueError(f"{field} must not be negative")


def _validate_positive_int(value: object, *, field: str) -> None:
    _validate_non_negative_int(value, field=field)
    if value == 0:
        raise ValueError(f"{field} must be positive")


def _error_summary(exc: Exception) -> str:
    detail = str(exc).strip()
    summary = type(exc).__name__ if not detail else f"{type(exc).__name__}: {detail}"
    return summary[:500]
