"""Pure, source-span-preserving rules for Chief Executive report extraction."""

from __future__ import annotations

import re

CE_REPORT_TYPE = "ce_report"

_AMOUNT = re.compile(
    r"(?<![A-Za-z0-9])(?:€|EUR)[ \t]*\d+(?:,\d{3})*(?:\.\d+)?"
    r"(?:[ \t]*(?:k|m|bn|thousand|million|billion))?",
    re.I,
)
_TENDER = re.compile(r"\b(?:invitation(?:s)? to tender|tender(?:ing|ed|s)?)\b", re.I)
_CONTRACT = re.compile(r"\bcontract(?:or|ors|ed|ing|s)?\b", re.I)
_AWARD = re.compile(r"\b(?:award(?:ed|ing|s)?|letter of acceptance)\b", re.I)
_APPOINTMENT = re.compile(r"\bappoint(?:ed|ment|ments|ing)?\b", re.I)
_COMMERCIAL_PARTY = re.compile(
    r"\b(?:consultant|contractor|supplier|architect|engineer|firm|company|service provider)s?\b",
    re.I,
)
_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z])|\n{2,}")


def amount_mentions(text: str) -> list[str]:
    """Return source-literal euro expressions without interpreting or summing them."""
    found: list[str] = []
    seen: set[str] = set()
    for match in _AMOUNT.finditer(str(text or "")):
        value = match.group(0).strip()
        key = value.casefold()
        if key not in seen:
            found.append(value)
            seen.add(key)
    return found


def _lead_spans(value: str) -> list[tuple[int, int]]:
    """Return candidate (start, end) spans over page text.

    Sentence boundaries first; any span over 800 chars is re-split on newlines, because
    page text without terminal punctuation (tables, bulleted schedules) would otherwise
    form one span covering the page. Recall measurement reuses this so probe and
    extractor agree on the unit being counted.
    """
    primary_spans = []
    start = 0
    for boundary in _SENTENCE_BOUNDARY.finditer(value):
        primary_spans.append((start, boundary.start()))
        start = boundary.end()
    primary_spans.append((start, len(value)))
    spans: list[tuple[int, int]] = []
    for span_start, span_end in primary_spans:
        if span_end - span_start <= 800:
            spans.append((span_start, span_end))
            continue
        line_start = span_start
        for newline in re.finditer(r"\n+", value[span_start:span_end]):
            line_end = span_start + newline.start()
            if line_end > line_start:
                spans.append((line_start, line_end))
            line_start = span_start + newline.end()
        if line_start < span_end:
            spans.append((line_start, span_end))
    return spans


def procurement_leads(text: str) -> list[dict]:
    """Return commercial-interest sentences with exact page-relative source spans.

    These are unreviewed follow-up leads, never claims of a tender opportunity, contract
    award, site relationship, or planning outcome.
    """
    value = str(text or "")
    rows: list[dict] = []
    for span_start, span_end in _lead_spans(value):
        raw = value[span_start:span_end]
        left = len(raw) - len(raw.lstrip())
        right = len(raw.rstrip())
        quote = raw[left:right]
        if not quote:
            continue
        has_tender = bool(_TENDER.search(quote))
        has_contract = bool(_CONTRACT.search(quote))
        lead_types: list[str] = []
        if has_tender:
            lead_types.append("tender")
        if has_contract:
            lead_types.append("contract")
        if _AWARD.search(quote) and (has_tender or has_contract):
            lead_types.append("award")
        if _APPOINTMENT.search(quote) and _COMMERCIAL_PARTY.search(quote):
            lead_types.append("appointment")
        if not lead_types:
            continue
        rows.append(
            {
                "source_span_start": span_start + left,
                "source_span_end": span_start + right,
                "quote": quote,
                "lead_types": lead_types,
                "amount_mentions": amount_mentions(quote),
            }
        )
    return rows
