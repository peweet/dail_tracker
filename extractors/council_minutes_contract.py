"""Pure publication rules for the council-minutes corpus.

The sandbox harvest ledger is useful provenance, but its historical ``doc_type``
labels are not a safe serving contract: agenda documents can quote the previous
meeting's minutes, and committee minutes were often labelled as plenary minutes.
This module is the defensive boundary used when materialising the searchable
gold corpus. It performs no I/O so the rules can be tested independently.
"""

from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

PUBLISHED_MINUTE_TYPES = frozenset({"plenary_minutes", "md_minutes", "committee_minutes"})

_MINUTES = re.compile(
    r"\bminutes?\s+of\b|\bmiontuairisc(?:í|i)?\b|\bmembers present\b|\bin attendance\b",
    re.I,
)
_COMMITTEE = re.compile(
    r"\blcdc\b|local community development committee|strategic policy committee|"
    r"\bspc\b|joint policing committee|\bjpc\b|audit committee|"
    r"(?:^|\W)committee(?:\W|$)",
    re.I,
)
_MUNICIPAL = re.compile(r"municipal district|(?:^|[_\W])md(?:[_\W]|$)", re.I)
_REPORT = re.compile(
    r"management report|chief executive.?s? (?:monthly )?report|annual report|"
    r"financial statement|local economic",
    re.I,
)


def _basename(source_url: str, meeting: str) -> str:
    """Return the decoded source filename without letting listing paths classify it."""
    candidate = urlsplit(str(source_url or "")).path.rsplit("/", 1)[-1] or str(meeting or "")
    return unquote(candidate).lower()


def classify_document(*, meeting: str, source_url: str, text: str, upstream_doc_type: str = "") -> str:
    """Classify a harvested meeting document for publication.

    Filename ``agenda`` is a hard signal unless that same filename also says
    ``minutes``. Agenda prose routinely includes "minutes of the previous
    meeting", which is why text-first classification contaminated the old corpus.
    Committee and municipal scope are retained rather than promoted to plenary.
    """
    name = _basename(source_url, meeting)
    head = str(text or "")[:4_000]
    title_block = head[:500]
    if "agenda" in name and "minute" not in name:
        return "agenda"
    # Report names are useful; report mentions inside genuine minutes are not.
    if _REPORT.search(name):
        return "report_or_plan"
    is_minutes = (
        upstream_doc_type in {"plenary_minutes", "md_minutes"} or "minute" in name or bool(_MINUTES.search(head))
    )
    if not is_minutes:
        return "other"
    # Scope markers must occur in the filename/title block. Plenary minutes often
    # discuss committees later on their first page; that does not make the meeting
    # itself a committee meeting.
    if _COMMITTEE.search(f"{name}\n{title_block}"):
        return "committee_minutes"
    if _MUNICIPAL.search(f"{name}\n{title_block}"):
        return "md_minutes"
    return "plenary_minutes"


def meeting_scope(doc_type: str) -> str:
    """Map the document label to the civic meeting scope exposed to readers."""
    return {
        "plenary_minutes": "plenary",
        "md_minutes": "municipal_district",
        "committee_minutes": "committee",
    }.get(doc_type, "")


def chunk_text(text: str, max_chars: int = 2_000) -> list[str]:
    """Split text into bounded search chunks, preferring paragraph boundaries.

    OCR and HTML extracts often contain no blank lines. The previous buffer-only
    splitter therefore emitted 10-40k character chunks, weakening BM25 relevance
    and producing poor snippets. Every returned chunk is now hard-bounded.
    """
    if max_chars < 200:
        raise ValueError("max_chars must be at least 200")
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return []

    def split_long(block: str) -> list[str]:
        parts: list[str] = []
        remaining = block.strip()
        while len(remaining) > max_chars:
            cut = max(
                remaining.rfind("\n", 0, max_chars + 1),
                remaining.rfind(" ", 0, max_chars + 1),
            )
            if cut < max_chars // 2:
                cut = max_chars
            parts.append(remaining[:cut].strip())
            remaining = remaining[cut:].strip()
        if remaining:
            parts.append(remaining)
        return parts

    chunks: list[str] = []
    buffer = ""
    for paragraph in re.split(r"\n\s*\n+", raw):
        for part in split_long(paragraph):
            proposed = f"{buffer}\n\n{part}" if buffer else part
            if len(proposed) <= max_chars:
                buffer = proposed
            else:
                if buffer:
                    chunks.append(buffer)
                buffer = part
    if buffer:
        chunks.append(buffer)
    return chunks
