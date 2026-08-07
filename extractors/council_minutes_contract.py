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
_CE_REPORT = re.compile(r"management report|chief executive.?s? (?:monthly )?report", re.I)
_OTHER_REPORT = re.compile(r"annual report|financial statement|local economic", re.I)

# These labels are deliberately conservative discovery aids for the separate
# planning ``Public Signal`` lane.  They are extracted from a bounded passage,
# retain the original minute as provenance, and never say that the passage is
# about the assessment site.
_ISSUE_THEMES: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "planning_housing",
        re.compile(
            r"\b(?:planning|zoning|development plan|local area plan|material contravention|"
            r"part\s*(?:8|viii)|housing|residential development)\b",
            re.I,
        ),
    ),
    (
        "traffic_access",
        re.compile(
            r"\b(?:road|traffic|access|road safety|junction|parking|sightline|taking in charge)\b",
            re.I,
        ),
    ),
    (
        "amenity",
        re.compile(r"\b(?:amenity|open space|overlooking|overshadowing|public realm)\b", re.I),
    ),
    (
        "environment_heritage",
        re.compile(r"\b(?:biodiversity|heritage|protected structure|archaeolog|landscape)\b", re.I),
    ),
    (
        "services_infrastructure",
        re.compile(
            r"\b(?:wastewater|sewerage|water supply|uisce|drainage|flood(?:ing)?|"
            r"infrastructure capacity|substation|grid)\b",
            re.I,
        ),
    ),
)
_PLANNING_REFERENCE = re.compile(
    r"\b(?:planning\s+(?:ref(?:erence)?|application)|planning\s+application\s+no\.?|"
    r"reg(?:ister)?\s*ref(?:erence)?)\s*[:#]?\s*([a-z]{0,5}\s*[-/]?\s*\d{5,9}(?:[-/]\d+)?)\b",
    re.I,
)
_BOARD_REFERENCE = re.compile(r"\b((?:abp|acp)\s*-\s*\d{4,}(?:\s*-\s*\d+){0,2})\b", re.I)
_COLLECTIVE_ORGANISATION = re.compile(
    r"\b("
    r"[A-Z][A-Za-z'\u2019&.-]*(?:\s+[A-Z][A-Za-z'\u2019&.-]*){0,6}\s+"
    r"(?:Residents'?\s+Association|Residents'?\s+Group|Action\s+Group|"
    r"Community\s+(?:Association|Group|Council|Alliance)|Tidy\s+Towns)"
    r")\b"
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
    # Report filenames/upstream labels are hard signals. Report phrases in body text are
    # considered only after genuine minutes evidence, because real minutes routinely list
    # the Chief Executive's management report as an agenda item.
    if upstream_doc_type == "ce_report" or _CE_REPORT.search(name):
        return "ce_report"
    if _OTHER_REPORT.search(name):
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
    if is_minutes:
        return "plenary_minutes"
    if _CE_REPORT.search(title_block):
        return "ce_report"
    if _OTHER_REPORT.search(title_block):
        return "report_or_plan"
    return "other"


def meeting_scope(doc_type: str) -> str:
    """Map the document label to the civic meeting scope exposed to readers."""
    return {
        "plenary_minutes": "plenary",
        "md_minutes": "municipal_district",
        "committee_minutes": "committee",
    }.get(doc_type, "")


def extract_participation_signals(text: str) -> dict[str, list[str]]:
    """Return narrow, source-preserving public-signal labels for one passage.

    The output is intentionally not a planning assessment.  In particular, a
    road, zoning, disposal, or wastewater mention does not establish a site
    relationship, a legal interest, a constraint, or an opportunity.  Callers
    must retain the source URL/status and present these only as Extracted-band
    leads for reviewer confirmation.
    """
    value = str(text or "")
    names: list[str] = []
    seen_names: set[str] = set()
    for match in _COLLECTIVE_ORGANISATION.finditer(value):
        name = re.sub(r"\s+", " ", match.group(1)).strip()
        key = name.casefold()
        if name and key not in seen_names:
            names.append(name)
            seen_names.add(key)

    participant_categories: list[str] = []
    if any(re.search(r"\bresidents?'?\s+(?:association|group)\b", name, re.I) for name in names):
        participant_categories.append("residents_association")
    if any(
        re.search(r"\b(?:action\s+group|community\s+(?:association|group|council|alliance)|tidy\s+towns)\b", name, re.I)
        for name in names
    ):
        participant_categories.append("community_organisation")

    def unique_matches(pattern: re.Pattern[str]) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()
        for match in pattern.finditer(value):
            item = re.sub(r"\s+", "", match.group(1)).upper()
            if item not in seen:
                found.append(item)
                seen.add(item)
        return found

    return {
        "participant_categories": participant_categories,
        "issue_themes": [key for key, pattern in _ISSUE_THEMES if pattern.search(value)],
        "planning_references": unique_matches(_PLANNING_REFERENCE),
        "board_references": unique_matches(_BOARD_REFERENCE),
        "collective_organisation_names": names,
    }


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
