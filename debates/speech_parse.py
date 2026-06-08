"""
speech_parse.py

Stage 2 of the debates integration: parse AKN (Akoma Ntoso) debate XML into a
flat silver table of one row per **member contribution** (speech / question /
answer) on the floor of either chamber.

This is the piece the listings index (debates/dbsect_listings_flatten.py) and
the dbsect harvest (services/dbsect_harvest.py) deliberately stopped short of:
those are structural-only. Here we open the transcript XML and extract who said
what, under which debate section, on which day.

Hierarchy (verified against real AKN):
    debate (a whole sitting day, main.xml)
      └─ debateSection  (one item of business, e.g. "Trade Relations")
           └─ speech / question / answer  (one member's contribution)

Grain: one row per contribution. `debate_section_id` + `section_heading` carry
the topic context; `(date, chamber, debate_section_id)` is the section identity
(dbsect ids recur every sitting day — never join on the id alone).

Member resolution is DETERMINISTIC, not fuzzy: each contribution's `by="#eId"`
points into the document's <references> <TLCPerson> table, whose `href` tail is
the canonical `unique_member_code` (e.g. /ie/oireachtas/member/id/
Erin-McGreehan.S.2020-06-29). No name-matching needed; the cross-house code
collision (e.g. Seán Kyne D vs S) is inherent in the code itself.

No inference here (logic firewall): structural extraction only. Language
detection / topic classification are downstream (Stage 3 gold).

Input  : AKN XML files (data/bronze/debates/akn/*.xml; probes for smoke-test)
Output : data/silver/parquet/speeches.parquet

Run standalone (smoke-test against probe files):
  python -m debates.speech_parse --probe
"""

from __future__ import annotations

import argparse
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from config import AKN_DIR, DEBATES_DIR, SILVER_PARQUET_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_PROBE_DIR = DEBATES_DIR / "probe"
_OUT = SILVER_PARQUET_DIR / "speeches.parquet"

# Member-attributed contribution elements on the floor. All carry a `by=` ref.
_CONTRIB_TAGS = {"speech", "question", "answer"}

_SCHEMA = [
    "date",
    "chamber",
    "debate_section_id",
    "section_heading",
    "contribution_type",
    "contribution_order",
    "akn_eid",
    "unique_member_code",
    "speaker_raw",
    "recorded_time",
    "speech_text",
]


def _local(el: ET.Element) -> str:
    """Tag name without its XML namespace.

    AKN declares a *versioned* default namespace (…/akn/3.0/CSD13); the suffix
    varies by year, so we never hardcode it — match on local name instead.
    """
    return el.tag.rsplit("}", 1)[-1]


def _text_of(el: ET.Element) -> str:
    """All descendant text of an element, whitespace-collapsed."""
    return " ".join(t.strip() for t in el.itertext() if t and t.strip())


def _frbr_date_chamber(root: ET.Element) -> tuple[str, str]:
    """Extract (date, chamber) from the document's FRBRthis URI.

    Shape: /akn/ie/debateRecord/<chamber>/<YYYY-MM-DD>/debate/...  The work-level
    FRBRthis is the authoritative day/chamber for the whole transcript.
    """
    for el in root.iter():
        if _local(el) == "FRBRthis":
            value = el.get("value") or ""
            parts = value.strip("/").split("/")
            # ['akn','ie','debateRecord', chamber, date, 'debate', ...]
            if len(parts) >= 5 and parts[2] == "debateRecord":
                return parts[4], parts[3]
    return "", ""


def _person_map(root: ET.Element) -> dict[str, str]:
    """eId -> unique_member_code, from the <references> <TLCPerson> table.

    The href tail is the canonical member code; '' when a ref lacks an href
    (e.g. a non-member office-holder reference), which downstream treats as
    unresolved rather than dropping the contribution.
    """
    out: dict[str, str] = {}
    for el in root.iter():
        if _local(el) == "TLCPerson":
            eid = el.get("eId")
            if eid:
                out[eid] = (el.get("href") or "").rstrip("/").split("/")[-1]
    return out


def _parent_sections(root: ET.Element) -> dict[ET.Element, ET.Element]:
    """Map every element to its nearest enclosing <debateSection> ancestor.

    ElementTree has no parent pointers, so we build a child->parent map once and
    climb it. A contribution may sit in a nested subsection; we attribute it to
    the closest section that has an eId.
    """
    parent = {c: p for p in root.iter() for c in p}
    nearest: dict[ET.Element, ET.Element] = {}
    for el in root.iter():
        if _local(el) in _CONTRIB_TAGS:
            cur = parent.get(el)
            while cur is not None and not (_local(cur) == "debateSection" and cur.get("eId")):
                cur = parent.get(cur)
            if cur is not None:
                nearest[el] = cur
    return nearest


def _section_heading(section: ET.Element) -> str:
    """First <heading> text directly describing the debate section."""
    for child in section.iter():
        if _local(child) == "heading":
            return _text_of(child)
    return ""


def _speaker_and_body(contrib: ET.Element) -> tuple[str, str, str]:
    """Return (speaker_raw, recorded_time, body_text) for a contribution.

    <from> holds the rendered speaker label; <recordedTime> the timestamp; the
    body is everything else (the <p> turns) with the speaker label removed so
    speech_text is the words spoken, not "Deputy X …" prefix noise.
    """
    speaker = ""
    recorded = ""
    body_parts: list[str] = []
    for child in contrib:
        tag = _local(child)
        if tag == "from":
            speaker = _text_of(child)
        else:
            # recordedTime may be nested inside <from>; also check descendants
            body_parts.append(_text_of(child))
    for el in contrib.iter():
        if _local(el) == "recordedTime":
            recorded = el.get("time") or _text_of(el)
            break
    body = " ".join(p for p in body_parts if p).strip()
    return speaker, recorded, body


def parse_akn(xml_text: str) -> pd.DataFrame:
    """Pure transform: AKN transcript XML -> one row per member contribution.

    No I/O. Returns an empty (schema-shaped) frame for non-debate or empty XML.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.warning("speech_parse: XML parse error (%s) — skipping document", exc)
        return pd.DataFrame(columns=_SCHEMA)

    date, chamber = _frbr_date_chamber(root)
    people = _person_map(root)
    sections = _parent_sections(root)

    rows: list[dict] = []
    order = 0
    for el in root.iter():
        if _local(el) not in _CONTRIB_TAGS:
            continue
        section = sections.get(el)
        if section is None:
            continue
        order += 1
        by = (el.get("by") or "").lstrip("#")
        speaker, recorded, body = _speaker_and_body(el)
        rows.append(
            {
                "date": date,
                "chamber": chamber,
                "debate_section_id": section.get("eId"),
                "section_heading": _section_heading(section),
                "contribution_type": _local(el),
                "contribution_order": order,
                "akn_eid": el.get("eId"),
                "unique_member_code": people.get(by, ""),
                "speaker_raw": speaker,
                "recorded_time": recorded,
                "speech_text": body,
            }
        )

    if not rows:
        return pd.DataFrame(columns=_SCHEMA)
    return pd.DataFrame(rows, columns=_SCHEMA)


def parse_files(paths: list[Path]) -> pd.DataFrame:
    """Parse a list of AKN XML files into one concatenated silver frame."""
    frames: list[pd.DataFrame] = []
    for p in paths:
        try:
            frames.append(parse_akn(p.read_text(encoding="utf-8")))
        except OSError as exc:
            logger.warning("speech_parse: cannot read %s (%s)", p, exc)
    if not frames:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.concat(frames, ignore_index=True)
    # Section ids recur every sitting day; identity is (date, chamber, dbsect, eid).
    return df.drop_duplicates(subset=["date", "chamber", "debate_section_id", "akn_eid"])


def run(probe: bool = False) -> int:
    """Parse the AKN pool (or probe files) into speeches.parquet."""
    src = _PROBE_DIR if probe else AKN_DIR
    paths = sorted(src.glob("*.xml"))
    if not paths:
        logger.warning("speech_parse: no AKN XML found in %s", src)
        return 0

    df = parse_files(paths)
    if df.empty:
        logger.warning("speech_parse: parsed 0 contributions from %d files", len(paths))
        return 0

    resolved = (df["unique_member_code"] != "").sum()
    logger.info(
        "speech_parse: files=%d contributions=%d resolved_members=%d/%d (%.0f%%) sections=%d chambers=%s types=%s",
        len(paths),
        len(df),
        resolved,
        len(df),
        100 * resolved / len(df),
        df["debate_section_id"].nunique(),
        sorted(df["chamber"].unique().tolist()),
        df["contribution_type"].value_counts().to_dict(),
    )

    if not probe:
        save_parquet(df, _OUT)
        logger.info("speech_parse: wrote %s (%d rows)", _OUT, len(df))
    return len(df)


def main(argv: list[str] | None = None) -> int:
    from services.logging_setup import setup_logging

    setup_logging()
    parser = argparse.ArgumentParser(description="Parse AKN debate XML to silver speeches.")
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Parse data/bronze/debates/probe/*.xml for a smoke-test (no parquet write).",
    )
    args = parser.parse_args(argv)
    return 0 if run(probe=args.probe) >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
