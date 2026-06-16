"""Resolve a catalogue node's `rule_ref` to VERBATIM text for the council in force.

The rulebook is per-council and time-versioned (memory project_planning_rules_collection):
  planning_rules/<subdir>/<slug>/required_assessments.md  — the checklist table
                                /dm_standards.md            — the standards text
A node's rule_ref names checklist row numbers and/or DM-Standard numbers; this module
returns the actual published wording so the UI can quote it (never paraphrase, never infer).

Two authoring shapes are supported (a council may use either, per its plan's structure):

  1. NUMBERED (the Galway County exemplar) — the plan numbers its standards "DM Standard N",
     so the catalogue's rule_ref `dm_std`/`checklist` numbers index straight into them. Parsed
     by parse_dm_standards() (DM headings) + parse_required_assessments() (a numbered pipe table).

  2. CONCEPT-KEYED — most councils' plans do NOT use "DM Standard N" numbering; their standards
     live in named chapter sections whose numbering doesn't line up with the catalogue. Those
     councils author each block tagged with the catalogue node id it answers:
         ### node: landscape_siting — Landscape & visual amenity
         ref: Galway City DP 2023–2029, Ch.11
         <verbatim text…>
     and a concept checklist table keyed by node id (first column = node id, not a digit).
     Parsed by parse_dm_concepts() / parse_checklist_concepts(). resolve() prefers a
     concept-keyed hit for a node, falling back to the numbered path.

No-inference contract: if a council does not publish a value/standard, we return nothing
for it and the caller shows "no fixed standard published" — we never synthesise one. Where a
ref cannot be resolved at all, it is recorded in `missing` rather than raising.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from .catalogue import REPO_ROOT, load_catalogue

PLANNING_RULES = REPO_ROOT / "planning_rules"
COUNCIL_SUBDIRS = ("county_councils", "city_councils", "city_and_county_councils")

# "DM Standard 51: Title" / "## DM Standard 8 – Title" / "**DM Standard 5** - Title" /
# "DM Std 9. Title" — tolerant of leading #/* markdown and :/–/-/. separators.
_DM_HEADING = re.compile(
    r"^\s*#{0,6}\s*\*{0,2}\s*DM\s+(?:Standard|Std)\s+(\d+)\s*\*{0,2}\s*[:.–—-]\s*(.*)$",
    re.IGNORECASE,
)
# concept-keyed heading: "### node: landscape_siting — Title" / "## node:bats - Title"
_CONCEPT_HEADING = re.compile(
    r"^\s*#{1,6}\s*node:\s*([a-z][a-z0-9_]+)\s*[:.–—-]\s*(.*)$", re.IGNORECASE
)
# an optional source-citation line directly under a concept heading: "ref: …" / "> ref: …"
_REF_LINE = re.compile(r"^\s*>?\s*ref:\s*(.*)$", re.IGNORECASE)


@dataclass(frozen=True)
class ChecklistItem:
    number: int
    document: str
    trigger: str
    dm_std: str
    layer: str


@dataclass(frozen=True)
class DMStandard:
    number: int          # the plan's "DM Standard N" number, or 0 for a concept-keyed section
    title: str
    text: str
    source_ref: str = ""  # the council's own plan citation (concept-keyed councils); else ""


@dataclass(frozen=True)
class ResolvedRule:
    council_slug: str
    council_name: str
    plan_name: str
    checklist: tuple[ChecklistItem, ...] = ()
    dm_standards: tuple[DMStandard, ...] = ()
    regs: str = ""
    guidance: str = ""
    override: dict = field(default_factory=dict)
    missing: tuple[str, ...] = ()  # human-readable notes on refs that couldn't resolve


def find_council_dir(slug: str) -> Path | None:
    for sub in COUNCIL_SUBDIRS:
        d = PLANNING_RULES / sub / slug
        if d.is_dir():
            return d
    return None


def _strip_md(cell: str) -> str:
    """Drop markdown bold and collapse whitespace in a table cell."""
    return re.sub(r"\s+", " ", cell.replace("**", "")).strip()


@lru_cache(maxsize=64)
def parse_required_assessments(slug: str) -> dict[int, ChecklistItem]:
    d = find_council_dir(slug)
    if not d:
        return {}
    p = d / "required_assessments.md"
    if not p.exists():
        return {}
    out: dict[int, ChecklistItem] = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 5 or not cells[0].isdigit():
            continue  # header / separator / malformed rows
        num = int(cells[0])
        out[num] = ChecklistItem(
            number=num,
            document=_strip_md(cells[1]),
            trigger=_strip_md(cells[2]),
            dm_std=_strip_md(cells[3]),
            layer=_strip_md(cells[4]),
        )
    return out


@lru_cache(maxsize=64)
def parse_dm_standards(slug: str) -> dict[int, DMStandard]:
    d = find_council_dir(slug)
    if not d:
        return {}
    p = d / "dm_standards.md"
    if not p.exists():
        return {}
    lines = p.read_text(encoding="utf-8").splitlines()
    # find every DM-Standard heading; body runs to the next heading (or EOF)
    heads: list[tuple[int, int, str]] = []  # (line_idx, number, title)
    for i, line in enumerate(lines):
        m = _DM_HEADING.match(line)
        if m:
            heads.append((i, int(m.group(1)), m.group(2).strip()))
    out: dict[int, DMStandard] = {}
    for j, (idx, num, title) in enumerate(heads):
        end = heads[j + 1][0] if j + 1 < len(heads) else len(lines)
        body = "\n".join(lines[idx + 1 : end]).strip()
        # keep the FIRST occurrence of a number (later dupes, if any, are TOC noise)
        if num not in out:
            out[num] = DMStandard(number=num, title=title, text=body)
    return out


@lru_cache(maxsize=64)
def parse_dm_concepts(slug: str) -> dict[str, DMStandard]:
    """Concept-keyed standards: node_id -> DMStandard, for plans that don't use DM numbers.

    Reads `### node: <node_id> — <Title>` blocks (optional `ref:` citation line first), body
    running to the next concept heading or EOF. number=0 marks it concept-keyed (the UI shows
    the council's own `source_ref` instead of a fabricated "DM Standard N").
    """
    d = find_council_dir(slug)
    if not d:
        return {}
    p = d / "dm_standards.md"
    if not p.exists():
        return {}
    lines = p.read_text(encoding="utf-8").splitlines()
    heads: list[tuple[int, str, str]] = []  # (line_idx, node_id, title)
    for i, line in enumerate(lines):
        m = _CONCEPT_HEADING.match(line)
        if m:
            heads.append((i, m.group(1).lower(), m.group(2).strip()))
    out: dict[str, DMStandard] = {}
    for j, (idx, node_id, title) in enumerate(heads):
        end = heads[j + 1][0] if j + 1 < len(heads) else len(lines)
        body_lines = lines[idx + 1 : end]
        source_ref = ""
        # pull a leading ref: line (skipping blank lines) out of the body
        for k, bl in enumerate(body_lines):
            if not bl.strip():
                continue
            rm = _REF_LINE.match(bl)
            if rm:
                source_ref = rm.group(1).strip()
                body_lines = body_lines[:k] + body_lines[k + 1 :]
            break
        body = "\n".join(body_lines).strip()
        if node_id not in out:  # first wins (later dupes = TOC/noise)
            out[node_id] = DMStandard(number=0, title=title, text=body, source_ref=source_ref)
    return out


@lru_cache(maxsize=64)
def parse_checklist_concepts(slug: str) -> dict[str, tuple[ChecklistItem, ...]]:
    """Concept-keyed checklist: node_id -> required documents, for non-numbered councils.

    Reads a pipe table whose first column is a node id (not a digit):
        | node | Required document | Trigger condition | Ref |
    Multiple rows may share a node id (a node can require several documents).
    """
    d = find_council_dir(slug)
    if not d:
        return {}
    p = d / "required_assessments.md"
    if not p.exists():
        return {}
    grouped: dict[str, list[ChecklistItem]] = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.lstrip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        node_id = cells[0].lower()
        # a concept row's key is a node id (letters/underscore), never a digit or separator
        if not re.fullmatch(r"[a-z][a-z0-9_]+", node_id):
            continue
        if node_id == "node":  # the table header row (| node | Required document | … |)
            continue
        ref = _strip_md(cells[3]) if len(cells) > 3 else ""
        item = ChecklistItem(
            number=0,
            document=_strip_md(cells[1]),
            trigger=_strip_md(cells[2]),
            dm_std=ref,
            layer=_strip_md(cells[4]) if len(cells) > 4 else "",
        )
        grouped.setdefault(node_id, []).append(item)
    return {k: tuple(v) for k, v in grouped.items()}


@lru_cache(maxsize=1)
def _council_names() -> dict[str, tuple[str, str]]:
    """slug -> (council_name, plan_name) from the _criteria_map JSONs."""
    import json

    out: dict[str, tuple[str, str]] = {}
    cm = PLANNING_RULES / "_criteria_map"
    if cm.is_dir():
        for f in cm.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                out[data.get("slug", f.stem)] = (
                    data.get("council", f.stem),
                    data.get("plan_name", ""),
                )
            except Exception:
                continue
    return out


def resolve(council_slug: str, node_id: str, catalogue_path: str | None = None) -> ResolvedRule:
    """Resolve a node's rule_ref to verbatim text for `council_slug`."""
    cat = load_catalogue(catalogue_path)
    node = cat.node(node_id)
    ref = node.rule_ref
    override = cat.override_for(council_slug, node_id)
    name, plan = _council_names().get(council_slug, (council_slug, ""))

    # council_overrides may swap which DM-standard numbers apply (numbering differs per plan)
    dm_nums = list(override.get("dm_std") or ref.get("dm_std") or [])
    chk_nums = list(ref.get("checklist") or [])

    checklist_map = parse_required_assessments(council_slug)
    dm_map = parse_dm_standards(council_slug)
    dm_concepts = parse_dm_concepts(council_slug)
    chk_concepts = parse_checklist_concepts(council_slug)

    missing: list[str] = []

    # DM standards: a concept-keyed block for this node wins; else index by number.
    if node_id in dm_concepts:
        dm_standards = [dm_concepts[node_id]]
    else:
        dm_standards = []
        for n in dm_nums:
            std = dm_map.get(int(n))
            if std:
                dm_standards.append(std)
            else:
                missing.append(f"DM Standard {n}")

    # Checklist: concept-keyed rows for this node win; else index by number.
    if node_id in chk_concepts:
        checklist = list(chk_concepts[node_id])
    else:
        checklist = []
        for n in chk_nums:
            item = checklist_map.get(int(n))
            if item:
                checklist.append(item)
            else:
                missing.append(f"checklist #{n}")

    return ResolvedRule(
        council_slug=council_slug,
        council_name=name,
        plan_name=str(override.get("plan") or plan),
        checklist=tuple(checklist),
        dm_standards=tuple(dm_standards),
        regs=str(ref.get("regs", "")),
        guidance=str(ref.get("guidance", "")),
        override=override,
        missing=tuple(missing),
    )
