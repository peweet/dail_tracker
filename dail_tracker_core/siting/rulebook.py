"""Resolve a catalogue node's `rule_ref` to VERBATIM text for the council in force.

The rulebook is per-council and time-versioned (memory project_planning_rules_collection):
  planning_rules/<subdir>/<slug>/required_assessments.md  — the checklist table
                                /dm_standards.md            — the numeric "DM Standard N" text
A node's rule_ref names checklist row numbers and/or DM-Standard numbers; this module
returns the actual published wording so the UI can quote it (never paraphrase, never infer).

No-inference contract: if a council does not publish a value/standard, we return nothing
for it and the caller must show "no fixed standard published" — we do not synthesise one.
Format note: parsing is validated against the Galway exemplar; other councils' markdown
shapes vary (the rulebook is non-uniform) — resolve() degrades gracefully (returns the
checklist row even if the DM-standard heading can't be located).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

from .catalogue import REPO_ROOT, load_catalogue

PLANNING_RULES = REPO_ROOT / "planning_rules"
COUNCIL_SUBDIRS = ("county_councils", "city_councils", "city_and_county_councils")

# "DM Standard 51: Title" / "DM Standard 8 – Title" / "DM Standard 5 - Title"
_DM_HEADING = re.compile(r"^\s*DM Standard\s+(\d+)\s*[:–-]\s*(.*)$", re.IGNORECASE)


@dataclass(frozen=True)
class ChecklistItem:
    number: int
    document: str
    trigger: str
    dm_std: str
    layer: str


@dataclass(frozen=True)
class DMStandard:
    number: int
    title: str
    text: str


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

    missing: list[str] = []
    checklist = []
    for n in chk_nums:
        item = checklist_map.get(int(n))
        if item:
            checklist.append(item)
        else:
            missing.append(f"checklist #{n}")
    dm_standards = []
    for n in dm_nums:
        std = dm_map.get(int(n))
        if std:
            dm_standards.append(std)
        else:
            missing.append(f"DM Standard {n}")

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
