"""Load planning_rules/issue_catalogue.yaml into typed, validated nodes.

The catalogue is the council-agnostic decision-tree config (one block per issue node).
Each node names a `rule_ref` into the per-council rulebook (resolved by rulebook.py) and
the `source_layers` its trigger needs (evaluated by the engine). This module only loads
and validates the config — no spatial work, no network — so it is cheap to unit-test.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# repo_root/dail_tracker_core/siting/catalogue.py -> repo_root
REPO_ROOT = Path(__file__).resolve().parents[2]
CATALOGUE_PATH = REPO_ROOT / "planning_rules" / "issue_catalogue.yaml"

# mitigation classes (P=procedural, D=mitigable-by-design, F=often-fatal); ranges like
# "D->F" / "F (Zone A) / D (Zone B)" are allowed — we keep the raw string and expose the
# set of base classes it mentions.
_VALID_CLASS_CHARS = {"P", "D", "F"}
# leaf outcomes a mitigation_path branch may declare (mirror the P/D/F vocabulary)
_VALID_OUTCOMES = {"clear", "mitigable", "fatal"}


@dataclass(frozen=True)
class Node:
    id: str
    title: str
    layer: str  # A=universal | B=location trigger | C=type & siting
    applies_to: tuple[str, ...]
    trigger: dict[str, Any]
    flag_template: str
    engage: tuple[str, ...]
    rule_ref: dict[str, Any]
    mitigation_class: str
    mitigates: str
    precedents: tuple[dict[str, Any], ...]
    risk_note: str
    # optional static if/then mitigation cascade (survey -> finding -> follow-on); empty for
    # nodes that keep the flat `mitigates` line. Rendered by brief.py.
    mitigation_path: tuple[dict[str, Any], ...] = ()

    @property
    def source_layers(self) -> tuple[str, ...]:
        return tuple(self.trigger.get("source_layers") or ())

    @property
    def mitigation_classes(self) -> frozenset[str]:
        """The base P/D/F classes mentioned in the (possibly ranged) class string."""
        return frozenset(c for c in self.mitigation_class.upper() if c in _VALID_CLASS_CHARS)

    def applies(self, dev_type: str) -> bool:
        return "all" in self.applies_to or dev_type in self.applies_to


@dataclass(frozen=True)
class Catalogue:
    meta: dict[str, Any]
    layers: dict[str, str]
    source_layers: dict[str, dict[str, Any]]
    nodes: tuple[Node, ...]
    council_overrides: dict[str, dict[str, Any]]

    def node(self, node_id: str) -> Node:
        for n in self.nodes:
            if n.id == node_id:
                return n
        raise KeyError(f"no catalogue node with id={node_id!r}")

    @property
    def disclaimer(self) -> str:
        return self.meta.get("disclaimer", "")

    def override_for(self, council_slug: str, node_id: str) -> dict[str, Any]:
        """Per-council specifics for a node (DM-standard numbers, zoning codes, …)."""
        return (self.council_overrides.get(council_slug, {}) or {}).get(node_id, {}) or {}


def _validate_path_step(step: dict[str, Any], node_id: str) -> None:
    """A mitigation_path step needs a `do`; each branch needs an `if` + a valid `outcome`."""
    assert isinstance(step, dict) and step.get("do"), (
        f"node {node_id}: mitigation_path step missing 'do'"
    )
    for br in step.get("findings") or ():
        assert br.get("if"), f"node {node_id}: mitigation_path branch missing 'if'"
        assert br.get("outcome") in _VALID_OUTCOMES, (
            f"node {node_id}: bad branch outcome {br.get('outcome')!r}"
        )
        for child in br.get("then") or ():
            _validate_path_step(child, node_id)


def _validate(cat: Catalogue) -> None:
    seen: set[str] = set()
    for n in cat.nodes:
        assert n.id, "node missing id"
        assert n.id not in seen, f"duplicate node id {n.id!r}"
        seen.add(n.id)
        assert n.layer in cat.layers, f"node {n.id}: unknown layer {n.layer!r}"
        assert n.mitigation_classes <= _VALID_CLASS_CHARS, (
            f"node {n.id}: bad mitigation_class {n.mitigation_class!r}"
        )
        for sl in n.source_layers:
            assert sl in cat.source_layers, f"node {n.id}: unknown source_layer {sl!r}"
        for step in n.mitigation_path:
            _validate_path_step(step, n.id)
    # council_overrides must reference real node ids
    for slug, ov in cat.council_overrides.items():
        for nid in ov:
            assert nid in seen, f"council_override {slug}: unknown node id {nid!r}"


@lru_cache(maxsize=4)
def load_catalogue(path: str | None = None) -> Catalogue:
    p = Path(path) if path else CATALOGUE_PATH
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    nodes = tuple(
        Node(
            id=n["id"],
            title=n["title"],
            layer=n["layer"],
            applies_to=tuple(n.get("applies_to") or ()),
            trigger=dict(n.get("trigger") or {}),
            flag_template=str(n.get("flag_template", "")).strip(),
            engage=tuple(n.get("engage") or ()),
            rule_ref=dict(n.get("rule_ref") or {}),
            mitigation_class=str(n.get("mitigation_class", "")),
            mitigates=str(n.get("mitigates", "")),
            precedents=tuple(n.get("precedents") or ()),
            risk_note=str(n.get("risk_note", "")),
            mitigation_path=tuple(n.get("mitigation_path") or ()),
        )
        for n in (raw.get("nodes") or [])
    )
    cat = Catalogue(
        meta=dict(raw.get("meta") or {}),
        layers=dict(raw.get("layers") or {}),
        source_layers=dict(raw.get("source_layers") or {}),
        nodes=nodes,
        council_overrides=dict(raw.get("council_overrides") or {}),
    )
    _validate(cat)
    return cat
