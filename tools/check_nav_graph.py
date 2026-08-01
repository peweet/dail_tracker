"""Navigation-graph ratchet — flags contextual cul-de-sacs statically.

Companion lint to doc/NAVIGATION_GRAPH.md. That doc's closing note asked for
exactly this: "flag a known entity column rendered in a card without a matching
entity_links helper — to stop regressions."

The governing axiom is [[feedback_entity_links_seamless_navigation]]: every named
entity on screen must be a clickable door that CARRIES the entity to its canonical
page. Score edges by "does the entity travel?", not "is there a link?". A page that
renders a known entity column but calls no entity_links builder for that entity is a
candidate CONTEXTUAL CUL-DE-SAC — the user can leave via nav chrome but drops the
entity and must re-search.

WHAT THIS IS AND ISN'T
  - Static + deterministic (AST, no server). Catches "entity named, not carried OUT".
  - NOT ground truth: edges are state-indexed and this can't see loop closure or raw
    ?param anchors. Confirm a real cul-de-sac on live DOM with
    tools/nav_graph_verify.py before treating a flag as fact.

RATCHET (mirrors tools/check_conventions.py): pages that legitimately don't carry an
entity are grandfathered in BASELINE below with a reason. A NEW offender fails. Fix a
baselined page -> the tool tells you to delete its baseline entry. Never add to the
baseline to silence a genuine cul-de-sac; wire the entity_links helper instead.

Run:  ./.venv/Scripts/python tools/check_nav_graph.py
Also runs in the fast pytest suite via test/tools/test_nav_graph.py.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGES = ROOT / "utility" / "pages_code"

# Builder (entity_links helper) -> the canonical entity it carries the reader TO.
BUILDER_TARGET: dict[str, str] = {
    "member_profile_url": "MEMBER",
    "member_votes_url": "MEMBER",
    "member_link_html": "MEMBER",
    "oireachtas_profile_url": "MEMBER",
    "division_url": "DIVISION",
    "bill_detail_url": "BILL",
    "si_detail_url": "SI",
    "company_profile_url": "COMPANY",
    "authority_profile_url": "AUTHORITY",
    "council_accountability_url": "COUNCIL",
    "council_spending_url": "COUNCIL",
    "lobbying_org_url": "LOBBY_ORG",
    "corporate_notices_url": "CORP_NOTICE",
}

# Known entity column -> the entity that SHOULD travel when the column is rendered.
# These are the pipeline join keys that identify a canonical node (see the nav-graph
# memory). Presence of the column in rendering code is the "entity is on screen" signal.
ENTITY_COLS: dict[str, str] = {
    "unique_member_code": "MEMBER",
    "member_code": "MEMBER",
    "bill_id": "BILL",
    "si_id": "SI",
    "vote_id": "DIVISION",
    "supplier_normalised": "COMPANY",
    "supplier_norm": "COMPANY",
    "contracting_authority": "AUTHORITY",
}

# A page IS the canonical destination for an entity -> referencing that entity's own
# column is expected (you don't link /company to /company). Slugs map to page files via
# utility/ui/entity_links.PAGES; keep filename<->entity here.
CANONICAL_PAGE: dict[str, str] = {
    "member_overview.py": "MEMBER",
    "company.py": "COMPANY",
    "statutory_instruments.py": "SI",
    "legislation.py": "BILL",
    "votes.py": "DIVISION",
}

# Grandfathered cul-de-sacs: {filename: (entity, reason)}. A baselined page still
# renders the entity without carrying it, but the drop is accepted (documented here).
# Confirm each on live DOM before deciding fix-vs-accept; do NOT add new entries to
# silence a real regression.
BASELINE: dict[str, tuple[str, str]] = {
    # follow_the_money DOES carry suppliers to /company — but via the shared procurement
    # renderers it imports (_render_payments_supplier_profile / _render_payment_lines,
    # both gated on awards_register_norms), not a direct builder call in THIS file. This
    # checker is file-level and can't see cross-file renderer reuse, so the flag is a known
    # blind spot, not a cul-de-sac. Confirmed carrying on live DOM 2026-07-23.
    "follow_the_money.py": ("COMPANY", "carries via imported procurement renderers (file-level blind spot)"),
}


def scan(path: Path) -> tuple[set[str], set[str]]:
    """Return (entities carried OUT via a builder, entity columns referenced in CODE).

    Column detection is AST-level (string constants + attribute names) so comments
    and docstrings never trigger a false positive.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    carried: set[str] = set()
    cols: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.id if isinstance(fn, ast.Name) else fn.attr if isinstance(fn, ast.Attribute) else None
            if name in BUILDER_TARGET:
                carried.add(BUILDER_TARGET[name])
        elif isinstance(node, ast.Attribute):
            if node.attr in ENTITY_COLS:
                cols.add(node.attr)
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in ENTITY_COLS:
                cols.add(node.value)
    return carried, cols


def main() -> int:
    live: list[tuple[str, str, list[str]]] = []  # real (non-baseline) cul-de-sacs
    stale_baseline: list[str] = []  # baselined but now clean -> remove
    for f in sorted(PAGES.glob("*.py")):
        if f.name == "__init__.py":
            continue
        carried, cols = scan(f)
        own = CANONICAL_PAGE.get(f.name)
        uncarried = sorted({ENTITY_COLS[c] for c in cols} - carried - ({own} if own else set()))
        if not uncarried:
            if f.name in BASELINE:
                stale_baseline.append(f.name)
            continue
        cols_for = sorted(c for c in cols if ENTITY_COLS[c] in uncarried)
        if f.name in BASELINE and BASELINE[f.name][0] in uncarried:
            continue  # accepted
        live.append((f.name, ", ".join(uncarried), cols_for))

    print("=" * 78)
    print("NAV-GRAPH CUL-DE-SAC CHECK  (entity column rendered, not carried out)")
    print("=" * 78)
    if live:
        for name, ents, cols_for in live:
            print(f"  FAIL  {name:30s} renders {cols_for} but does not carry {ents}")
        print(f"\n  {len(live)} un-baselined cul-de-sac(s). Wire the entity_links helper,")
        print("  or (if the drop is genuinely correct) add to BASELINE with a reason.")
    else:
        print("  PASS — every rendered entity column is carried to its canonical page")
        print("         (baselined exceptions excluded).")
    if stale_baseline:
        print(f"\n  BASELINE STALE — now clean, delete from BASELINE: {stale_baseline}")

    return 1 if (live or stale_baseline) else 0


if __name__ == "__main__":
    sys.exit(main())
