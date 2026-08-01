"""Money-grain lint — no SQL view may SUM or UNION across money grains.

The never-break rule (CLAUDE.md): the money grains never union/sum together —
procurement awarded vs. payments vs. budget are different money, and TED
overlaps awards. Until 2026-08-01 this rule was enforced by metadata only
(fact-card ``never_sum_with`` carriage + per-row ``value_safe_to_sum`` flags,
test/tools/test_fact_cards.py, test/contracts/test_gold_invariants.py) — the
*act* of writing a cross-grain SUM in a new view had no machine check. This is
that check, at zero baseline.

What fails (per view file in sql_views/):
  * a single SUM(...) whose column qualifiers resolve to sources of >=2
    different money grains (e.g. ``SUM(a.value + p.amount)``);
  * a UNION whose sides read sources of different money grains.

What deliberately does NOT fail:
  * a view that references two grains and SUMs each separately — comparison
    views (procurement_afs_vs_po_coverage) are the sanctioned pattern: compare
    side by side, never add together. These are printed as NOTEs for visibility.

Known limits (v1, stated per the no-silent-caps rule):
  * alias->source mapping is file-global, not per-scope — two subqueries reusing
    one alias for different-grain sources would confuse attribution (none exist
    today; the zero baseline holds under this approximation);
  * SUM over a column of a referenced VIEW is attributed via that view's
    transitive grain set only when the set is unambiguous (single-grain).

Grain map comes from data/_meta/fact_cards.json (``money_grain``) — the same
single source of truth the metadata tests already assert against. Needs sqlglot
(the mcp extra); exits SKIP when absent, and the pytest wrapper skips too.

Run:  ./.venv/Scripts/python tools/check_money_grain_sums.py
Also runs in the fast pytest suite via test/tools/test_money_grain_sums.py.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FACT_CARDS = ROOT / "data" / "_meta" / "fact_cards.json"
SQL_VIEWS = ROOT / "sql_views"


def load_grain_map() -> dict[str, str]:
    """Dataset stem -> money grain, from fact_cards.json."""
    cards = json.loads(FACT_CARDS.read_text(encoding="utf-8"))["facts"]
    return {stem: card["money_grain"] for stem, card in cards.items() if card.get("money_grain")}


def _stem_of(path_literal: str) -> str:
    name = path_literal.replace("\\", "/").rstrip("*").split("/")[-1]
    for suffix in (".parquet", ".csv"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


def analyse(sql_text: str, view_name: str, grain_map: dict[str, str], view_grains: dict[str, set[str]]):
    """Returns (violations, grains_touched) for one view body."""
    import sqlglot
    from sqlglot import exp

    violations: list[str] = []
    try:
        statements = sqlglot.parse(sql_text, read="duckdb")
    except sqlglot.errors.ParseError as e:  # a view we can't parse is a visibility gap, not a pass
        return [f"{view_name}: sqlglot could not parse the view ({str(e).splitlines()[0]}) — check it by hand"], set()

    # alias/name -> grain set, file-global (see docstring limit note)
    source_grains: dict[str, set[str]] = {}
    grains_touched: set[str] = set()

    def grains_for_table(node) -> set[str]:
        # read_parquet('...') appears as an anonymous/table function inside FROM
        for lit in node.find_all(exp.Literal):
            if lit.is_string:
                g = grain_map.get(_stem_of(lit.this))
                if g:
                    return {g}
        name = (node.name or "").lower()
        if name in grain_map:
            return {grain_map[name]}
        if name in view_grains:
            return set(view_grains[name])
        return set()

    for stmt in statements:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            g = grains_for_table(table)
            if not g:
                continue
            grains_touched |= g
            alias = (table.alias or table.name or "").lower()
            if alias:
                source_grains.setdefault(alias, set()).update(g)

        # UNION sides must not mix grains
        for union in stmt.find_all(exp.Union):
            sides = []
            for side in (union.left, union.right):
                side_g: set[str] = set()
                for table in side.find_all(exp.Table):
                    side_g |= grains_for_table(table)
                sides.append(side_g)
            left, right = sides
            if left and right and left != right:
                violations.append(
                    f"{view_name}: UNION mixes money grains {sorted(left)} vs {sorted(right)} — "
                    f"the grains are different money and never union (CLAUDE.md never-break rule)"
                )

        # a single SUM must not draw columns from >=2 grains
        for func in stmt.find_all(exp.Sum):
            arg_grains: set[str] = set()
            for col in func.find_all(exp.Column):
                qual = (col.table or "").lower()
                g = source_grains.get(qual, set())
                if len(g) == 1:  # ambiguous (multi-grain view source) stays unattributed, see docstring
                    arg_grains |= g
            if len(arg_grains) >= 2:
                violations.append(
                    f"{view_name}: SUM() draws columns from money grains {sorted(arg_grains)} — "
                    f"never sum across grains (CLAUDE.md never-break rule)"
                )

    return violations, grains_touched


def main() -> int:
    try:
        import sqlglot  # noqa: F401
    except ImportError:
        print("SKIP — sqlglot not installed (mcp extra); money-grain lint not evaluated.")
        return 0
    if not FACT_CARDS.exists() or not SQL_VIEWS.exists():
        print("SKIP — fact_cards.json or sql_views/ missing in this checkout.")
        return 0

    grain_map = load_grain_map()
    files = sorted(SQL_VIEWS.rglob("*.sql"))

    # Pass 1+2: propagate grain sets through view->view references to fixpoint,
    # so a view over a cross-grain view still counts as touching both grains.
    view_grains: dict[str, set[str]] = {f.stem.lower(): set() for f in files}
    texts = {f: f.read_text(encoding="utf-8", errors="replace") for f in files}
    for _ in range(4):  # dependency chains are shallow; fixpoint in practice
        changed = False
        for f in files:
            _, touched = analyse(texts[f], f.stem, grain_map, view_grains)
            if touched - view_grains[f.stem.lower()]:
                view_grains[f.stem.lower()] |= touched
                changed = True
        if not changed:
            break

    violations: list[str] = []
    notes: list[str] = []
    for f in files:
        v, touched = analyse(texts[f], f.relative_to(ROOT).as_posix(), grain_map, view_grains)
        violations.extend(v)
        if len(touched) >= 2 and not v:
            notes.append(f"{f.relative_to(ROOT).as_posix()} touches grains {sorted(touched)} (comparison-only — OK)")

    for n in notes:
        print(f"NOTE  {n}")
    if violations:
        for v in violations:
            print(f"FAIL  {v}")
        print(f"\nFAIL — {len(violations)} money-grain violation(s).")
        return 1
    print(f"OK — no cross-grain SUM/UNION in {len(files)} views ({len(notes)} sanctioned comparison view(s) noted).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
