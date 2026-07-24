"""Join-graph reader — maps how the registers are actually wired, statically.

Answers "how deep are the joins really?" by extracting every join site in the
repo and grading its key, instead of asserting depth from memory. Two surfaces,
two techniques:

  - SQL  (sql_views/*.sql): regex pass. Resolves table aliases within each file,
    then reads `x.col = y.col` from each JOIN's ON clause into an edge
    (table -> table on key). The table names ARE the registers, so this half
    gives the connectivity graph.
  - Polars (*.py): stdlib `ast` pass. Matches `.join(...)` calls that carry
    `on=`/`left_on=`/`right_on=`/`how=` kwargs — the signature that separates a
    DataFrame join from `str.join`/`os.path.join`. Records file:line + keys.
    Variable names aren't registers, so this half grades key HYGIENE, not the
    graph.

KEY GRADES (the depth signal). Per the join map, the canonical keys are the
normalised ones — ORG via shared/name_norm.py (`*_norm`), PERSON via
shared/normalise_join_key.py (`join_key`). Structured IDs (cro_number,
unique_member_code, rcn...) are the strongest joins. A join on a RAW name column
(full_name, member_name, supplier_name — no `_norm`) is the fragile anti-pattern
the join map warns about: not accent-folded, silently misses twins. Those are
the actionable flags.

WHAT THIS IS AND ISN'T
  - A reader, not a ratchet. Exit code is always 0 unless --strict is passed
    (then a raw-name join fails). It measures wiring; it does NOT prove a join
    returns correct rows (0 = not-matched vs absent stays invisible to a parser).
  - SQL table resolution is regex-level: a JOIN onto a CTE name resolves to the
    CTE, not a real fact, and appears as a node. sqlglot would resolve those
    properly — declare it in pyproject and swap in _sql_edges_sqlglot() if the
    CTE noise gets in the way. This first cut takes no new dependency.

Run:  ./.venv/Scripts/python tools/join_graph.py [--json] [--strict]
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql_views"
# Python trees where DataFrame joins live. UI (pandas) excluded — ETL is polars.
PY_DIRS = ["extractors", "dail_tracker_core", "shared", "charity", "pipeline_sandbox"]

# --- key grading -----------------------------------------------------------
# CANON      — the documented normalised keys (name_norm family + join_key).
# ID         — structured exact identifiers (strongest joins).
# CONTROLLED — a name column, but over a CLOSED vocabulary (council/constituency
#              names — 31 LAs, ~40 constituencies). Safe by design; the SQL layer
#              joins these deliberately (often documented in the view header).
# RAW        — an OPEN-name-space column (person/company): full_name, member_name,
#              supplier_name with no _norm. The fragile, off-contract join.
# OTHER      — composite/dimension keys (year, house) — fine.
_ID_SUFFIX = re.compile(r".*(_id|_code|_num|_number|_uid|_ref|_pk)$")
_ID_EXACT = {"rcn", "eircode", "abpcaseid", "cro_number", "company_num", "vote_id"}
# closed-vocabulary name columns — a join on these is a controlled-set match, not
# an open person/company name match. Keep this list explicit and small.
_CONTROLLED = {
    "council", "constituency", "constituency_name", "publisher_name",
    "la_name", "local_authority",
}


def grade_key(col: str) -> str:
    c = col.strip().lower().strip('"`[]')
    if c.endswith("_norm") or c == "join_key" or c.endswith("_normalised"):
        return "CANON"
    if c in _ID_EXACT or _ID_SUFFIX.match(c):
        return "ID"
    if c in _CONTROLLED:
        return "CONTROLLED"
    if "name" in c:  # full_name, member_name, supplier_name, minister_name...
        return "RAW"
    return "OTHER"


# --- SQL pass --------------------------------------------------------------
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
# a table reference: `FROM|JOIN table [AS] alias`  (table may be schema.qualified)
_TABLE_REF = re.compile(
    r"\b(?:FROM|JOIN)\s+([\w.]+)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?",
    re.IGNORECASE,
)
# a JOIN ... ON <expr> up to the next clause boundary
_JOIN_ON = re.compile(
    r"\bJOIN\s+([\w.]+)(?:\s+(?:AS\s+)?([A-Za-z_]\w*))?\s+ON\s+(.*?)"
    r"(?=\b(?:LEFT|RIGHT|INNER|FULL|CROSS|JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|WINDOW|QUALIFY|UNION)\b|$)",
    re.IGNORECASE | re.DOTALL,
)
# an equijoin predicate a.col = b.col
_EQUI = re.compile(r"([A-Za-z_]\w*)\.([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\.([A-Za-z_]\w*)")
_CLAUSE_KW = {"select", "from", "where", "on", "and", "or", "left", "right",
              "inner", "full", "cross", "join", "group", "order", "having"}


def _strip_sql(text: str) -> str:
    return _LINE_COMMENT.sub("", _BLOCK_COMMENT.sub("", text))


def _alias_map(sql: str) -> dict[str, str]:
    """alias (or table basename) -> table name, for the whole file."""
    m: dict[str, str] = {}
    for table, alias in _TABLE_REF.findall(sql):
        base = table.split(".")[-1]
        m[base.lower()] = base
        if alias and alias.lower() not in _CLAUSE_KW:
            m[alias.lower()] = base
    return m


def parse_sql(sql: str, rel: str) -> list[dict]:
    """Extract equijoin edges from one SQL string. Pure — no filesystem."""
    sql = _strip_sql(sql)
    aliases = _alias_map(sql)
    edges: list[dict] = []
    seen: set[tuple] = set()
    for jtable, _jalias, on_expr in _JOIN_ON.findall(sql):
        for la, lc, ra, rc in _EQUI.findall(on_expr):
            left = aliases.get(la.lower(), la.lower())
            right = aliases.get(ra.lower(), ra.lower())
            lc, rc = lc.lower(), rc.lower()
            # self-referential predicate (a.x = a.x) is an alias-collapse
            # artifact from a self-join, not a real edge — drop it.
            if left == right and lc == rc:
                continue
            key = (left, lc, right, rc)
            if key in seen:
                continue
            seen.add(key)
            # both sides of an equijoin usually share a column name; grade the
            # worse of the two so a raw-name side is never hidden by an id side
            grade = min((grade_key(lc), grade_key(rc)), key=_GRADE_RANK.get)
            edges.append({
                "file": rel, "surface": "sql",
                "left": left, "right": right,
                "left_key": lc, "right_key": rc,
                "grade": grade, "joined_table": jtable.split(".")[-1],
            })
    return edges


def sql_edges() -> list[dict]:
    edges: list[dict] = []
    for f in sorted(SQL_DIR.rglob("*.sql")):
        edges.extend(parse_sql(f.read_text(encoding="utf-8"), f.relative_to(ROOT).as_posix()))
    return edges


# rank so min() picks the weakest grade (RAW is worst to surface)
_GRADE_RANK = {"RAW": 0, "OTHER": 1, "CONTROLLED": 2, "ID": 3, "CANON": 4}
_GRADES = ("CANON", "ID", "CONTROLLED", "RAW", "OTHER")


# --- polars pass -----------------------------------------------------------
_JOIN_KW = {"on", "left_on", "right_on", "how"}


def _kw_key(call: ast.Call, name: str) -> list[str]:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
            return [kw.value.value]
        if kw.arg == name and isinstance(kw.value, ast.List):
            return [e.value for e in kw.value.elts
                    if isinstance(e, ast.Constant) and isinstance(e.value, str)]
    return []


def _kw_const(call: ast.Call, name: str) -> str | None:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


def parse_python(src: str, rel: str) -> list[dict]:
    """Extract DataFrame-join sites from one Python source string. Pure."""
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    sites: list[dict] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "join":
            continue
        kwargs = {kw.arg for kw in node.keywords}
        if not (kwargs & _JOIN_KW):
            continue  # not a DataFrame join (str.join / path.join)
        left = _kw_key(node, "left_on")
        right = _kw_key(node, "right_on")
        keys = sorted(set(_kw_key(node, "on") + left + right))
        grade = (min((grade_key(k) for k in keys), key=_GRADE_RANK.get)
                 if keys else "OTHER")
        # polars default is an inner join — a silent row-dropper. Worth surfacing
        # when the key is already fragile (RAW + inner = quiet miss AND row loss).
        how = _kw_const(node, "how") or "inner"
        # left_on != right_on means the two frames name the key differently — a
        # rename-mismatch smell if the columns aren't truly the same concept.
        asymmetric = bool(left and right and left != right)
        # validate='m:1'/'1:1'/... makes polars RAISE on an unexpected fan-out.
        # A join with no validate= silently multiplies rows if the key isn't
        # unique on the side you assumed — the costliest quiet join bug.
        validate = _kw_const(node, "validate")
        sites.append({"file": rel, "surface": "polars", "line": node.lineno,
                      "keys": keys, "grade": grade, "how": how,
                      "asymmetric": asymmetric, "validate": validate})
    return sites


def polars_joins() -> list[dict]:
    sites: list[dict] = []
    for d in PY_DIRS:
        base = ROOT / d
        if not base.exists():
            continue
        for f in sorted(base.rglob("*.py")):
            sites.extend(parse_python(f.read_text(encoding="utf-8"), f.relative_to(ROOT).as_posix()))
    return sites


# --- report ----------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    ap.add_argument("--strict", action="store_true", help="exit 1 if any RAW-name join exists")
    args = ap.parse_args()

    edges = sql_edges()
    sites = polars_joins()

    raw_sql = [e for e in edges if e["grade"] == "RAW"]      # informational
    raw_poly = [s for s in sites if s["grade"] == "RAW"]     # actionable

    if args.json:
        print(json.dumps({"sql": edges, "polars": sites}, indent=2))
        return 1 if (args.strict and raw_poly) else 0

    def grade_counts(rows):
        c = Counter(r["grade"] for r in rows)
        return "  ".join(f"{g}:{c.get(g, 0)}" for g in _GRADES)

    print("=" * 78)
    print("JOIN GRAPH  —  how the registers are actually wired (static read)")
    print("=" * 78)
    print(f"\nSQL     : {len(edges)} join edges across {len({e['file'] for e in edges})} view files")
    print(f"          key grades  {grade_counts(edges)}")
    print(f"Polars  : {len(sites)} DataFrame joins across {len({s['file'] for s in sites})} py files")
    print(f"          key grades  {grade_counts(sites)}")

    # Polars is where the real matching lives — grade it in more depth.
    inner_raw = [s for s in raw_poly if s["how"] == "inner"]
    asym = [s for s in sites if s["asymmetric"]]
    no_validate = [s for s in sites if s["validate"] is None]
    print(f"          {len(asym)} asymmetric (left_on != right_on); "
          f"{len(inner_raw)} of the RAW joins are INNER (silent row-drop)")
    print(f"          {len(no_validate)}/{len(sites)} declare NO validate= "
          f"(no guard against silent row fan-out)")

    print("\n" + "-" * 78)
    print(f"ACTIONABLE — polars open-name joins (the layer that owns matching): {len(raw_poly)}")
    print("-" * 78)
    for s in sorted(raw_poly, key=lambda x: (x["how"] != "inner", x["file"])):
        flag = "  ⚠ INNER" if s["how"] == "inner" else f"  how={s['how']}"
        asy = "  asym" if s["asymmetric"] else ""
        val = "  no-validate" if s["validate"] is None else f"  validate={s['validate']}"
        print(f"  {s['file']}:{s['line']}  on={s['keys']}{flag}{asy}{val}")
    if not raw_poly:
        print("  none — every polars join uses a normalised key or structured id.")

    print("\n" + "-" * 78)
    print(f"INFORMATIONAL — SQL name-joins (pass through upstream sanitisation): {len(raw_sql)}")
    print("-" * 78)
    print("  These join on name columns but the SQL layer is fed pre-sanitised /")
    print("  intra-lineage data, so most are safe by design. Verify upstream, don't")
    print("  assume fragile. (CONTROLLED-vocabulary joins already excluded.)")
    for e in raw_sql:
        print(f"  {e['file']}  {e['left']}.{e['left_key']} = {e['right']}.{e['right_key']}")

    print("\nNote: reads WIRING, not correctness. Can't see 0 = not-matched vs absent,")
    print("nor intra-lineage vs cross-register — that needs column provenance.")

    return 1 if (args.strict and raw_poly) else 0


if __name__ == "__main__":
    sys.exit(main())
