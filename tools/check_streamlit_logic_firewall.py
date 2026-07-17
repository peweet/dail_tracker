"""
Streamlit logic-firewall checker.

Usage:
    python tools/check_streamlit_logic_firewall.py [PATH ...]

If no paths supplied, scans:
    utility/pages_code/
    utility/data_access/
    utility/ui/

Returns exit code 0 when clean, 1 when any violation is found. Designed for CI.

Violations detected (per docs/PIPELINE_VIEW_BOUNDARY.md):
    - pd.read_parquet / pd.read_csv / pl.read_* / pl.scan_*
    - duckdb.connect(":memory:")
    - register pandas frame (con.register pattern)
    - DataFrame.merge / pd.merge
    - DataFrame.groupby followed by agg / sum / count / first / value_counts
    - SQL string literals containing JOIN, GROUP BY, HAVING, OVER (
    - DataFrame.pivot / pivot_table / melt
    - DataFrame/Series.apply / applymap (row-wise callables)
    - Series.map with a lambda / call / function reference (dict relabelling is fine)
    - collections.Counter / defaultdict (manual Python aggregation containers)
    - dict-accumulation counting (d[k] = d.get(k, 0) + 1 / d[k] += 1)

Allow-list:
    A line ending with `# logic_firewall: display_only` is exempt. Use for
    sanctioned display-only aggregations (e.g. value_counts driving a chip
    layout on an already-filtered subset, or a .melt reshaping an
    already-aggregated frame into chart input).

The checker is intentionally conservative: it works on the AST + string
literals, never executes user code. False positives can be silenced with
the allow-list marker; do not change the checker to widen its definitions.
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Violation:
    path: Path
    line: int
    code: str
    message: str

    def render(self) -> str:
        try:
            rel = self.path.relative_to(_PROJECT_ROOT).as_posix()
        except ValueError:
            rel = self.path.as_posix()
        return f"{rel}:{self.line}: [{self.code}] {self.message}"


# Bare-attribute calls that are always forbidden.
_FORBIDDEN_CALLS = {
    # pandas
    "pd.read_parquet": "raw parquet read in UI / data-access layer",
    "pd.read_csv": "raw CSV read in UI / data-access layer",
    "pd.merge": "pandas merge — joins belong in the pipeline view",
    # polars
    "pl.read_parquet": "raw parquet read in UI / data-access layer",
    "pl.read_csv": "raw CSV read in UI / data-access layer",
    "pl.scan_parquet": "raw parquet scan in UI / data-access layer",
    "pl.scan_csv": "raw CSV scan in UI / data-access layer",
}

# Manual Python aggregation containers — a groupby/count written as a loop.
_PY_ROLLUP_CALLS = {
    "Counter",
    "collections.Counter",
    "defaultdict",
    "collections.defaultdict",
}

# Method calls on DataFrames that are forbidden (matched on attribute name).
_FORBIDDEN_METHODS = {
    "merge": "DataFrame.merge — joins belong in the pipeline view",
    "pivot": "DataFrame.pivot — reshape belongs in the pipeline view",
    "pivot_table": "DataFrame.pivot_table — reshape belongs in the pipeline view",
    "melt": "DataFrame.melt — reshape belongs in the pipeline view "
    "(silence with the marker when reshaping an already-aggregated frame into chart input)",
    "apply": "DataFrame/Series.apply — row-wise callable; the transform belongs in the pipeline view "
    "(silence with the marker for pure display formatting on the active set)",
    "applymap": "DataFrame.applymap — element-wise callable; the transform belongs in the pipeline view",
}

# Aggregations chained after groupby/value_counts: scan for these chained calls.
_AGGREGATION_TERMINALS = {"agg", "sum", "count", "first", "last", "value_counts"}

# SQL keywords that are forbidden in retrieval SQL string literals
# (registered views inside sql_views/ are exempt; we only check pages_code/
# and data_access/).
_FORBIDDEN_SQL_PATTERNS = [
    (re.compile(r"\bJOIN\b", re.IGNORECASE), "JOIN in retrieval SQL"),
    (re.compile(r"\bGROUP\s+BY\b", re.IGNORECASE), "GROUP BY in retrieval SQL"),
    (re.compile(r"\bHAVING\b", re.IGNORECASE), "HAVING in retrieval SQL"),
    (re.compile(r"\bOVER\s*\("), "WINDOW function in retrieval SQL"),
]

# Marker that exempts a line.
_ALLOW_MARKER = "logic_firewall: display_only"


def _dotted_name(node: ast.AST) -> str | None:
    """Render an Attribute/Name node as a dotted string like 'pd.read_parquet'."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        head = _dotted_name(node.value)
        return f"{head}.{node.attr}" if head else None
    return None


_ALLOW_LOOKBACK = 6  # lines above the violation to scan for a marker


def _line_is_allowed(source_lines: list[str], lineno: int) -> bool:
    """Return True iff the violation line, or any of the _ALLOW_LOOKBACK
    lines above it, carries the allow marker. Comment blocks immediately
    above a value_counts() / groupby() call count as documentation of why
    the aggregation is sanctioned."""
    idx = lineno - 1
    start = max(0, idx - _ALLOW_LOOKBACK)
    return any(0 <= ln < len(source_lines) and _ALLOW_MARKER in source_lines[ln] for ln in range(start, idx + 1))


def _looks_like_retrieval_sql(text: str) -> bool:
    """Heuristic: the string is actual retrieval SQL, not prose mentioning it.

    Require either:
      - the string, when stripped, starts with SELECT / WITH / INSERT / UPDATE / DELETE
      - or a SELECT ... FROM pattern within ~500 characters
    A docstring like 'Forbidden: JOIN, GROUP BY, HAVING, WINDOW' won't match.
    """
    s = text.lstrip()
    if re.match(r"(?i)^(SELECT|WITH|INSERT|UPDATE|DELETE)\b", s):
        return True
    return bool(re.search(r"(?is)\bSELECT\b.{0,500}?\bFROM\b", text))


def _scan_sql_literal(text: str, lineno: int, path: Path, source_lines: list[str]) -> list[Violation]:
    """Inspect a Python string literal for forbidden SQL keywords."""
    out: list[Violation] = []
    if not _looks_like_retrieval_sql(text):
        return out
    if _line_is_allowed(source_lines, lineno):
        return out
    for pattern, msg in _FORBIDDEN_SQL_PATTERNS:
        if pattern.search(text):
            out.append(Violation(path, lineno, "SQL", msg))
    return out


def _collect_docstring_lines(tree: ast.Module) -> set[int]:
    """Return the set of line numbers that fall inside a docstring of any
    module / function / class. Used to skip prose that quotes SQL examples."""
    doc_lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        body = getattr(node, "body", None) or []
        if not body:
            continue
        first = body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            start = first.lineno
            end = getattr(first, "end_lineno", start) or start
            for ln in range(start, end + 1):
                doc_lines.add(ln)
    return doc_lines


def _is_callable_map_arg(arg: ast.AST) -> bool:
    """True when a .map() argument is a callable-style transform (lambda,
    call result, or function reference like json.loads) rather than a plain
    dict / dict variable used for display relabelling."""
    return isinstance(arg, (ast.Lambda, ast.Call, ast.Attribute))


class _Visitor(ast.NodeVisitor):
    def __init__(self, path: Path, source_lines: list[str], doc_lines: set[int]):
        self.path = path
        self.source_lines = source_lines
        self.doc_lines = doc_lines
        self.violations: list[Violation] = []

    # ── Forbidden function/method calls ─────────────────────────────────
    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        dotted = _dotted_name(node.func)
        if dotted in _FORBIDDEN_CALLS and not _line_is_allowed(self.source_lines, node.lineno):
            self.violations.append(Violation(self.path, node.lineno, "CALL", _FORBIDDEN_CALLS[dotted]))

        # Counter(...) / defaultdict(...) — a rollup written as a Python loop.
        if dotted in _PY_ROLLUP_CALLS and not _line_is_allowed(self.source_lines, node.lineno):
            self.violations.append(
                Violation(
                    self.path,
                    node.lineno,
                    "PY_ROLLUP",
                    f"{dotted}(...) — manual Python aggregation; the rollup belongs in the pipeline view "
                    "(silence with `# logic_firewall: display_only` for render-time chips/stripes "
                    "on the active filter set)",
                )
            )

        # Method-style: <expr>.merge(...), <expr>.pivot(...), etc.
        if isinstance(node.func, ast.Attribute):
            attr = node.func.attr
            if attr in _FORBIDDEN_METHODS and not _line_is_allowed(self.source_lines, node.lineno):
                self.violations.append(Violation(self.path, node.lineno, "METHOD", _FORBIDDEN_METHODS[attr]))

            # Series.map(<lambda/call/function>) — a row-wise transform or an
            # enrichment join in disguise. st.map (the chart) and .map(dict)
            # relabelling are fine.
            if (
                attr == "map"
                and dotted != "st.map"
                and node.args
                and _is_callable_map_arg(node.args[0])
                and not _line_is_allowed(self.source_lines, node.lineno)
            ):
                self.violations.append(
                    Violation(
                        self.path,
                        node.lineno,
                        "MAP",
                        "Series.map(<callable>) — row-wise transform/enrichment belongs in the "
                        "pipeline view (a plain dict relabelling for display is allowed)",
                    )
                )

            # groupby(...).<agg>(...) — flag the agg, since groupby alone
            # is sometimes used for iteration patterns inside transitional
            # code. We require the aggregation terminal to count it as a
            # modelling rollup.
            if attr in _AGGREGATION_TERMINALS:
                inner = node.func.value
                if _is_groupby_chain(inner) and not _line_is_allowed(self.source_lines, node.lineno):
                    self.violations.append(
                        Violation(
                            self.path,
                            node.lineno,
                            "GROUPBY",
                            f".groupby(...).{attr}(...) — rollup belongs in the pipeline view",
                        )
                    )

            # bare .value_counts() on a column / Series
            if (
                attr == "value_counts"
                and not _is_inside_groupby_chain(node.func)
                and not _line_is_allowed(self.source_lines, node.lineno)
            ):
                self.violations.append(
                    Violation(
                        self.path,
                        node.lineno,
                        "VALUE_COUNTS",
                        "Series.value_counts() — frequency rollup belongs in the pipeline view "
                        "(silence with `# logic_firewall: display_only` if this is a "
                        "render-time aggregation on the active filter set)",
                    )
                )

        # duckdb.connect(":memory:") — in-memory frame registration pattern
        if dotted == "duckdb.connect" and node.args:
            first = node.args[0]
            if (
                isinstance(first, ast.Constant)
                and first.value == ":memory:"
                and not _line_is_allowed(self.source_lines, node.lineno)
            ):
                self.violations.append(
                    Violation(
                        self.path,
                        node.lineno,
                        "DUCKDB_MEMORY",
                        "duckdb.connect(':memory:') — register-a-frame pattern; "
                        "use the page's shared get_*_conn() and a registered view",
                    )
                )

        # con.register('view_name', df) — registering a DataFrame as a view
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "register"
            and not _line_is_allowed(self.source_lines, node.lineno)
        ):
            self.violations.append(
                Violation(
                    self.path,
                    node.lineno,
                    "REGISTER",
                    "con.register(...) — registering a frame as a view is pipeline territory",
                )
            )

        self.generic_visit(node)

    # ── Manual counting loops ───────────────────────────────────────────
    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802
        # d[k] += 1  (numeric accumulation into a subscript = counting loop)
        if (
            isinstance(node.target, ast.Subscript)
            and isinstance(node.op, ast.Add)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, (int, float))
            and not _line_is_allowed(self.source_lines, node.lineno)
        ):
            self.violations.append(
                Violation(
                    self.path,
                    node.lineno,
                    "DICT_ACCUM",
                    "d[k] += n — manual counting loop; the rollup belongs in the pipeline view",
                )
            )
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        # d[k] = d.get(k, 0) + 1  (dict-accumulation counting)
        if (
            len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.value, ast.BinOp)
            and isinstance(node.value.op, ast.Add)
            and not _line_is_allowed(self.source_lines, node.lineno)
        ):
            for side in (node.value.left, node.value.right):
                if isinstance(side, ast.Call) and isinstance(side.func, ast.Attribute) and side.func.attr == "get":
                    self.violations.append(
                        Violation(
                            self.path,
                            node.lineno,
                            "DICT_ACCUM",
                            "d[k] = d.get(k, …) + … — dict-accumulation counting; "
                            "the rollup belongs in the pipeline view",
                        )
                    )
                    break
        self.generic_visit(node)

    # ── SQL string literals ─────────────────────────────────────────────
    def visit_Constant(self, node: ast.Constant) -> None:  # noqa: N802
        if isinstance(node.value, str) and node.lineno not in self.doc_lines:
            self.violations.extend(_scan_sql_literal(node.value, node.lineno, self.path, self.source_lines))
        self.generic_visit(node)


def _is_groupby_chain(node: ast.AST) -> bool:
    """Return True iff node is a `.groupby(...)` call chain."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr == "groupby"
    return False


def _is_inside_groupby_chain(attr_node: ast.Attribute) -> bool:
    """Walk down attr.value to detect a groupby further down — to avoid
    double-counting `.groupby(...).value_counts()` which the GROUPBY branch
    already catches."""
    cur: ast.AST = attr_node.value
    while True:
        if isinstance(cur, ast.Call):
            if isinstance(cur.func, ast.Attribute):
                if cur.func.attr == "groupby":
                    return True
                cur = cur.func.value
            else:
                return False
        elif isinstance(cur, ast.Attribute):
            cur = cur.value
        else:
            return False


def check_file(path: Path) -> list[Violation]:
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as e:
        return [Violation(path, e.lineno or 0, "PARSE", f"could not parse: {e.msg}")]
    doc_lines = _collect_docstring_lines(tree)
    visitor = _Visitor(path, text.splitlines(), doc_lines)
    visitor.visit(tree)
    return visitor.violations


def _default_targets() -> list[Path]:
    return [
        _PROJECT_ROOT / "utility" / "pages_code",
        _PROJECT_ROOT / "utility" / "data_access",
        _PROJECT_ROOT / "utility" / "ui",
    ]


def main(argv: list[str]) -> int:
    targets: list[Path] = [Path(a).resolve() for a in argv[1:]] or _default_targets()
    files: list[Path] = []
    for t in targets:
        if t.is_dir():
            files.extend(sorted(t.rglob("*.py")))
        elif t.is_file() and t.suffix == ".py":
            files.append(t)

    violations: list[Violation] = []
    for f in files:
        if f.name == "__init__.py":
            continue
        violations.extend(check_file(f))

    if not violations:
        print(f"OK — scanned {len(files)} files, no logic-firewall violations.")
        return 0

    for v in violations:
        print(v.render())
    print(f"\nFAIL — {len(violations)} violation(s) across {len(files)} files.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
