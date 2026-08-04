"""AST scan of framework coupling — how hard is this codebase to move off Streamlit?

Answers, from the AST rather than from grep or estimation:
  1. Which modules import streamlit DIRECTLY, and which reach it TRANSITIVELY
     (the import-graph question deferred in feedback_cheap_code_scanning as
     "the first step of Ph3 decoupling").
  2. What the real Streamlit API surface is — every `st.<attr>` usage counted by
     symbol, so the framework dependency can be sized instead of guessed.
  3. Per-module portability: HTML-emission (portable markup) vs widget wiring
     (Streamlit-only) vs pure logic.

Usage
-----
    python tools/scan_framework_coupling.py                # markdown report
    python tools/scan_framework_coupling.py --json out.json
    python tools/scan_framework_coupling.py --layer utility/ui

The survey exits 1 when any source cannot be decoded or parsed; a partial report
would understate coupling. The optional inline-markup mode is also a CI ratchet.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
import tokenize
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Directories that are never part of the shipped import graph.
SKIP_DIRS = {
    ".cache",
    ".venv",
    ".git",
    "__pycache__",
    "node_modules",
    "data",
    "doc",
    ".pytest_cache",
    ".ruff_cache",
    "htmlcov",
    ".mypy_cache",
}

# Layers, most-specific first — a file is assigned to the first prefix it matches.
LAYERS: list[tuple[str, str]] = [
    ("utility/pages_code", "page"),
    ("utility/data_access", "data_access"),
    ("utility/ui", "ui_component"),
    ("utility", "app_shell"),
    ("dail_tracker_core", "core"),
    ("api", "api"),
    ("mcp_server", "mcp"),
    ("services", "services"),
    ("extractors", "extractor"),
    ("iris", "extractor"),
    ("tools", "tooling"),
    ("test", "test"),
    ("pipeline_sandbox", "sandbox"),
]

# Streamlit symbols that are genuinely framework-bound: they wire input/state and
# have no meaning outside a Streamlit runtime. Everything else (markdown/html/
# write) is markup emission that a React component can reproduce.
WIDGET_API = {
    "button",
    "download_button",
    "checkbox",
    "radio",
    "selectbox",
    "multiselect",
    "slider",
    "select_slider",
    "text_input",
    "number_input",
    "text_area",
    "date_input",
    "time_input",
    "file_uploader",
    "color_picker",
    "form",
    "form_submit_button",
    "toggle",
    "camera_input",
    "data_editor",
    "pills",
    "segmented_control",
    "feedback",
    "chat_input",
    "chat_message",
}
STATE_API = {"session_state", "query_params", "rerun", "stop", "switch_page", "navigation", "Page"}
CACHE_API = {"cache_data", "cache_resource", "experimental_memo", "experimental_singleton"}
LAYOUT_API = {
    "columns",
    "container",
    "expander",
    "tabs",
    "sidebar",
    "empty",
    "popover",
    "dialog",
    "status",
    "spinner",
    "form",
}
MARKUP_API = {
    "markdown",
    "html",
    "write",
    "caption",
    "text",
    "title",
    "header",
    "subheader",
    "divider",
    "code",
    "latex",
    "badge",
}
DATA_API = {
    "dataframe",
    "table",
    "metric",
    "json",
    "altair_chart",
    "plotly_chart",
    "line_chart",
    "bar_chart",
    "area_chart",
    "map",
    "pyplot",
    "vega_lite_chart",
    "graphviz_chart",
}


def classify_symbol(attr: str) -> str:
    if attr in WIDGET_API:
        return "widget"
    if attr in STATE_API:
        return "state"
    if attr in CACHE_API:
        return "cache"
    if attr in LAYOUT_API:
        return "layout"
    if attr in MARKUP_API:
        return "markup"
    if attr in DATA_API:
        return "data_render"
    return "other"


@dataclass
class ModuleInfo:
    path: Path
    rel: str
    dotted: str
    layer: str
    loc: int = 0
    imports_streamlit: bool = False
    raw_imports: set[str] = field(default_factory=set)
    internal_edges: set[str] = field(default_factory=set)
    st_symbols: Counter = field(default_factory=Counter)
    html_literals: int = 0  # string constants containing markup tags
    reaches_streamlit_via: str | None = None


class AnalysisError(RuntimeError):
    """A source file could not be decoded or parsed safely."""

    def __init__(self, path: Path, cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"{path}: {type(cause).__name__}: {cause}")


def parse_module(path: Path) -> tuple[str, ast.Module]:
    try:
        with tokenize.open(path) as stream:
            source = stream.read()
        return source, ast.parse(source, filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise AnalysisError(path, exc) from exc


def _docstring_values(tree: ast.AST) -> set[ast.AST]:
    values: set[ast.AST] = set()
    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, owners) or not node.body:
            continue
        first = node.body[0]
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            values.add(first.value)
    return values


def iter_python_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        out.append(p)
    return sorted(out)


def layer_for(rel: str) -> str:
    norm = rel.replace("\\", "/")
    for prefix, name in LAYERS:
        if norm.startswith(prefix + "/") or norm == prefix + ".py":
            return name
    return "root"


def dotted_name(rel: str) -> str:
    norm = rel.replace("\\", "/")
    if norm.endswith("/__init__.py"):
        norm = norm[: -len("/__init__.py")]
    elif norm.endswith(".py"):
        norm = norm[: -len(".py")]
    return norm.replace("/", ".")


def looks_like_markup(value: str) -> bool:
    """A string constant that carries HTML — the portable part of the UI layer."""
    if "<" not in value or ">" not in value:
        return False
    return any(
        tag in value
        for tag in (
            "<div",
            "<span",
            "<a ",
            "<p ",
            "<p>",
            "<section",
            "<table",
            "<ul",
            "<li",
            "<img",
            "<h1",
            "<h2",
            "<h3",
            "<h4",
            "<style",
            "<svg",
            "<button",
        )
    )


def scan_module(path: Path, root: Path = PROJECT_ROOT) -> ModuleInfo:
    rel = str(path.relative_to(root))
    src, tree = parse_module(path)
    docstrings = _docstring_values(tree)

    info = ModuleInfo(
        path=path,
        rel=rel.replace("\\", "/"),
        dotted=dotted_name(rel),
        layer=layer_for(rel),
        loc=src.count("\n") + 1,
    )

    for node in ast.walk(tree):
        if node in docstrings:
            continue
        if isinstance(node, ast.Import):
            for alias in node.names:
                info.raw_imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                info.raw_imports.add(node.module)
        elif isinstance(node, ast.Attribute):
            # st.<attr> — the actual framework surface
            if isinstance(node.value, ast.Name) and node.value.id == "st":
                info.st_symbols[node.attr] += 1
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and looks_like_markup(node.value):
            info.html_literals += 1

    info.imports_streamlit = any(m == "streamlit" or m.startswith("streamlit.") for m in info.raw_imports)
    return info


def build_resolver(modules: dict[str, ModuleInfo]) -> dict[str, str]:
    """Map every importable spelling of a module to its canonical dotted name.

    The app runs from `utility/`, so pages import `from pages_code.x import y`
    while tests import `utility.pages_code.x`. Both must resolve to one node.
    """
    index: dict[str, str] = {}
    for dotted in modules:
        parts = dotted.split(".")
        for i in range(len(parts)):
            suffix = ".".join(parts[i:])
            # First writer wins for ambiguous suffixes; canonical (full) always set.
            index.setdefault(suffix, dotted)
        index[dotted] = dotted
    return index


def link_edges(modules: dict[str, ModuleInfo], resolver: dict[str, str]) -> None:
    for info in modules.values():
        for imp in info.raw_imports:
            target = resolver.get(imp)
            if target is None:
                # `from pages_code.x import y` where x is a package attribute
                head = ".".join(imp.split(".")[:-1])
                target = resolver.get(head)
            if target and target != info.dotted:
                info.internal_edges.add(target)


def propagate_streamlit(modules: dict[str, ModuleInfo]) -> None:
    """BFS: mark every module that reaches a streamlit importer through imports."""
    for start, info in modules.items():
        if info.imports_streamlit:
            info.reaches_streamlit_via = "direct"
            continue
        seen: set[str] = {start}
        queue: list[tuple[str, list[str]]] = [(start, [])]
        while queue:
            node, trail = queue.pop(0)
            for nxt in sorted(modules[node].internal_edges):
                if nxt in seen or nxt not in modules:
                    continue
                seen.add(nxt)
                path = [*trail, nxt]
                if modules[nxt].imports_streamlit:
                    info.reaches_streamlit_via = " -> ".join(path)
                    queue.clear()
                    break
                queue.append((nxt, path))


def render_report(modules: dict[str, ModuleInfo], layer_filter: str | None) -> str:
    out: list[str] = []
    w = out.append

    by_layer: dict[str, list[ModuleInfo]] = defaultdict(list)
    for info in modules.values():
        by_layer[info.layer].append(info)

    w("# Framework coupling scan (AST)\n")
    w(f"Scanned **{len(modules)}** Python modules under `{PROJECT_ROOT.name}/`.\n")

    # ---- 1. Coupling by layer -------------------------------------------------
    w("## 1. Streamlit reachability by layer\n")
    w("| Layer | Modules | LOC | Direct `import streamlit` | Transitive only | Streamlit-free |")
    w("|---|---:|---:|---:|---:|---:|")
    for layer in sorted(by_layer):
        mods = by_layer[layer]
        direct = sum(1 for m in mods if m.imports_streamlit)
        trans = sum(1 for m in mods if not m.imports_streamlit and m.reaches_streamlit_via)
        free = len(mods) - direct - trans
        loc = sum(m.loc for m in mods)
        w(f"| {layer} | {len(mods)} | {loc:,} | {direct} | {trans} | {free} |")
    w("")

    # ---- 2. Core purity check -------------------------------------------------
    w("## 2. Core purity (must stay zero)\n")
    tainted = [m for m in modules.values() if m.layer in {"core", "api", "services"} and m.reaches_streamlit_via]
    if tainted:
        for m in sorted(tainted, key=lambda x: x.rel):
            w(f"- **LEAK** `{m.rel}` -> {m.reaches_streamlit_via}")
    else:
        w(
            "No module in `dail_tracker_core/`, `api/` or `services/` reaches streamlit. "
            "The portable core is genuinely portable."
        )
    w("")

    # ---- 3. The real Streamlit API surface ------------------------------------
    w("## 3. Streamlit API surface actually used\n")
    total = Counter()
    per_kind: Counter = Counter()
    for m in modules.values():
        total.update(m.st_symbols)
    for sym, n in total.items():
        per_kind[classify_symbol(sym)] += n

    w("Grouped by what a migration would have to do with it:\n")
    w("| Kind | Call sites | Migration cost |")
    w("|---|---:|---|")
    meaning = {
        "markup": "Portable — becomes JSX/HTML",
        "layout": "Portable — becomes CSS grid/flex",
        "data_render": "Replace with a chart/table lib",
        "widget": "Rebuild — real framework coupling",
        "state": "Rebuild — routing/state contract",
        "cache": "Evaporates — server-side caching moves to the API",
        "other": "Inspect individually",
    }
    for kind, n in per_kind.most_common():
        w(f"| {kind} | {n:,} | {meaning.get(kind, '')} |")
    w("")
    w(f"**Distinct `st.*` symbols used: {len(total)}** (of a Streamlit API surface far larger).\n")
    w("Top 25 by call sites:\n")
    w("| Symbol | Calls | Kind |")
    w("|---|---:|---|")
    for sym, n in total.most_common(25):
        w(f"| `st.{sym}` | {n:,} | {classify_symbol(sym)} |")
    w("")

    # ---- 4. Per-module portability in the UI layer ----------------------------
    w("## 4. UI-layer portability (markup vs framework)\n")
    w(
        "`html_literals` = string constants containing real HTML tags. A module that is "
        "mostly markup emission ports as a component spec; one that is mostly widgets is a rebuild.\n"
    )
    w("| Module | LOC | HTML literals | Widget calls | State calls | Verdict |")
    w("|---|---:|---:|---:|---:|---|")
    ui_layers = {"ui_component", "page"} if layer_filter is None else {layer_filter}
    ui_mods = [m for m in modules.values() if m.layer in ui_layers]
    for m in sorted(ui_mods, key=lambda x: -x.loc)[:40]:
        widgets = sum(n for s, n in m.st_symbols.items() if classify_symbol(s) == "widget")
        state = sum(n for s, n in m.st_symbols.items() if classify_symbol(s) == "state")
        if m.html_literals and widgets == 0:
            verdict = "portable markup"
        elif widgets > m.html_literals:
            verdict = "widget-bound"
        else:
            verdict = "mixed"
        w(f"| `{m.rel}` | {m.loc:,} | {m.html_literals} | {widgets} | {state} | {verdict} |")
    w("")

    # ---- 5. Cheapest decoupling wins ------------------------------------------
    w("## 5. Decoupling opportunities (ranked by cheapness)\n")
    # (a) modules whose ONLY streamlit use is caching
    cache_only = []
    for m in modules.values():
        if not m.imports_streamlit or not m.st_symbols:
            continue
        kinds = {classify_symbol(s) for s in m.st_symbols}
        if kinds <= {"cache"}:
            cache_only.append(m)
    w(
        f"**(a) Streamlit used ONLY for caching — {len(cache_only)} modules.** "
        "These need no rewrite; swap the decorator for a framework-neutral cache "
        "(functools.lru_cache / cachetools) and they become portable as-is.\n"
    )
    for m in sorted(cache_only, key=lambda x: x.rel)[:30]:
        w(f"- `{m.rel}` ({m.loc:,} LOC, {sum(m.st_symbols.values())} cache decorators)")
    w("")

    # (b) pure-markup modules
    pure_markup = [
        m
        for m in modules.values()
        if m.layer in {"ui_component", "page"}
        and m.html_literals >= 5
        and not any(classify_symbol(s) == "widget" for s in m.st_symbols)
    ]
    w(
        f"**(b) Markup-only UI modules — {len(pure_markup)}.** No widgets at all: "
        "these are HTML generators wearing a Streamlit hat, and port to components "
        "with the class names (and therefore the styling) intact.\n"
    )
    for m in sorted(pure_markup, key=lambda x: -x.loc)[:20]:
        w(f"- `{m.rel}` ({m.loc:,} LOC, {m.html_literals} HTML literals)")
    w("")

    # (c) heaviest widget users = the genuine rebuild
    widget_heavy = sorted(
        (m for m in modules.values() if any(classify_symbol(s) == "widget" for s in m.st_symbols)),
        key=lambda m: -sum(n for s, n in m.st_symbols.items() if classify_symbol(s) == "widget"),
    )
    w(
        f"**(c) Genuine rebuild surface — {len(widget_heavy)} modules use widgets.** "
        "Ranked by widget call sites; this is where migration effort actually lives.\n"
    )
    for m in widget_heavy[:15]:
        n = sum(v for s, v in m.st_symbols.items() if classify_symbol(s) == "widget")
        w(f"- `{m.rel}` — {n} widget calls, {m.loc:,} LOC")
    w("")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Markup ratchet — cap the anonymous-component debt
# ---------------------------------------------------------------------------
# 78% of markup call sites pass an inline f-string or constant straight to
# st.html(). Each one is an unnamed component: portable in content (it is HTML
# carrying our own class names) but with no name, no props and no reuse, so a
# migration has to rediscover it. The 111 functions that already RETURN markup
# are the opposite — they are components in all but location.
#
# Paying that debt down in one go is not worth it. NOT LETTING IT GROW is. This
# ratchet freezes the per-file count of inline markup sites; new markup must go
# through a named function. Same convention as the other baselines: never add,
# only remove.

MARKUP_SINKS = {"html", "markdown", "write", "caption"}
MARKUP_BASELINE = PROJECT_ROOT / "tools" / "baselines" / "markup_inline_baseline.json"
MARKUP_SCAN_DIRS = ("utility/pages_code", "utility/ui")
# Version 2 records path lineage. A split/rename keeps its predecessor in
# ``counts`` and allocates exactly the same budget across explicit successor
# paths. New successor files therefore still start at zero, and debt cannot be
# shuffled between modules merely because they share a package.
MARKUP_BASELINE_VERSION = 2

# Raw HTML, not ordinary prose or lightweight Markdown emphasis. ``st.html`` is
# intrinsically an HTML sink, but the other Streamlit methods are also used for
# hundreds of plain captions and explanatory sentences. Counting those as
# anonymous components made the ratchet measure editorial copy instead of UI
# structure.
_HTML_TAG_RE = re.compile(r"<\s*/?\s*[a-zA-Z][\w:-]*(?:\s[^<>]*?)?\s*/?>", re.DOTALL)


class MarkupBaselineError(ValueError):
    """The committed markup policy is malformed or internally ambiguous."""


@dataclass(frozen=True)
class MarkupLineage:
    """Map one or more historical files to their explicit current paths."""

    name: str
    baseline_paths: tuple[str, ...]
    current_counts: dict[str, int]


@dataclass(frozen=True)
class MarkupBaseline:
    """Per-file debt limits plus explicit file split/rename lineage."""

    counts: dict[str, int]
    lineage: tuple[MarkupLineage, ...] = ()


@dataclass(frozen=True)
class MarkupComparison:
    """One independently ratcheted file or declared lineage group."""

    label: str
    baseline: int
    current: int


def _render_argument(node: ast.Call) -> ast.expr | None:
    """Return the rendered body from positional or documented keyword form."""
    if node.args:
        return node.args[0]
    for keyword in node.keywords:
        if keyword.arg in {"body", "text"}:
            return keyword.value
    return None


def _embedded_literal_text(node: ast.AST) -> str | None:
    """Collect only literal string fragments embedded in an inline expression."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.FormattedValue):
        return ""
    if isinstance(node, ast.JoinedStr):
        parts = [_embedded_literal_text(value) for value in node.values]
        return "".join(part for part in parts if part is not None)
    if isinstance(node, ast.BinOp):
        left = _embedded_literal_text(node.left)
        right = _embedded_literal_text(node.right)
        if left is None and right is None:
            return None
        return (left or "") + (right or "")
    return None


def _is_anonymous_markup(sink: str, argument: ast.expr) -> bool:
    """Whether an inline expression defines a rendered HTML fragment.

    A name or helper call is already a named component boundary. ``st.html``
    always renders HTML, including a text-only HTML fragment. For the more
    general prose sinks, require an actual HTML tag so ordinary captions,
    ``st.write`` copy, and Markdown emphasis do not become component debt.
    """
    if not isinstance(argument, (ast.JoinedStr, ast.Constant, ast.BinOp)):
        return False
    text = _embedded_literal_text(argument)
    if text is None:
        return False
    return sink == "html" or bool(_HTML_TAG_RE.search(text))


def count_inline_markup(path: Path) -> int:
    """Count anonymous rendered HTML fragments, excluding ordinary UI copy."""
    _, tree = parse_module(path)
    n = 0
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        fn = node.func
        if fn.attr not in MARKUP_SINKS:
            continue
        if not (isinstance(fn.value, ast.Name) and fn.value.id == "st"):
            continue
        argument = _render_argument(node)
        if argument is not None and _is_anonymous_markup(fn.attr, argument):
            n += 1
    return n


def markup_counts() -> dict[str, int]:
    counts: dict[str, int] = {}
    for d in MARKUP_SCAN_DIRS:
        for path in sorted((PROJECT_ROOT / d).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            n = count_inline_markup(path)
            if n:
                counts[path.relative_to(PROJECT_ROOT).as_posix()] = n
    return counts


def _normalised_count_map(raw: object, *, field: str) -> dict[str, int]:
    if not isinstance(raw, dict):
        raise MarkupBaselineError(f"{field} must be an object mapping paths to counts")
    counts: dict[str, int] = {}
    for path, count in raw.items():
        if not isinstance(path, str) or not path or "\\" in path or Path(path).is_absolute() or ".." in path.split("/"):
            raise MarkupBaselineError(f"{field} contains an invalid project-relative path: {path!r}")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise MarkupBaselineError(f"{field}[{path!r}] must be a non-negative integer")
        counts[path] = count
    return counts


def _load_markup_baseline() -> MarkupBaseline:
    try:
        raw = json.loads(MARKUP_BASELINE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MarkupBaselineError(f"cannot read {MARKUP_BASELINE}: {exc}") from exc

    # Read the original count-only shape so an older checkout fails on actual
    # regressions rather than on a migration detail. All newly written policy
    # uses the versioned shape below.
    if isinstance(raw, dict) and "version" not in raw:
        return MarkupBaseline(_normalised_count_map(raw, field="baseline"))
    if not isinstance(raw, dict) or raw.get("version") != MARKUP_BASELINE_VERSION:
        raise MarkupBaselineError(f"markup baseline version must be {MARKUP_BASELINE_VERSION}")

    counts = _normalised_count_map(raw.get("counts"), field="counts")
    raw_lineage = raw.get("lineage", [])
    if not isinstance(raw_lineage, list):
        raise MarkupBaselineError("lineage must be a list")
    lineage: list[MarkupLineage] = []
    claimed_baseline_paths: set[str] = set()
    claimed_current_paths: set[str] = set()
    for index, item in enumerate(raw_lineage):
        if not isinstance(item, dict):
            raise MarkupBaselineError(f"lineage[{index}] must be an object")
        name = item.get("name")
        baseline_paths = item.get("baseline_paths")
        current_counts = _normalised_count_map(item.get("current_counts"), field=f"lineage[{index}].current_counts")
        if not isinstance(name, str) or not name.strip():
            raise MarkupBaselineError(f"lineage[{index}].name must be a non-empty string")
        if (
            not isinstance(baseline_paths, list)
            or not baseline_paths
            or not all(isinstance(path, str) for path in baseline_paths)
        ):
            raise MarkupBaselineError(f"lineage[{index}].baseline_paths must be a non-empty string list")
        if not current_counts:
            raise MarkupBaselineError(f"lineage[{index}].current_counts must not be empty")
        if len(set(baseline_paths)) != len(baseline_paths):
            raise MarkupBaselineError(f"lineage[{index}].baseline_paths contains duplicates")
        missing = sorted(set(baseline_paths) - counts.keys())
        if missing:
            raise MarkupBaselineError(f"lineage[{index}] references missing baseline paths: {missing}")
        overlap = claimed_baseline_paths.intersection(baseline_paths)
        if overlap:
            raise MarkupBaselineError(f"baseline paths occur in multiple lineage groups: {sorted(overlap)}")
        current_overlap = claimed_current_paths.intersection(current_counts)
        if current_overlap:
            raise MarkupBaselineError(f"current paths occur in multiple lineage groups: {sorted(current_overlap)}")
        exact_overlap = set(current_counts).intersection(counts.keys() - set(baseline_paths))
        if exact_overlap:
            raise MarkupBaselineError(
                f"lineage current paths also have exact baseline entries: {sorted(exact_overlap)}"
            )
        source_budget = sum(counts[path] for path in baseline_paths)
        current_budget = sum(current_counts.values())
        if source_budget != current_budget:
            raise MarkupBaselineError(
                f"lineage[{index}] reallocates {source_budget} source sites to {current_budget} current sites"
            )
        claimed_baseline_paths.update(baseline_paths)
        claimed_current_paths.update(current_counts)
        lineage.append(MarkupLineage(name.strip(), tuple(baseline_paths), current_counts))
    return MarkupBaseline(counts, tuple(lineage))


def _markup_comparisons(current: dict[str, int], baseline: MarkupBaseline) -> list[MarkupComparison]:
    """Build independent anti-regression units, honouring declared path lineage."""
    remaining_current = set(current)
    remaining_baseline = set(baseline.counts)
    comparisons: list[MarkupComparison] = []

    for lineage in baseline.lineage:
        remaining_current.difference_update(lineage.current_counts)
        remaining_baseline.difference_update(lineage.baseline_paths)
        comparisons.extend(
            MarkupComparison(f"{path} [{lineage.name}]", limit, current.get(path, 0))
            for path, limit in lineage.current_counts.items()
        )

    for path in sorted(remaining_baseline | remaining_current):
        comparisons.append(MarkupComparison(path, baseline.counts.get(path, 0), current.get(path, 0)))
    return comparisons


def run_markup_ratchet(update: bool) -> int:
    counts = markup_counts()
    total = sum(counts.values())

    if update:
        MARKUP_BASELINE.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": MARKUP_BASELINE_VERSION, "counts": counts, "lineage": []}
        MARKUP_BASELINE.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"[markup baseline written: {total} inline sites across {len(counts)} files]")
        return 0

    if not MARKUP_BASELINE.exists():
        print("No markup baseline. Create it with --update-markup-baseline", file=sys.stderr)
        return 1

    baseline = _load_markup_baseline()
    base_total = sum(baseline.counts.values())
    comparisons = _markup_comparisons(counts, baseline)
    regressions = [unit for unit in comparisons if unit.current > unit.baseline]

    print(f"Inline markup sites: {total} (baseline {base_total}, delta {total - base_total:+d})")
    improved = sum(unit.current < unit.baseline for unit in comparisons)
    if improved:
        print(f"Improved files: {improved} — re-baseline to lock the gain in.")

    if regressions:
        print(f"\nFAIL — {len(regressions)} file(s) grew inline markup:", file=sys.stderr)
        for unit in sorted(regressions, key=lambda item: item.label):
            print(
                f"  {unit.label}: {unit.baseline} -> {unit.current} (+{unit.current - unit.baseline})",
                file=sys.stderr,
            )
        print(
            "\nRoute new markup through a named function that RETURNS the html "
            "string, then pass it to st.html(). That function is the component.",
            file=sys.stderr,
        )
        return 1

    print("OK — no new anonymous markup.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", type=Path, help="also write the raw scan as JSON")
    ap.add_argument("--layer", help="restrict the per-module table to one layer")
    ap.add_argument("--root", type=Path, default=PROJECT_ROOT)
    ap.add_argument("--check-markup", action="store_true", help="run the inline-markup ratchet")
    ap.add_argument("--update-markup-baseline", action="store_true", help="rewrite the markup baseline")
    args = ap.parse_args()

    if args.check_markup or args.update_markup_baseline:
        try:
            return run_markup_ratchet(update=args.update_markup_baseline)
        except (AnalysisError, MarkupBaselineError) as exc:
            print(f"Framework markup analysis failed closed: {exc}", file=sys.stderr)
            return 1

    files = iter_python_files(args.root)
    modules: dict[str, ModuleInfo] = {}
    errors: list[AnalysisError] = []
    for f in files:
        try:
            info = scan_module(f, args.root)
        except AnalysisError as exc:
            errors.append(exc)
            continue
        modules[info.dotted] = info

    if errors:
        for error in errors:
            print(f"Framework coupling analysis failed closed: {error}", file=sys.stderr)
        return 1

    resolver = build_resolver(modules)
    link_edges(modules, resolver)
    propagate_streamlit(modules)

    report = render_report(modules, args.layer)
    sys.stdout.write(report)

    if args.json:
        payload = {
            m.rel: {
                "layer": m.layer,
                "loc": m.loc,
                "imports_streamlit": m.imports_streamlit,
                "reaches_streamlit_via": m.reaches_streamlit_via,
                "st_symbols": dict(m.st_symbols),
                "html_literals": m.html_literals,
                "internal_edges": sorted(m.internal_edges),
            }
            for m in modules.values()
        }
        args.json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"\n[wrote {args.json}]", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
