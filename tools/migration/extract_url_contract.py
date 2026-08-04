"""Extract the app's URL contract — the deep-link spec — from the AST.

The URL is the ONLY part of the Streamlit app that outside parties depend on:
bookmarks, shared links, search-engine results, and any future React router must
honour the same `?key=value` vocabulary. Unlike CSS or widgets, it cannot be
redesigned during a migration without breaking links that already exist in the
wild.

This tool reads it out of the code rather than out of anyone's memory:
  * `st.Page(..., title=, url_path=)` registrations in utility/app.py  -> routes
  * every literal `st.query_params[...]` key, per page module          -> params
  * string constants that build `?k=` links                            -> emitters

Usage
-----
    python tools/migration/extract_url_contract.py                  # markdown to stdout
    python tools/migration/extract_url_contract.py -o doc/URL_CONTRACT.md
    python tools/migration/extract_url_contract.py --check          # exit 1 if drifted

`--check` compares against the committed doc/URL_CONTRACT.md and fails when a
route record or query parameter changes without the contract being regenerated.
Application line numbers are deliberately ignored. That is the ratchet: the
contract cannot silently rot after harmless source movement or real URL drift.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tokenize
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_FILE = PROJECT_ROOT / "utility" / "app.py"
PAGES_DIR = PROJECT_ROOT / "utility" / "pages_code"
UI_DIR = PROJECT_ROOT / "utility" / "ui"
DEFAULT_OUT = PROJECT_ROOT / "doc" / "URL_CONTRACT.md"

# Attribute names on query_params that are dict METHODS, not parameter keys.
QP_METHODS = {
    "get",
    "update",
    "clear",
    "to_dict",
    "keys",
    "items",
    "values",
    "pop",
    "setdefault",
    "from_dict",
    "get_all",
    "setlist",
}

LINK_RE = re.compile(r"[?&]([a-zA-Z_][a-zA-Z0-9_]*)=")


def output_path(path: Path) -> Path:
    """Resolve CLI output against the project, independent of process CWD."""
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate.resolve()


class AnalysisError(RuntimeError):
    """A source file could not be decoded or parsed safely."""

    def __init__(self, path: Path, cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"{path}: {type(cause).__name__}: {cause}")


def parse_module(path: Path) -> ast.Module:
    try:
        with tokenize.open(path) as stream:
            source = stream.read()
        return ast.parse(source, filename=str(path))
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


def _live_nodes(tree: ast.AST):
    docstrings = _docstring_values(tree)
    yield from (node for node in ast.walk(tree) if node not in docstrings)


def is_query_params(node: ast.AST, aliases: set[str]) -> bool:
    """True for `st.query_params` or a local alias bound to it."""
    if isinstance(node, ast.Attribute) and node.attr == "query_params":
        return True
    return isinstance(node, ast.Name) and node.id in aliases


def collect_aliases(tree: ast.AST) -> set[str]:
    aliases: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and is_query_params(node.value, set()):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    aliases.add(tgt.id)
    return aliases


def keys_in_module(path: Path) -> tuple[set[str], set[str]]:
    """Return (read_or_written_keys, keys_appearing_in_built_links)."""
    tree = parse_module(path)

    aliases = collect_aliases(tree)
    keys: set[str] = set()
    link_keys: set[str] = set()

    for node in _live_nodes(tree):
        # st.query_params["member"]
        if isinstance(node, ast.Subscript) and is_query_params(node.value, aliases):
            if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                keys.add(node.slice.value)

        # st.query_params.get("member") / .pop("x") / .setdefault("y", ...)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            fn = node.func
            if is_query_params(fn.value, aliases):
                if fn.attr in {"get", "pop", "setdefault", "get_all"} and node.args:
                    a0 = node.args[0]
                    if isinstance(a0, ast.Constant) and isinstance(a0.value, str):
                        keys.add(a0.value)
                elif fn.attr == "update":
                    for a in node.args:
                        if isinstance(a, ast.Dict):
                            for k in a.keys:
                                if isinstance(k, ast.Constant) and isinstance(k.value, str):
                                    keys.add(k.value)
                    for kw in node.keywords:
                        if kw.arg:
                            keys.add(kw.arg)

        # st.query_params.page = "x"   (attribute-style access)
        elif isinstance(node, ast.Attribute) and is_query_params(node.value, aliases):
            if node.attr not in QP_METHODS:
                keys.add(node.attr)

        # "member" in st.query_params
        elif isinstance(node, ast.Compare) and len(node.comparators) == 1:
            if (
                is_query_params(node.comparators[0], aliases)
                and isinstance(node.left, ast.Constant)
                and isinstance(node.left.value, str)
            ):
                keys.add(node.left.value)

        # link-building string constants: "...?member=" / "&page="
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if "?" in node.value or "&" in node.value:
                link_keys.update(LINK_RE.findall(node.value))

        # f-strings that build links: f"/?page={x}&member={y}"
        elif isinstance(node, ast.JoinedStr):
            literal = "".join(v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str))
            if "?" in literal or "&" in literal:
                link_keys.update(LINK_RE.findall(literal))

    return keys, link_keys


def parse_routes() -> list[dict[str, str]]:
    """Read st.Page(...) registrations out of utility/app.py."""
    tree = parse_module(APP_FILE)

    # Map the local callable name back to its page module.
    func_to_module: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                local = alias.asname or alias.name
                func_to_module[local] = node.module

    routes: list[dict[str, str]] = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr != "Page":
            continue
        entry: dict[str, str] = {"title": "", "url_path": "", "module": "", "line": str(node.lineno)}
        if node.args:
            a0 = node.args[0]
            if isinstance(a0, ast.Name):
                entry["module"] = func_to_module.get(a0.id, a0.id)
        for kw in node.keywords:
            if kw.arg in {"title", "url_path"} and isinstance(kw.value, ast.Constant):
                entry[kw.arg] = str(kw.value.value)
        routes.append(entry)
    return routes


def module_to_path(module: str) -> Path | None:
    """Resolve an imported page/UI module without discarding package ownership."""
    parts = module.split(".")
    for marker, base in (("pages_code", PAGES_DIR), ("ui", UI_DIR)):
        if marker not in parts:
            continue
        relative = parts[parts.index(marker) + 1 :]
        if not relative:
            continue
        module_file = base.joinpath(*relative).with_suffix(".py")
        if module_file.exists():
            return module_file
        package_file = base.joinpath(*relative, "__init__.py")
        if package_file.exists():
            return package_file
    return None


def build_report() -> str:
    routes = parse_routes()

    per_module_keys: dict[str, set[str]] = {}
    per_module_links: dict[str, set[str]] = {}
    sources = [*PAGES_DIR.rglob("*.py"), *UI_DIR.rglob("*.py"), APP_FILE]
    for path in sorted(set(sources)):
        if "__pycache__" in path.parts:
            continue
        keys, links = keys_in_module(path)
        rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        if keys:
            per_module_keys[rel] = keys
        if links:
            per_module_links[rel] = links

    global_keys: set[str] = set()
    for ks in per_module_keys.values():
        global_keys |= ks

    key_owners: dict[str, list[str]] = defaultdict(list)
    for rel, ks in per_module_keys.items():
        for k in ks:
            key_owners[k].append(rel)

    out: list[str] = []
    w = out.append

    w("# URL contract — the deep-link spec\n")
    w(
        "> **GENERATED — do not hand-edit.** Regenerate with "
        "`python tools/migration/extract_url_contract.py -o doc/URL_CONTRACT.md`.\n"
    )
    w("> Verify with `python tools/migration/extract_url_contract.py --check` (fails on drift).\n")
    w(
        "This is the one part of the UI that outside parties depend on: bookmarks, shared links, "
        "search results, and any future React router. Streamlit widgets and CSS can be "
        "redesigned freely; **these strings cannot** without breaking links already in the wild.\n"
    )

    w(f"## Routes ({len(routes)})\n")
    w("| Route (`url_path`) | Title | Page module | app.py line |")
    w("|---|---|---|---:|")
    for r in sorted(routes, key=lambda x: x["url_path"] or x["title"]):
        path_disp = f"`?page={r['url_path']}`" if r["url_path"] else "_(default)_"
        w(f"| {path_disp} | {r['title']} | `{r['module']}` | {r['line']} |")
    w("")

    w(f"## Query parameters ({len(global_keys)} distinct)\n")
    w(
        "Every literal key read from or written to `st.query_params`, with the modules "
        "that use it. A key used by more than one module is a **shared contract** — "
        "changing it breaks every listed consumer.\n"
    )
    w("| Parameter | Modules | Shared? |")
    w("|---|---|---|")
    for k in sorted(global_keys):
        owners = sorted(key_owners[k])
        shared = "**yes**" if len(owners) > 1 else "no"
        owner_list = "<br>".join(f"`{o}`" for o in owners)
        w(f"| `{k}` | {owner_list} | {shared} |")
    w("")

    if per_module_links:
        w("## Link emitters\n")
        w(
            "Modules that BUILD urls (as opposed to reading them). These are what put "
            "links into the wild, so they define what must keep working.\n"
        )
        w("| Module | Keys emitted |")
        w("|---|---|")
        for rel in sorted(per_module_links):
            ks = ", ".join(f"`{k}`" for k in sorted(per_module_links[rel]))
            w(f"| `{rel}` | {ks} |")
        w("")

    w("## Migration rule\n")
    w(
        "A React router must accept every route and parameter above **unchanged**. "
        "New parameters may be added; existing ones may only be removed behind a "
        "redirect that preserves the old link. Treat this table as the acceptance "
        "test for routing parity.\n"
    )

    return "\n".join(out)


def extract_keys_from_doc(text: str) -> set[str]:
    return set(re.findall(r"^\| `([a-zA-Z_][a-zA-Z0-9_]*)` \|", text, re.MULTILINE))


def extract_routes_from_doc(text: str) -> list[tuple[str, str, str]]:
    """Return normalized ``(url_path, title, module)`` route records.

    The generated table includes ``app.py`` line numbers for navigation, but a
    source-only line move is not a public contract change. Keeping duplicate
    records in the sorted list also makes an accidental duplicate route fail.
    """
    records: list[tuple[str, str, str]] = []
    in_routes = False
    for line in text.splitlines():
        if line.startswith("## Routes ("):
            in_routes = True
            continue
        if in_routes and line.startswith("## "):
            break
        if not in_routes or not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 4 or not cells[3].isdigit():
            continue
        route, title, module, _line_number = cells
        if route == "_(default)_":
            url_path = ""
        elif route.startswith("`?page=") and route.endswith("`"):
            url_path = route[len("`?page=") : -1]
        else:
            continue
        records.append((url_path, title, module.strip("`")))
    return sorted(records)


def main() -> int:
    reconfigure = getattr(sys.stdout, "reconfigure", None)
    if reconfigure is not None:
        reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, help="write markdown here")
    ap.add_argument("--check", action="store_true", help="fail if the committed contract has drifted")
    args = ap.parse_args()

    try:
        report = build_report()
    except AnalysisError as exc:
        print(f"URL contract analysis failed closed: {exc}", file=sys.stderr)
        return 1

    if args.check:
        if not DEFAULT_OUT.exists():
            print(f"URL contract missing: {DEFAULT_OUT}", file=sys.stderr)
            return 1
        committed = DEFAULT_OUT.read_text(encoding="utf-8")
        now_keys = extract_keys_from_doc(report)
        was_keys = extract_keys_from_doc(committed)
        added_keys, removed_keys = now_keys - was_keys, was_keys - now_keys
        now_routes = extract_routes_from_doc(report)
        was_routes = extract_routes_from_doc(committed)
        if added_keys or removed_keys or now_routes != was_routes:
            if added_keys:
                print(f"URL contract DRIFT — new parameters in code: {sorted(added_keys)}", file=sys.stderr)
            if removed_keys:
                print(f"URL contract DRIFT — parameters gone from code: {sorted(removed_keys)}", file=sys.stderr)
            if now_routes != was_routes:
                added_routes = sorted(set(now_routes) - set(was_routes))
                removed_routes = sorted(set(was_routes) - set(now_routes))
                if added_routes:
                    print(f"URL contract DRIFT — new/changed route records: {added_routes}", file=sys.stderr)
                if removed_routes:
                    print(f"URL contract DRIFT — removed/changed route records: {removed_routes}", file=sys.stderr)
                if not added_routes and not removed_routes:
                    print("URL contract DRIFT — duplicate route records changed", file=sys.stderr)
            print(
                "Regenerate: python tools/migration/extract_url_contract.py -o doc/URL_CONTRACT.md",
                file=sys.stderr,
            )
            return 1
        print(f"URL contract OK — {len(now_routes)} routes and {len(now_keys)} parameters match the committed spec.")
        return 0

    if args.out:
        destination = output_path(args.out)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(report, encoding="utf-8")
        print(f"[wrote {destination}]", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
