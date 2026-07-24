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
    python tools/extract_url_contract.py                  # markdown to stdout
    python tools/extract_url_contract.py -o doc/URL_CONTRACT.md
    python tools/extract_url_contract.py --check          # exit 1 if drifted

`--check` compares against the committed doc/URL_CONTRACT.md and fails when the
code has grown or dropped a parameter without the contract being regenerated.
That is the ratchet: the contract cannot silently rot.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_FILE = PROJECT_ROOT / "utility" / "app.py"
PAGES_DIR = PROJECT_ROOT / "utility" / "pages_code"
UI_DIR = PROJECT_ROOT / "utility" / "ui"
DEFAULT_OUT = PROJECT_ROOT / "doc" / "URL_CONTRACT.md"

# Attribute names on query_params that are dict METHODS, not parameter keys.
QP_METHODS = {
    "get", "update", "clear", "to_dict", "keys", "items", "values",
    "pop", "setdefault", "from_dict", "get_all", "setlist",
}

LINK_RE = re.compile(r"[?&]([a-zA-Z_][a-zA-Z0-9_]*)=")


def is_query_params(node: ast.AST, aliases: set[str]) -> bool:
    """True for `st.query_params` or a local alias bound to it."""
    if isinstance(node, ast.Attribute) and node.attr == "query_params":
        return True
    if isinstance(node, ast.Name) and node.id in aliases:
        return True
    return False


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
    try:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src)
    except (OSError, UnicodeDecodeError, SyntaxError):
        return set(), set()

    aliases = collect_aliases(tree)
    keys: set[str] = set()
    link_keys: set[str] = set()

    for node in ast.walk(tree):
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
            if is_query_params(node.comparators[0], aliases):
                if isinstance(node.left, ast.Constant) and isinstance(node.left.value, str):
                    keys.add(node.left.value)

        # link-building string constants: "...?member=" / "&page="
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if "?" in node.value or "&" in node.value:
                link_keys.update(LINK_RE.findall(node.value))

        # f-strings that build links: f"/?page={x}&member={y}"
        elif isinstance(node, ast.JoinedStr):
            literal = "".join(
                v.value for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
            )
            if "?" in literal or "&" in literal:
                link_keys.update(LINK_RE.findall(literal))

    return keys, link_keys


def parse_routes() -> list[dict[str, str]]:
    """Read st.Page(...) registrations out of utility/app.py."""
    src = APP_FILE.read_text(encoding="utf-8")
    tree = ast.parse(src)

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
    """`pages_code.votes` -> utility/pages_code/votes.py"""
    tail = module.split(".")[-1]
    for base in (PAGES_DIR, UI_DIR):
        cand = base / f"{tail}.py"
        if cand.exists():
            return cand
    return None


def build_report() -> str:
    routes = parse_routes()

    per_module_keys: dict[str, set[str]] = {}
    per_module_links: dict[str, set[str]] = {}
    for path in sorted([*PAGES_DIR.glob("*.py"), *UI_DIR.glob("*.py"), APP_FILE]):
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
    w("> **GENERATED — do not hand-edit.** Regenerate with "
      "`python tools/extract_url_contract.py -o doc/URL_CONTRACT.md`.\n")
    w("> Verify with `python tools/extract_url_contract.py --check` (fails on drift).\n")
    w("This is the one part of the UI that outside parties depend on: bookmarks, shared links, "
      "search results, and any future React router. Streamlit widgets and CSS can be "
      "redesigned freely; **these strings cannot** without breaking links already in the wild.\n")

    w(f"## Routes ({len(routes)})\n")
    w("| Route (`url_path`) | Title | Page module | app.py line |")
    w("|---|---|---|---:|")
    for r in sorted(routes, key=lambda x: x["url_path"] or x["title"]):
        path_disp = f"`?page={r['url_path']}`" if r["url_path"] else "_(default)_"
        w(f"| {path_disp} | {r['title']} | `{r['module']}` | {r['line']} |")
    w("")

    w(f"## Query parameters ({len(global_keys)} distinct)\n")
    w("Every literal key read from or written to `st.query_params`, with the modules "
      "that use it. A key used by more than one module is a **shared contract** — "
      "changing it breaks every listed consumer.\n")
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
        w("Modules that BUILD urls (as opposed to reading them). These are what put "
          "links into the wild, so they define what must keep working.\n")
        w("| Module | Keys emitted |")
        w("|---|---|")
        for rel in sorted(per_module_links):
            ks = ", ".join(f"`{k}`" for k in sorted(per_module_links[rel]))
            w(f"| `{rel}` | {ks} |")
        w("")

    w("## Migration rule\n")
    w("A React router must accept every route and parameter above **unchanged**. "
      "New parameters may be added; existing ones may only be removed behind a "
      "redirect that preserves the old link. Treat this table as the acceptance "
      "test for routing parity.\n")

    return "\n".join(out)


def extract_keys_from_doc(text: str) -> set[str]:
    return set(re.findall(r"^\| `([a-zA-Z_][a-zA-Z0-9_]*)` \|", text, re.MULTILINE))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, help="write markdown here")
    ap.add_argument("--check", action="store_true", help="fail if the committed contract has drifted")
    args = ap.parse_args()

    report = build_report()

    if args.check:
        if not DEFAULT_OUT.exists():
            print(f"URL contract missing: {DEFAULT_OUT}", file=sys.stderr)
            return 1
        committed = DEFAULT_OUT.read_text(encoding="utf-8")
        now_keys = extract_keys_from_doc(report)
        was_keys = extract_keys_from_doc(committed)
        added, removed = now_keys - was_keys, was_keys - now_keys
        if added or removed:
            if added:
                print(f"URL contract DRIFT — new parameters in code: {sorted(added)}", file=sys.stderr)
            if removed:
                print(f"URL contract DRIFT — parameters gone from code: {sorted(removed)}", file=sys.stderr)
            print("Regenerate: python tools/extract_url_contract.py -o doc/URL_CONTRACT.md", file=sys.stderr)
            return 1
        print(f"URL contract OK — {len(now_keys)} parameters match the committed spec.")
        return 0

    if args.out:
        args.out.write_text(report, encoding="utf-8")
        print(f"[wrote {args.out}]", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
