"""Extract the CSS class contract — the styling vocabulary the UI actually emits.

Styling survives a framework migration if, and only if, the new components emit
the SAME class names against the SAME stylesheet. That makes the class vocabulary
a contract in exactly the way the URL parameters are — invisible, unwritten, and
load-bearing.

This reads it out of the AST and cross-references it against the CSS:

  emitted ∩ defined   -> the contract a React component must reproduce
  defined \\ emitted   -> DEAD CSS: styled but never rendered (drop it)
  emitted \\ defined   -> UNSTYLED: rendered but never styled (a live bug)

The last two pay off today, independent of any migration: dead CSS is payload
nobody needs, and an unstyled class is markup that is silently not being styled.

Usage
-----
    python tools/extract_class_contract.py                 # markdown to stdout
    python tools/extract_class_contract.py -o doc/CLASS_CONTRACT.md
    python tools/extract_class_contract.py --check         # fail on new unstyled classes
    python tools/extract_class_contract.py --orphans       # dead + unstyled detail
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PAGES_DIR = PROJECT_ROOT / "utility" / "pages_code"
UI_DIR = PROJECT_ROOT / "utility" / "ui"
CSS_FILE = PROJECT_ROOT / "utility" / "shared_css.py"
APP_FILE = PROJECT_ROOT / "utility" / "app.py"
DEFAULT_OUT = PROJECT_ROOT / "doc" / "CLASS_CONTRACT.md"
BASELINE = PROJECT_ROOT / "tools" / "baselines" / "unstyled_classes_baseline.txt"

CLASS_ATTR_RE = re.compile(r"""class\s*=\s*["']([^"']*)["']""")
# A CSS class selector: .foo-bar (not .5rem, not a decimal)
CSS_SELECTOR_RE = re.compile(r"\.(-?[_a-zA-Z][_a-zA-Z0-9-]*)")
# Tokens that came from an f-string hole, e.g. "pill-" from f'class="pill-{kind}"'
DYNAMIC_HINT_RE = re.compile(r"[-_]$")

# Streamlit's own DOM classes — present in our CSS on purpose, never emitted by us.
FRAMEWORK_PREFIXES = ("st", "block-", "main", "row-widget", "element-container", "css-")


def _literal_parts(node: ast.AST) -> list[str]:
    """String pieces of a Constant or JoinedStr, holes replaced by a marker."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        out: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                out.append(v.value)
            else:
                out.append("\x00")  # interpolation hole
        return ["".join(out)]
    return []


def emitted_classes(path: Path) -> tuple[set[str], set[str]]:
    """(static class tokens, dynamic/partial tokens) emitted by one module."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return set(), set()

    static: set[str] = set()
    dynamic: set[str] = set()

    for node in ast.walk(tree):
        for text in _literal_parts(node):
            if "class" not in text:
                continue
            for attr_value in CLASS_ATTR_RE.findall(text):
                for token in attr_value.split():
                    if "\x00" in token:
                        # f-string hole inside the token: dynamic class name
                        stem = token.split("\x00")[0]
                        if stem and DYNAMIC_HINT_RE.search(stem):
                            dynamic.add(stem)
                        continue
                    if token:
                        static.add(token)
    return static, dynamic


def defined_selectors() -> set[str]:
    """Class selectors defined anywhere in the project's CSS."""
    found: set[str] = set()
    sources = [CSS_FILE, *PAGES_DIR.glob("*.py"), *UI_DIR.glob("*.py")]
    for path in sources:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        # Only look inside <style> blocks so ordinary code can't add false selectors.
        for block in re.findall(r"<style>(.*?)</style>", text, re.DOTALL):
            found.update(CSS_SELECTOR_RE.findall(block))
    return found


def family_of(name: str) -> str:
    if "-" in name:
        return name.split("-", 1)[0] + "-*"
    return "(no prefix)"


def load_baseline() -> set[str]:
    if not BASELINE.exists():
        return set()
    return {
        ln.strip() for ln in BASELINE.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.startswith("#")
    }


def collect() -> tuple[dict[str, set[str]], set[str], set[str], set[str]]:
    per_module: dict[str, set[str]] = {}
    all_static: set[str] = set()
    all_dynamic: set[str] = set()

    # app.py and shared_css.py emit markup too (nav chrome, site banner) — omitting
    # them would mis-report their classes as dead CSS.
    emitters = [*PAGES_DIR.glob("*.py"), *UI_DIR.glob("*.py"), APP_FILE, CSS_FILE]
    for path in sorted(set(emitters)):
        static, dynamic = emitted_classes(path)
        if static or dynamic:
            rel = str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
            per_module[rel] = static
            all_static |= static
            all_dynamic |= dynamic

    defined = defined_selectors()
    return per_module, all_static, all_dynamic, defined


def is_framework(name: str) -> bool:
    return name.startswith(FRAMEWORK_PREFIXES)


def build_report(show_orphans: bool) -> str:
    per_module, emitted, dynamic, defined = collect()

    contract = emitted & defined
    unstyled = {c for c in emitted - defined if not is_framework(c)}
    dead = {c for c in defined - emitted if not is_framework(c)}

    # A dead selector whose stem matches a dynamic token is NOT dead — it is
    # reached through an f-string hole (e.g. .pill-active from f"pill-{state}").
    reachable_dynamically = {
        c for c in dead if any(c.startswith(stem) for stem in dynamic)
    }
    dead -= reachable_dynamically

    fam_emitted: dict[str, int] = defaultdict(int)
    for c in contract:
        fam_emitted[family_of(c)] += 1
    fam_dead: dict[str, int] = defaultdict(int)
    for c in dead:
        fam_dead[family_of(c)] += 1

    out: list[str] = []
    w = out.append

    w("# CSS class contract — the styling vocabulary\n")
    w("> **GENERATED — do not hand-edit.** Regenerate with "
      "`python tools/extract_class_contract.py -o doc/CLASS_CONTRACT.md`.\n")
    w("> Verify with `python tools/extract_class_contract.py --check`.\n")
    w("Styling is preserved across a framework change if the new components emit these "
      "class names against this stylesheet. That makes the vocabulary below a contract — "
      "the same kind as [URL_CONTRACT.md](URL_CONTRACT.md), and equally unwritten until now.\n")

    w("## Summary\n")
    w("| Measure | Count |")
    w("|---|---:|")
    w(f"| Class names emitted by the UI | {len(emitted):,} |")
    w(f"| Class selectors defined in CSS | {len(defined):,} |")
    w(f"| **The contract** (emitted AND styled) | **{len(contract):,}** |")
    w(f"| Dynamic stems (f-string built, e.g. `pill-{{kind}}`) | {len(dynamic):,} |")
    w(f"| Dead CSS (styled, never emitted) | {len(dead):,} |")
    w(f"| Unstyled (emitted, never styled) | {len(unstyled):,} |")
    w("")

    w("## Contract by family\n")
    w("A React component reproducing one of these families must emit the same names.\n")
    w("| Family | In contract | Dead |")
    w("|---|---:|---:|")
    for fam in sorted(fam_emitted, key=lambda f: -fam_emitted[f]):
        w(f"| `{fam}` | {fam_emitted[fam]} | {fam_dead.get(fam, 0)} |")
    w("")

    w("## Per-module vocabulary\n")
    w("| Module | Classes emitted |")
    w("|---|---:|")
    for rel in sorted(per_module, key=lambda r: -len(per_module[r])):
        if per_module[rel]:
            w(f"| `{rel}` | {len(per_module[rel])} |")
    w("")

    if show_orphans:
        if unstyled:
            w("## Unstyled classes (emitted, no CSS rule)\n")
            w("Each is markup rendering without the style it names. Either a typo, a "
              "leftover, or a rule that was deleted from under it. **These are live "
              "defects, not migration debt.**\n")
            for c in sorted(unstyled):
                w(f"- `{c}`")
            w("")
        if dead:
            w("## Dead CSS (styled, never emitted)\n")
            w("Selectors with no corresponding markup. Safe to delete; each one shrinks "
              "the stylesheet a migration has to carry.\n")
            for c in sorted(dead):
                w(f"- `.{c}`")
            w("")

    w("## Migration rule\n")
    w("Components may add classes freely. A class in the contract may only be renamed "
      "if the CSS is renamed with it in the same change. Treat this table as the "
      "acceptance test for visual parity: same names + same stylesheet = same design.\n")

    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path)
    ap.add_argument("--check", action="store_true", help="fail on unstyled classes not in the baseline")
    ap.add_argument("--orphans", action="store_true", help="include dead/unstyled detail")
    ap.add_argument("--update-baseline", action="store_true")
    args = ap.parse_args()

    if args.check or args.update_baseline:
        _, emitted, _, defined = collect()
        unstyled = sorted(c for c in emitted - defined if not is_framework(c))

        if args.update_baseline:
            BASELINE.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                "# Classes emitted by the UI with no CSS rule behind them.",
                "# Generated by tools/extract_class_contract.py --update-baseline.",
                "# NEVER add by hand — each entry is markup that is not being styled.",
                "",
                *unstyled,
            ]
            BASELINE.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(f"[baseline written: {len(unstyled)} unstyled classes]")
            return 0

        baseline = load_baseline()
        new = [c for c in unstyled if c not in baseline]
        print(f"Emitted classes: {len(emitted):,} | styled: {len(emitted & defined):,} | "
              f"unstyled: {len(unstyled)} (baselined {len(unstyled) - len(new)})")
        if new:
            print(f"\nFAIL — {len(new)} newly unstyled class(es):", file=sys.stderr)
            for c in new:
                print(f"  {c}", file=sys.stderr)
            print("\nAdd the CSS rule, fix the typo, or drop the class.", file=sys.stderr)
            return 1
        print("OK — no new unstyled classes.")
        return 0

    report = build_report(show_orphans=args.orphans)
    if args.out:
        args.out.write_text(report, encoding="utf-8")
        print(f"[wrote {args.out}]", file=sys.stderr)
    else:
        sys.stdout.write(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
