"""SECTION MAPS — keep big files navigable so agents never have to read them whole.

THE PROBLEM. A handful of files are so large that opening one costs more than most tasks are
worth: utility/shared_css.py ~72k tokens, utility/pages_code/procurement.py ~59k. An agent that
Reads one has spent a large slice of its context before doing any work.

THE FIX. Every file over MIN_LINES carries a `# ── SECTION MAP ──` block in its first ~60 lines
listing its sections with line ranges. The agent reads the header, then jumps:
    Read(file, offset=<start>, limit=<n>)
Turns a 59k-token read into ~600 tokens of header + ~3k of the one section that matters.

⚠️ THE TRAP THIS TOOL EXISTS TO PREVENT: inserting the map SHIFTS every line number below it,
so a hand-written map is wrong the moment it is saved (this happened while writing the first one
— every range was off by 49). `--write` accounts for the header's own length; `--check` fails CI
when a map has drifted, so it cannot silently rot.

Usage:
    python tools/section_map.py --check          # CI gate: any >1500-line file missing/stale map?
    python tools/section_map.py --list           # what would be mapped, and how big
    python tools/section_map.py --write <file>   # (re)generate the map for one file
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

MIN_LINES = 1500  # below this, a file is readable whole and needs no map
MARKER = "# ── SECTION MAP ──"
END_MARKER = "# ── END SECTION MAP ──"

# Files we deliberately never map (generated, vendored, or data)
SKIP = ("/.venv/", "/data/", "/node_modules/", "/__pycache__/", "/doc/source_pdfs/")

# Where to look. pipeline_sandbox is excluded — it is experiments, not load-bearing.
SEARCH = ["utility", "extractors", "dail_tracker_core", "test", "tools", "mcp_server", "shared", "services"]

_DEF = re.compile(r"^(?:def|class)\s+(\w+)")
_BANNER = re.compile(r"^\s*#\s*[─=-]{2,}\s*(.+?)\s*[─=-]*\s*$")


def big_files() -> list[tuple[Path, int]]:
    out = []
    for d in SEARCH:
        for p in (ROOT / d).rglob("*.py"):
            s = str(p).replace("\\", "/")
            if any(k in s for k in SKIP):
                continue
            n = sum(1 for _ in p.open(encoding="utf-8", errors="replace"))
            if n >= MIN_LINES:
                out.append((p, n))
    return sorted(out, key=lambda t: -t[1])


def has_map(p: Path) -> bool:
    head = "".join(p.open(encoding="utf-8", errors="replace").readlines()[:80])
    return MARKER in head


def sections(p: Path) -> list[tuple[int, str]]:
    """Top-level defs/classes + existing `# ──` banners = the natural section boundaries."""
    out: list[tuple[int, str]] = []
    for i, line in enumerate(p.open(encoding="utf-8", errors="replace"), 1):
        m = _DEF.match(line)
        if m:
            out.append((i, m.group(1)))
            continue
        b = _BANNER.match(line)
        if b and len(b.group(1)) > 3 and not b.group(1).startswith("END"):
            out.append((i, b.group(1)[:60]))
    return out


def render(p: Path, total: int, secs: list[tuple[int, str]], shift: int) -> str:
    """Build the map block. `shift` = the number of lines this block will itself add, so the
    printed ranges are correct AFTER insertion — the whole point of the tool."""
    tok = p.stat().st_size // 4
    lines = [
        MARKER + " " + "─" * max(0, 60 - len(MARKER)),
        f"# ⚠️  DO NOT READ WHOLE — ~{tok:,} tokens ({total + shift:,} lines after this header).",
        "#     Read this map, then jump:  Read(file, offset=<start>, limit=<n>)",
        "#",
    ]
    for i, (ln, name) in enumerate(secs):
        end = (secs[i + 1][0] - 1 + shift) if i + 1 < len(secs) else (total + shift)
        lines.append(f"#   {ln + shift:5}-{end:<5}  {name}")
    lines.append(END_MARKER + " " + "─" * max(0, 56 - len(END_MARKER)))
    return "\n".join(lines)


_DOCSTART = re.compile(r'^\s*(?:from __future__ import annotations\s*\n)?\s*("""|\'\'\')')


def write_map(p: Path) -> bool:
    """Insert the map. If the module already has a docstring, the map goes INSIDE it — prepending
    a second docstring would demote the original to a dead string expression and lose __doc__."""
    src = p.read_text(encoding="utf-8", errors="replace")
    if MARKER in src:
        print(f"  {p.relative_to(ROOT)}: already mapped")
        return False
    total = src.count("\n") + 1
    secs = sections(p)
    if not secs:
        print(f"  {p.relative_to(ROOT)}: no sections detected; skipped")
        return False

    lines = src.splitlines(keepends=True)
    # Does the module open with a MULTI-LINE docstring? (allow a leading comment/blank run)
    # Single-line docstrings are left alone — splicing into them is fiddly and prepending the map
    # as leading comments is just as findable (and keeps __doc__ clean).
    doc_close = None
    for i, ln in enumerate(lines[:15]):
        s = ln.lstrip()
        if s.startswith(('"""', "'''")):
            q = s[:3]
            if not (s.count(q) >= 2 and len(s.strip()) > 3):  # multi-line only
                for j in range(i + 1, len(lines)):
                    if q in lines[j]:
                        doc_close = j
                        break
            break
        if s and not s.startswith("#"):
            break  # real code before any docstring → no module docstring

    if doc_close is not None:  # splice INTO the existing docstring, just before its closing quotes
        shift = len(render(p, total, secs, 0).splitlines()) + 1
        block = "\n" + render(p, total, secs, shift) + "\n"
        lines.insert(doc_close, block)
        p.write_text("".join(lines), encoding="utf-8")
        where = "inside existing docstring"
    else:  # no docstring → add one
        shift = len(render(p, total, secs, 0).splitlines()) + 3
        block = f'"""\n{render(p, total, secs, shift)}\n"""\n\n'
        p.write_text(block + src, encoding="utf-8")
        where = "new docstring"
    print(f"  {p.relative_to(ROOT)}: mapped ({len(secs)} sections, +{shift} ln, {where})")
    return True


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="CI gate: fail if a big file has no map")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--write", metavar="FILE")
    args = ap.parse_args()

    if args.write:
        return 0 if write_map(Path(args.write).resolve()) else 1

    files = big_files()
    if args.list:
        for p, n in files:
            flag = "map" if has_map(p) else "NO MAP"
            print(f"  {n:5} ln  ~{p.stat().st_size // 4:6,} tok  [{flag:6}]  {p.relative_to(ROOT)}")
        return 0

    # --check (default)
    missing = [(p, n) for p, n in files if not has_map(p)]
    print(f"section-map check: {len(files)} files >{MIN_LINES} lines, {len(missing)} without a map")
    for p, n in missing:
        print(f"  MISSING  {n:5} ln  ~{p.stat().st_size // 4:6,} tok  {p.relative_to(ROOT)}")
    if args.check and missing:
        print("\nAdd a section map (tools/section_map.py --write <file>) — an agent must never")
        print("have to read one of these whole.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
