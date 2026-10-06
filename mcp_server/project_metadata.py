"""Bounded public metadata inventory and project-index construction."""

from __future__ import annotations

import contextlib
import re
from pathlib import Path

from mcp_server import code_index


def metadata_sources(root: Path, fact_cards_path: Path) -> list[tuple[str, Path]]:
    """Return the bounded, public source set used by the metadata index."""
    sources: list[tuple[str, Path]] = []
    for path in (fact_cards_path, root / "doc" / "INDEX.md"):
        try:
            rel = path.resolve().relative_to(root).as_posix()
        except (OSError, ValueError):
            continue
        sources.append((rel, path))
    with contextlib.suppress(Exception):
        for path in code_index.iter_repository_files(root, {".py", ".sql"}, code_index.DEFAULT_SCAN_POLICY):
            rel = path.relative_to(root).as_posix()
            if path.suffix.casefold() == ".py" or rel.startswith("sql_views/"):
                sources.append((rel, path))
    return sorted(sources, key=lambda item: item[0])


def metadata_fingerprint(root: Path, fact_cards_path: Path) -> tuple[tuple[str, int, int] | tuple[str, str], ...]:
    """Fingerprint metadata inputs with relative paths and stat mtime/size."""
    by_path = {rel: path for rel, path in metadata_sources(root, fact_cards_path)}
    expected = {
        "data/_meta/fact_cards.json": fact_cards_path,
        "doc/INDEX.md": root / "doc" / "INDEX.md",
    }
    by_path.update({rel: path for rel, path in expected.items()})
    fingerprint: list[tuple[str, int, int] | tuple[str, str]] = []
    for rel, path in sorted(by_path.items()):
        try:
            stat = path.stat()
        except OSError:
            fingerprint.append((rel, "missing"))
        else:
            fingerprint.append((rel, stat.st_mtime_ns, stat.st_size))
    return tuple(fingerprint)


def build_project_index(root: Path, _fact_cards_path: Path, cards: dict) -> list[dict]:
    """Build the metadata index for one repository root and fact-card snapshot."""
    idx: list[dict] = []

    # 1) Datasets — from the fact cards (name + purpose + grain + columns).
    for name, c in cards.items():
        cols = c.get("columns", "")
        desc = c.get("purpose") or c.get("grain") or ""
        idx.append(
            {
                "kind": "dataset",
                "name": name,
                "path": c.get("file", f"data/**/{name}.parquet"),
                "desc": desc,
                "haystack": " ".join([name, desc, c.get("grain", ""), str(cols), c.get("money_grain", "")]),
            }
        )

    # 2) Docs — parse the generated doc/INDEX.md table rows (title | domain | … | read when).
    index_md = root / "doc" / "INDEX.md"
    if index_md.exists():
        with contextlib.suppress(Exception):
            for line in index_md.read_text(encoding="utf-8").splitlines():
                m = re.match(r"\|\s*\[([^\]]+)\]\(([^)]+)\)\s*\|(.*)$", line)
                if not m:
                    continue
                title, rel, rest = m.group(1), m.group(2), m.group(3)
                doc_path = Path("doc") / rel
                if not code_index.DEFAULT_SCAN_POLICY.allows(doc_path):
                    continue
                cells = [x.strip() for x in rest.split("|")]
                domain = cells[0] if cells else ""
                read_when = cells[-2] if len(cells) >= 2 else ""  # last real cell before trailing ''
                idx.append(
                    {
                        "kind": "doc",
                        "name": title,
                        "path": doc_path.as_posix(),
                        "desc": read_when or domain,
                        "haystack": " ".join([title, domain, read_when]),
                    }
                )

    # 3) SQL views — the leading comment block + the CREATE VIEW name. Read only the
    #    header (first ~15 lines), never the query body.
    views_dir = root / "sql_views"
    if views_dir.is_dir():
        for sql in code_index.iter_repository_files(root, {".sql"}):
            if not sql.relative_to(root).as_posix().startswith("sql_views/"):
                continue
            with contextlib.suppress(Exception):
                head = []
                view_name = ""
                for i, ln in enumerate(sql.read_text(encoding="utf-8").splitlines()):
                    if i > 24:
                        break
                    s = ln.strip()
                    cm = re.search(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)", s, re.I)
                    if cm:
                        view_name = cm.group(1)
                        break
                    if s.startswith("--"):
                        head.append(s.lstrip("-").strip())
                desc = " ".join(head)[:240]
                rel = sql.relative_to(root).as_posix()
                name = view_name or sql.stem
                idx.append(
                    {
                        "kind": "sql_view",
                        "name": name,
                        "path": rel,
                        "desc": desc,
                        "haystack": f"{name} {sql.stem} {desc}",
                    }
                )

    # 4) Code — one entry per repo .py (module path + docstring first line + def/class
    #    names), parsed with stdlib ast; never executed, bodies never read into results.
    with contextlib.suppress(Exception):
        idx.extend(code_index.build_code_index(root))

    return idx
