#!/usr/bin/env python
"""What is already known about a code file, printed in a few lines before you read it.

Refactoring, simplification and speed-up sessions leave verdicts, decisions and
measurements in many places. This gathers what names one file, so the next session starts
from them instead of re-deriving them:

* verdicts recorded in ``<root>/tools/refactor_ledger.jsonl``, the latest per function, each
  marked ``fresh`` or ``changed since`` by comparing a token hash of the function now with
  the one recorded (comments, blank lines and line wrapping are not changes);
* the ADRs the file cites, with status and title;
* tracked Markdown (ADRs, ledgers, lessons, runbooks, memory cards) and the discoveries file
  that name it, as line numbers plus any ledger-row id, so only those lines need reading.

    python tools/code_context.py show core/evaluation/engine.py --root planning/product --rev origin/main
    python tools/code_context.py add core/evaluation/engine.py _septic candidate --why "..." --root planning/product
    python tools/code_context.py check --root planning/product

Verdicts: leave, candidate, measure-first, done, rejected. Record leave and rejected verdicts
too: they took the most reading to reach. Stdlib only. Read-only except ``add``, which
rewrites the ledger through a temporary file.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import datetime as dt
import functools
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import textwrap
import tokenize
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LEDGER = "tools/refactor_ledger.jsonl"
VERDICTS = ("leave", "candidate", "measure-first", "done", "rejected")
REQUIRED = ("path", "verdict", "why", "date", "commit")
WHY_MAX = 400
DOC_LIMIT = 12
LINES_PER_DOC = 6
EVIDENCE_MAX = 240
_QUOTEPATH_OFF = ("-c", "core.quotepath=false")
_ADR_REF = re.compile(r"\bADR-(\d{4})\b")
_ADR_FILE = re.compile(r"(?:^|/)(?:decisions|adr)/ADR-(\d{4})-[^/]*\.md$", re.IGNORECASE)
_ROW_ID = re.compile(r"^\|\s*`([^`]+)`")
_SNAPSHOT_DIRS = {"doc", "docs", "archive"}
_DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
_DROPPED_TOKENS = {tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.ENCODING, tokenize.ENDMARKER}
_SHAPE_TOKENS = {tokenize.INDENT, tokenize.DEDENT}


# ── git and file access ──────────────────────────────────────────────────────


def _git(root: Path, *args: str) -> str | None:
    """Stdout of a git command run in root, or None when git fails (unknown rev, no match)."""
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        stdin=subprocess.DEVNULL,
        env=env,
        check=False,
    )
    return proc.stdout if proc.returncode == 0 else None


def read_text(root: Path, rel: str, rev: str | None) -> str | None:
    """A file's text at rev, or in the working tree when rev is None; None when absent."""
    if not _contained(root, rel):
        raise ValueError(f"path outside repository root: {rel}")
    if rev:
        text = _git(root, "show", f"{rev}:{rel}")
    else:
        try:
            text = (root / rel).read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
    return None if text is None else text.removeprefix("﻿")


@functools.lru_cache(maxsize=8)
def tracked(root: Path, rev: str | None) -> tuple[str, ...]:
    """Tracked paths at rev, or in the index when rev is None."""
    args = ("ls-tree", "-r", "--name-only", rev) if rev else ("ls-files",)
    return tuple((_git(root, *_QUOTEPATH_OFF, *args) or "").splitlines())


@functools.lru_cache(maxsize=32)
def _parsed(root: Path, rel: str, rev: str | None) -> tuple[str, ast.Module | None] | None:
    source = read_text(root, rel, rev)
    if source is None:
        return None
    try:
        return source, ast.parse(source)
    except SyntaxError:
        return source, None


def _is_dir(root: Path, rel: str, rev: str | None) -> bool:
    if rev:
        prefix = rel.rstrip("/") + "/"
        return any(path.startswith(prefix) for path in tracked(root, rev))
    return (root / rel).is_dir()


def normalise(root: Path, raw: str, rev: str | None = None) -> str:
    """A root-relative POSIX path; accepts absolute paths and monorepo-prefixed ones."""
    path = Path(raw)
    if path.is_absolute():
        try:
            raw = path.resolve().relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError(f"path outside repository root: {raw}") from exc
    rel = raw.replace("\\", "/")
    while rel.startswith("./"):
        rel = rel[2:]
    rel = _strip_root_prefix(root, rel)
    if not _contained(root, rel):
        raise ValueError(f"path outside repository root: {raw}")
    rel = Path(os.path.normpath(rel)).as_posix()
    if not rel.endswith("/") and _is_dir(root, rel, rev):
        rel += "/"
    return rel


def _contained(root: Path, rel: str) -> bool:
    """Resolve filesystem links before admitting a path under the selected repository."""
    try:
        return (root / rel).resolve().is_relative_to(root)
    except (OSError, RuntimeError):
        return False


def _strip_root_prefix(root: Path, rel: str) -> str:
    """'planning/product/core/x.py' becomes 'core/x.py' when the root ends in planning/product."""
    parts = rel.split("/")
    for size in range(min(len(root.parts), len(parts) - 1), 0, -1):
        if [p.lower() for p in root.parts[-size:]] == [p.lower() for p in parts[:size]]:
            return "/".join(parts[size:])
    return rel


# ── function identity ────────────────────────────────────────────────────────


def _find(tree: ast.Module, symbol: str) -> ast.AST | None:
    """The last def or class a dotted name reaches through module, class and function bodies."""
    body: list[ast.stmt] = tree.body
    node: ast.AST | None = None
    for part in symbol.split("."):
        found = [n for n in body if isinstance(n, _DEFS) and n.name == part]
        if not found:
            return None
        node = found[-1]
        body = node.body
    return node


def symbol_hash(source: str, symbol: str, tree: ast.Module | None = None) -> str | None:
    """Token hash of one def or class; comments, blank lines and line wrapping do not count."""
    try:
        node = _find(tree or ast.parse(source), symbol)
    except SyntaxError:
        return None
    if node is None:
        return None
    first = min([node.lineno, *(d.lineno for d in node.decorator_list)])
    segment = textwrap.dedent("".join(source.splitlines(keepends=True)[first - 1 : node.end_lineno]))
    digest = hashlib.sha256()
    try:
        for tok in tokenize.generate_tokens(io.StringIO(segment).readline):
            if tok.type not in _DROPPED_TOKENS:
                text = "" if tok.type in _SHAPE_TOKENS else tok.string
                digest.update(f"{tok.type}:{text}\0".encode())
    except (tokenize.TokenError, SyntaxError):
        return None
    return "sha256:" + digest.hexdigest()[:16]


def freshness(root: Path, row: dict, rev: str | None) -> str:
    """'fresh', 'changed since', 'symbol gone' or 'file gone'; a commit count for a whole-file row."""
    if not row.get("symbol"):
        if not (_is_dir(root, row["path"], rev) or read_text(root, row["path"], rev) is not None):
            return "file gone"
        if rev is None and _working_tree_changed(root, row["path"]):
            return "changed in working tree"
        return _commits_since(root, row["path"], row.get("commit", ""), rev)
    parsed = _parsed(root, row["path"], rev)
    if parsed is None:
        return "file gone"
    now = symbol_hash(parsed[0], row["symbol"], parsed[1])
    if now is None:
        return "symbol gone"
    return "fresh" if now == row.get("hash") else "changed since"


def _commits_since(root: Path, rel: str, commit: str, rev: str | None) -> str:
    if _git(root, "merge-base", "--is-ancestor", commit, rev or "HEAD") is None:
        return "commit not an ancestor or unknown here"
    out = _git(root, "rev-list", "--count", f"{commit}..{rev or 'HEAD'}", "--", rel) if commit else None
    if out is None:
        return "commit unknown here"
    count = int(out.strip() or "0")
    return "fresh" if count == 0 else f"{count} commit{'s' if count != 1 else ''} since"


def _working_tree_changed(root: Path, rel: str) -> bool:
    """Whether a path has tracked edits or untracked files."""
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all", "--", rel)
    return bool(status and status.strip())


# ── the ledger ───────────────────────────────────────────────────────────────


def _field_problems(row: dict) -> list[str]:
    problems = [f"{key} must be a string" for key in REQUIRED if not isinstance(row.get(key), str)]
    problems += [f"missing {key}" for key in REQUIRED if isinstance(row.get(key), str) and not row[key].strip()]
    problems += [
        f"{key} must be a string"
        for key in ("symbol", "evidence", "hash")
        if key in row and not isinstance(row[key], str)
    ]
    return problems


def _valid_date(value: str) -> bool:
    try:
        return dt.date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def validate(row: object) -> list[str]:
    if not isinstance(row, dict):
        return ["not a JSON object"]
    problems = _field_problems(row)
    if problems:
        return problems
    if not _valid_date(row["date"]):
        problems.append("date must be YYYY-MM-DD")
    verdict = row.get("verdict")
    if isinstance(verdict, str) and verdict and verdict not in VERDICTS:
        problems.append(f"verdict {verdict!r} is not one of {', '.join(VERDICTS)}")
    why = row.get("why")
    if isinstance(why, str) and len(why) > WHY_MAX:
        problems.append(f"why runs past {WHY_MAX} characters; link the evidence instead")
    if isinstance(row.get("symbol"), str) and row["symbol"] and not row.get("hash"):
        problems.append("a function row needs its hash; write rows with `add`")
    return problems


def load_ledger(root: Path, rev: str | None = None) -> tuple[list[dict], list[str]]:
    """Rows at rev plus rows in the working tree, so verdicts not yet merged still show."""
    rows: list[dict] = []
    errors: list[str] = []
    seen: set[str] = set()
    for source in ([rev] if rev else []) + [None]:
        label = f"{source}:{LEDGER}" if source else LEDGER
        for number, line in enumerate((read_text(root, LEDGER, source) or "").splitlines(), 1):
            if not line.strip() or line in seen:
                continue
            seen.add(line)
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                errors.append(f"{label}:{number}: not JSON ({exc.msg})")
                continue
            problems = validate(row)
            if not problems and (Path(row["path"]).is_absolute() or not _contained(root, row["path"])):
                problems.append("path outside repository root")
            errors.extend(f"{label}:{number}: {problem}" for problem in problems)
            if isinstance(row, dict) and row.get("path") and not problems:
                rows.append({**row, "_source": source})
    return rows, errors


def latest(rows: list[dict]) -> list[dict]:
    """The newest row per (path, symbol) by date; on the same date the later line wins."""
    ordered = sorted(rows, key=lambda row: str(row.get("date") or ""))
    return list({(row["path"], row.get("symbol") or ""): row for row in ordered}.values())


def rows_for(rows: list[dict], rel: str) -> list[dict]:
    """Rows on this path, on a directory holding it, or on anything inside it."""
    return [
        row
        for row in latest(rows)
        if row["path"] == rel
        or (row["path"].endswith("/") and rel.startswith(row["path"]))
        or (rel.endswith("/") and row["path"].startswith(rel))
    ]


def _append(path: Path, row: dict) -> None:
    """Rewrite the ledger with one more line, through a temporary file and os.replace."""
    old = path.read_text(encoding="utf-8") if path.exists() else ""
    if old and not old.endswith("\n"):
        old += "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, tmp = tempfile.mkstemp(dir=path.parent, prefix=".refactor_ledger.", suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as out:
            out.write(old + json.dumps(row, ensure_ascii=False) + "\n")
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)


# ── what names the file ──────────────────────────────────────────────────────


def _unique_name(root: Path, name: str, rev: str | None) -> bool:
    """True when one tracked file outside doc/ and archive/ snapshot copies has this name."""
    live = [
        p for p in tracked(root, rev) if p.rsplit("/", 1)[-1] == name and not _SNAPSHOT_DIRS & set(p.split("/")[:-1])
    ]
    return len(live) == 1


def _mention_pattern(root: Path, rel: str, rev: str | None) -> tuple[list[str], re.Pattern[str]]:
    """Fixed strings for git grep, and a regex that keeps only real mentions of rel."""
    end = "" if rel.endswith("/") else r"(?!\w)"
    needles, parts = [rel], [re.escape(rel) + end]
    name = rel.rsplit("/", 1)[-1]
    if end and name != rel and _unique_name(root, name, rev):
        needles.append(name)
        parts.append(r"(?<![\w-])" + re.escape(name) + end)
    return needles, re.compile("|".join(parts))


def _split_grep_line(line: str, rev: str | None) -> tuple[str, str, str]:
    if rev and line.startswith(rev + ":"):
        line = line[len(rev) + 1 :]
    path, _, rest = line.partition(":")
    number, _, text = rest.partition(":")
    return (path, number, text) if number.isdigit() else ("", "", "")


def doc_hits(root: Path, rel: str, rev: str | None) -> dict[str, list[str]]:
    """{doc: ['L12 `row-id`', 'L40', ...]} for tracked Markdown and discoveries naming rel."""
    needles, pattern = _mention_pattern(root, rel, rev)
    args = [*_QUOTEPATH_OFF, "grep", "-n", "-I", "-F"]
    for needle in needles:
        args += ["-e", needle]
    if rev:
        args.append(rev)
    hits: dict[str, list[str]] = {}
    for line in (_git(root, *args, "--", "*.md", "tools/discoveries.jsonl") or "").splitlines():
        path, number, text = _split_grep_line(line, rev)
        if path and pattern.search(text):
            row_id = _ROW_ID.match(text)
            hits.setdefault(path, []).append(f"L{number}" + (f" `{row_id.group(1)}`" if row_id else ""))
    return hits


def adr_label(root: Path, rel: str, rev: str | None) -> str:
    """'ADR-0185: Title [Accepted]' from the head of an ADR file."""
    head = (read_text(root, rel, rev) or "")[:4000].splitlines()
    title = next((line[2:].strip() for line in head if line.startswith("# ")), rel.rsplit("/", 1)[-1])
    status = next((line.split(":", 1)[1] for line in head if line.lower().startswith("status:")), "")
    words = status.replace("`", " ").split()
    return f"{title[:90]} [{words[0].rstrip('.;,') if words else 'no status'}]"


@functools.lru_cache(maxsize=8)
def _adr_files(root: Path, rev: str | None) -> dict[str, list[str]]:
    files: dict[str, list[str]] = {}
    for path in tracked(root, rev):
        match = _ADR_FILE.search(path)
        if match and "archive" not in path.split("/"):
            files.setdefault(match.group(1), []).append(path)
    return files


def cited_adrs(root: Path, rel: str, rev: str | None) -> list[str]:
    """A label for every ADR number the file's own text cites."""
    if rel.endswith("/"):
        return []
    numbers = sorted(set(_ADR_REF.findall(read_text(root, rel, rev) or "")))
    files = _adr_files(root, rev)
    labels: list[str] = []
    for number in numbers:
        found = files.get(number)
        labels += [adr_label(root, path, rev) for path in found] if found else [f"ADR-{number} (no file in this root)"]
    return labels


# ── commands ─────────────────────────────────────────────────────────────────


def header(root: Path, rev: str | None) -> str:
    head = (_git(root, "rev-parse", "--short=9", "HEAD") or "?").strip()
    line = f"code_context: {root} at {rev or 'the working tree'} (HEAD {head})"
    behind = None if rev else _git(root, "rev-list", "--count", "HEAD..origin/main")
    if behind and behind.strip() not in ("", "0"):
        line += f"\n  HEAD is {behind.strip()} commits behind origin/main; add --rev origin/main for current records"
    return line


def _describe(root: Path, rel: str, rev: str | None) -> str:
    if rel.endswith("/"):
        return f"{rel} (directory)"
    text = read_text(root, rel, rev)
    return (
        f"{rel} (not found at {rev or 'the working tree'})"
        if text is None
        else f"{rel} ({len(text.splitlines())} lines)"
    )


def _print_verdicts(root: Path, rel: str, rows: list[dict], rev: str | None, limit: int) -> None:
    if not rows:
        print("  verdicts: none recorded")
        return
    shown = "" if len(rows) <= limit else f", first {limit}"
    print(f"  verdicts ({LEDGER}){shown}:")
    for row in rows[:limit]:
        where = row.get("symbol") or "(whole)"
        if row["path"] != rel:
            where = f"{row['path']} {where}"
        evidence = row.get("evidence", "")
        if len(evidence) > EVIDENCE_MAX:
            evidence = evidence[:EVIDENCE_MAX] + "... (truncated)"
        evidence = f" [{evidence}]" if evidence else ""
        origin = f" [working-tree verdict; code at requested rev {rev}]" if rev and row.get("_source") is None else ""
        print(
            f"    {where}: {row['verdict']}, {freshness(root, row, rev)}, {row['date']}. {row['why']}{evidence}{origin}"
        )
    if len(rows) > limit:
        print(f"    (+{len(rows) - limit} more)")


def _print_docs(root: Path, hits: dict[str, list[str]], rev: str | None, limit: int) -> None:
    if not hits:
        print("  named in: no tracked Markdown or discoveries")
        return
    ordered = sorted(
        hits, key=lambda path: (not _ADR_FILE.search(path), "archive" in path.split("/"), -len(hits[path]), path)
    )
    shown = "" if len(hits) <= limit else f", first {limit}"
    print(f"  named in {len(hits)} doc(s){shown}:")
    for path in ordered[:limit]:
        label = f" {adr_label(root, path, rev)}" if _ADR_FILE.search(path) else ""
        lines = hits[path]
        more = f" (+{len(lines) - LINES_PER_DOC} more)" if len(lines) > LINES_PER_DOC else ""
        print(f"    {path}{label}: {', '.join(lines[:LINES_PER_DOC])}{more}")


def show(root: Path, raws: list[str], rev: str | None, limit: int) -> int:
    print(header(root, rev))
    rows, errors = load_ledger(root, rev)
    for raw in raws:
        rel = normalise(root, raw, rev)
        print(f"\n{_describe(root, rel, rev)}")
        _print_verdicts(root, rel, rows_for(rows, rel), rev, limit)
        labels = cited_adrs(root, rel, rev)
        for label in labels[:limit]:
            print(f"  cites {label}")
        if len(labels) > limit:
            print(f"  cites (+{len(labels) - limit} more)")
        _print_docs(root, doc_hits(root, rel, rev), rev, limit)
    if errors:
        print(f"\n{len(errors)} malformed ledger row(s); run `check`")
    return 0


def _locate(root: Path, row: dict, rev: str | None) -> str | None:
    """Fill the row's hash, or say why its path or function cannot be found."""
    where = rev or "the working tree"
    if not row["symbol"]:
        exists = _is_dir(root, row["path"], rev) or read_text(root, row["path"], rev) is not None
        return None if exists else f"no file or directory at {where}"
    parsed = _parsed(root, row["path"], rev)
    digest = symbol_hash(parsed[0], row["symbol"], parsed[1]) if parsed else None
    if digest is None:
        return f"no def or class {row['symbol']!r} at {where}"
    row["hash"] = digest
    return None


def add(args: argparse.Namespace) -> int:
    root = args.root
    normalise(root, LEDGER)
    rel = normalise(root, args.path, args.commit)
    row = {
        "path": rel,
        "symbol": "" if args.symbol in ("-", "") else args.symbol,
        "verdict": args.verdict,
        "why": " ".join(args.why.split()),
        "evidence": args.evidence.strip(),
        "date": args.date or dt.date.today().isoformat(),
        "commit": (_git(root, "rev-parse", "--short=9", args.commit or "HEAD") or "").strip(),
    }
    problem = _locate(root, row, args.commit)
    problems = ([problem] if problem else []) + validate(row)
    if problems:
        print(f"{rel}: {'; '.join(problems)}", file=sys.stderr)
        return 2
    _append(root / LEDGER, row)
    print(f"recorded {rel} {row['symbol'] or '(whole)'}: {row['verdict']} at {row['commit']}")
    return 0


def check(root: Path, rev: str | None) -> int:
    rows, errors = load_ledger(root, rev)
    for error in errors:
        print(error)
    recheck = [(row, state) for row in latest(rows) if (state := freshness(root, row, rev)) != "fresh"]
    for row, state in recheck:
        print(f"{state}: {row['path']} {row.get('symbol') or '(whole)'} ({row['verdict']}, {row['date']})")
    print(f"{len(rows)} row(s), {len(errors)} problem(s), {len(recheck)} to re-judge")
    return 1 if errors else 0


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--root", type=Path, default=REPO, help="repository root; planning/product for the engine")
    reading = argparse.ArgumentParser(add_help=False)
    reading.add_argument("--rev", help="read records and code at this revision, e.g. origin/main")
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    show_cmd = sub.add_parser("show", parents=[common, reading], help="what is known about files")
    show_cmd.add_argument("paths", nargs="+")
    show_cmd.add_argument("--limit", type=int, default=DOC_LIMIT, help="entries per section per file (at least 1)")
    add_cmd = sub.add_parser("add", parents=[common], help="record a verdict")
    add_cmd.add_argument("path")
    add_cmd.add_argument("symbol", help="function, Class or Class.method; '-' for the whole file or directory")
    add_cmd.add_argument("verdict", choices=VERDICTS)
    add_cmd.add_argument("--why", required=True, help=f"the reason, at most {WHY_MAX} characters")
    add_cmd.add_argument("--evidence", default="", help="where the proof is: a doc, commit or command")
    add_cmd.add_argument("--commit", help="judge the code at this commit (default: the working tree)")
    add_cmd.add_argument("--date", help="decision date, YYYY-MM-DD (default: today)")
    sub.add_parser("check", parents=[common, reading], help="validate the ledger; list verdicts to re-judge")
    return parser


def main(argv: list[str] | None = None) -> int:
    with contextlib.suppress(AttributeError, ValueError):
        sys.stdout.reconfigure(errors="replace")
    args = _parser().parse_args(argv)
    args.root = args.root.resolve()
    for cache in (tracked, _parsed, _adr_files):
        cache.cache_clear()
    try:
        top = _git(args.root, "rev-parse", "--show-toplevel")
        if top is None or Path(top.strip()).resolve() != args.root:
            raise ValueError("--root must be a Git repository top-level")
        rev = args.commit if args.command == "add" else args.rev
        if rev and _git(args.root, "rev-parse", "--verify", "--end-of-options", f"{rev}^{{commit}}") is None:
            raise ValueError(f"unknown revision {rev!r}")
        if args.command == "add":
            return add(args)
        if args.command == "check":
            return check(args.root, args.rev)
        if args.limit < 1:
            raise ValueError("--limit must be at least 1")
        return show(args.root, args.paths, args.rev, args.limit)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
