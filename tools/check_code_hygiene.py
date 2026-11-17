#!/usr/bin/env python
"""Code-hygiene ratchet: changed Python files may not get more complex, noisier or more duplicated.

Two commands:

    gate    compare the changed files against a base ref, key by key, and fail on a regression
    report  scan every gated tracked file once and print totals and the worst files

The measure is fixed. Ruff runs with --isolated and an explicit rule list, so the repo's own
ruff config never moves it. Each finding has a weight (the measured number in a message such as
"(15 > 10)", else 1). The gate sums weights over the whole changed set, on both sides, so moving
a complex function from one changed file to another is not a regression.

    changed files --> ruff (head text) ----+
                  \\-> git show base:path --> ruff (base text) --> sum per key --> head > base ?
                  \\-> function-hash index (DUP: a new copy of an existing function)

Only the owner adds a waiver (tools/code_hygiene_waivers.json); this tool never writes it.
Exit codes: 0 clean, 1 regression, 2 tool error.
"""

from __future__ import annotations

import argparse
import ast
import contextlib
import functools
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.runtime_env  # noqa: E402,F401  (first project import: caps BLAS threads)

RULES = ("C901", "PLR0911", "PLR0912", "PLR0913", "PLR0915", "SIM", "ERA001", "F401", "F841")
KEYS = (*RULES, "DUP")
LABELS = {
    "C901": "complexity",
    "PLR0911": "return-count",
    "PLR0912": "branch-count",
    "PLR0913": "argument-count",
    "PLR0915": "statement-count",
    "SIM": "simplification",
    "ERA001": "commented-out-code",
    "F401": "unused-import",
    "F841": "unused-variable",
    "DUP": "new-duplicate-functions",
    "unparseable": "unparseable-files",
}
MIN_DUP_STATEMENTS = 6
CACHE_DIR = Path(tempfile.gettempdir()) / "dail_code_hygiene"
WAIVER_FILE = "tools/code_hygiene_waivers.json"
NO_BUDGET = 1e9
_WEIGHT_RE = re.compile(r"\((\d+) > (\d+)\)")


class ToolError(Exception):
    """The tool itself failed (git or ruff unavailable, bad input); not a code regression."""


# -- subprocess helpers ---------------------------------------------------------------------


def _run(cmd: list[str], cwd: Path | None = None, data: bytes | None = None, git: bool = False):
    # Hooks export GIT_DIR/GIT_INDEX_FILE; a nested repo or worktree must not inherit them.
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")} if git else None
    kwargs = {"input": data} if data is not None else {"stdin": subprocess.DEVNULL}
    try:
        return subprocess.run(cmd, capture_output=True, cwd=cwd, env=env, check=False, timeout=600, **kwargs)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ToolError(f"cannot run {cmd[0]}: {exc}") from exc


def _git(root: Path, *args: str) -> str | None:
    res = _run(["git", "-C", str(root), *args], git=True)
    return res.stdout.decode("utf-8", "replace") if res.returncode == 0 else None


def _split_z(out: str) -> list[str]:
    return [p for p in out.split("\0") if p]


def git_root(directory: Path) -> Path | None:
    out = _git(directory, "rev-parse", "--show-toplevel")
    return Path(out.strip()).resolve() if out and out.strip() else None


def base_ref(root: Path, override: str | None = None) -> str:
    if override:
        return override
    out = _git(root, "merge-base", "HEAD", "origin/main")
    return out.strip() if out and out.strip() else "HEAD"


# -- gating ---------------------------------------------------------------------------------


@functools.cache
def _nudge():
    path = ROOT / "tools" / "hooks" / "verifier_nudge.py"
    spec = importlib.util.spec_from_file_location("verifier_nudge_for_hygiene", path)
    if spec is None or spec.loader is None:
        raise ToolError("cannot load verifier_nudge")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def is_gated(rel: str) -> bool:
    path = Path(rel)
    return path.suffix.lower() == ".py" and bool(_nudge().is_code(path))


def tracked_py(root: Path) -> list[str]:
    out = _git(root, "ls-files", "-z", "*.py")
    if out is None:
        raise ToolError(f"git ls-files failed in {root}")
    return sorted(p for p in _split_z(out) if is_gated(p) and (root / p).is_file())


def changed_files(root: Path, base: str, include_untracked: bool = False) -> list[str]:
    """Gated files that differ from `base`; untracked files only when asked for."""
    names: list[str] = []
    queries = [("diff", "--name-only", "-z", base)]
    if include_untracked:
        queries.append(("ls-files", "-z", "--others", "--exclude-standard"))
    for args in queries:
        out = _git(root, *args)
        if out is None:
            raise ToolError(f"git {args[0]} failed in {root} (base {base})")
        names += _split_z(out)
    return sorted({n for n in names if is_gated(n) and (root / n).is_file()})


# -- ruff measure ---------------------------------------------------------------------------


def _weight(message: str) -> int:
    # Weight is the EXCESS over ruff's threshold ("(20 > 10)" weighs 10), not the measured value.
    # With the measured value, extracting one function at 20 into two at 11 read as 20 -> 22 and
    # the gate blocked the very refactor it exists to encourage (verifier finding, 2026-11-15).
    match = _WEIGHT_RE.search(message)
    return max(int(match.group(1)) - int(match.group(2)), 1) if match else 1


def _key(code: str | None) -> str | None:
    if not code:
        return None
    if code.startswith("SIM"):
        return "SIM"
    return code if code in RULES else None


def _ruff_cmd(*tail: str) -> list[str]:
    head = [sys.executable, "-m", "ruff", "check", "--isolated", "--no-cache"]
    return [*head, "--select", ",".join(RULES), "--output-format", "json", *tail]


def _parse_ruff(raw: bytes) -> list[dict]:
    try:
        items = json.loads(raw.decode("utf-8", "replace") or "[]")
    except ValueError as exc:
        raise ToolError(f"ruff output is not JSON: {exc}") from exc
    out = []
    for item in items:
        key = _key(item.get("code"))
        if key is None:
            continue
        message = str(item.get("message", ""))
        out.append(
            {
                "key": key,
                "weight": _weight(message),
                "line": int(item["location"]["row"]),
                "message": message,
                "path": str(item.get("filename", "")),
            }
        )
    return out


def _check_ruff(res) -> bytes:
    if res.returncode not in (0, 1):
        raise ToolError("ruff failed: " + res.stderr.decode("utf-8", "replace")[:300])
    return res.stdout


def lint_source(text: str, relpath: str) -> list[dict]:
    """Findings for one file version, linted from stdin so the repo's ruff config is not read."""
    res = _run(_ruff_cmd("--stdin-filename", relpath, "-"), data=text.encode("utf-8"))
    findings = _parse_ruff(_check_ruff(res))
    for finding in findings:
        finding["path"] = relpath
    return findings


def summarize(findings: list[dict]) -> dict:
    out: dict = {}
    for finding in findings:
        slot = out.setdefault(finding["key"], {"count": 0, "weight": 0})
        slot["count"] += 1
        slot["weight"] += finding["weight"]
    return out


def measure_source(text: str, relpath: str) -> dict:
    """Metrics for one file version: {key: {"count": n, "weight": w}}."""
    return summarize(lint_source(text, relpath))


# -- duplicate-function index ---------------------------------------------------------------


def _body_without_doc(node) -> list:
    body = node.body
    first = body[0] if body else None
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
        return body[1:]
    return body


def _count_statements(body: list) -> int:
    return sum(1 for stmt in body for node in ast.walk(stmt) if isinstance(node, ast.stmt))


def function_hashes(text: str) -> list[dict]:
    """Functions with at least MIN_DUP_STATEMENTS statements, hashed on body structure only."""
    try:
        tree = ast.parse(text)
    except (SyntaxError, ValueError):
        return []
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = _body_without_doc(node)
        if _count_statements(body) >= MIN_DUP_STATEMENTS:
            blob = "\n".join(ast.dump(stmt) for stmt in body).encode("utf-8")
            out.append({"name": node.name, "line": node.lineno, "hash": hashlib.sha1(blob).hexdigest()})
    return out


def _cache_path(root: Path) -> Path:
    return CACHE_DIR / (hashlib.sha1(str(root).encode("utf-8")).hexdigest() + ".json")


def _load_cache(root: Path) -> dict:
    try:
        data = json.loads(_cache_path(root).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_cache(root: Path, entries: dict) -> None:
    path = _cache_path(root)
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    with contextlib.suppress(OSError):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(entries), encoding="utf-8")
        os.replace(tmp, path)


def _entry(root: Path, rel: str, cached: dict | None) -> tuple[dict | None, bool]:
    """Return (entry, parsed_now). An entry is reused when size and mtime_ns are unchanged."""
    path = root / rel
    try:
        stat = path.stat()
    except OSError:
        return None, False
    if cached and cached.get("size") == stat.st_size and cached.get("mtime_ns") == stat.st_mtime_ns:
        return cached, False
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None, False
    return {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "funcs": function_hashes(text)}, True


def build_index(root: Path, extra: list[str], budget: float) -> dict:
    """Index tracked gated files plus `extra`; stops at the soft budget and keeps what it parsed."""
    cache = _load_cache(root)
    rels = sorted(set(tracked_py(root)) | set(extra))
    deadline = time.monotonic() + budget
    entries: dict = {}
    parsed = 0
    complete = True
    for rel in rels:
        if time.monotonic() >= deadline:
            complete = False
            break
        entry, fresh = _entry(root, rel, cache.get(rel))
        parsed += int(fresh)
        if entry is not None:
            entries[rel] = entry
    merged = {k: v for k, v in cache.items() if k in set(rels)}
    merged.update(entries)
    _save_cache(root, merged)
    return {"entries": entries, "complete": complete, "parsed": parsed, "files": len(rels)}


def _locations(entries: dict) -> dict:
    locs: dict = defaultdict(list)
    for rel, entry in entries.items():
        for fn in entry.get("funcs", []):
            locs[fn["hash"]].append((rel, fn["line"], fn["name"]))
    return locs


def _new_dups(rel: str, funcs: list, known: set, locs: dict) -> list[dict]:
    out = []
    for fn in funcs:
        if fn["hash"] in known:
            continue
        others = sorted(loc for loc in locs.get(fn["hash"], []) if (loc[0], loc[1]) != (rel, fn["line"]))
        if not others:
            continue
        other_path, other_line, other_name = others[0]
        out.append(
            {
                "key": "DUP",
                "weight": 1,
                "path": rel,
                "line": fn["line"],
                "name": fn["name"],
                "other_path": other_path,
                "other_line": other_line,
                "other_name": other_name,
                "message": f"{fn['name']} duplicates {other_path}:{other_line} {other_name}",
            }
        )
    return out


def _dup_findings(root: Path, states: list[dict], budget: float, notes: list[str]) -> list[dict]:
    if not states:
        return []
    index = build_index(root, [s["rel"] for s in states if s["parse_ok"]], budget)
    if not index["complete"]:
        notes.append("DUP not checked (budget)")
        return []
    locs = _locations(index["entries"])
    out: list[dict] = []
    for state in states:
        known = {fn["hash"] for fn in function_hashes(state["base_text"] or "")}
        funcs = index["entries"].get(state["rel"], {}).get("funcs", [])
        out += _new_dups(state["rel"], funcs, known, locs)
    return out


# -- the gate -------------------------------------------------------------------------------


def _parses(text: str) -> bool:
    try:
        ast.parse(text)
    except (SyntaxError, ValueError):
        return False
    return True


def _file_state(root: Path, ref: str, rel: str) -> dict:
    head_text = (root / rel).read_text(encoding="utf-8", errors="replace")
    base_text = _git(root, "show", f"{ref}:{rel}")
    parse_ok = _parses(head_text)
    head = lint_source(head_text, rel) if parse_ok else []
    if base_text is None:
        base: list[dict] = []
    elif base_text == head_text:
        base = head
    else:
        base = lint_source(base_text, rel)
    return {"rel": rel, "base_text": base_text, "parse_ok": parse_ok, "head": head, "base": base}


def load_waivers(root: Path, changed: set[str]) -> dict[str, int]:
    path = root / WAIVER_FILE
    if not path.is_file():
        return {}
    try:
        items = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ToolError(f"cannot read {WAIVER_FILE}: {exc}") from exc
    allow: dict[str, int] = defaultdict(int)
    for item in items if isinstance(items, list) else []:
        if isinstance(item, dict) and item.get("path") in changed:
            allow[str(item.get("key"))] += int(item.get("allow") or 0)
    return dict(allow)


def _totals(findings: list[dict]) -> dict[str, int]:
    totals: dict[str, int] = defaultdict(int)
    for finding in findings:
        totals[finding["key"]] += finding["weight"]
    return totals


def _evaluate(states: list[dict], dups: list[dict], waivers: dict[str, int]) -> tuple[list, list]:
    head = [f for s in states for f in s["head"]] + dups
    base = [f for s in states for f in s["base"]]
    head_totals, base_totals = _totals(head), _totals(base)
    regressions: list[dict] = []
    improved: list[dict] = []
    for key in KEYS:
        h, b = head_totals.get(key, 0), base_totals.get(key, 0)
        if h > b + waivers.get(key, 0):
            found = sorted((f for f in head if f["key"] == key), key=lambda f: (-f["weight"], f["path"], f["line"]))
            regressions.append({"key": key, "base": b, "head": h, "findings": found})
        elif h < b:
            improved.append({"key": key, "base": b, "head": h})
    bad = [s["rel"] for s in states if not s["parse_ok"]]
    if bad:
        found = [
            {"key": "unparseable", "weight": 1, "path": p, "line": 1, "message": "file does not parse"} for p in bad
        ]
        regressions.append({"key": "unparseable", "base": 0, "head": len(bad), "findings": found})
    return regressions, improved


def _abs_path(name: str | Path, root: str | Path | None) -> Path:
    path = Path(name)
    if not path.is_absolute():
        path = (Path(root) if root else Path.cwd()) / path
    return path.resolve()


def _root_of(path: Path, cache: dict) -> Path | None:
    directory = path.parent
    while not directory.exists():
        directory = directory.parent
    if directory not in cache:
        cache[directory] = git_root(directory)
    return cache[directory]


def _plan(files, root, notes: list[str]) -> dict[Path, list[str] | None]:
    """Group the requested files by their own git root; None means 'use the changed set'."""
    if files is None:
        top = Path(root).resolve() if root else git_root(Path.cwd())
        if top is None:
            raise ToolError("not inside a git repository")
        return {top: None}
    groups: dict[Path, list[str] | None] = {}
    cache: dict = {}
    for name in files:
        path = _abs_path(name, root)
        top = _root_of(path, cache)
        if top is None:
            notes.append(f"no git root: {name}")
            continue
        rel = Path(os.path.relpath(path, top)).as_posix()
        if is_gated(rel) and path.is_file():
            groups.setdefault(top, []).append(rel)  # type: ignore[union-attr]
    return groups


def deleted_files(root: Path, ref: str) -> list[str]:
    """Gated paths deleted relative to `ref` (a rename shows as a delete plus an add)."""
    out = _git(root, "diff", "--name-status", "--no-renames", "-z", ref)
    if out is None:
        raise ToolError(f"git diff --name-status failed in {root} (base {ref})")
    tokens = _split_z(out)
    pairs = zip(tokens[0::2], tokens[1::2], strict=False)
    return sorted(p for status, p in pairs if status == "D" and is_gated(p) and not (root / p).exists())


def _credit_deleted(root: Path, ref: str, states: list[dict], notes: list[str]) -> list[dict]:
    """Base-only states for deleted files, but only when the change adds a file (a move or split)."""
    if not any(s["base_text"] is None for s in states):
        return []
    credited = []
    for rel in deleted_files(root, ref):
        text = _git(root, "show", f"{ref}:{rel}")
        if text is not None:
            credited.append(
                {"rel": rel, "base_text": text, "parse_ok": True, "head": [], "base": lint_source(text, rel)}
            )
    if credited:
        names = ", ".join(s["rel"] for s in credited)
        notes.append(f"credited {len(credited)} deleted file(s) at base: {names}")
    return credited


def _gate_root(root: Path, rels: list[str] | None, opts: dict, result: dict) -> None:
    ref = base_ref(root, opts["base"])
    if rels is None:
        rels = changed_files(root, ref, opts["untracked"])
    states = [_file_state(root, ref, rel) for rel in rels]
    dups = _dup_findings(root, states, opts["budget"], result["notes"])
    credit = _credit_deleted(root, ref, states, result["notes"])
    regressions, improved = _evaluate(states + credit, dups, load_waivers(root, set(rels)))
    for item in regressions + improved:
        item["root"] = str(root)
    result["regressions"] += regressions
    result["improved"] += improved
    result["checked_files"] += rels


def gate(files=None, root=None, base=None, dup_budget_seconds: float = 20, include_untracked: bool = False) -> dict:
    """Compare head against base for the changed set. Result: regressions, improved, checked_files, notes.

    Explicit `files` are always gated, tracked or not. The default changed set (no `files`) skips
    untracked files unless `include_untracked` is set.
    """
    result: dict = {"regressions": [], "improved": [], "checked_files": [], "notes": []}
    opts = {"base": base, "budget": dup_budget_seconds, "untracked": include_untracked}
    for top, rels in _plan(files, root, result["notes"]).items():
        _gate_root(top, rels, opts, result)
    return result


# -- report ---------------------------------------------------------------------------------


def _ruff_tree(root: Path) -> list[dict]:
    res = _run(_ruff_cmd("."), cwd=root)
    findings = _parse_ruff(_check_ruff(res))
    for finding in findings:
        finding["path"] = Path(os.path.relpath(finding["path"], root)).as_posix()
    return findings


def _top_files(findings: list[dict], top: int) -> list[dict]:
    per: dict[str, dict] = {}
    for finding in findings:
        slot = per.setdefault(finding["path"], {"path": finding["path"], "weight": 0, "count": 0})
        slot["weight"] += finding["weight"]
        slot["count"] += 1
    return sorted(per.values(), key=lambda s: (-s["weight"], s["path"]))[:top]


def report(root=None, top: int = 10, dup_budget_seconds: float | None = None) -> dict:
    """One scan of every gated tracked file: per-key totals, duplicate groups, worst files."""
    start = time.monotonic()
    top_dir = Path(root).resolve() if root else git_root(Path.cwd())
    if top_dir is None:
        raise ToolError("not inside a git repository")
    tracked = set(tracked_py(top_dir))
    findings = [f for f in _ruff_tree(top_dir) if f["path"] in tracked]
    mark = time.monotonic()
    budget = NO_BUDGET if dup_budget_seconds is None else dup_budget_seconds
    index = build_index(top_dir, [], budget)
    dup_seconds = time.monotonic() - mark
    groups = sum(1 for v in _locations(index["entries"]).values() if len(v) > 1) if index["complete"] else None
    return {
        "root": str(top_dir),
        "files": len(tracked),
        "totals": summarize(findings),
        "dup_groups": groups,
        "top": _top_files(findings, top),
        "dup_index_seconds": round(dup_seconds, 2),
        "dup_index_parsed": index["parsed"],
        "dup_index_files": index["files"],
        "seconds": round(time.monotonic() - start, 2),
    }


# -- output ---------------------------------------------------------------------------------


def _ascii(text: str) -> str:
    return text.encode("ascii", "replace").decode("ascii")


def _header(item: dict) -> str:
    return (
        f"{item['key']} {LABELS[item['key']]} weight {item['base']} -> {item['head']} (+{item['head'] - item['base']})"
    )


def regression_lines(result: dict) -> list[str]:
    return [_ascii(_header(r)) for r in result["regressions"]]


def _finding_line(finding: dict) -> str:
    if finding["key"] == "DUP":
        other = f"{finding['other_path']}:{finding['other_line']} {finding['other_name']}"
        return f"  {finding['path']}:{finding['line']} {finding['name']} duplicates {other}"
    return f"  {finding['path']}:{finding['line']} {finding['message']}"


def format_result(result: dict) -> list[str]:
    lines: list[str] = []
    for item in result["regressions"]:
        lines.append(_header(item))
        lines += [_finding_line(f) for f in item["findings"][:5]]
    lines += [f"improved: {i['key']} {i['base']} -> {i['head']}" for i in result["improved"]]
    lines += [f"note: {n}" for n in result["notes"]]
    return [_ascii(line) for line in lines]


def format_report(rep: dict) -> list[str]:
    lines = [f"root: {rep['root']}", f"gated tracked files: {rep['files']}"]
    for key in RULES:
        slot = rep["totals"].get(key, {"count": 0, "weight": 0})
        lines.append(f"{key} {LABELS[key]} count {slot['count']} weight {slot['weight']}")
    groups = "not checked (budget)" if rep["dup_groups"] is None else rep["dup_groups"]
    lines.append(f"duplicate groups: {groups}")
    lines.append("top files by weight:")
    lines += [f"  {t['weight']:>5} {t['path']} ({t['count']} findings)" for t in rep["top"]]
    lines.append(
        f"time: {rep['seconds']}s total, dup index {rep['dup_index_seconds']}s "
        f"({rep['dup_index_parsed']} of {rep['dup_index_files']} files parsed, rest from cache)"
    )
    return [_ascii(line) for line in lines]


# -- CLI ------------------------------------------------------------------------------------


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="check_code_hygiene", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    gate_p = sub.add_parser("gate", help="fail when changed files regress against the base")
    gate_p.add_argument("--root")
    gate_p.add_argument("--base")
    gate_p.add_argument("--files", nargs="+")
    gate_p.add_argument("--json", action="store_true")
    gate_p.add_argument("--include-untracked", action="store_true")
    gate_p.add_argument("--dup-budget-seconds", type=float, default=20)
    rep_p = sub.add_parser("report", help="totals over every gated tracked file")
    rep_p.add_argument("--root")
    rep_p.add_argument("--top", type=int, default=10)
    rep_p.add_argument("--json", action="store_true")
    rep_p.add_argument("--dup-budget-seconds", type=float, default=None)
    return parser


def _cmd_gate(args) -> int:
    result = gate(
        args.files,
        root=args.root,
        base=args.base,
        dup_budget_seconds=args.dup_budget_seconds,
        include_untracked=args.include_untracked,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        lines = format_result(result)
        if not result["regressions"]:
            lines.insert(0, f"clean: {len(result['checked_files'])} file(s) checked")
        print("\n".join(lines))
    incomplete = any(note.startswith("DUP not checked (budget)") for note in result["notes"])
    return 1 if result["regressions"] or incomplete else 0


def _cmd_report(args) -> int:
    rep = report(root=args.root, top=args.top, dup_budget_seconds=args.dup_budget_seconds)
    print(json.dumps(rep, indent=2) if args.json else "\n".join(format_report(rep)))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return _cmd_gate(args) if args.cmd == "gate" else _cmd_report(args)
    except ToolError as exc:
        print(f"error: {_ascii(str(exc))}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
