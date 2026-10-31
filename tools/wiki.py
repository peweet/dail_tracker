#!/usr/bin/env python
"""The two-tier mini-wiki: status, upkeep, global-tier setup, new pages.

Decision (2026-10-25, doc/AGENT_WORKFLOW_PLAN_2026_10_25.md): the project already keeps its
knowledge in small files -- `.claude/rules/*.md` (instructions, path-scoped), `memory/*.md`
(tracked lessons), `doc/*.md` (reference, indexed by doc/INDEX.md), `tools/discoveries.jsonl`
(the fast index) and the per-user auto-memory store. What it lacked was a GLOBAL tier for
personal, cross-project rules (`~/.claude/rules/`, loaded in every project without an approval
dialog) and one place that measures whether the wiki is still small and still maintained.

This tool is deliberately a thin layer over existing tools. It does not invent a new store,
a graph index or an automatic rewriter (memory/memory_recall_policy_2026_10_18.md).

    python tools/wiki.py status            # both tiers: sizes, missing pages, upkeep backlog
    python tools/wiki.py status --strict   # exit 1 on page-size and global-tier problems
    python tools/wiki.py setup-global      # create ~/.claude/CLAUDE.md + ~/.claude/rules/ from
                                           # tools/wiki_templates/global (never overwrites)
    python tools/wiki.py upkeep            # one consolidated backlog with the command for each
    python tools/wiki.py page <tier> <slug> --title "..."   # new small page from a template

Size limits come from the Claude Code memory docs (target under 200 lines per CLAUDE.md;
MEMORY.md loads only its first 200 lines / 25 KB) and are enforced as a ratchet, not a
rewrite: an oversize page is reported, never trimmed by this tool.

No model calls. On CI (no home-dir wiki, no auto-memory) every check that needs the local
tiers reports SKIPPED and exits 0.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import services.runtime_env  # noqa: E402,F401  (first project import: caps BLAS threads)

HOME = Path(os.environ.get("DAIL_WIKI_HOME") or Path.home())
GLOBAL_DIR = HOME / ".claude"
GLOBAL_RULES = GLOBAL_DIR / "rules"
GLOBAL_INDEX = GLOBAL_DIR / "CLAUDE.md"
TEMPLATES = ROOT / "tools" / "wiki_templates"
PROJECT_RULES = ROOT / ".claude" / "rules"
PROJECT_SKILLS = ROOT / ".claude" / "skills"
PROJECT_SETTINGS = ROOT / ".claude" / "settings.json"
PROJECT_TEMPLATES = TEMPLATES / "project"
PROJECT_CARDS = ROOT / "memory"
AUTO_MEMORY = HOME / ".claude" / "projects" / "c--Users-pglyn-PycharmProjects-dail-extractor" / "memory"
VALIDATION_LEDGER = ROOT / "tools" / "evals" / "validation_ledger.json"

# Claude Code memory docs: "target under 200 lines per CLAUDE.md file"; MEMORY.md is read to
# its first 200 lines or 25 KB. Rules and cards get a tighter page limit because "all files
# are small" is the point of a wiki page: one fact, read in one bounded look.
MAX_CLAUDE_MD_LINES = 200
MAX_PAGE_LINES = 120
MAX_INDEX_LINES = 200
MAX_INDEX_BYTES = 25 * 1024
_FRONT = re.compile(r"\A---\r?\n(.*?)\r?\n---", re.S)


@dataclass
class Finding:
    level: str  # "FAIL" (strict exit 1) | "WARN" | "INFO" | "SKIP"
    tier: str  # "global" | "project" | "memory"
    text: str
    fix: str = ""


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)

    def add(self, level: str, tier: str, text: str, fix: str = "") -> None:
        self.findings.append(Finding(level, tier, text, fix))

    @property
    def failed(self) -> bool:
        return any(f.level == "FAIL" for f in self.findings)


def _lines(path: Path) -> int:
    try:
        return sum(1 for _ in path.open(encoding="utf-8", errors="replace"))
    except OSError:
        return 0


def _front_matter(path: Path) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}
    m = _FRONT.match(text)
    if not m:
        return {}
    out: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" in line and not line.startswith((" ", "\t", "-")):
            k, _, v = line.partition(":")
            out[k.strip().lower()] = v.strip()
    return out


def on_ci() -> bool:
    return bool(os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS"))


def _load_tool(name: str) -> ModuleType:
    """Import tools/<name>.py by path, lazily: these tools pull polars and are optional here."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "tools" / f"{name}.py")
    if spec is None or spec.loader is None:
        raise ImportError(f"tools/{name}.py not loadable")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── global tier ───────────────────────────────────────────────────────────────


def global_templates() -> list[Path]:
    return sorted((TEMPLATES / "global").glob("*.md"))


def check_global(report: Report) -> None:
    if on_ci():
        report.add("SKIP", "global", "CI: no home-directory tier to check")
        return
    if not GLOBAL_RULES.is_dir():
        report.add(
            "FAIL",
            "global",
            f"{GLOBAL_RULES} does not exist -- personal cross-project rules have no home",
            "python tools/wiki.py setup-global",
        )
        return
    if not GLOBAL_INDEX.exists():
        report.add(
            "WARN", "global", f"{GLOBAL_INDEX} missing (the one-paragraph index)", "python tools/wiki.py setup-global"
        )
    pages = sorted(GLOBAL_RULES.glob("*.md"))
    missing = [t.name for t in global_templates() if not (GLOBAL_RULES / t.name).exists()]
    for name in missing:
        report.add("WARN", "global", f"template page {name} not installed", "python tools/wiki.py setup-global")
    for page in pages:
        n = _lines(page)
        if n > MAX_PAGE_LINES:
            report.add("FAIL", "global", f"{page.name}: {n} lines (page limit {MAX_PAGE_LINES})", "split the page")
    report.add("INFO", "global", f"{len(pages)} page(s) in {GLOBAL_RULES}")


def setup_global(force: bool = False) -> int:
    """Copy template pages into ~/.claude/rules and write the tiny index. Never overwrites."""
    if not TEMPLATES.is_dir():
        print(f"no templates at {TEMPLATES}")
        return 1
    GLOBAL_RULES.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    kept: list[str] = []
    for src in global_templates():
        dst = GLOBAL_RULES / src.name
        if dst.exists() and not force:
            kept.append(dst.name)
            continue
        shutil.copyfile(src, dst)
        written.append(dst.name)
    index_src = TEMPLATES / "global_CLAUDE.md"
    if index_src.exists() and (force or not GLOBAL_INDEX.exists()):
        shutil.copyfile(index_src, GLOBAL_INDEX)
        written.append(GLOBAL_INDEX.name)
    elif GLOBAL_INDEX.exists():
        kept.append(GLOBAL_INDEX.name)
    print(f"global tier at {GLOBAL_DIR}")
    print(f"  written: {', '.join(written) or 'nothing'}")
    print(f"  kept (already present): {', '.join(kept) or 'nothing'}")
    return 0


# ── project tier templates (the whole .claude/ tree is local by repo convention) ─


def project_templates() -> list[tuple[Path, Path]]:
    """(tracked template, installed copy) pairs for the rule pages and skills."""
    pairs: list[tuple[Path, Path]] = []
    for src in sorted((PROJECT_TEMPLATES / "rules").glob("*.md")):
        pairs.append((src, PROJECT_RULES / src.name))
    for src in sorted((PROJECT_TEMPLATES / "skills").glob("*/SKILL.md")):
        pairs.append((src, PROJECT_SKILLS / src.parent.name / "SKILL.md"))
    return pairs


def _hook_fragment() -> dict:
    path = PROJECT_TEMPLATES / "hooks.json"
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    hooks = data.get("hooks") if isinstance(data, dict) else None
    return hooks if isinstance(hooks, dict) else {}


def _commands(groups: list) -> set[str]:
    out: set[str] = set()
    for group in groups:
        for h in group.get("hooks", []) if isinstance(group, dict) else []:
            if isinstance(h, dict) and h.get("command"):
                out.add(str(h["command"]))
    return out


def merge_hooks(settings: dict, fragment: dict) -> int:
    """Add each fragment hook whose command is absent from the event. Returns how many were added."""
    hooks = settings.setdefault("hooks", {})
    added = 0
    for event, groups in fragment.items():
        existing = hooks.setdefault(event, [])
        present = _commands(existing)
        for group in groups:
            missing = [h for h in group.get("hooks", []) if str(h.get("command")) not in present]
            if not missing:
                continue
            target = next(
                (g for g in existing if isinstance(g, dict) and g.get("matcher") == group.get("matcher")),
                None,
            )
            if target is None:
                target = {k: v for k, v in group.items() if k != "hooks"}
                target["hooks"] = []
                existing.append(target)
            target.setdefault("hooks", []).extend(missing)
            present.update(str(h.get("command")) for h in missing)
            added += len(missing)
    return added


def missing_hooks() -> list[str]:
    """Fragment commands not present in the installed settings (all of them when none exist)."""
    fragment = _hook_fragment()
    wanted = _commands([g for groups in fragment.values() for g in groups])
    try:
        settings = json.loads(PROJECT_SETTINGS.read_text(encoding="utf-8")) if PROJECT_SETTINGS.exists() else {}
    except (OSError, ValueError):
        settings = {}
    have = _commands([g for groups in (settings.get("hooks") or {}).values() for g in groups])
    return sorted(wanted - have)


def setup_project(force: bool = False, hooks: bool = False) -> int:
    """Install the tracked project templates into the local .claude/ tree. Never overwrites pages."""
    written: list[str] = []
    kept: list[str] = []
    for src, dst in project_templates():
        if dst.exists() and not force:
            kept.append(dst.relative_to(ROOT).as_posix())
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        written.append(dst.relative_to(ROOT).as_posix())
    print("project tier (local .claude/ copies of tools/wiki_templates/project/)")
    print(f"  written: {', '.join(written) or 'nothing'}")
    print(f"  kept (already present): {', '.join(kept) or 'nothing'}")
    if hooks:
        fragment = _hook_fragment()
        settings: dict = {}
        if PROJECT_SETTINGS.exists():
            settings = json.loads(PROJECT_SETTINGS.read_text(encoding="utf-8"))
        added = merge_hooks(settings, fragment)
        if added:
            PROJECT_SETTINGS.parent.mkdir(parents=True, exist_ok=True)
            PROJECT_SETTINGS.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8", newline="\n")
        print(f"  hooks merged into {PROJECT_SETTINGS.name}: {added} added")
    elif missing_hooks():
        print(f"  {len(missing_hooks())} hook command(s) not wired; re-run with --hooks")
    return 0


# These are unresolved client variants, not permission for arbitrary future drift.
# Keep them visible until the owner selects their intended contracts.
SHARED_SKILL_EXCEPTIONS = {
    "shape": "Dail-specific v5 brief versus generic v2 interview; preserve pending contract selection",
    "skill-creator": "Client-specific workflow fork; preserve pending replacement/retention decision",
}
KNOWN_SHARED_SKILLS = {
    "civic-ui-review",
    "display-data",
    "impeccable",
    "pipeline-view",
    "streamlit-frontend",
    "tdd",
    "wiki",
    *SHARED_SKILL_EXCEPTIONS,
}


def check_shared_skills(report: Report) -> None:
    other = ROOT / ".agents" / "skills"
    if not PROJECT_SKILLS.is_dir() or not other.is_dir():
        report.add("SKIP", "project", "shared skills: one client tree is absent")
        return
    left = {p.parent.name for p in PROJECT_SKILLS.glob("*/SKILL.md")}
    right = {p.parent.name for p in other.glob("*/SKILL.md")}
    # Check existing pairs plus deletion drift in known pairs. New client-only
    # skills do not acquire a mirror requirement merely by being installed.
    for name in sorted((left & right) | (KNOWN_SHARED_SKILLS & (left | right))):
        src = PROJECT_SKILLS / name / "SKILL.md"
        dst = other / name / "SKILL.md"
        if not src.is_file() or not dst.is_file():
            client = ".claude" if not src.is_file() else ".agents"
            if name in SHARED_SKILL_EXCEPTIONS:
                report.add(
                    "WARN",
                    "project",
                    f"shared skill exception {name}: missing {client} copy; " + SHARED_SKILL_EXCEPTIONS[name],
                )
            else:
                report.add("FAIL", "project", f"shared skill {name}: missing {client} copy")
            continue
        if src.read_bytes() == dst.read_bytes():
            continue
        if name in SHARED_SKILL_EXCEPTIONS:
            report.add("WARN", "project", f"shared skill exception {name}: {SHARED_SKILL_EXCEPTIONS[name]}")
        else:
            report.add(
                "FAIL",
                "project",
                f"shared skill {name}: .agents copy differs from .claude source",
                "Review the difference and restore the shared contract; do not rename client paths blindly",
            )


def check_project_templates(report: Report) -> None:
    if not PROJECT_TEMPLATES.is_dir():
        return
    if on_ci():
        report.add("SKIP", "project", "CI: no local .claude/ tree to compare with the project templates")
        return
    check_shared_skills(report)
    for src, dst in project_templates():
        rel = dst.relative_to(ROOT).as_posix()
        if not dst.exists():
            report.add(
                "WARN", "project", f"{rel} not installed from its template", "python tools/wiki.py setup-project"
            )
        elif src.read_bytes() != dst.read_bytes():
            report.add(
                "WARN",
                "project",
                f"{rel} differs from tools/wiki_templates/project/ (the tracked source of truth)",
                "copy the edit back into the template, or `python tools/wiki.py setup-project --force` to discard it",
            )
    missing = missing_hooks()
    if missing:
        report.add(
            "WARN",
            "project",
            f"{len(missing)} package hook command(s) not wired in .claude/settings.json",
            "python tools/wiki.py setup-project --hooks",
        )


# ── project tier ──────────────────────────────────────────────────────────────


def check_project(report: Report) -> None:
    claude_md = ROOT / "CLAUDE.md"
    n = _lines(claude_md)
    level = "FAIL" if n > MAX_CLAUDE_MD_LINES else "INFO"
    report.add(level, "project", f"CLAUDE.md: {n} lines (docs target under {MAX_CLAUDE_MD_LINES})")

    always: list[tuple[str, int]] = []
    scoped: list[tuple[str, int]] = []
    for rule in sorted(PROJECT_RULES.glob("*.md")):
        fm = _front_matter(rule)
        text = rule.read_text(encoding="utf-8", errors="replace")
        m = _FRONT.match(text)
        paths_block = m.group(1) if m else ""
        globs = (
            re.findall(r'^\s*-\s*"?([^"\n]+)"?\s*$', paths_block.split("paths:", 1)[1], re.M)
            if "paths:" in paths_block
            else []
        )
        words = len(text.split())
        n = _lines(rule)
        if n > MAX_PAGE_LINES and rule.name not in {"communication.md", "evidence.md"}:
            report.add(
                "WARN", "project", f"rules/{rule.name}: {n} lines (page limit {MAX_PAGE_LINES})", "split the page"
            )
        if not globs or any(g.strip() in {"**", "**/*"} for g in globs):
            always.append((rule.name, words))
        else:
            scoped.append((rule.name, words))
        if not fm.get("description"):
            report.add("WARN", "project", f"rules/{rule.name}: no description in front matter")
    always_words = sum(w for _, w in always)
    report.add(
        "INFO",
        "project",
        f"{len(always)} rule page(s) load in every session ({always_words} words): "
        + ", ".join(n for n, _ in always)
        + f"; {len(scoped)} path-scoped",
    )

    cards = sorted(PROJECT_CARDS.glob("*.md"))
    big = [(c.name, _lines(c)) for c in cards if _lines(c) > MAX_PAGE_LINES and c.name != "README.md"]
    for name, n in big:
        report.add("WARN", "project", f"memory/{name}: {n} lines (page limit {MAX_PAGE_LINES})", "split the page")
    report.add("INFO", "project", f"{len(cards)} tracked card(s) in memory/")


# ── upkeep backlog (existing tools, one view) ─────────────────────────────────


def check_memory(report: Report) -> None:
    if on_ci() or not AUTO_MEMORY.is_dir():
        report.add("SKIP", "memory", "no auto-memory store on this machine")
        return
    index = AUTO_MEMORY / "MEMORY.md"
    n = _lines(index)
    size = index.stat().st_size if index.exists() else 0
    level = "FAIL" if (n > MAX_INDEX_LINES or size > MAX_INDEX_BYTES) else "INFO"
    report.add(
        level,
        "memory",
        f"MEMORY.md: {n} lines, {size // 1024} KB (Claude Code loads only the first {MAX_INDEX_LINES} lines / 25 KB)",
        "move entries to MEMORY_COLD.md",
    )
    cards = [p for p in AUTO_MEMORY.glob("*.md") if p.name not in {"MEMORY.md", "MEMORY_COLD.md"}]
    report.add("INFO", "memory", f"{len(cards)} auto-memory card(s)")
    try:
        mod = _load_tool("memory_gc")
        r = mod.scan(60)
        low = [
            name
            for name, c in r["currency"].items()
            if mod._TIER_RANK[c["tier"]] <= mod._TIER_RANK[mod._LABEL_TO_TIER["Extracted"]]
            and "correction_ceiling" not in c["binding"]
        ]
        report.add(
            "WARN" if (r["archive_candidates"] or low) else "INFO",
            "memory",
            f"{len(r['archive_candidates'])} archive candidate(s), {len(low)} card(s) at or below Extracted, "
            f"{len(r['broken_links'])} broken [[link]](s)",
            "python tools/memory_gc.py --min-band Extracted --actionable   (then --archive once reviewed)",
        )
    except Exception as exc:  # the GC tool is optional here; never let it break status
        report.add("WARN", "memory", f"memory_gc unavailable: {type(exc).__name__}: {exc}")


def check_backlogs(report: Report) -> None:
    if on_ci():
        report.add("SKIP", "project", "CI: validation and closeout backlogs are local state")
        return
    try:
        ledger = json.loads(VALIDATION_LEDGER.read_text(encoding="utf-8")) if VALIDATION_LEDGER.exists() else {}
        unvalidated = [p for p, e in ledger.items() if isinstance(e, dict) and e.get("status") == "UNVALIDATED"]
        report.add(
            "WARN" if unvalidated else "INFO",
            "project",
            f"{len(unvalidated)} customization edit(s) UNVALIDATED",
            "python tools/evals/validate_customizations.py --run   (plain terminal; spends API money)",
        )
    except Exception as exc:
        report.add("WARN", "project", f"validation ledger unreadable: {exc}")
    try:
        pend = _load_tool("session_closeout").pending()
        report.add(
            "WARN" if pend else "INFO",
            "project",
            f"{len(pend)} long session(s) awaiting closeout",
            "python tools/session_closeout.py",
        )
    except Exception as exc:
        report.add("WARN", "project", f"closeout ledger unreadable: {type(exc).__name__}: {exc}")
    index_check = ROOT / "tools" / "build_doc_index.py"
    if index_check.exists():
        report.add("INFO", "project", "doc/INDEX.md currency is checked by `dev.py doc-index`")


# ── pages ─────────────────────────────────────────────────────────────────────


def new_page(tier: str, slug: str, title: str, paths: list[str]) -> int:
    slug = re.sub(r"[^a-z0-9_-]+", "-", slug.lower()).strip("-")
    if not slug:
        print("slug must contain a letter or digit")
        return 1
    today = date.today().isoformat()
    if tier == "global":
        dst = GLOBAL_RULES / f"{slug}.md"
        body = f"---\ndescription: {title}\n---\n\n# {title}\n\n<!-- one personal, cross-project rule; keep under {MAX_PAGE_LINES} lines -->\n"
    elif tier == "rule":
        dst = PROJECT_RULES / f"{slug}.md"
        globs = "".join(f'  - "{p}"\n' for p in (paths or ["**"]))
        body = f"---\ndescription: {title}\npaths:\n{globs}---\n\n# {title}\n\n<!-- one project rule; scope `paths:` tightly so it loads only when relevant -->\n"
    elif tier == "card":
        dst = PROJECT_CARDS / f"{slug}.md"
        body = (
            f"# {title}\n\nDate: {today}. Scope: public.\n\n## Lesson\n\n<!-- one concrete lesson with its evidence and date -->\n\n"
            "## Evidence\n\n<!-- file:line, query result, or URL -->\n"
        )
    else:
        print("tier must be global | rule | card")
        return 1
    if dst.exists():
        print(f"exists: {dst}")
        return 1
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(body, encoding="utf-8", newline="\n")
    print(f"created {dst}")
    if tier == "card":
        print("add one trigger-keyed row to tools/discoveries.jsonl so the lesson is recallable")
    return 0


# ── status / upkeep ───────────────────────────────────────────────────────────


def build_report() -> Report:
    report = Report()
    check_global(report)
    check_project_templates(report)
    check_project(report)
    check_memory(report)
    check_backlogs(report)
    return report


def print_report(report: Report, upkeep_only: bool = False) -> None:
    order = {"FAIL": 0, "WARN": 1, "INFO": 2, "SKIP": 3}
    rows = [f for f in report.findings if not upkeep_only or f.fix]
    for f in sorted(rows, key=lambda x: (order[x.level], x.tier)):
        line = f"{f.level:<4} [{f.tier}] {f.text}"
        if f.fix:
            line += f"\n       -> {f.fix}"
        print(line)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd")
    st = sub.add_parser("status", help="both tiers: sizes, missing pages, upkeep backlog")
    st.add_argument("--strict", action="store_true", help="exit 1 on FAIL findings (page size, missing global tier)")
    st.add_argument("--json", action="store_true")
    sg = sub.add_parser("setup-global", help="install ~/.claude/CLAUDE.md and ~/.claude/rules/ pages from templates")
    sg.add_argument("--force", action="store_true", help="overwrite pages that already exist")
    sp = sub.add_parser(
        "setup-project",
        help="install the tracked project templates (rule pages, skills, hook wiring) into the local .claude/ tree",
    )
    sp.add_argument("--force", action="store_true", help="overwrite installed pages that differ from their template")
    sp.add_argument(
        "--hooks", action="store_true", help="also merge tools/wiki_templates/project/hooks.json into settings"
    )
    sub.add_parser("upkeep", help="only the findings that come with a command to run")
    pg = sub.add_parser("page", help="create a new small page")
    pg.add_argument("tier", choices=("global", "rule", "card"))
    pg.add_argument("slug")
    pg.add_argument("--title", required=True)
    pg.add_argument("--paths", nargs="*", default=[], help="rule tier only: glob(s) the rule applies to")
    args = ap.parse_args(argv)

    if args.cmd == "setup-global":
        return setup_global(force=args.force)
    if args.cmd == "setup-project":
        return setup_project(force=args.force, hooks=args.hooks)
    if args.cmd == "page":
        return new_page(args.tier, args.slug, args.title, args.paths)
    report = build_report()
    if args.cmd == "upkeep":
        print_report(report, upkeep_only=True)
        return 0
    if getattr(args, "json", False):
        print(json.dumps([f.__dict__ for f in report.findings], indent=2))
    else:
        print_report(report)
    if getattr(args, "strict", False) and report.failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
