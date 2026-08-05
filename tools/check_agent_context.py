#!/usr/bin/env python
"""Ratchet reusable agent prompts without turning prose style into policy.

The checks are intentionally narrow: keep prompts small, keep shared prompts
provider-neutral, and require review prompts to return evidence-bearing verdicts
instead of subjective numeric scores.
"""

from __future__ import annotations

import argparse
import json
import re
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROMPT_ROOTS = (
    ROOT / ".github" / "prompts",
    ROOT / "dail_tracker_bold_ui_contract_pack_v5" / "prompts",
)
CODEX_ROLE_ROOT = ROOT / ".codex" / "agents"
ROLE_ROOTS = (ROOT / ".claude" / "agents", CODEX_ROLE_ROOT)
MAX_PROMPT_WORDS = 600
TASK_PACKET_HEADINGS = (
    "## Objective",
    "## Scope",
    "## Invariants",
    "## Acceptance",
    "## Result contract",
)
REQUIRED_CODEX_ROLE_FIELDS = ("name", "description", "developer_instructions")
NUMERIC_SCORE_RE = re.compile(r"\bscore\s+\d+\s*[-–]\s*\d+", re.IGNORECASE)


@dataclass(frozen=True)
class PromptRecord:
    path: str
    words: int
    kind: str


def prompt_files() -> list[Path]:
    prompts = [path for root in PROMPT_ROOTS for path in root.glob("*.md")]
    roles = [path for root in ROLE_ROOTS for pattern in ("*.md", "*.toml") for path in root.glob(pattern)]
    return sorted([*prompts, *roles])


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _kind(path: Path) -> str:
    name = path.name.lower()
    if "review" in name or "critique" in name:
        return "review"
    if "build" in name or "apply" in name or "wire" in name:
        return "build"
    return "task"


def check_prompt(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    rel = _relative(path)
    errors: list[str] = []
    words = len(text.split())
    if words > MAX_PROMPT_WORDS:
        errors.append(f"{rel}: {words} words exceeds the {MAX_PROMPT_WORDS}-word prompt budget")
    is_shared_prompt = any(path.resolve().parent == root for root in PROMPT_ROOTS)
    if is_shared_prompt and "CLAUDE.md" in text:
        errors.append(f"{rel}: shared prompts must route through canonical AGENTS.md guidance")

    if path.resolve().parent == ROOT / ".github" / "prompts":
        missing = [heading for heading in TASK_PACKET_HEADINGS if heading not in text]
        if missing:
            errors.append(f"{rel}: missing task-packet headings: {', '.join(missing)}")

    if path.resolve().parent == CODEX_ROLE_ROOT and path.suffix == ".toml":
        try:
            role = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            errors.append(f"{rel}: invalid Codex agent TOML: {exc}")
        else:
            missing = [
                field
                for field in REQUIRED_CODEX_ROLE_FIELDS
                if not isinstance(role.get(field), str) or not role[field].strip()
            ]
            if missing:
                errors.append(f"{rel}: Codex agent is missing required fields: {', '.join(missing)}")
            elif role["name"] != path.stem:
                errors.append(f"{rel}: Codex agent name must match its filename stem ({path.stem})")

    if _kind(path) == "review":
        if NUMERIC_SCORE_RE.search(text):
            errors.append(f"{rel}: numeric review scores are not evidence-bearing acceptance checks")
        # .claude/ is a workstation-local compatibility surface ignored by Git.
        # Inventory and budget it, but ratchet durable shared/Codex prompts only.
        if path.resolve().parent != ROOT / ".claude" / "agents":
            missing = [term for term in ("Verdict", "Severity", "Evidence") if term.lower() not in text.lower()]
            if missing:
                errors.append(f"{rel}: review result contract is missing: {', '.join(missing)}")
    return errors


def check_repository(paths: list[Path] | None = None) -> list[str]:
    selected = prompt_files() if paths is None else paths
    errors: list[str] = []
    for path in selected:
        if not path.is_file():
            errors.append(f"{_relative(path)}: prompt file does not exist")
            continue
        errors.extend(check_prompt(path))
    return errors


def catalog(paths: list[Path] | None = None) -> list[PromptRecord]:
    selected = prompt_files() if paths is None else paths
    return [
        PromptRecord(path=_relative(path), words=len(path.read_text(encoding="utf-8").split()), kind=_kind(path))
        for path in selected
        if path.is_file()
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--catalog", action="store_true", help="emit the discovered prompt catalog as JSON")
    args = parser.parse_args(argv)
    paths = args.paths or None
    errors = check_repository(paths)
    if args.catalog:
        print(json.dumps([asdict(row) for row in catalog(paths)], indent=2))
    for error in errors:
        print(f"ERROR {error}")
    if not errors and not args.catalog:
        print(f"PASS agent-context: {len(catalog(paths))} reusable prompts")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
