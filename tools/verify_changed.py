#!/usr/bin/env python
"""Run conservative verification selected from the current Git change set.

The verifier deliberately treats selection and execution as separate layers:
``build_checks`` is a pure policy function, while the Git/filesystem helpers only
discover state and persist successful receipts.  This keeps the risk rules easy to
unit-test and makes ``--plan`` useful to both people and coding agents.

Examples::

    python tools/verify_changed.py --plan
    python tools/verify_changed.py
    python tools/verify_changed.py --base upstream/main --no-cache
    python tools/verify_changed.py --full

By default, committed changes are measured from the merge-base with
``origin/main``.  A clone without that ref falls back to ``HEAD``; staged,
unstaged, and untracked files are included either way.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shlex
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
CACHE_ROOT = ROOT / ".cache" / "verify-changed"
POLICY_VERSION = "2026-08-04.1"
FAST_MARKERS = "not integration and not sql and not sources and not bronze and not layers"
NON_SOURCE_PREFIXES = ("logs/", ".cache/verify-changed/")


class GitError(RuntimeError):
    """A Git query needed to construct a trustworthy plan failed."""


@dataclass(frozen=True)
class BaseRef:
    label: str
    tip: str
    merge_base: str
    used_fallback: bool = False


@dataclass(frozen=True)
class ChangeSet:
    committed: tuple[str, ...] = ()
    staged: tuple[str, ...] = ()
    unstaged: tuple[str, ...] = ()
    untracked: tuple[str, ...] = ()

    @property
    def all_files(self) -> tuple[str, ...]:
        return tuple(sorted(set().union(*self.as_dict().values())))

    def as_dict(self) -> dict[str, tuple[str, ...]]:
        return {
            "committed": filter_verification_paths(self.committed),
            "staged": filter_verification_paths(self.staged),
            "unstaged": filter_verification_paths(self.unstaged),
            "untracked": filter_verification_paths(self.untracked),
        }


@dataclass(frozen=True)
class CheckSpec:
    key: str
    argv: tuple[str, ...]
    reason: str
    env: tuple[tuple[str, str], ...] = ()

    def cache_payload(self) -> dict[str, object]:
        return {
            "key": self.key,
            "argv": list(self.argv),
            "reason": self.reason,
            "env": list(self.env),
        }


@dataclass(frozen=True)
class FileState:
    path: str
    exists: bool
    size: int | None
    sha256: str
    mode: int | None = None


@dataclass(frozen=True)
class VerificationPlan:
    base: BaseRef
    head: str
    changes: ChangeSet
    checks: tuple[CheckSpec, ...]
    full: bool = False


@dataclass(frozen=True)
class CheckResult:
    key: str
    returncode: int
    elapsed_seconds: float
    log_path: str


Runner = Callable[..., subprocess.CompletedProcess[str]]


# Areas whose tests mirror their source directory closely enough for focused use.
AREA_TEST_TARGETS: Mapping[str, str] = {
    "api": "test/api",
    "charity": "test/charity",
    "committees": "test/committees",
    "corporate": "test/corporate",
    "debates": "test/debates",
    "extractors": "test/extractors",
    "iris": "test/iris",
    "legislation": "test/legislation",
    "lobbying": "test/lobbying",
    "mcp_server": "test/mcp_server",
    "members": "test/members",
    "payments": "test/payments",
    "pdf_infra": "test/pdf_infra",
    "reference": "test/reference",
    "services": "test/services",
    "shared": "test/shared",
    "utility": "test/utility",
    "votes": "test/votes",
    "wikidata": "test/wikidata",
}

ROOT_SCRIPT_TEST_TARGETS: Mapping[str, str] = {
    "pipeline.py": "test/test_pipeline_chains.py",
    "config.py": "test/test_config_parity.py",
    "manifest.py": "test/pipeline",
    "attendance_refresh.py": "test/pipeline",
    "interests_refresh.py": "test/pipeline",
    "iris_refresh.py": "test/iris",
    "legislation_refresh.py": "test/legislation",
    "lobbying_refresh.py": "test/lobbying",
    "members_refresh.py": "test/members",
    "payments_refresh.py": "test/payments",
    "seanad_refresh.py": "test/seanad",
}

DEPENDENCY_FILES = frozenset({"pyproject.toml", "uv.lock", "requirements.txt"})
MCP_CONFIG_FILES = frozenset({".mcp.json", ".vscode/mcp.json"})
NO_VERIFICATION_PREFIXES = ("logs/", "doc/archive/", "memory/")


def normalize_path(path: str) -> str:
    """Return a stable repo-relative POSIX path without a leading ``./``."""

    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return PurePosixPath(normalized).as_posix()


def is_non_source_state(path: str) -> bool:
    """Whether ``path`` is append-only/derived state, never verification input."""

    return normalize_path(path).startswith(NON_SOURCE_PREFIXES)


def filter_verification_paths(paths: Iterable[str]) -> tuple[str, ...]:
    """Normalize/dedupe paths and remove telemetry that would churn receipts."""

    result: set[str] = set()
    for path in paths:
        if not path:
            continue
        normalized = normalize_path(path)
        if not is_non_source_state(normalized):
            result.add(normalized)
    return tuple(sorted(result))


def _dedupe_checks(checks: Iterable[CheckSpec]) -> tuple[CheckSpec, ...]:
    seen: set[str] = set()
    result: list[CheckSpec] = []
    for check in checks:
        if check.key not in seen:
            result.append(check)
            seen.add(check.key)
    return tuple(result)


def _python_check(key: str, python: str, script: str, reason: str, *args: str) -> CheckSpec:
    return CheckSpec(key, (python, script, *args), reason)


def _pytest_check(key: str, python: str, targets: Sequence[str], reason: str) -> CheckSpec:
    return CheckSpec(
        key,
        (python, "-m", "pytest", "-q", "-m", FAST_MARKERS, *targets),
        reason,
    )


def _is_doc_only_path(path: str) -> bool:
    p = PurePosixPath(path)
    if path in {"AGENTS.md", "CLAUDE.md", "README.MD", "README.md", "CONTRIBUTING.md", ".rgignore"}:
        return True
    if path.startswith(".github/prompts/"):
        return True
    return p.suffix.lower() in {".md", ".rst"}


def _is_portable_context_path(path: str) -> bool:
    p = PurePosixPath(path)
    return (
        p.name in {"AGENTS.md", "CLAUDE.md"}
        or path.startswith(".github/prompts/")
        or path.startswith(".codex/agents/")
        or path.startswith("dail_tracker_bold_ui_contract_pack_v5/prompts/")
        or path
        in {
            ".codex/config.toml",
            ".gitignore",
            ".mcp.json",
            ".vscode/mcp.json",
            "CONTRIBUTING.md",
            "dail_tracker_bold_ui_contract_pack_v5/tools/check_prompt_context_budget.py",
            "tools/check_agent_context.py",
        }
    )


def _is_doc_index_input(path: str) -> bool:
    p = PurePosixPath(path)
    return path == "tools/build_doc_index.py" or (
        len(p.parts) == 2 and p.parts[0] == "doc" and p.suffix.lower() == ".md"
    )


def _is_firewall_related(path: str) -> bool:
    return path == "tools/check_streamlit_logic_firewall.py" or (
        path.endswith(".py") and path.startswith(("utility/pages_code/", "utility/data_access/", "utility/ui/"))
    )


def _is_convention_related(path: str) -> bool:
    return path == "tools/check_conventions.py" or (
        path.endswith(".py") and path.startswith(("extractors/", "planning/civic/extractors/", "utility/pages_code/"))
    )


def _is_mcp_related(path: str) -> bool:
    return (
        path in MCP_CONFIG_FILES
        or path in {"tools/check_mcp_catalog.py", "test/tools/test_mcp_catalog.py"}
        or (path.endswith(".py") and path.startswith("mcp_server/"))
    )


def _is_sql_related(path: str) -> bool:
    return (
        (path.endswith(".sql") and path.startswith("sql_views/"))
        or (path.endswith(".py") and path.startswith("test/sql_views/"))
        or path.startswith("data/gold/")
        or path
        in {
            "dail_tracker_core/db.py",
            "data/_meta/output_baseline.json",
            "data/_meta/gold_quality_baseline.json",
        }
    )


def _is_typecheck_related(path: str) -> bool:
    return (
        path == "pyproject.toml"
        or path
        in {
            "config.py",
            "manifest.py",
            "shared/normalise_join_key.py",
            "votes/enrich.py",
            "votes/transform_votes.py",
            "legislation/questions.py",
        }
        or (path.endswith(".py") and path.startswith(("services/", "dail_tracker_core/", "api/")))
    )


def _focused_test_target(path: str, *, exists: bool) -> str | None:
    """Return a reliable focused pytest target, or ``None`` to force fallback."""

    p = PurePosixPath(path)
    if path in {
        "tools/check_agent_context.py",
        "dail_tracker_bold_ui_contract_pack_v5/tools/check_prompt_context_budget.py",
    }:
        return "test/tools/test_agent_context.py"
    if path.startswith("test/"):
        return path if exists and p.suffix == ".py" else None
    if path.startswith("planning/civic/extractors/"):
        return "test/planning"
    if path.startswith("tools/"):
        if path == "tools/verify_changed.py":
            return "test/tools/test_verify_changed.py"
        return "test/tools"
    if len(p.parts) == 1:
        return ROOT_SCRIPT_TEST_TARGETS.get(path)
    return AREA_TEST_TARGETS.get(p.parts[0])


def build_checks(
    changed_paths: Iterable[str],
    *,
    existing_paths: Iterable[str] | None = None,
    python_executable: str = sys.executable,
    full: bool = False,
) -> tuple[CheckSpec, ...]:
    """Purely select checks from paths.

    ``existing_paths`` distinguishes deleted files from files Ruff/pytest can
    still accept as positional arguments. Unknown executable/config changes fail
    safe by selecting the complete fast pytest lane.
    """

    paths = filter_verification_paths(changed_paths)
    existing = set(paths if existing_paths is None else (normalize_path(path) for path in existing_paths))
    py = python_executable

    if full:
        return (
            CheckSpec("ruff-check", (py, "-m", "ruff", "check", "."), "full verification"),
            CheckSpec("ruff-format", (py, "-m", "ruff", "format", "--check", "."), "full verification"),
            _python_check("firewall", py, "tools/check_streamlit_logic_firewall.py", "full verification"),
            _python_check("conventions", py, "tools/check_conventions.py", "full verification"),
            _python_check("dependency-declarations", py, "tools/check_dependency_declarations.py", "full verification"),
            _python_check("dependency-state", py, "tools/check_dependency_state.py", "full verification"),
            _python_check("doc-index", py, "tools/build_doc_index.py", "full verification", "--check"),
            _python_check("mcp-catalog", py, "tools/check_mcp_catalog.py", "full verification"),
            _python_check("agent-context", py, "tools/check_agent_context.py", "full verification"),
            CheckSpec("typecheck", (py, "-m", "basedpyright"), "full verification"),
            _python_check("expected-failures", py, "tools/check_expected_failures.py", "full verification"),
            _pytest_check("pytest-fast", py, (), "full fast test lane"),
            CheckSpec(
                "pytest-sql",
                (py, "-m", "pytest", "-q", "-m", "sql"),
                "full SQL contract lane",
                (("DAIL_INTEGRATION_TESTS", "1"),),
            ),
        )

    if not paths:
        return ()

    checks: list[CheckSpec] = []
    dependency_change = bool(DEPENDENCY_FILES.intersection(paths))
    python_paths = [path for path in paths if PurePosixPath(path).suffix == ".py"]
    lint_paths = [path for path in python_paths if path in existing]
    global_lint = dependency_change or len(lint_paths) > 80 or sum(map(len, lint_paths)) > 6_000

    if lint_paths or global_lint:
        lint_targets = (".",) if global_lint else tuple(lint_paths)
        checks.extend(
            (
                CheckSpec(
                    "ruff-check",
                    (py, "-m", "ruff", "check", *lint_targets),
                    "changed Python or lint/dependency configuration",
                ),
                CheckSpec(
                    "ruff-format",
                    (py, "-m", "ruff", "format", "--check", *lint_targets),
                    "changed Python or formatter configuration",
                ),
            )
        )

    if any(_is_firewall_related(path) for path in paths):
        firewall_targets = tuple(
            path
            for path in paths
            if path in existing
            and path.endswith(".py")
            and path.startswith(("utility/pages_code/", "utility/data_access/", "utility/ui/"))
        )
        checks.append(
            _python_check(
                "firewall",
                py,
                "tools/check_streamlit_logic_firewall.py",
                "UI/data-access boundary changed",
                *firewall_targets,
            )
        )

    if any(_is_convention_related(path) for path in paths):
        checks.append(_python_check("conventions", py, "tools/check_conventions.py", "ratcheted area changed"))

    if python_paths or dependency_change:
        checks.append(
            _python_check(
                "dependency-declarations",
                py,
                "tools/check_dependency_declarations.py",
                "Python imports or dependency declarations changed",
            )
        )

    if dependency_change:
        checks.append(
            _python_check(
                "dependency-state",
                py,
                "tools/check_dependency_state.py",
                "dependency intent, lock, or runtime export changed",
            )
        )

    if any(_is_doc_index_input(path) for path in paths):
        checks.append(
            _python_check("doc-index", py, "tools/build_doc_index.py", "root documentation changed", "--check")
        )

    if any(_is_portable_context_path(path) for path in paths):
        checks.append(
            _pytest_check(
                "pytest-agent-context",
                py,
                ("test/tools/test_agent_context.py",),
                "portable agent guidance or prompt routing changed",
            )
        )

    if any(_is_mcp_related(path) for path in paths):
        checks.append(_python_check("mcp-catalog", py, "tools/check_mcp_catalog.py", "MCP surface changed"))

    if any(_is_typecheck_related(path) for path in paths):
        checks.append(CheckSpec("typecheck", (py, "-m", "basedpyright"), "typed surface changed"))

    if dependency_change or any(path in {"test/conftest.py", "test/expected_failures.yaml"} for path in paths):
        checks.append(
            _python_check(
                "expected-failures",
                py,
                "tools/check_expected_failures.py",
                "test/dependency policy changed",
            )
        )

    if any(_is_sql_related(path) for path in paths):
        checks.append(
            CheckSpec(
                "pytest-sql",
                (py, "-m", "pytest", "-q", "-m", "sql"),
                "SQL view, registry, contract test, or committed gold changed",
                (("DAIL_INTEGRATION_TESTS", "1"),),
            )
        )

    focused_targets: set[str] = set()
    needs_fast = dependency_change
    for path in python_paths:
        target = _focused_test_target(path, exists=path in existing)
        if target is None:
            needs_fast = True
        else:
            focused_targets.add(target)

    # Non-document/config/data changes without a specific gate also fail safe.
    specifically_handled = {
        path
        for path in paths
        if path.endswith(".py")
        or path in DEPENDENCY_FILES
        or _is_doc_only_path(path)
        or _is_portable_context_path(path)
        or _is_doc_index_input(path)
        or _is_mcp_related(path)
        or _is_sql_related(path)
        or path.startswith(NO_VERIFICATION_PREFIXES)
    }
    if set(paths) - specifically_handled:
        needs_fast = True

    if "test/conftest.py" in paths or len(focused_targets) > 10:
        needs_fast = True

    if needs_fast:
        checks.append(_pytest_check("pytest-fast", py, (), "broad or unmapped change; fail-safe fast lane"))
    elif focused_targets:
        checks.append(
            _pytest_check(
                "pytest-focused",
                py,
                tuple(sorted(focused_targets)),
                "tests mirrored to changed source areas",
            )
        )

    return _dedupe_checks(checks)


def _run_git(repo: Path, args: Sequence[str], *, check: bool = True) -> bytes:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise GitError(f"git {' '.join(args)} failed: {message or f'exit {result.returncode}'}")
    return result.stdout


def _git_text(repo: Path, args: Sequence[str], *, check: bool = True) -> str:
    return _run_git(repo, args, check=check).decode("utf-8", errors="replace").strip()


def _git_ref(repo: Path, ref: str) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "--verify", f"{ref}^{{commit}}"],
        cwd=repo,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def resolve_base(repo: Path, requested: str | None = None) -> BaseRef:
    """Resolve an explicit base, or ``origin/main`` with a ``HEAD`` fallback."""

    head = _git_ref(repo, "HEAD")
    if head is None:
        raise GitError("HEAD does not resolve to a commit")

    label = requested or "origin/main"
    tip = _git_ref(repo, label)
    if tip is None:
        if requested is not None:
            raise GitError(f"base ref {requested!r} does not resolve to a commit")
        return BaseRef("HEAD", head, head, used_fallback=True)

    merge = _git_text(repo, ["merge-base", "HEAD", tip], check=False)
    if not merge:
        if requested is not None:
            raise GitError(f"base ref {requested!r} has no merge-base with HEAD")
        return BaseRef("HEAD", head, head, used_fallback=True)
    return BaseRef(label, tip, merge)


def _nul_paths(raw: bytes) -> tuple[str, ...]:
    return filter_verification_paths(
        item.decode("utf-8", errors="surrogateescape") for item in raw.split(b"\0") if item
    )


def discover_changes(repo: Path, base_commit: str) -> ChangeSet:
    """Collect committed-since-base, staged, unstaged, and untracked paths."""

    common = ["--name-only", "-z", "--no-renames", "--diff-filter=ACDMRTUXB"]
    return ChangeSet(
        committed=_nul_paths(_run_git(repo, ["diff", *common, f"{base_commit}..HEAD", "--"])),
        staged=_nul_paths(_run_git(repo, ["diff", "--cached", *common, "--"])),
        unstaged=_nul_paths(_run_git(repo, ["diff", *common, "--"])),
        untracked=_nul_paths(_run_git(repo, ["ls-files", "--others", "--exclude-standard", "-z"])),
    )


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_file_states(repo: Path, paths: Iterable[str]) -> tuple[FileState, ...]:
    states: list[FileState] = []
    for relative in filter_verification_paths(paths):
        path = repo.joinpath(*PurePosixPath(relative).parts)
        if path.is_symlink():
            target = os.readlink(path)
            states.append(
                FileState(
                    relative, True, len(target.encode("utf-8")), hashlib.sha256(target.encode()).hexdigest(), None
                )
            )
        elif path.is_file():
            stat = path.stat()
            states.append(FileState(relative, True, stat.st_size, _hash_file(path), stat.st_mode & 0o777))
        else:
            states.append(FileState(relative, False, None, "<missing>", None))
    return tuple(states)


def compute_fingerprint(
    *,
    head: str,
    base_commit: str,
    changes: ChangeSet,
    file_states: Sequence[FileState],
    checks: Sequence[CheckSpec],
    interpreter: str,
    python_version: str,
    platform_name: str,
    policy_identity: str,
    full: bool,
) -> str:
    """Return a stable digest for code state, selected proof, and runtime."""

    payload = {
        "head": head,
        "base_commit": base_commit,
        "changes": {key: list(value) for key, value in changes.as_dict().items()},
        "files": [
            asdict(state)
            for state in sorted(file_states, key=lambda item: item.path)
            if not is_non_source_state(state.path)
        ],
        "checks": [check.cache_payload() for check in checks],
        "interpreter": interpreter,
        "python_version": python_version,
        "platform": platform_name,
        "policy": policy_identity,
        "full": full,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def current_policy_identity(script: Path = Path(__file__)) -> str:
    return f"{POLICY_VERSION}:{_hash_file(script)}"


def receipt_matches(receipt: Mapping[str, object], fingerprint: str, checks: Sequence[CheckSpec]) -> bool:
    return (
        receipt.get("status") == "success"
        and receipt.get("fingerprint") == fingerprint
        and receipt.get("policy_version") == POLICY_VERSION
        and receipt.get("checks") == [check.cache_payload() for check in checks]
    )


def load_receipt(path: Path) -> dict[str, object] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def write_json_atomic(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            temporary = Path(handle.name)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def compact_output(text: str, *, max_lines: int = 18, max_chars: int = 3_000) -> str:
    """Keep a diagnostic head/tail while the complete result remains in a log."""

    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    if len(lines) > max_lines:
        head_count = max_lines // 2
        tail_count = max_lines - head_count
        omitted = len(lines) - max_lines
        lines = [*lines[:head_count], f"... {omitted} line(s) omitted ...", *lines[-tail_count:]]
    rendered = "\n".join(lines)
    if len(rendered) > max_chars:
        half = max_chars // 2
        rendered = f"{rendered[:half]}\n... output truncated ...\n{rendered[-half:]}"
    return rendered


def _configure_console_output() -> None:
    """Keep captured UTF-8 diagnostics safe on legacy Windows consoles."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        with suppress(OSError, TypeError, ValueError):
            reconfigure(errors="backslashreplace")


def format_command(argv: Sequence[str]) -> str:
    return subprocess.list2cmdline(list(argv)) if os.name == "nt" else shlex.join(argv)


def _raw_log(check: CheckSpec, result: subprocess.CompletedProcess[str], elapsed: float) -> str:
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    return (
        f"$ {format_command(check.argv)}\n"
        f"exit={result.returncode} elapsed_seconds={elapsed:.3f}\n"
        f"\n[stdout]\n{stdout}"
        f"\n[stderr]\n{stderr}"
    )


def execute_checks(
    *,
    repo: Path,
    checks: Sequence[CheckSpec],
    fingerprint: str,
    cache_root: Path,
    receipt_path: Path,
    runner: Runner = subprocess.run,
) -> tuple[bool, tuple[CheckResult, ...]]:
    """Run checks, write raw logs, and atomically receipt only total success."""

    # An explicit rerun supersedes any older success for this exact fingerprint.
    receipt_path.unlink(missing_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    log_dir = cache_root / "logs" / f"{stamp}-{os.getpid()}-{fingerprint[:12]}"
    log_dir.mkdir(parents=True, exist_ok=True)
    results: list[CheckResult] = []

    for index, check in enumerate(checks, start=1):
        print(f"[{index}/{len(checks)}] {check.key}: {format_command(check.argv)}")
        env = os.environ.copy()
        env.update(dict(check.env))
        started = time.monotonic()
        try:
            completed = runner(
                list(check.argv),
                cwd=repo,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
        except OSError as exc:
            completed = subprocess.CompletedProcess(list(check.argv), 127, "", f"{type(exc).__name__}: {exc}\n")
        elapsed = time.monotonic() - started
        log_path = log_dir / f"{index:02d}-{check.key}.log"
        log_path.write_text(_raw_log(check, completed, elapsed), encoding="utf-8")
        relative_log = log_path.relative_to(repo).as_posix() if log_path.is_relative_to(repo) else str(log_path)
        results.append(CheckResult(check.key, completed.returncode, elapsed, relative_log))

        combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
        if completed.returncode != 0:
            excerpt = compact_output(combined)
            print(f"FAIL {check.key} ({elapsed:.1f}s); raw log: {relative_log}")
            if excerpt:
                print(excerpt)
            receipt_path.unlink(missing_ok=True)
            return False, tuple(results)

        summary = compact_output(combined, max_lines=2, max_chars=500)
        suffix = f" - {summary}" if summary else ""
        print(f"PASS {check.key} ({elapsed:.1f}s){suffix}")

    payload: dict[str, object] = {
        "status": "success",
        "fingerprint": fingerprint,
        "policy_version": POLICY_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "checks": [check.cache_payload() for check in checks],
        "results": [asdict(result) for result in results],
    }
    write_json_atomic(receipt_path, payload)
    return True, tuple(results)


def build_plan(repo: Path, *, requested_base: str | None = None, full: bool = False) -> VerificationPlan:
    base = resolve_base(repo, requested_base)
    head = _git_text(repo, ["rev-parse", "HEAD"])
    changes = discover_changes(repo, base.merge_base)
    states = collect_file_states(repo, changes.all_files)
    existing = [state.path for state in states if state.exists]
    checks = build_checks(changes.all_files, existing_paths=existing, full=full)
    return VerificationPlan(base, head, changes, checks, full)


def plan_fingerprint(plan: VerificationPlan, repo: Path = ROOT) -> str:
    states = collect_file_states(repo, plan.changes.all_files)
    return compute_fingerprint(
        head=plan.head,
        base_commit=plan.base.merge_base,
        changes=plan.changes,
        file_states=states,
        checks=plan.checks,
        interpreter=str(Path(sys.executable).resolve()),
        python_version=sys.version,
        platform_name=platform.platform(),
        policy_identity=current_policy_identity(),
        full=plan.full,
    )


def render_plan(plan: VerificationPlan, fingerprint: str, *, cached: bool) -> str:
    lines = [
        f"base: {plan.base.label} ({plan.base.merge_base[:12]})"
        + (" [origin/main unavailable; HEAD fallback]" if plan.base.used_fallback else ""),
        f"head: {plan.head[:12]}",
        (
            f"changes: {len(plan.changes.all_files)} "
            f"(committed={len(plan.changes.committed)}, staged={len(plan.changes.staged)}, "
            f"unstaged={len(plan.changes.unstaged)}, untracked={len(plan.changes.untracked)})"
        ),
    ]
    for category, paths in plan.changes.as_dict().items():
        if paths:
            shown = paths[:20]
            suffix = f", ... +{len(paths) - len(shown)} more" if len(paths) > len(shown) else ""
            lines.append(f"  {category}: {', '.join(shown)}{suffix}")
    lines.append(f"fingerprint: {fingerprint}")
    lines.append(f"cache: {'hit' if cached else 'miss'}")
    if not plan.checks:
        lines.append("checks: none")
    else:
        lines.append(f"checks ({len(plan.checks)}):")
        for check in plan.checks:
            lines.append(f"  - {check.key}: {format_command(check.argv)}")
            lines.append(f"    why: {check.reason}")
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="print selected changes/checks without running them")
    parser.add_argument("--base", help="Git ref to compare from (default: origin/main; fallback: HEAD)")
    parser.add_argument("--no-cache", action="store_true", help="ignore an existing success receipt and rerun")
    parser.add_argument("--full", action="store_true", help="run every deterministic local verification lane")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    _configure_console_output()
    args = parse_args(argv)
    try:
        plan = build_plan(ROOT, requested_base=args.base, full=args.full)
        fingerprint = plan_fingerprint(plan, ROOT)
    except (GitError, OSError) as exc:
        print(f"verify-changed: {exc}", file=sys.stderr)
        return 2

    receipt_path = CACHE_ROOT / "receipts" / f"{fingerprint}.json"
    receipt = load_receipt(receipt_path)
    cached = bool(not args.no_cache and receipt and receipt_matches(receipt, fingerprint, plan.checks))
    if args.plan:
        print(render_plan(plan, fingerprint, cached=cached))
        return 0

    if not plan.checks:
        print(render_plan(plan, fingerprint, cached=False))
        print("No verification-relevant changes selected.")
        return 0

    if cached and not args.no_cache:
        print(render_plan(plan, fingerprint, cached=True))
        print(f"PASS cached successful verification ({receipt_path.relative_to(ROOT).as_posix()})")
        return 0

    print(render_plan(plan, fingerprint, cached=False))
    success, _ = execute_checks(
        repo=ROOT,
        checks=plan.checks,
        fingerprint=fingerprint,
        cache_root=CACHE_ROOT,
        receipt_path=receipt_path,
    )
    if success:
        print(f"PASS all {len(plan.checks)} checks; receipt: {receipt_path.relative_to(ROOT).as_posix()}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
