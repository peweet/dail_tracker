#!/usr/bin/env python
"""Create and use named, non-competing development environments.

The repository has multiple valid dependency profiles.  Reusing ``.venv`` for
all of them makes an exact ``uv sync`` remove packages that belong only to the
previous profile.  This wrapper gives each profile its own project environment
and keeps environment mutation behind the explicit ``sync`` command.

Examples (Windows)::

    py -3.12 tools/dev_env.py sync public
    py -3.12 tools/dev_env.py run public python tools/dev.py verify
    py -3.12 tools/dev_env.py sync siting
    py -3.12 tools/dev_env.py doctor siting

``check``, ``doctor``, and ``run`` never repair an environment.  ``run`` first
checks the selected profile and refuses to execute when it is stale.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_ROOT = ROOT / ".uv-envs"
ENV_ROOT_VAR = "DAIL_ENV_ROOT"
PROFILE_VAR = "DAIL_ENV_PROFILE"
UV_EXECUTABLE_VAR = "UV_EXECUTABLE"


@dataclass(frozen=True)
class Profile:
    extras: tuple[str, ...]
    groups: tuple[str, ...] = ("dev",)


PROFILES: dict[str, Profile] = {
    "public": Profile(("pipeline", "api", "mcp")),
    "siting": Profile(("pipeline", "api", "mcp", "siting")),
    # Keep the model edge separate: deterministic planning work must not acquire an SDK,
    # tokenizer or API-key-shaped runtime just by selecting the standard Siting profile.
    "siting-ai": Profile(("pipeline", "api", "mcp", "siting", "siting-ai")),
}


@dataclass(frozen=True)
class UvFailure:
    kind: str
    summary: str


def environment_root(override: str | Path | None = None) -> Path:
    selected = override or os.environ.get(ENV_ROOT_VAR) or DEFAULT_ENV_ROOT
    return Path(selected).expanduser().resolve()


def environment_path(profile: str, override: str | Path | None = None) -> Path:
    if profile not in PROFILES:
        raise KeyError(profile)
    return environment_root(override) / profile


def environment_bin(path: Path) -> Path:
    return path / ("Scripts" if os.name == "nt" else "bin")


def environment_python(path: Path) -> Path:
    return environment_bin(path) / ("python.exe" if os.name == "nt" else "python")


def python_request() -> str:
    """Return the exact 64-bit Python 3.12 interpreter running this wrapper."""

    bits = struct.calcsize("P") * 8
    if sys.version_info[:2] != (3, 12) or bits != 64:
        raise RuntimeError(
            f"bootstrap must be 64-bit Python 3.12; got {sys.version_info.major}.{sys.version_info.minor} ({bits}-bit) "
            f"at {sys.executable}"
        )
    return sys.executable


def uv_profile_args(profile: str) -> tuple[str, ...]:
    spec = PROFILES[profile]
    args: list[str] = ["--locked", "--python", python_request()]
    for group in spec.groups:
        args.extend(("--group", group))
    for extra in spec.extras:
        args.extend(("--extra", extra))
    return tuple(args)


def profile_environment(profile: str, override: str | Path | None = None) -> dict[str, str]:
    path = environment_path(profile, override)
    env = dict(os.environ)
    env["UV_PROJECT_ENVIRONMENT"] = str(path)
    env[PROFILE_VAR] = profile
    env["VIRTUAL_ENV"] = str(path)
    env["PATH"] = str(environment_bin(path)) + os.pathsep + env.get("PATH", "")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    return env


def classify_uv_failure(output: str) -> UvFailure:
    lowered = output.lower()
    if "uv executable was not found" in lowered or "uv is not on path" in lowered:
        return UvFailure(
            "uv_unavailable",
            "uv was not found on PATH, via UV_EXECUTABLE, or in the per-user .local/bin location; "
            "dependency state was not evaluated",
        )
    if "failed to initialize cache" in lowered or "failed to open file" in lowered and "cache" in lowered:
        return UvFailure("cache_unavailable", "uv cache is inaccessible; dependency state was not evaluated")
    if "access is denied" in lowered or "permission denied" in lowered:
        return UvFailure("permission_denied", "uv could not access a required path; dependency state was not evaluated")
    if "environment is outdated" in lowered or "would install" in lowered or "would uninstall" in lowered:
        return UvFailure("environment_outdated", "the selected environment does not match its locked profile")
    if "lockfile" in lowered and ("needs to be updated" in lowered or "out of date" in lowered):
        return UvFailure("lock_outdated", "uv.lock does not match pyproject.toml")
    return UvFailure("uv_failed", "uv failed; dependency state was not established")


def uv_executable() -> str | None:
    """Resolve uv without requiring a user-local installation to be on PATH."""

    configured = os.environ.get(UV_EXECUTABLE_VAR, "").strip()
    if configured:
        configured_path = Path(configured).expanduser()
        if configured_path.is_file():
            return str(configured_path.resolve())
        discovered = shutil.which(configured)
        if discovered:
            return str(Path(discovered).resolve())

    discovered = shutil.which("uv")
    if discovered:
        return str(Path(discovered).resolve())

    executable = "uv.exe" if os.name == "nt" else "uv"
    homes = [os.environ.get("USERPROFILE"), str(Path.home())]
    for home in dict.fromkeys(value for value in homes if value):
        candidate = Path(home) / ".local" / "bin" / executable
        if candidate.is_file():
            return str(candidate.resolve())
    return None


def _run_uv(
    action: str,
    profile: str,
    *,
    env_root: str | Path | None = None,
    no_cache: bool = False,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    uv = uv_executable()
    if not uv:
        return subprocess.CompletedProcess(
            ("uv",),
            127,
            "",
            "uv executable was not found on PATH, via UV_EXECUTABLE, or in the per-user .local/bin location",
        )
    command = [uv, "sync", *uv_profile_args(profile)]
    if action == "check":
        command.append("--check")
    elif action != "sync":
        raise ValueError(action)
    if no_cache:
        command.append("--no-cache")
    return subprocess.run(
        command,
        cwd=ROOT,
        env=profile_environment(profile, env_root),
        capture_output=capture,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def check_profile(
    profile: str,
    *,
    env_root: str | Path | None = None,
    no_cache: bool = False,
    quiet: bool = False,
) -> int:
    result = _run_uv("check", profile, env_root=env_root, no_cache=no_cache, capture=True)
    output = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part and part.strip())
    if result.returncode:
        failure = classify_uv_failure(output)
        if not quiet:
            if output:
                print(output)
            print(f"FAIL [{failure.kind}] {failure.summary}")
        return result.returncode
    if not quiet:
        print(f"OK profile={profile} environment={environment_path(profile, env_root)}")
    return 0


def sync_profile(profile: str, *, env_root: str | Path | None = None, no_cache: bool = False) -> int:
    path = environment_path(profile, env_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    result = _run_uv("sync", profile, env_root=env_root, no_cache=no_cache, capture=True)
    output = "\n".join(part.strip() for part in (result.stdout, result.stderr) if part and part.strip())
    if output:
        print(output)
    if result.returncode == 0:
        print(f"READY profile={profile} environment={path}")
    else:
        failure = classify_uv_failure(output)
        print(f"FAIL [{failure.kind}] {failure.summary}")
    return result.returncode


def _environment_probe(python: Path) -> tuple[int, dict[str, object] | None, str]:
    if not python.is_file():
        return 1, None, f"environment interpreter is missing: {python}"
    script = (
        "import json,platform,struct,sys;"
        "print(json.dumps({'executable':sys.executable,'version':platform.python_version(),"
        "'bits':struct.calcsize('P')*8}))"
    )
    result = subprocess.run(
        [str(python), "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode:
        return result.returncode, None, (result.stderr or result.stdout).strip()
    try:
        return 0, json.loads(result.stdout), ""
    except json.JSONDecodeError:
        return 1, None, f"invalid interpreter probe output: {result.stdout!r}"


def doctor_profile(profile: str, *, env_root: str | Path | None = None, no_cache: bool = False) -> int:
    path = environment_path(profile, env_root)
    python = environment_python(path)
    print(
        f"host_python={sys.executable} host_version={sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro} "
        f"host_bits={struct.calcsize('P') * 8}"
    )
    print(f"profile={profile} environment={path}")

    failures = 0
    probe_rc, probe, probe_error = _environment_probe(python)
    if probe_rc:
        print(f"FAIL [environment_missing] {probe_error}")
        failures += 1
    else:
        assert probe is not None
        print(
            f"environment_python={probe['executable']} environment_version={probe['version']} "
            f"environment_bits={probe['bits']}"
        )
        if int(probe["bits"]) != 64:
            print("FAIL [wrong_architecture] development environments must use 64-bit Python")
            failures += 1

    if check_profile(profile, env_root=env_root, no_cache=no_cache) != 0:
        failures += 1

    if probe_rc == 0:
        pip_check = subprocess.run(
            [str(python), "-m", "pip", "check"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        pip_output = (pip_check.stdout or pip_check.stderr).strip()
        if pip_check.returncode:
            print(pip_output)
            print("FAIL [broken_dependencies] pip check found an inconsistent installed environment")
            failures += 1
        else:
            print(f"OK pip_check: {pip_output or 'no broken requirements found'}")

    if failures:
        print(f"VERDICT UNSTABLE ({failures} failure(s))")
        return 1
    print("VERDICT STABLE")
    return 0


def run_in_profile(
    profile: str,
    command: Sequence[str],
    *,
    env_root: str | Path | None = None,
    no_cache: bool = False,
) -> int:
    if check_profile(profile, env_root=env_root, no_cache=no_cache, quiet=True) != 0:
        print(f"REFUSING to run in stale profile {profile!r}; run `tools/dev_env.py sync {profile}` first")
        check_profile(profile, env_root=env_root, no_cache=no_cache)
        return 1
    if not command:
        print("FAIL no command supplied after the profile")
        return 2
    selected = list(command)
    if selected[0] in {"python", "python3", "python3.12"}:
        selected[0] = str(environment_python(environment_path(profile, env_root)))
    return subprocess.run(selected, cwd=ROOT, env=profile_environment(profile, env_root)).returncode


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-root", type=Path, help=f"override {ENV_ROOT_VAR} / {DEFAULT_ENV_ROOT}")
    parser.add_argument("--no-cache", action="store_true", help="tell uv to use a temporary cache")
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("sync", "check", "doctor", "path"):
        child = subparsers.add_parser(action)
        child.add_argument("profile", choices=tuple(PROFILES))
    run = subparsers.add_parser("run")
    run.add_argument("profile", choices=tuple(PROFILES))
    run.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)

    try:
        if args.action == "path":
            print(environment_path(args.profile, args.env_root))
            return 0
        if args.action == "sync":
            return sync_profile(args.profile, env_root=args.env_root, no_cache=args.no_cache)
        if args.action == "check":
            return check_profile(args.profile, env_root=args.env_root, no_cache=args.no_cache)
        if args.action == "doctor":
            return doctor_profile(args.profile, env_root=args.env_root, no_cache=args.no_cache)
        command = list(args.command)
        if command[:1] == ["--"]:
            command.pop(0)
        return run_in_profile(args.profile, command, env_root=args.env_root, no_cache=args.no_cache)
    except RuntimeError as exc:
        print(f"FAIL [invalid_bootstrap] {exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
