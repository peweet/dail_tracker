from __future__ import annotations

import os
import subprocess
from pathlib import Path

from tools import dev_env


def test_profiles_do_not_share_an_environment(tmp_path: Path) -> None:
    assert dev_env.environment_path("public", tmp_path) == (tmp_path / "public").resolve()
    assert dev_env.environment_path("siting", tmp_path) == (tmp_path / "siting").resolve()


def test_siting_profile_is_a_superset_of_public() -> None:
    public = set(dev_env.PROFILES["public"].extras)
    siting = set(dev_env.PROFILES["siting"].extras)
    assert public < siting
    assert siting - public == {"siting"}


def test_profile_arguments_are_locked_and_explicit() -> None:
    args = dev_env.uv_profile_args("public")
    assert args[:3] == ("--locked", "--python", dev_env.python_request())
    assert args.count("--group") == 1
    assert args.count("--extra") == 3
    assert "pipeline" in args and "api" in args and "mcp" in args


def test_python_request_is_an_exact_64_bit_312_interpreter() -> None:
    assert Path(dev_env.python_request()).resolve() == Path(dev_env.sys.executable).resolve()
    assert dev_env.struct.calcsize("P") * 8 == 64


def test_uv_failure_classification_does_not_call_cache_failure_lock_drift() -> None:
    failure = dev_env.classify_uv_failure(
        "Failed to initialize cache at C:/Users/me/AppData/Local/uv/cache: Access is denied"
    )
    assert failure.kind == "cache_unavailable"
    assert "not evaluated" in failure.summary


def test_uv_failure_classifies_environment_drift() -> None:
    failure = dev_env.classify_uv_failure("Would install 4 packages\nThe environment is outdated")
    assert failure.kind == "environment_outdated"


def test_uv_failure_classifies_missing_executable_as_setup_failure() -> None:
    failure = dev_env.classify_uv_failure("uv executable was not found on PATH")
    assert failure.kind == "uv_unavailable"
    assert "not evaluated" in failure.summary


def test_uv_discovery_uses_the_standard_per_user_install_when_path_is_stale(
    monkeypatch, tmp_path: Path
) -> None:
    executable = tmp_path / ".local" / "bin" / ("uv.exe" if os.name == "nt" else "uv")
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"")
    monkeypatch.setattr(dev_env.shutil, "which", lambda _name: None)
    monkeypatch.setattr(dev_env.Path, "home", classmethod(lambda _cls: tmp_path))
    monkeypatch.delenv("UV_EXECUTABLE", raising=False)

    assert dev_env.uv_executable() == str(executable.resolve())


def test_run_rewrites_python_to_the_profile_interpreter(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(dev_env, "check_profile", lambda *args, **kwargs: 0)

    def run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(dev_env.subprocess, "run", run)
    assert dev_env.run_in_profile("public", ["python", "-V"], env_root=tmp_path) == 0
    assert captured["command"][0] == str(dev_env.environment_python((tmp_path / "public").resolve()))
    assert captured["env"][dev_env.PROFILE_VAR] == "public"
    assert captured["env"]["VIRTUAL_ENV"] == str((tmp_path / "public").resolve())
    assert captured["env"]["PATH"].split(os.pathsep)[0] == str(dev_env.environment_bin((tmp_path / "public").resolve()))


def test_run_refuses_to_mutate_or_execute_a_stale_profile(monkeypatch, capsys) -> None:
    calls = 0

    def stale(*args, **kwargs):
        nonlocal calls
        calls += 1
        return 1

    monkeypatch.setattr(dev_env, "check_profile", stale)
    monkeypatch.setattr(
        dev_env.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected command execution")),
    )
    assert dev_env.run_in_profile("public", ["python", "-V"]) == 1
    assert calls == 2
    assert "REFUSING" in capsys.readouterr().out


def test_canonical_guidance_separates_persistent_and_isolated_environments() -> None:
    agents = (dev_env.ROOT / "AGENTS.md").read_text(encoding="utf-8")
    private_agents = (dev_env.ROOT / "planning" / "product" / "AGENTS.md").read_text(encoding="utf-8")

    assert "uv run --isolated --locked" in private_agents
    assert "uv run --locked --group dev --extra pipeline --extra api --extra mcp" in agents
    assert "tools/dev_env.py sync siting" in private_agents
