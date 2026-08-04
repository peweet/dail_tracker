"""Static tripwires for wheel, container, and CI delivery contracts."""

from __future__ import annotations

import tomllib
from pathlib import Path

from pipeline import CHAINS

ROOT = Path(__file__).resolve().parents[2]


def _wheel_includes(path: str, entries: list[str]) -> bool:
    candidate = Path(path).as_posix()
    return any(candidate == entry or candidate.startswith(entry.rstrip("/") + "/") for entry in entries)


def test_wheel_declares_runtime_packages_resources_and_chains() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    wheel = project["tool"]["hatch"]["build"]["targets"]["wheel"]
    entries = wheel["only-include"]

    required = ["paths.py", "api", "mcp_server", "services/schemas", "sql_views"]
    required.extend(script for _, script in CHAINS)
    missing = [path for path in required if not _wheel_includes(path, entries)]

    assert not missing, f"wheel omits runtime paths: {missing}"


def test_api_container_copies_services_and_drops_root() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY services ./services" in dockerfile
    assert 'RUN python -c "import api.main"' in dockerfile
    assert "USER 10001:10001" in dockerfile


def test_ci_installs_api_and_mcp_and_smokes_delivery() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    audit = (ROOT / ".github" / "workflows" / "audit.yml").read_text(encoding="utf-8")

    assert "--extra pipeline --extra api --extra mcp --group dev" in ci
    assert 'dail-pipeline" --list' in ci
    assert "docker build --tag dailtracker-api:ci ." in ci
    assert "uv sync --frozen --all-extras --group dev" in audit
