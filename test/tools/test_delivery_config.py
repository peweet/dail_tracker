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

    required = [
        "paths.py",
        "api",
        "mcp_server",
        "services/schemas",
        "sql_views",
        "tools/build_delivery_smoke_fixture.py",
    ]
    required.extend(script for _, script in CHAINS)
    missing = [path for path in required if not _wheel_includes(path, entries)]

    assert not missing, f"wheel omits runtime paths: {missing}"


def test_api_container_copies_services_and_drops_root() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "COPY services ./services" in dockerfile
    assert 'RUN python -c "import api.main"' in dockerfile
    assert "USER 10001:10001" in dockerfile


def test_public_docker_context_excludes_the_private_planning_overlay() -> None:
    public_ignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    private_ignore = (ROOT / "planning" / "product" / "Dockerfile.dockerignore").read_text(encoding="utf-8")

    assert "!planning/product/" not in public_ignore
    assert "!planning/product/**" in private_ignore
    assert "planning/product/.env*" in private_ignore


def test_ci_installs_api_and_mcp_and_smokes_delivery() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    audit = (ROOT / ".github" / "workflows" / "audit.yml").read_text(encoding="utf-8")
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "--extra pipeline --extra api --extra mcp --group dev" in ci
    assert 'dail-pipeline" --list' in ci
    assert 'export DAIL_DATA_DIR="$RUNNER_TEMP/dail-data"' in ci
    assert "tools/build_delivery_smoke_fixture.py" in ci
    assert 'uvicorn" api.main:app' in ci
    assert "127.0.0.1:8091/v1/health" in ci
    assert "127.0.0.1:8080/v1/readiness" in ci
    assert "/v1/readiness" in dockerfile
    assert "docker build --tag dailtracker-api:ci ." in ci
    assert "uv sync --frozen --all-extras --group dev" in audit
