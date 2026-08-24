"""Static Dockerfile contracts for keeping build tools out of public runtimes."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_public_python_images_use_a_disposable_uv_dependency_stage() -> None:
    for name in ("Dockerfile", "Dockerfile.web"):
        source = (ROOT / name).read_text(encoding="utf-8")

        assert "FROM ${PYTHON_IMAGE} AS runtime-base" in source
        assert "FROM runtime-base AS deps" in source
        assert "FROM runtime-base AS runtime" in source
        assert "--mount=type=cache,target=/root/.cache/uv,sharing=locked" in source
        assert "COPY --from=deps /app/.venv /app/.venv" in source
        assert "FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim" not in source
