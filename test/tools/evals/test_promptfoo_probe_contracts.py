from __future__ import annotations

import sys
from pathlib import Path

import anyio
import pytest

from tools.evals import package_bench, routing_probe

ROOT = Path(__file__).resolve().parents[3]


def test_package_require_perfect_rejects_partial(monkeypatch):
    async def fake_run_task(task, variant):
        return {"task": task, "variant": variant, "score": 0.6}

    monkeypatch.setattr(package_bench, "run_task", fake_run_task)
    monkeypatch.setattr(sys, "argv", ["package_bench.py", "--require-perfect", "newtools", "topic-speeches"])

    with pytest.raises(SystemExit, match="require-perfect"):
        anyio.run(package_bench.main)


def test_routing_require_all_rejects_failed_probe(monkeypatch):
    async def fake_run_probe(name, prompt):
        return {"probe": name, "pass": name != "data-shape"}

    monkeypatch.setattr(routing_probe, "run_probe", fake_run_probe)
    monkeypatch.setattr(sys, "argv", ["routing_probe.py", "--require-all"])

    with pytest.raises(SystemExit, match="require-all"):
        anyio.run(routing_probe.main)


def test_promptfoo_config_uses_fail_closed_probe_modes():
    config = (ROOT / "tools" / "evals" / "promptfooconfig.yaml").read_text(encoding="utf-8")

    assert "routing_probe.py --require-all" in config
    assert "package_bench.py --require-perfect newtools" in config
    assert "harness_bench.py --require-perfect on" in config
    assert "keep beating baseline" not in config.lower()
