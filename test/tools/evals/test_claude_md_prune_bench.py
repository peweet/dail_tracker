from __future__ import annotations

from types import SimpleNamespace

import anyio

from tools.evals import claude_md_prune_bench as prune


def test_prune_uses_current_harness_exports_and_exact_policies(monkeypatch, tmp_path):
    captured = []

    async def fake_run_eval(request):
        captured.append(request)
        return SimpleNamespace(
            is_error=False,
            error=None,
            tool_names=[],
            final_text="{}",
            cost_usd=None,
            provider="codex",
            model="test",
        )

    monkeypatch.setattr(prune, "run_eval", fake_run_eval)
    anyio.run(
        prune.run_one,
        "shape",
        tmp_path,
        prune.STEER_POLICIES["steer-datashape"],
    )
    assert captured[-1].allowed_tools == prune.STEER_POLICIES["steer-datashape"]
    assert captured[-1].mcp_servers

    anyio.run(prune.run_one, "where", tmp_path, prune.STEER_POLICIES["steer-wherelives"])
    assert captured[-1].allowed_tools == ["mcp__dail-tracker__search_project"]
    assert captured[-1].mcp_servers

    anyio.run(prune.run_one, "no policy", tmp_path)
    assert captured[-1].allowed_tools is None
    assert captured[-1].mcp_servers == {}


def test_prune_task_exports_match_harness():
    assert set(prune.PUBLIC_TASKS) == {
        "never-sum",
        "data-shape",
        "code-nav",
        "conventions",
        "memory-xbrl",
    }
    assert callable(prune.score_answer)
