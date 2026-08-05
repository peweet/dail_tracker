from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from tools.evals import harness_bench


def test_private_task_contract_scores_expected_json_leaves():
    task = {"kind": "private-exact", "expected": {"allowed": False, "detail": {"grain": "award"}}}

    assert harness_bench.score_answer(task, {"allowed": False, "detail": {"grain": "award"}}) == 1.0
    assert harness_bench.score_answer(task, {"allowed": True, "detail": {"grain": "award"}}) == 0.5


def test_private_task_file_must_be_outside_repo(tmp_path):
    inside = harness_bench.PROJ_PATH / "tools" / "evals" / "private-test-fixture.json"
    with pytest.raises(ValueError, match="outside"):
        harness_bench.load_private_tasks(inside)

    external = tmp_path / "holdout.json"
    external.write_text(
        json.dumps({"tasks": {"hidden": {"prompt": "Return JSON", "expected": {"answer": 7}}}}),
        encoding="utf-8",
    )
    assert harness_bench.load_private_tasks(external)["hidden"]["kind"] == "private-exact"


def test_summary_reports_repeat_range_and_errors():
    attempts = [
        {
            "variant": "on",
            "task": "x",
            "score": 1.0,
            "elapsed_seconds": 2.0,
            "tool_calls": 2,
            "mcp_calls": 1,
            "cost_usd": 0.1,
            "usage": {"input_tokens": 10, "output_tokens": 4},
        },
        {
            "variant": "on",
            "task": "x",
            "score": 0.0,
            "error": "timeout",
            "elapsed_seconds": 4.0,
            "tool_calls": 1,
            "mcp_calls": 0,
            "cost_usd": 0.2,
            "usage": {"input_tokens": 20, "output_tokens": 2},
        },
    ]

    assert harness_bench.summary_rows(attempts, "run-1") == [
        {
            "type": "summary",
            "run_id": "run-1",
            "variant": "on",
            "task": "x",
            "n": 2,
            "score_mean": 0.5,
            "score_min": 0.0,
            "score_max": 1.0,
            "error_count": 1,
            "elapsed_seconds_mean": 3.0,
            "elapsed_seconds_min": 2.0,
            "elapsed_seconds_max": 4.0,
            "tool_calls_total": 3,
            "mcp_calls_total": 1,
            "cost_usd_total": 0.3,
            "input_tokens_total": 30,
            "cache_read_input_tokens_total": 0,
            "raw_input_tokens_total": 0,
            "output_tokens_total": 6,
            "reasoning_output_tokens_total": 0,
        }
    ]


def test_preflight_requires_portable_agent_and_mcp_files(tmp_path):
    for relative in harness_bench.PREFLIGHT_REQUIRED_FILES:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("configured\n", encoding="utf-8")
    (tmp_path / ".eval-cleanroom.json").write_text(
        json.dumps({"files_copied": 5, "source_revision": "abc123"}),
        encoding="utf-8",
    )

    report = harness_bench.preflight_report(Path(tmp_path))

    assert report["ok"] is True
    assert report["provider_calls"] == 0
    assert report["scorer_excluded"] is True
    assert report["private_overlay_excluded"] is True


@pytest.mark.parametrize(("variant", "expected"), [("on", True), ("offclean", False)])
def test_variant_chains_project_and_hook_settings_to_provider(monkeypatch, tmp_path, variant, expected):
    captured = {}

    async def fake_run_eval(request):
        captured["request"] = request
        return SimpleNamespace(
            is_error=False,
            error=None,
            tool_names=[],
            final_text='{"combined_figure_allowed": false, "reason": "different grains"}',
            cost_usd=None,
            provider="codex",
            model="test",
            usage={},
        )

    async def invoke():
        return await harness_bench.run_task(
            "never-sum",
            harness_bench.PUBLIC_TASKS["never-sum"],
            variant,
            cwd=tmp_path,
            repeat_index=1,
            run_id="run-1",
        )

    monkeypatch.setattr(harness_bench, "run_eval", fake_run_eval)
    anyio.run(invoke)

    request = captured["request"]
    assert request.project_settings is expected
    assert request.trusted_project_hooks is expected
