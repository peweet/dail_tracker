from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from tools.evals import harness_bench, provider_adapter


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
        json.dumps(
            {
                "tasks": {
                    "hidden": {
                        "prompt": "Return JSON",
                        "expected": {"answer": 7},
                        "allowed_mcp_tools": ["mcp__dail-tracker__search_project"],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    loaded = harness_bench.load_private_tasks(external)
    assert loaded["hidden"]["kind"] == "private-exact"
    assert loaded["hidden"]["allowed_mcp_tools"] == ["mcp__dail-tracker__search_project"]


def test_public_task_mcp_policies_are_narrow_and_no_policy_is_empty():
    assert harness_bench.PUBLIC_TASKS["never-sum"]["allowed_mcp_tools"] == []
    assert harness_bench.PUBLIC_TASKS["conventions"]["allowed_mcp_tools"] == []
    assert harness_bench.PUBLIC_TASKS["data-shape"]["allowed_mcp_tools"] == [
        "mcp__dail-tracker__describe_dataset",
        "mcp__dail-tracker__list_datasets",
    ]
    assert harness_bench.PUBLIC_TASKS["code-nav"]["allowed_mcp_tools"] == [
        "mcp__dail-tracker__search_project",
        "mcp__dail-tracker__code_outline",
    ]
    assert harness_bench.PUBLIC_TASKS["memory-xbrl"]["allowed_mcp_tools"] == ["mcp__dail-tracker__search_project"]


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
            "cache_observation_count": 0,
            "cache_observation_coverage": 0.0,
            "cache_read_input_tokens_observed_total": None,
            "cache_creation_input_tokens_total": None,
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


@pytest.mark.parametrize(
    ("task_id", "variant", "allowed", "has_mcp"),
    [
        ("never-sum", "on", [], False),
        (
            "data-shape",
            "on",
            ["mcp__dail-tracker__describe_dataset", "mcp__dail-tracker__list_datasets"],
            True,
        ),
        ("data-shape", "offclean", [], False),
    ],
)
def test_harness_request_uses_only_task_mcp_policy(monkeypatch, tmp_path, task_id, variant, allowed, has_mcp):
    captured = {}

    async def fake_run_eval(request):
        captured["command"] = provider_adapter.build_codex_command(request, executable="codex", model=None)
        captured["request"] = request
        return SimpleNamespace(
            is_error=False,
            error=None,
            tool_names=[],
            final_text='{"combined_figure_allowed": false}',
            cost_usd=None,
            provider="codex",
            model="test",
            usage={},
        )

    monkeypatch.setattr(harness_bench, "run_eval", fake_run_eval)

    async def invoke():
        return await harness_bench.run_task(
            task_id,
            harness_bench.PUBLIC_TASKS[task_id],
            variant,
            cwd=tmp_path,
            repeat_index=1,
            run_id="run-1",
        )

    anyio.run(invoke)
    request = captured["request"]
    assert request.allowed_tools == (allowed or None)
    assert bool(request.mcp_servers) is has_mcp
    command = captured["command"]
    rendered = "\n".join(command)
    if variant == "on":
        assert "--dangerously-bypass-hook-trust" in command
        assert "--ignore-rules" not in command
        assert "project_doc_max_bytes" not in rendered
    else:
        assert "--dangerously-bypass-hook-trust" not in command
        assert "--ignore-rules" in command
        assert "project_doc_max_bytes=0" in rendered
    if not has_mcp:
        command = provider_adapter.build_codex_command(request, executable="codex", model=None)
        rendered = "\n".join(command)
        assert "mcp_servers={}" in rendered
        assert request.allowed_tools is None


def test_main_emits_balanced_unique_attempt_order_and_selected_manifest(monkeypatch, tmp_path, capsys):
    @contextmanager
    def fake_cleanroom(_repo):
        yield tmp_path

    async def fake_run_eval(request):
        return provider_adapter.EvalResult(
            provider="codex",
            model="test",
            reasoning_effort="low",
            final_text='{"combined_figure_allowed": false}',
            usage={"input_tokens": 100, "cached_input_tokens": 0},
            usage_reported_fields=frozenset({"input_tokens", "cached_input_tokens"}),
        )

    monkeypatch.setattr(harness_bench, "prepare_cleanroom", fake_cleanroom)
    monkeypatch.setattr(harness_bench, "preflight_report", lambda _path: {"ok": True})
    monkeypatch.setattr(harness_bench, "run_eval", fake_run_eval)

    anyio.run(
        harness_bench.main,
        ["--repeat", "2", "never-sum", "conventions", "off", "offclean", "on"],
    )

    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    manifest = records[0]
    attempts = [record for record in records if record["type"] == "attempt"]
    assert manifest["task_ids"] == ["never-sum", "conventions"]
    assert manifest["variants"] == ["off", "offclean", "on"]
    assert manifest["order_policy"] == "balanced"
    assert manifest["cache_policy"] == "provider-managed-uncontrolled"
    assert [(record["repeat"], record["task"], record["variant"]) for record in attempts] == [
        (1, "never-sum", "off"),
        (1, "never-sum", "offclean"),
        (1, "never-sum", "on"),
        (1, "conventions", "offclean"),
        (1, "conventions", "on"),
        (1, "conventions", "off"),
        (2, "never-sum", "offclean"),
        (2, "never-sum", "on"),
        (2, "never-sum", "off"),
        (2, "conventions", "on"),
        (2, "conventions", "off"),
        (2, "conventions", "offclean"),
    ]
    assert len({(record["repeat"], record["task"], record["variant"]) for record in attempts}) == 12
    assert [record["execution_index"] for record in attempts] == list(range(1, 13))
    assert [record["execution_position"] for record in attempts] == list(range(12))
    assert all(record["observed_utc"].endswith("+00:00") for record in attempts)
    assert all(record["reasoning_effort"] == "low" for record in attempts)


def test_ordered_attempts_support_single_variant_and_legacy_off():
    assert harness_bench.ordered_attempts(["a", "b"], ["on"], repeats=2) == [
        (1, "a", "on"),
        (1, "b", "on"),
        (2, "a", "on"),
        (2, "b", "on"),
    ]
    assert harness_bench.ordered_attempts(["a", "b"], ["off", "on"], repeats=2, order_policy="fixed") == [
        (1, "a", "off"),
        (1, "b", "off"),
        (1, "a", "on"),
        (1, "b", "on"),
        (2, "a", "off"),
        (2, "b", "off"),
        (2, "a", "on"),
        (2, "b", "on"),
    ]


def test_cache_observation_preserves_missing_and_reports_explicit_zero():
    missing = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": 0},
        provider="claude",
        reported_fields=frozenset({"input_tokens"}),
    )
    explicit_zero = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}),
    )
    assert missing["state"] == "unknown"
    assert missing["cache_read_input_tokens"] is None
    assert explicit_zero["state"] == "no_reuse_reported"
    assert explicit_zero["cache_read_input_tokens"] == 0
    assert explicit_zero["cache_ratio"] == 0.0


def test_cache_observation_keeps_provider_accounting_disjoint_and_rejects_inconsistent_counts():
    claude = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": 20, "cache_creation_input_tokens": 5},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}),
    )
    codex = harness_bench.cache_observation(
        {"raw_input_tokens": 100, "input_tokens": 80, "cache_read_input_tokens": 20},
        provider="codex",
        reported_fields=frozenset(
            {"input_tokens", "cached_input_tokens", "raw_input_tokens", "cache_read_input_tokens"}
        ),
    )
    inconsistent = harness_bench.cache_observation(
        {"raw_input_tokens": 100, "cache_read_input_tokens": 101},
        provider="codex",
        reported_fields=frozenset(
            {"input_tokens", "cached_input_tokens", "raw_input_tokens", "cache_read_input_tokens"}
        ),
    )
    invalid_creation = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": 0, "cache_creation_input_tokens": -1},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}),
    )
    assert claude["cache_ratio"] == 0.16
    assert codex["cache_ratio"] == 0.2
    assert inconsistent["state"] == "unknown"
    assert invalid_creation["state"] == "unknown"


def test_actual_codex_adapter_usage_reaches_attempt_cache_observation(monkeypatch, tmp_path):
    result = provider_adapter.parse_codex_jsonl(
        json.dumps(
            {"type": "turn.completed", "usage": {"input_tokens": 100, "cached_input_tokens": 90, "output_tokens": 2}}
        )
        + "\n"
        + json.dumps(
            {"type": "item.completed", "item": {"type": "agent_message", "text": '{"combined_figure_allowed": false}'}}
        ),
        model="gpt-test",
        reasoning_effort="medium",
    )

    async def fake_run_eval(_request):
        return result

    monkeypatch.setattr(harness_bench, "run_eval", fake_run_eval)

    async def invoke():
        return await harness_bench.run_task(
            "never-sum",
            harness_bench.PUBLIC_TASKS["never-sum"],
            "off",
            cwd=tmp_path,
            repeat_index=1,
            run_id="run-1",
            execution_index=1,
            execution_position=0,
        )

    row = anyio.run(invoke)
    assert row["cache_observation_state"] == "reuse_reported"
    assert row["cache_read_input_tokens_observed"] == 90
    assert row["cache_reuse_ratio"] == 0.9


@pytest.mark.parametrize(
    "bad_input_tokens",
    [100.5, True, -1, "100", None, {"count": 100}],
    ids=["fractional", "boolean", "negative", "numeric-string", "null", "object"],
)
def test_actual_codex_adapter_rejects_malformed_usage_before_cache_observation(bad_input_tokens):
    result = provider_adapter.parse_codex_jsonl(
        json.dumps(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": bad_input_tokens, "cached_input_tokens": 90, "output_tokens": 2},
            }
        )
        + "\n"
        + json.dumps(
            {
                "type": "item.completed",
                "item": {"id": "cmd-1", "type": "command_execution", "command": "rg needle"},
            }
        )
        + "\n"
        + json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "ok"}}),
        model="gpt-test",
        reasoning_effort="medium",
    )

    observation = harness_bench.cache_observation(
        result.usage,
        provider=result.provider,
        reported_fields=result.usage_reported_fields,
    )

    assert result.final_text == "ok"
    assert result.tool_names == ["Grep"]
    assert result.usage == {}
    assert result.usage_reported_fields == frozenset({"input_tokens", "cached_input_tokens", "output_tokens"})
    assert observation["state"] == "unknown"
    assert any("invalid Codex usage" in diagnostic for diagnostic in result.diagnostics)


def test_cache_observation_claude_boundaries_preserve_known_counters():
    zero_fresh = harness_bench.cache_observation(
        {"input_tokens": 0, "cache_read_input_tokens": 90, "cache_creation_input_tokens": 10},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens"}),
    )
    missing_creation = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": 20},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_read_input_tokens"}),
    )
    missing_read = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_creation_input_tokens": 10},
        provider="claude",
        reported_fields=frozenset({"input_tokens", "cache_creation_input_tokens"}),
    )
    assert zero_fresh["state"] == "reuse_reported"
    assert zero_fresh["cache_ratio"] == 0.9
    assert missing_creation["state"] == "reuse_reported"
    assert missing_creation["cache_read_input_tokens"] == 20
    assert missing_creation["cache_creation_input_tokens"] is None
    assert missing_creation["cache_ratio"] is None
    assert missing_read["state"] == "unknown"
    assert missing_read["cache_read_input_tokens"] is None
    assert missing_read["cache_creation_input_tokens"] == 10
    assert missing_read["cache_ratio"] is None


@pytest.mark.parametrize("provider", ["codex", "claude"])
@pytest.mark.parametrize("bad_count", ["90", 90.0, True, -1])
def test_cache_observation_rejects_noninteger_native_counts(provider, bad_count):
    observation = harness_bench.cache_observation(
        {"input_tokens": 100, "cache_read_input_tokens": bad_count, "cache_creation_input_tokens": 0},
        provider=provider,
    )
    assert observation["state"] == "unknown"
    assert observation["cache_read_input_tokens"] is None


@pytest.mark.parametrize(
    "last_completion",
    [{"type": "turn.completed"}, {"type": "turn.completed", "usage": None}, {"type": "turn.completed", "usage": []}],
)
def test_codex_completion_without_usage_does_not_reuse_prior_counts(last_completion):
    result = provider_adapter.parse_codex_jsonl(
        json.dumps({"type": "turn.completed", "usage": {"input_tokens": 100, "cached_input_tokens": 90}})
        + "\n"
        + json.dumps(last_completion),
    )
    observation = harness_bench.cache_observation(
        result.usage, provider=result.provider, reported_fields=result.usage_reported_fields
    )
    assert result.usage == {}
    assert result.usage_reported_fields is None
    assert observation["state"] == "unknown"


@pytest.mark.parametrize("native,alias", [(0, 90), (90, 0)])
def test_codex_conflicting_cache_aliases_are_unknown(native, alias):
    result = provider_adapter.parse_codex_jsonl(
        json.dumps(
            {
                "type": "turn.completed",
                "usage": {"input_tokens": 100, "cached_input_tokens": native, "cache_read_input_tokens": alias},
            }
        )
    )
    observation = harness_bench.cache_observation(
        result.usage, provider=result.provider, reported_fields=result.usage_reported_fields
    )
    assert result.usage == {}
    assert observation["state"] == "unknown"
    assert any("conflicting cache counters" in diagnostic for diagnostic in result.diagnostics)
