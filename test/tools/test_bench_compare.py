"""Tests for tools/evals/bench_compare.py — the n>=5 A/B decision rule.

Pure functions over synthetic log rows: the delta-vs-spread verdict, the
tool-sequence agreement metrics, and the grouping that keeps arms comparable
(same candidate + prompt_sha256 + model; arms = git_head)."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from tools.evals import bench_compare as bc  # noqa: E402


def test_incomplete_identities_never_receive_comparison_verdicts():
    rows = [_row("a", 10, ["Read"]), _row("b", 5, ["Read"])]
    for row in rows:
        row.pop("harness_sha256", None)
    text = "\n".join(bc.compare(rows, "C2", min_runs=2))
    assert "INCOMPARABLE" in text
    assert "VERDICTS" not in text


def test_failure_spending_cannot_create_an_apparent_saving():
    rows = [
        _row("a", 10, ["Read"]),
        _row("a", 10, ["Read"]),
        _row("b", 100, ["Read"], err=True),
        _row("b", 5, ["Read"]),
    ]
    for row in rows:
        row.update(
            provider="codex",
            reasoning_effort="medium",
            harness_sha256="h",
            tool_policy_identity="p",
            input_sha256="i",
            usage_schema_identity="u",
            quality_passed=True,
        )
    assert bc.arm_stats(rows[2:])["cost_per_success"] == 105
    text = "\n".join(bc.compare(rows, "C2", min_runs=2))
    assert "failure-affected" in text
    assert "DECIDABLE" not in text


def test_provider_completion_alone_is_not_a_quality_success():
    row = _row("a", 5, ["Read"])
    row.pop("quality_passed", None)
    assert bc.arm_stats([row])["successful_n"] == 0


def test_missing_failure_cost_makes_cost_per_success_unknown():
    rows = [_row("a", 5, ["Read"]), _row("a", None, ["Read"], err=True)]
    rows[0]["quality_passed"] = True
    assert bc.arm_stats(rows)["cost_per_success"] is None


def test_absent_cost_observations_keep_observed_total_unknown():
    assert bc.arm_stats([_row("a", None, ["Read"])])["cost_usd_observed_total"] is None


def test_output_metric_requires_native_presence_and_preserves_explicit_zero():
    row = _row("a", 5, ["Read"])
    row.update(output_tokens=0, usage_reported_fields=["input_tokens"])
    assert bc.arm_stats([row])["output_tokens"] is None
    row["usage_reported_fields"].append("output_tokens")
    assert bc.arm_stats([row])["output_tokens"]["median"] == 0


def test_malformed_fresh_usage_is_unknown():
    for usage in (
        {"input_tokens": "10", "cache_creation_input_tokens": 0},
        {"input_tokens": True, "cache_creation_input_tokens": 0},
        {"input_tokens": 1, "cache_creation_input_tokens": -1},
        {"input_tokens": 1, "cache_creation_input_tokens": 0.5},
        [],
    ):
        assert bc.fresh_tokens({"usage": usage}) is None


def _row(head, cost, seq, fresh=100, cand="C2", sha="abc", model="m", err=False):
    return {
        "provider": "codex",
        "reasoning_effort": "medium",
        "harness_sha256": "harness-v1",
        "tool_policy_identity": "policy-v1",
        "input_sha256": sha,
        "usage_schema_identity": "usage-v1",
        "quality_passed": True,
        "candidate": cand,
        "prompt_sha256": sha,
        "model": model,
        "git_head": head,
        "cost_usd": cost,
        "num_turns": 10,
        "usage": {"input_tokens": fresh, "cache_creation_input_tokens": 0, "output_tokens": 50},
        "output_tokens": 50,
        "tool_sequence": seq,
        "is_error": err,
    }


def test_seq_agreement_identical_and_disjoint():
    assert bc.seq_agreement([["Read", "Edit"], ["Read", "Edit"]]) == 1.0
    assert bc.seq_agreement([["Read"], ["Glob"]]) == 0.0
    assert bc.seq_agreement([["Read"]]) is None  # one run: no pairs


def test_first2_modal_share():
    seqs = [["Read", "Edit", "X"], ["Read", "Edit"], ["Glob", "Read"]]
    assert bc.first2_modal_share(seqs) == "2/3"


def test_verdict_noise_within_spread():
    a = bc.arm_stats([_row("h1", c, ["Read"]) for c in (0.40, 0.50, 0.60)])
    b = bc.arm_stats([_row("h2", c, ["Read"]) for c in (0.45, 0.55, 0.65)])
    assert "NOISE" in bc.verdict(a, b, "cost_usd")  # Δ0.05 < spread 0.20


def test_verdict_decidable_beyond_spread():
    a = bc.arm_stats([_row("h1", c, ["Read"]) for c in (0.40, 0.42, 0.44)])
    b = bc.arm_stats([_row("h2", c, ["Read"]) for c in (0.80, 0.82, 0.84)])
    assert "DECIDABLE" in bc.verdict(a, b, "cost_usd")


def test_compare_groups_by_prompt_and_retains_failure_cost():
    rows = [
        _row("h1", 0.4, ["Read"], sha="OLD"),  # different prompt — must be ignored
        _row("h1", 9.9, ["Read"], err=True),  # spent error cost must remain visible
        *[_row("h1", c, ["Read", "Edit"]) for c in (0.40, 0.41, 0.42)],
        *[_row("h2", c, ["Read", "Edit"]) for c in (0.90, 0.91, 0.92)],
    ]
    lines = "\n".join(bc.compare(rows, "C2", min_runs=3))
    assert "arm h1 n=4 successful_n=3 failure_n=1" in lines
    assert "arm h2 n=3" in lines
    assert "1 row(s) under other experiment identity ignored" in lines
    assert "cost_per_success=3.71" in lines
    assert "failure-affected" in lines
    assert "DECIDABLE" not in lines


def test_verdict_single_run_arms_never_decidable():
    a = bc.arm_stats([_row("h1", 0.40, ["Read"])])
    b = bc.arm_stats([_row("h2", 0.90, ["Read"])])
    assert "INDICATIVE" in bc.verdict(a, b, "cost_usd")


def test_compare_single_arm_and_low_n_warnings():
    rows = [_row("h1", 0.4, ["Read"]), _row("h1", 0.5, ["Glob"])]
    lines = "\n".join(bc.compare(rows, "C2", min_runs=5))
    assert "one arm only" in lines
    assert "n<5" in lines
    assert "low within-arm agreement" in lines  # Read vs Glob = 0.0


def test_compare_keeps_failures_and_separates_full_identity():
    rows = [
        {**_row("h1", 0.4, ["Read"]), "provider": "codex"},
        {**_row("h1", 0.2, ["Read"], err=True), "provider": "codex"},
        {**_row("h1", 0.9, ["Read"]), "provider": "other"},
        {**_row("h2", 0.5, ["Read"]), "provider": "codex"},
    ]
    lines = "\n".join(bc.compare(rows, "C2", min_runs=2))
    assert "failure_n=1" in lines
    assert "spent" in lines
    assert "codex" in lines
