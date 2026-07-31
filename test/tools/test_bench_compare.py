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


def _row(head, cost, seq, fresh=100, cand="C2", sha="abc", model="m", err=False):
    return {
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


def test_compare_groups_by_prompt_and_skips_errors_and_other_prompts():
    rows = [
        _row("h1", 0.4, ["Read"], sha="OLD"),  # different prompt — must be ignored
        _row("h1", 9.9, ["Read"], err=True),  # error row — must be ignored
        *[_row("h1", c, ["Read", "Edit"]) for c in (0.40, 0.41, 0.42)],
        *[_row("h2", c, ["Read", "Edit"]) for c in (0.90, 0.91, 0.92)],
    ]
    lines = "\n".join(bc.compare(rows, "C2", min_runs=3))
    assert "arm h1 n=3" in lines
    assert "arm h2 n=3" in lines
    assert "1 row(s) under other prompt/model ignored" in lines
    assert "cost_usd: Δmedian +0.5" in lines
    assert "DECIDABLE" in lines


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
