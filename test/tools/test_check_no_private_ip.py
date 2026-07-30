"""Tests for the private-IP / secret leak guard (tools/check_no_private_ip.py).

Covers three things:
  1. Known siting-IP + secret shapes are flagged.
  2. The CIVIC planning-stats lane and ordinary app files are NOT flagged
     (the guard must never block the public transparency data).
  3. The real tracked tree is clean — this is the live regression that fails
     the moment private IP is committed.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SPEC = importlib.util.spec_from_file_location("check_no_private_ip", _REPO / "tools" / "check_no_private_ip.py")
guard = importlib.util.module_from_spec(_SPEC)
assert _SPEC and _SPEC.loader
_SPEC.loader.exec_module(guard)


# ── 1. sensitive shapes must be flagged ──────────────────────────────────────
FLAGGED = [
    "dail_tracker_core/siting/engine.py",
    "planning_rules/county_councils/mayo_county_council/dm_standards.md",
    "test/siting/test_engine.py",
    "doc/private/SITING_COMMERCIALISATION_REVIEW.md",
    "mcp_server/precedent_fts.py",
    "tools/build_point_scoped_layers.py",
    "utility/pages_code/siting_remote.py",
    "pipeline_sandbox/test_planning_siting_layers.py",
    "data/silver/parquet/planning_layers/curated_landmarks_resolve_cache.jsonl",
    "config/prod.pem",
    ".env",
    "app/secrets.toml",
]


def test_sensitive_paths_are_flagged() -> None:
    for path in FLAGGED:
        assert guard.classify(path) is not None, f"should be flagged: {path}"


# ── 2. civic lane + ordinary files must NOT be flagged ───────────────────────
ALLOWED = [
    # civic transparency lane (deliberately public)
    "extractors/planning_appeal_outcomes.py",
    "extractors/planning_decision_profiles.py",
    "extractors/planning_applications_ingest.py",
    "extractors/planning_cpo_compensation.py",
    "sql_views/constituency/constituency_la_planning_overturn.sql",
    "test/sql_views/test_la_planning_overturn.py",
    "pipeline_sandbox/planning_partv_liability_probe.py",
    "pipeline_sandbox/iris_planning_notices_audit.py",
    "doc/archive/PLANNING_DEVELOPMENT_CONTRIBUTIONS.md",
    "data/silver/parquet/planning_appeal_outcomes.parquet",
    # ordinary app / pipeline files
    "utility/app.py",
    "dail_tracker_core/queries/procurement.py",
    "mcp_server/server.py",
    "logs/session_token_ledger.jsonl",  # 'token' must not trip a secret rule
]


def test_public_paths_are_not_flagged() -> None:
    for path in ALLOWED:
        assert guard.classify(path) is None, f"should NOT be flagged: {path}"


def test_find_offenders_returns_pairs() -> None:
    offenders = guard.find_offenders(["utility/app.py", "dail_tracker_core/siting/engine.py"])
    assert offenders == [("dail_tracker_core/siting/engine.py", offenders[0][1])]
    assert "siting" in offenders[0][1].lower()


# ── 3. the live tracked tree must be clean ───────────────────────────────────
def test_real_tracked_tree_is_clean() -> None:
    """Fails the instant private siting IP is committed to the tracked tree."""
    result = subprocess.run(
        [sys.executable, str(_REPO / "tools" / "check_no_private_ip.py")],
        cwd=_REPO,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"leak guard flagged tracked files:\n{result.stdout}\n{result.stderr}"
