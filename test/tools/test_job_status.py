from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import job_status


def _start(ledger: Path, **overrides):
    values = {
        "job_id": "nlc-point-sibling",
        "owner": "task-01a046f5",
        "kind": "build",
        "artifact": "data/layers/_point_scoped/national_land_cover.parquet",
        "source_snapshot": "git:bda499d",
        "phase": "external-sort",
        "evidence": "DuckDB count at 09:10Z",
        "total": 10_114_816,
        "unit": "rows",
        "observed_at": "2026-08-28T09:10:00Z",
    }
    values.update(overrides)
    return job_status.start_job(ledger, **values)


def test_start_and_update_keep_one_measured_source_of_truth(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    first = _start(ledger)
    second = job_status.update_job(
        ledger,
        job_id=first["job_id"],
        owner=first["owner"],
        current=5_000_000,
        phase="bbox-proof",
        eta="unknown",
        evidence="COUNT(*) from .part file",
        observed_at="2026-08-28T09:20:00Z",
    )

    latest = job_status.latest_by_job(ledger)[first["job_id"]]
    assert latest == second
    assert latest["sequence"] == 2
    assert latest["current"] == 5_000_000
    assert latest["artifact"] == first["artifact"]
    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 2


def test_build_and_transfer_are_distinct_jobs(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    _start(ledger)
    _start(
        ledger,
        job_id="nlc-box-transfer",
        owner="task-01a04528",
        kind="transfer",
        artifact="box:/srv/siting/staging/national_land_cover.parquet",
        phase="scp",
        evidence="remote byte count at 09:12Z",
        total=31_400_000_000,
        unit="bytes",
    )

    rows = job_status.latest_by_job(ledger)
    assert set(rows) == {"nlc-point-sibling", "nlc-box-transfer"}
    assert rows["nlc-point-sibling"]["kind"] == "build"
    assert rows["nlc-box-transfer"]["kind"] == "transfer"


def test_atomic_claim_blocks_a_second_starter_without_a_ledger_row(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    claim = job_status._claim_path(ledger, "nlc-point-sibling")
    claim.parent.mkdir(parents=True)
    claim.write_text("other-task\n", encoding="utf-8")

    with pytest.raises(job_status.StatusError, match="ownership claim"):
        _start(ledger)
    assert not ledger.exists()


def test_invalid_start_does_not_strand_claim_or_block_valid_retry(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    with pytest.raises(job_status.StatusError, match="artifact must not be empty"):
        _start(ledger, artifact=" ")

    assert not job_status._claim_path(ledger, "nlc-point-sibling").exists()
    retried = _start(ledger)
    assert retried["job_id"] == "nlc-point-sibling"


def test_non_owner_and_backwards_progress_are_rejected(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    _start(ledger)
    with pytest.raises(job_status.StatusError, match="owned by"):
        job_status.update_job(
            ledger,
            job_id="nlc-point-sibling",
            owner="another-task",
            evidence="process exists",
        )

    job_status.update_job(
        ledger,
        job_id="nlc-point-sibling",
        owner="task-01a046f5",
        current=100,
        evidence="measured rows",
    )
    with pytest.raises(job_status.StatusError, match="backwards"):
        job_status.update_job(
            ledger,
            job_id="nlc-point-sibling",
            owner="task-01a046f5",
            current=99,
            evidence="new sample",
        )


def test_terminal_job_cannot_be_updated_or_reused(tmp_path: Path):
    ledger = tmp_path / "jobs.jsonl"
    _start(ledger)
    finished = job_status.update_job(
        ledger,
        job_id="nlc-point-sibling",
        owner="task-01a046f5",
        state="succeeded",
        current=10_114_816,
        evidence="final count and SHA-256 verified",
    )
    assert finished["state"] == "succeeded"

    with pytest.raises(job_status.StatusError, match="already succeeded"):
        job_status.update_job(
            ledger,
            job_id="nlc-point-sibling",
            owner="task-01a046f5",
            evidence="late update",
        )
    with pytest.raises(job_status.StatusError, match="already exists"):
        _start(ledger)


def test_cli_json_shows_only_latest_active_rows(tmp_path: Path, capsys):
    ledger = tmp_path / "jobs.jsonl"
    _start(ledger)
    assert job_status.main(["--ledger", str(ledger), "show", "--active", "--json"]) == 0
    row = json.loads(capsys.readouterr().out)
    assert row["job_id"] == "nlc-point-sibling"
    assert row["state"] == "running"
