from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

import tools.sidecar_handoff as handoff

TARGET = "01a02ed6-e965-7b60-b3fc-808470752f0e"
RECEIPT = "11111111-2222-4333-8444-555555555555"
SNAPSHOT = "git:aaaaaaaaaaaa@bbbbbbbbbbbbbbbb+dirty:cccccccccccccccc"


def packet(
    *,
    task_key: str = "review.shared-path",
    snapshot: str = SNAPSHOT,
    supersedes: str = "none",
    findings: str = "High - tools/example.py:4 demonstrates the bounded defect.",
) -> str:
    return f"""---
task_key: {task_key}
source_snapshot_id: {snapshot}
role: reviewer
read_paths: tools/example.py, test/tools/test_example.py
write_owner: target
write_paths: none
verification_status: reported
supersedes: {supersedes}
---
# Sidecar handoff

## Scope

Review one bounded path; the target's completed discovery was not repeated.

## Ownership

The target session is captain and sole writer; this sidecar was read-only.

## Findings

{findings}

## Checks run

Focused static inspection passed.

## Checks not run

The full repository gate was not run.

## Integration

The target must adjudicate, integrate, and verify this finding.
"""


def queue_result(command: list[str], returncode: int = 0) -> subprocess.CompletedProcess[str]:
    stdout = f"Queued message {RECEIPT} for thread {TARGET}.\n" if returncode == 0 else ""
    stderr = "queue failed" if returncode else ""
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


def write_packet(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "handoff.md"
    path.write_text(body, encoding="utf-8")
    return path


def make_source_root(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    root.mkdir(exist_ok=True)
    return root


def git_run(root: Path, *arguments: str) -> None:
    subprocess.run(
        ["git", "-C", str(root), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )


def init_git_repo(root: Path) -> None:
    root.mkdir()
    git_run(root, "init", "--quiet")
    git_run(root, "config", "user.email", "sidecar@example.invalid")
    git_run(root, "config", "user.name", "Sidecar Test")
    example = root / "tools" / "example.py"
    example.parent.mkdir()
    example.write_text("value = 1\n", encoding="utf-8")
    git_run(root, "add", "tools/example.py")
    git_run(root, "commit", "--quiet", "-m", "fixture")


def accepted_row(rendered: handoff.RenderedHandoff, *, state: str = "accepted_unconsumed") -> dict:
    return {
        "handoff_id": rendered["handoff_id"],
        "receipt": RECEIPT,
        "sha256": rendered["sha256"],
        "source_session": rendered["source_session"],
        "source_snapshot_id": rendered["source_snapshot_id"],
        "state": state,
        "target_session": TARGET,
        "task_key": rendered["task_key"],
        "verification_status": rendered["verification_status"],
    }


def test_packet_metadata_and_handoff_id_are_deterministic():
    body = packet()

    first = handoff.build_message(target_session=TARGET, packet=body)
    second = handoff.build_message(target_session=TARGET, packet=body)

    assert first == second
    assert first["handoff_id"].startswith("sc-")
    assert first["task_key"] == "review.shared-path"
    assert first["source_snapshot_id"] == SNAPSHOT
    assert "Queue acceptance is not delivery" in first["message"]


@pytest.mark.parametrize(
    ("old", "new", "match"),
    [
        ("role: reviewer", "role: worker", "read-only"),
        ("write_paths: none", "write_paths: tools/example.py", "write_paths must be none"),
        ("verification_status: reported", "verification_status: passed", "not_run or reported"),
        ("## Checks not run", "## Deferred checks", "missing required section"),
        (
            "read_paths: tools/example.py, test/tools/test_example.py",
            "read_paths: all",
            "relative and bounded",
        ),
        (
            "read_paths: tools/example.py, test/tools/test_example.py",
            "read_paths: ../secrets.txt",
            "escapes the source root",
        ),
        (
            "read_paths: tools/example.py, test/tools/test_example.py",
            "read_paths: C:/secrets.txt",
            "relative and bounded",
        ),
    ],
)
def test_packet_rejects_unsafe_or_incomplete_contract(old: str, new: str, match: str):
    with pytest.raises(handoff.HandoffError, match=match):
        handoff.validate_packet(packet().replace(old, new))


def test_git_snapshot_changes_with_dirty_and_untracked_contents(tmp_path: Path):
    root = tmp_path / "repo"
    init_git_repo(root)
    read_paths = ("tools/example.py",)

    first_dirty_file = root / "tools" / "example.py"
    first_dirty_file.write_text("value = 2\n", encoding="utf-8")
    first = handoff.git_snapshot(root, read_paths)
    first_dirty_file.write_text("value = 3\n", encoding="utf-8")
    second = handoff.git_snapshot(root, read_paths)

    scratch = root / "notes.txt"
    scratch.write_text("first\n", encoding="utf-8")
    first_untracked = handoff.git_snapshot(root, read_paths)
    scratch.write_text("second\n", encoding="utf-8")
    second_untracked = handoff.git_snapshot(root, read_paths)

    assert first != second
    assert first_untracked != second_untracked
    assert "+dirty:" in second_untracked


def test_git_snapshot_binds_the_canonical_worktree_root(tmp_path: Path):
    root = tmp_path / "repo"
    other = tmp_path / "other-worktree"
    init_git_repo(root)
    git_run(root, "worktree", "add", "--quiet", "--detach", str(other), "HEAD")

    primary = handoff.git_snapshot(root, ("tools/example.py",))
    alternate = handoff.git_snapshot(other, ("tools/example.py",))

    assert primary != alternate
    assert "+clean:" in primary
    assert "+clean:" in alternate


def test_queue_records_acceptance_without_packet_body(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet())
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"
    captured_command: list[str] = []

    def runner(command: list[str], **_kwargs):
        captured_command.extend(command)
        return queue_result(command)

    result = handoff.queue_handoff(
        target_session=TARGET,
        packet_path=packet_path,
        source_root=source_root,
        ledger=ledger,
        claims_dir=claims,
        codex_path="codex.exe",
        runner=runner,
        snapshotter=lambda _root, _read_paths: SNAPSHOT,
    )

    assert result["state"] == "accepted_unconsumed"
    assert captured_command[:4] == ["codex.exe", "queue", "--thread", TARGET]
    assert "task_key: review.shared-path" in captured_command[-1]
    assert list(claims.iterdir()) == []
    row = json.loads(ledger.read_text(encoding="utf-8"))
    assert row["receipt"] == RECEIPT
    assert row["read_paths"] == ["tools/example.py", "test/tools/test_example.py"]
    assert row["role"] == "reviewer"
    assert row["task_key"] == "review.shared-path"
    assert row["packet"] == packet_path.name
    assert row["write_owner"] == "target"
    assert row["write_paths"] == "none"
    assert "Findings" not in ledger.read_text(encoding="utf-8")


def test_same_task_requires_explicit_superseding_correction(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet())
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    def runner(command, **_kwargs):
        return queue_result(command)

    common = {
        "target_session": TARGET,
        "packet_path": packet_path,
        "source_root": source_root,
        "ledger": ledger,
        "claims_dir": claims,
        "codex_path": "codex.exe",
        "runner": runner,
        "snapshotter": lambda _root, _read_paths: SNAPSHOT,
    }

    first = handoff.queue_handoff(**common)
    with pytest.raises(handoff.HandoffError, match="identical handoff is already active"):
        handoff.queue_handoff(**common)

    write_packet(tmp_path, packet(findings="Medium - corrected evidence, still for target review."))
    with pytest.raises(handoff.HandoffError, match="task review.shared-path is already active"):
        handoff.queue_handoff(**common)

    write_packet(
        tmp_path,
        packet(
            findings="Medium - corrected evidence, still for target review.",
            supersedes=str(first["handoff_id"]),
        ),
    )
    corrected = handoff.queue_handoff(**common)
    assert corrected["state"] == "accepted_unconsumed"
    assert corrected["supersedes"] == first["handoff_id"]


def test_stale_snapshot_is_rejected_before_codex_queue(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet())
    source_root = make_source_root(tmp_path)

    def runner(*_args, **_kwargs):
        raise AssertionError("codex queue must not run for stale evidence")

    with pytest.raises(handoff.HandoffError, match="stale packet snapshot"):
        handoff.queue_handoff(
            target_session=TARGET,
            packet_path=packet_path,
            source_root=source_root,
            ledger=tmp_path / "ledger.jsonl",
            claims_dir=tmp_path / "claims",
            codex_path="codex.exe",
            runner=runner,
            snapshotter=lambda _root, _read_paths: "git:dddddddddddd@eeeeeeeeeeeeeeee+clean:ffffffffffffffff",
        )


def test_packet_inside_source_root_is_rejected_as_self_referential(tmp_path: Path):
    source_root = make_source_root(tmp_path)
    packet_path = write_packet(source_root, packet())

    with pytest.raises(handoff.HandoffError, match="self-referential"):
        handoff.queue_handoff(
            target_session=TARGET,
            packet_path=packet_path,
            source_root=source_root,
            ledger=tmp_path / "ledger.jsonl",
            claims_dir=tmp_path / "claims",
            codex_path="codex.exe",
            runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected queue")),
            snapshotter=lambda *_args: (_ for _ in ()).throw(AssertionError("unexpected snapshot")),
        )


def test_failed_queue_releases_claim_and_can_retry(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet())
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"
    attempts = iter(("start-error", "success"))

    def runner(command, **_kwargs):
        if next(attempts) == "start-error":
            raise FileNotFoundError("simulated process start failure")
        return queue_result(command)

    kwargs = {
        "target_session": TARGET,
        "packet_path": packet_path,
        "source_root": source_root,
        "ledger": ledger,
        "claims_dir": claims,
        "codex_path": "codex.exe",
        "runner": runner,
        "snapshotter": lambda _root, _read_paths: SNAPSHOT,
    }
    with pytest.raises(handoff.HandoffError, match="codex queue could not start"):
        handoff.queue_handoff(**kwargs)
    assert list(claims.iterdir()) == []

    assert handoff.queue_handoff(**kwargs)["state"] == "accepted_unconsumed"


def test_malformed_receipt_retains_recovery_claim_until_confirmed_failed(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet())
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    def malformed_runner(command, **_kwargs):
        stdout = f"Queued message {'-' * 36} for thread {TARGET}.\n"
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="")

    common = {
        "target_session": TARGET,
        "packet_path": packet_path,
        "source_root": source_root,
        "ledger": ledger,
        "claims_dir": claims,
        "codex_path": "codex.exe",
        "snapshotter": lambda _root, _read_paths: SNAPSHOT,
    }
    with pytest.raises(handoff.HandoffError, match="queue receipt must be a UUID"):
        handoff.queue_handoff(**common, runner=malformed_runner)

    unknown = json.loads(ledger.read_text(encoding="utf-8"))
    assert unknown["state"] == "unknown"
    retained = list(claims.iterdir())
    assert len(retained) == 1
    status = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=unknown["handoff_id"],
        ledger=ledger,
        claims_dir=claims,
        codex_home=tmp_path,
    )
    assert status["state"] == "recovery_required"
    assert status["claim_file"] == str(retained[0])

    recovered = handoff.recover_claim(
        target_session=TARGET,
        task_key=unknown["task_key"],
        handoff_id=unknown["handoff_id"],
        resolution="failed",
        ledger=ledger,
        claims_dir=claims,
    )
    assert recovered["state"] == "failed_claim_released"
    assert list(claims.iterdir()) == []
    retried = handoff.queue_handoff(
        **common,
        runner=lambda command, **_kwargs: queue_result(command),
    )
    assert retried["state"] == "accepted_unconsumed"


def test_unexpected_runner_error_records_unknown_and_retains_claim(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet(task_key="review.runner-crash"))
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    def runner(*_args, **_kwargs):
        raise RuntimeError("simulated interruption")

    with pytest.raises(handoff.HandoffError, match="queue outcome is unknown"):
        handoff.queue_handoff(
            target_session=TARGET,
            packet_path=packet_path,
            source_root=source_root,
            ledger=ledger,
            claims_dir=claims,
            codex_path="codex.exe",
            runner=runner,
            snapshotter=lambda _root, _read_paths: SNAPSHOT,
        )

    assert json.loads(ledger.read_text(encoding="utf-8"))["state"] == "unknown"
    claim_files = list(claims.iterdir())
    assert len(claim_files) == 1
    assert "Findings" not in claim_files[0].read_text(encoding="utf-8")


def test_nonzero_queue_exit_is_ambiguous_and_retains_claim(tmp_path: Path):
    packet_path = write_packet(tmp_path, packet(task_key="review.nonzero-exit"))
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    with pytest.raises(handoff.HandoffError, match="outcome is unknown"):
        handoff.queue_handoff(
            target_session=TARGET,
            packet_path=packet_path,
            source_root=source_root,
            ledger=ledger,
            claims_dir=claims,
            codex_path="codex.exe",
            runner=lambda command, **_kwargs: queue_result(command, returncode=7),
            snapshotter=lambda _root, _read_paths: SNAPSHOT,
        )

    assert json.loads(ledger.read_text(encoding="utf-8"))["state"] == "unknown"
    assert len(list(claims.iterdir())) == 1


def test_accepted_queue_with_ledger_failure_retains_claim(monkeypatch, tmp_path: Path):
    packet_path = write_packet(tmp_path, packet(task_key="review.ledger-crash"))
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    def fail_append(*_args, **_kwargs):
        raise OSError("simulated disk failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(handoff, "_append_row", fail_append)
        with pytest.raises(handoff.HandoffError, match="receipt ledger append failed"):
            handoff.queue_handoff(
                target_session=TARGET,
                packet_path=packet_path,
                source_root=source_root,
                ledger=ledger,
                claims_dir=claims,
                codex_path="codex.exe",
                runner=lambda command, **_kwargs: queue_result(command),
                snapshotter=lambda _root, _read_paths: SNAPSHOT,
            )

    assert not ledger.exists()
    claim_files = list(claims.iterdir())
    assert len(claim_files) == 1
    retained = json.loads(claim_files[0].read_text(encoding="utf-8"))
    assert retained["receipt"] == RECEIPT
    assert retained["state"] == "accepted_receipt_pending_ledger"
    rendered = handoff.build_message(target_session=TARGET, packet=packet(task_key="review.ledger-crash"))
    status = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=str(rendered["handoff_id"]),
        ledger=ledger,
        claims_dir=claims,
        codex_home=tmp_path,
    )
    assert status["state"] == "recovery_required"

    recovered = handoff.recover_claim(
        target_session=TARGET,
        task_key=retained["task_key"],
        handoff_id=retained["handoff_id"],
        resolution="accepted",
        ledger=ledger,
        claims_dir=claims,
    )
    assert recovered["state"] == "accepted_claim_released"
    accepted = json.loads(ledger.read_text(encoding="utf-8"))
    assert accepted["state"] == "accepted_unconsumed"
    assert accepted["receipt"] == RECEIPT
    assert list(claims.iterdir()) == []


def test_accepted_ledger_with_cleanup_failure_uses_accepted_recovery(monkeypatch, tmp_path: Path):
    packet_path = write_packet(tmp_path, packet(task_key="review.cleanup-crash"))
    source_root = make_source_root(tmp_path)
    ledger = tmp_path / "ledger.jsonl"
    claims = tmp_path / "claims"

    with monkeypatch.context() as scoped:
        scoped.setattr(
            handoff,
            "_release_claim",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(handoff.HandoffError("cleanup failed")),
        )
        with pytest.raises(handoff.HandoffError, match="cleanup failed"):
            handoff.queue_handoff(
                target_session=TARGET,
                packet_path=packet_path,
                source_root=source_root,
                ledger=ledger,
                claims_dir=claims,
                codex_path="codex.exe",
                runner=lambda command, **_kwargs: queue_result(command),
                snapshotter=lambda _root, _read_paths: SNAPSHOT,
            )

    accepted = json.loads(ledger.read_text(encoding="utf-8"))
    status = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=accepted["handoff_id"],
        ledger=ledger,
        claims_dir=claims,
        codex_home=tmp_path,
    )
    assert status["state"] == "accepted_unconsumed"
    assert status["claim_cleanup_required"] is True

    recovered = handoff.recover_claim(
        target_session=TARGET,
        task_key=accepted["task_key"],
        handoff_id=accepted["handoff_id"],
        resolution="accepted",
        ledger=ledger,
        claims_dir=claims,
    )
    assert recovered["state"] == "accepted_claim_released"
    assert list(claims.iterdir()) == []


def test_snapshot_rejects_symlinks_instead_of_following_them(tmp_path: Path):
    root = tmp_path / "repo"
    init_git_repo(root)
    outside = tmp_path / "outside.txt"
    outside.write_text("external\n", encoding="utf-8")
    link = root / "tools" / "external-link.py"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    with pytest.raises(handoff.HandoffError, match="symlink"):
        handoff.git_snapshot(root, ("tools/external-link.py",))


def test_status_requires_user_message_evidence_for_delivery(tmp_path: Path):
    body = packet()
    rendered = handoff.build_message(target_session=TARGET, packet=body)
    ledger = tmp_path / "ledger.jsonl"
    ledger.write_text(
        json.dumps(accepted_row(rendered)) + "\n",
        encoding="utf-8",
    )
    sessions = tmp_path / "sessions" / "2026" / "08" / "23"
    sessions.mkdir(parents=True)
    transcript = sessions / f"rollout-{TARGET}.jsonl"
    assistant_item = {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": rendered["message"]}],
        },
    }
    transcript.write_text(json.dumps(assistant_item) + "\n", encoding="utf-8")

    accepted = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=str(rendered["handoff_id"]),
        ledger=ledger,
        claims_dir=tmp_path / "claims",
        codex_home=tmp_path,
    )
    assert accepted["state"] == "accepted_unconsumed"

    quoted_user_item = {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": f"Quoted report:\n{rendered['message']}"}],
        },
    }
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(quoted_user_item) + "\n")

    quoted = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=str(rendered["handoff_id"]),
        ledger=ledger,
        claims_dir=tmp_path / "claims",
        codex_home=tmp_path,
    )
    assert quoted["state"] == "accepted_unconsumed"

    exact_user_item = {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": rendered["message"]}],
        },
    }
    with transcript.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(exact_user_item) + "\n")

    delivered = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=str(rendered["handoff_id"]),
        ledger=ledger,
        claims_dir=tmp_path / "claims",
        codex_home=tmp_path,
    )
    assert delivered["state"] == "delivered"
    assert delivered["session_file"] == str(transcript)


@pytest.mark.parametrize(("ledger_row", "expected"), [(None, "untracked"), ("failed", "failed")])
def test_status_never_delivers_without_an_accepted_receipt(tmp_path: Path, ledger_row: str | None, expected: str):
    rendered = handoff.build_message(target_session=TARGET, packet=packet())
    ledger = tmp_path / "ledger.jsonl"
    if ledger_row:
        ledger.write_text(
            json.dumps(accepted_row(rendered, state=ledger_row)) + "\n",
            encoding="utf-8",
        )
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    transcript = sessions / f"rollout-{TARGET}.jsonl"
    transcript.write_text(
        json.dumps(
            {
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": rendered["message"]}],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    observed = handoff.status_handoff(
        target_session=TARGET,
        handoff_id=str(rendered["handoff_id"]),
        ledger=ledger,
        claims_dir=tmp_path / "claims",
        codex_home=tmp_path,
    )
    assert observed["state"] == expected
    assert observed["session_file"] is None
