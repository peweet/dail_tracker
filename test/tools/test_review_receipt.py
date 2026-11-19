from __future__ import annotations

import json
from pathlib import Path

import tools.review_receipt as review_receipt
from tools.review_receipt import build_receipt, default_output, main


def test_build_receipt_binds_current_content_and_paths(tmp_path: Path):
    checkout = tmp_path / "repo"
    checkout.mkdir()
    target = checkout / "mod.py"
    target.write_text("X = 1\n", encoding="utf-8")
    receipt = build_receipt(
        session_id="session-1",
        checkout=checkout,
        paths=[target],
        acceptance="MATCHES BRIEF",
        result="PASS",
    )
    assert receipt["reviewer_role"] == "reviewer"
    assert receipt["result"] == "PASS"
    assert list(receipt["reviewed_paths"]) == [str(target).replace("\\", "/")]


def test_cli_writes_receipt(tmp_path: Path, capsys):
    target = tmp_path / "mod.py"
    target.write_text("X = 1\n", encoding="utf-8")
    output = tmp_path / "receipt.json"
    assert (
        main(
            [
                "--session-id",
                "session-2",
                "--checkout",
                str(tmp_path),
                "--path",
                str(target),
                "--acceptance",
                "MATCHES BRIEF",
                "--result",
                "PASS",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert json.loads(output.read_text(encoding="utf-8"))["session_id"] == "session-2"
    assert str(output) in capsys.readouterr().out


def test_the_default_output_is_one_receipt_per_checkout(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(review_receipt, "_RECEIPT_DIR", tmp_path)
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    out = default_output("session-123456789", first)
    assert out != default_output("session-123456789", second)
    assert out == default_output("session-123456789", second / ".." / "first")
    assert out.parent == tmp_path and out.name.startswith("dail_review_receipt_session-1234_") and out.suffix == ".json"


def test_cli_without_output_writes_the_checkout_receipt(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(review_receipt, "_RECEIPT_DIR", tmp_path / "receipts")
    monkeypatch.delenv("DAIL_REVIEW_RECEIPT", raising=False)
    (tmp_path / "mod.py").write_text("X = 1\n", encoding="utf-8")
    args = [
        "--session-id",
        "s-1",
        "--checkout",
        str(tmp_path),
        "--path",
        "mod.py",
        "--acceptance",
        "OK",
        "--result",
        "PASS",
    ]
    assert main(args) == 0
    assert json.loads(default_output("s-1", tmp_path).read_text(encoding="utf-8"))["checkout"] == str(
        tmp_path.resolve()
    )


def test_relative_review_path_is_resolved_from_selected_checkout(tmp_path: Path, monkeypatch):
    checkout = tmp_path / "selected"
    checkout.mkdir()
    (checkout / "mod.py").write_text("X = 1\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    receipt = build_receipt(
        session_id="s", checkout=checkout, paths=[Path("mod.py")], acceptance="MATCHES BRIEF", result="PASS"
    )
    assert list(receipt["reviewed_paths"]) == [str(checkout / "mod.py").replace("\\", "/")]


def _issue(checkout: Path, paths: list[str], result: str, acceptance: str = "OK") -> dict:
    args = ["--session-id", "s-1", "--checkout", str(checkout), "--acceptance", acceptance, "--result", result]
    for path in paths:
        args += ["--path", path]
    assert main(args) == 0
    return json.loads(default_output("s-1", checkout).read_text(encoding="utf-8"))


def _name(checkout: Path, file: str) -> str:
    return str((checkout / file).resolve()).replace("\\", "/")


def test_a_later_pass_keeps_earlier_reviewed_files_whose_content_is_unchanged(tmp_path: Path, monkeypatch):
    # A second receipt for one checkout replaced the first, so files a reviewer had passed lost their cover
    # (2026-11-19). An unchanged file stays covered; one edited since its review does not.
    monkeypatch.setattr(review_receipt, "_RECEIPT_DIR", tmp_path / "receipts")
    monkeypatch.delenv("DAIL_REVIEW_RECEIPT", raising=False)
    for name in ("a.py", "b.py", "c.py"):
        (tmp_path / name).write_text(f"X = '{name}'\n", encoding="utf-8")
    _issue(tmp_path, ["a.py", "b.py"], "PASS", "first review")
    (tmp_path / "b.py").write_text("X = 'edited after review'\n", encoding="utf-8")
    receipt = _issue(tmp_path, ["c.py"], "PASS", "second review")
    assert sorted(receipt["reviewed_paths"]) == sorted([_name(tmp_path, "a.py"), _name(tmp_path, "c.py")])
    assert receipt["acceptance"] == "second review"
    assert receipt["carried"] == [
        {
            "acceptance": "first review",
            "issued_utc": receipt["carried"][0]["issued_utc"],
            "paths": [_name(tmp_path, "a.py")],
        }
    ]


def test_a_fail_carries_nothing_and_a_pass_after_it_carries_nothing(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(review_receipt, "_RECEIPT_DIR", tmp_path / "receipts")
    monkeypatch.delenv("DAIL_REVIEW_RECEIPT", raising=False)
    for name in ("a.py", "c.py"):
        (tmp_path / name).write_text(f"X = '{name}'\n", encoding="utf-8")
    _issue(tmp_path, ["a.py"], "PASS")
    failed = _issue(tmp_path, ["c.py"], "FAIL")
    assert list(failed["reviewed_paths"]) == [_name(tmp_path, "c.py")] and "carried" not in failed
    passed = _issue(tmp_path, ["c.py"], "PASS")
    assert list(passed["reviewed_paths"]) == [_name(tmp_path, "c.py")] and "carried" not in passed


def test_a_previous_receipt_of_the_wrong_shape_gives_a_plain_receipt(tmp_path: Path, monkeypatch):
    # Valid JSON in the wrong shape crashed the carry instead of being ignored (review, 2026-11-19).
    monkeypatch.setattr(review_receipt, "_RECEIPT_DIR", tmp_path / "receipts")
    monkeypatch.delenv("DAIL_REVIEW_RECEIPT", raising=False)
    (tmp_path / "c.py").write_text("X = 1\n", encoding="utf-8")
    output = default_output("s-1", tmp_path)
    output.parent.mkdir(parents=True)
    common = {"result": "PASS", "session_id": "s-1", "checkout": str(tmp_path.resolve()), "acceptance": "old"}
    for previous in (
        {**common, "reviewed_paths": ["a.py"]},
        {**common, "reviewed_paths": {"a.py": 7}},
        {**common, "reviewed_paths": {}, "carried": [{"paths": None}, "row", {"paths": [3]}]},
        ["not", "a", "receipt"],
    ):
        output.write_text(json.dumps(previous), encoding="utf-8")
        receipt = _issue(tmp_path, ["c.py"], "PASS")
        assert list(receipt["reviewed_paths"]) == [_name(tmp_path, "c.py")] and "carried" not in receipt
