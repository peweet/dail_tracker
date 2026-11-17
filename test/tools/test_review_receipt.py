from __future__ import annotations

import json
from pathlib import Path

from tools.review_receipt import build_receipt, main


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


def test_relative_review_path_is_resolved_from_selected_checkout(tmp_path: Path, monkeypatch):
    checkout = tmp_path / "selected"
    checkout.mkdir()
    (checkout / "mod.py").write_text("X = 1\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    receipt = build_receipt(
        session_id="s", checkout=checkout, paths=[Path("mod.py")], acceptance="MATCHES BRIEF", result="PASS"
    )
    assert list(receipt["reviewed_paths"]) == [str(checkout / "mod.py").replace("\\", "/")]
