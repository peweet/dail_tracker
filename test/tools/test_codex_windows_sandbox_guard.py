from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GUARD = ROOT / "tools" / "codex_windows_sandbox_guard.ps1"
POWERSHELL = shutil.which("powershell.exe")

pytestmark = pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="Codex native sandbox guard is Windows-specific",
)


def run_guard(tmp_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(GUARD),
            "-StatePath",
            str(tmp_path / "deny_read_acl_state.json"),
            "-SetupErrorPath",
            str(tmp_path / "setup_error.json"),
            "-LogPath",
            str(tmp_path / "auto-repair.log"),
            "-RetryCount",
            "1",
            "-RetryDelayMilliseconds",
            "0",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_valid_state_is_untouched(tmp_path: Path) -> None:
    state = tmp_path / "deny_read_acl_state.json"
    original = b'{"principals": {}}'
    state.write_bytes(original)

    result = run_guard(tmp_path)

    assert result.returncode == 0, result.stderr
    assert state.read_bytes() == original
    assert not list(tmp_path.glob("*.corrupt-auto-*.bak"))
    assert not (tmp_path / "auto-repair.log").exists()
    assert "no repair needed" in result.stdout


def test_missing_state_is_a_quiet_noop(tmp_path: Path) -> None:
    result = run_guard(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "missing; no repair needed" in result.stdout
    assert not (tmp_path / "auto-repair.log").exists()


def test_nul_state_and_linked_parse_error_are_preserved_then_quarantined(tmp_path: Path) -> None:
    state = tmp_path / "deny_read_acl_state.json"
    setup_error = tmp_path / "setup_error.json"
    corrupt = b"\x00" * 22
    state.write_bytes(corrupt)
    setup_error.write_text(
        '{"error":"setup error: apply deny-read ACLs; parse deny-read ACL state"}',
        encoding="utf-8",
    )

    result = run_guard(tmp_path)

    assert result.returncode == 0, result.stderr
    assert not state.exists()
    assert not setup_error.exists()
    state_backups = list(tmp_path.glob("deny_read_acl_state.json.corrupt-auto-*.bak"))
    error_backups = list(tmp_path.glob("setup_error.json.corrupt-auto-*.bak"))
    assert len(state_backups) == 1
    assert state_backups[0].read_bytes() == corrupt
    assert len(error_backups) == 1
    assert "Repaired Codex sandbox startup state" in result.stdout
    log = (tmp_path / "auto-repair.log").read_text(encoding="utf-8-sig")
    assert "reason=contains-nul" in log
    assert "linked_setup_error_backup=" in log


def test_unrelated_setup_error_is_not_moved(tmp_path: Path) -> None:
    state = tmp_path / "deny_read_acl_state.json"
    setup_error = tmp_path / "setup_error.json"
    state.write_text("{partial", encoding="utf-8")
    setup_error.write_text('{"error":"unrelated ACL warning"}', encoding="utf-8")

    result = run_guard(tmp_path)

    assert result.returncode == 0, result.stderr
    assert not state.exists()
    assert setup_error.exists()
    assert setup_error.read_text(encoding="utf-8") == '{"error":"unrelated ACL warning"}'
    assert len(list(tmp_path.glob("deny_read_acl_state.json.corrupt-auto-*.bak"))) == 1
    assert not list(tmp_path.glob("setup_error.json.corrupt-auto-*.bak"))
