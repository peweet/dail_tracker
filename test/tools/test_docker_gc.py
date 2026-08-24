"""Contract tests for tools/docker_gc.py and its SessionStart note.

The safety-critical property is the reclaim command set: it must never remove named
volumes (`-a` on `volume prune`) and must never call `wsl --shutdown` unattended, since
that stops every WSL distro on the box. Both are asserted directly against the constant
rather than described in a docstring, so widening them breaks a test.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "tools"))
sys.path.insert(0, str(REPO / "tools" / "hooks"))

import docker_gc  # noqa: E402
import session_context  # noqa: E402

DF_SAMPLE = """TYPE            TOTAL     ACTIVE    SIZE      RECLAIMABLE
Images          80        7         104.3GB   101.4GB (97%)
Containers      9         4         8.973MB   127.2kB (1%)
Local Volumes   17        4         4.129GB   3.892GB (94%)
Build Cache     457       49        21.43GB   12.06GB
"""


def test_df_report_parses_real_docker_output(monkeypatch):
    """Parses the exact shape `docker system df` emitted on 2026-08-24."""

    class _Out:
        returncode = 0
        stdout = DF_SAMPLE

    monkeypatch.setattr(docker_gc.subprocess, "run", lambda *a, **k: _Out())
    rows = docker_gc.df_report()
    assert rows is not None
    assert rows["Images"]["total"] == 80
    assert rows["Images"]["reclaimable_pct"] == 97
    assert rows["Images"]["reclaimable_mb"] == pytest.approx(101.4 * 1024, rel=0.01)
    # Build Cache has no percentage column -- must parse, not be skipped.
    assert rows["Build Cache"]["reclaimable_mb"] == pytest.approx(12.06 * 1024, rel=0.01)
    assert rows["Build Cache"]["reclaimable_pct"] is None


def test_df_report_returns_none_when_docker_unreachable(monkeypatch):
    """A missing daemon must read as 'can't tell', never as zero reclaimable."""
    monkeypatch.setattr(docker_gc.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError()))
    assert docker_gc.df_report() is None


def test_reclaim_never_removes_named_volumes():
    """`docker volume prune -a` would delete named volumes with no running container --
    real data, not a reproducible build artifact. The reclaim path must stay anonymous-only."""
    volume_cmd = dict(docker_gc.RECLAIM_COMMANDS)["volume prune"]
    assert "-a" not in volume_cmd and "--all" not in volume_cmd


def test_reclaim_commands_are_age_filtered():
    """Both prune commands must carry an `until=` filter so a build from earlier today,
    or a cache entry from this week, survives an unattended run."""
    commands = dict(docker_gc.RECLAIM_COMMANDS)
    assert any(a.startswith("until=") for a in commands["image prune"])
    assert any(a.startswith("until=") for a in commands["builder prune"])


def test_reclaim_never_shuts_down_wsl():
    """`wsl --shutdown` stops every distro on the box. It belongs only in the
    interactive compact path, never in what the scheduled task runs."""
    flat = " ".join(" ".join(cmd) for _, cmd in docker_gc.RECLAIM_COMMANDS)
    assert "wsl" not in flat and "shutdown" not in flat


def test_scheduled_task_does_not_register_compact():
    """The registered task must invoke --reclaim only; --compact needs a human."""
    ps1 = (REPO / "tools" / "register_docker_gc_task.ps1").read_text(encoding="utf-8")
    assert "--reclaim" in ps1
    assert "--compact" not in ps1.split("#>")[-1]  # allowed in the header comment only


def test_register_task_script_is_ascii():
    """PowerShell 5.1 cannot parse BOM-less UTF-8 (MEMORY.md: keep .ps1 ASCII-only)."""
    raw = (REPO / "tools" / "register_docker_gc_task.ps1").read_bytes()
    assert all(b < 128 for b in raw)


def test_session_note_fires_above_threshold(monkeypatch, tmp_path):
    """The note must actually render when the store is bloated -- proving the gate can
    fire, not just that it stays quiet (MEMORY.md: prove a gate can fail)."""
    monkeypatch.setattr(session_context, "REPO", tmp_path)
    cache = tmp_path / "logs" / "docker_disk_cache.json"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({"reclaimable_gb": 101.4, "vhdx_gb": 152.6}), encoding="utf-8")
    note = session_context._docker_disk_note()
    assert "101 GB reclaimable" in note
    assert "153 GB on disk" in note


def test_session_note_silent_below_threshold(monkeypatch, tmp_path):
    monkeypatch.setattr(session_context, "REPO", tmp_path)
    cache = tmp_path / "logs" / "docker_disk_cache.json"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({"reclaimable_gb": 2.0, "vhdx_gb": 12.0}), encoding="utf-8")
    assert session_context._docker_disk_note() == ""


def test_session_note_silent_when_docker_absent(monkeypatch, tmp_path):
    """Docker not running is not a status line -- the note must fail open to ''."""
    monkeypatch.setattr(session_context, "REPO", tmp_path)
    monkeypatch.setattr(docker_gc, "df_report", lambda timeout=4.0: None)
    assert session_context._docker_disk_note() == ""


def test_compact_reports_elevation_cleanly(monkeypatch, capsys, tmp_path):
    """diskpart ALWAYS needs elevation and a Claude session cannot elevate, so this is a
    path the user hits every time -- it must print the exact command, not a traceback."""
    fake_vhdx = tmp_path / "docker_data.vhdx"
    fake_vhdx.write_bytes(b"x")
    monkeypatch.setattr(docker_gc, "VHDX_CANDIDATES", (fake_vhdx,))
    monkeypatch.setattr(docker_gc, "REPO", tmp_path)
    monkeypatch.setattr(docker_gc, "trim", lambda distro="docker-desktop": 0)

    def _run(cmd, *a, **k):
        if cmd and cmd[0] == "diskpart":
            # 4-arg form is how Windows raises it: (errno, strerror, filename, winerror).
            # A 2-arg OSError sets .errno instead and would NOT reproduce the real shape.
            raise OSError(22, "elevation required", None, 740)
        return type("R", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr(docker_gc.subprocess, "run", _run)
    monkeypatch.setattr(docker_gc.time, "sleep", lambda _s: None)
    rc = docker_gc.compact(assume_yes=True)
    out = capsys.readouterr().out
    assert rc == 2
    assert "ELEVATED" in out
    assert "diskpart /s" in out  # the route that acts on the right file
    # set-sparse was recommended here on 2026-08-24 and was WRONG: it targets the distro's
    # root VHD, not docker_data.vhdx. If it is mentioned at all it must be as a warning --
    # asserting only that the string appears would pass for a recommendation too.
    if "--set-sparse" in out:
        assert "does NOT fix this file" in out


def test_dev_task_registered():
    """`tools/dev.py docker-gc` must exist -- the discoverable surface for this."""
    from tools import dev

    assert "docker-gc" in dev.TASKS
