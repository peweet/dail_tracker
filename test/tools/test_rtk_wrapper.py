"""Scope contract for the project-local RTK failure-triage wrapper."""

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tools" / "rtk.ps1"


def test_rtk_wrapper_is_limited_to_last_failed_triage() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "@('pytest-last-failed', 'gain', 'version', 'adoption')" in script
    assert "@('pytest', 'gain', 'version', 'adoption')" not in script
    assert "& $rtkExe pytest --last-failed --last-failed-no-failures=none --maxfail=1 @CommandArgs" in script


def test_rtk_wrapper_cannot_be_overridden_into_a_general_pytest_run() -> None:
    script = SCRIPT.read_text(encoding="utf-8")

    assert "[AllowEmptyCollection()][string[]]$Values = @()" in script
    for option in (
        "'^--(?:last-failed|lf)(?:$|=)'",
        "'^--(?:last-failed-no-failures|lfnf)(?:$|=)'",
        "'^--maxfail(?:$|=)'",
        "'^-x$'",
    ):
        assert option in script
