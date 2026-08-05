"""Static safety rails for unattended data-refresh workflow changes."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"


def _workflow(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_changed_refresh_workflows_remain_valid_yaml() -> None:
    for name in (
        "ci.yml",
        "money_flow_refresh.yml",
        "live_tenders_refresh.yml",
        "legal_diary_openview_refresh.yml",
    ):
        assert yaml.safe_load(_workflow(name))


def test_refresh_workflows_are_manual_and_serialised_while_repair_is_pending() -> None:
    for name in (
        "money_flow_refresh.yml",
        "live_tenders_refresh.yml",
        "legal_diary_openview_refresh.yml",
    ):
        text = _workflow(name)
        assert "if: github.event_name == 'workflow_dispatch'" in text
        assert "group: data-refresh-publish" in text


def test_money_flow_never_publishes_or_backs_up_after_a_pipeline_failure() -> None:
    text = _workflow("money_flow_refresh.yml")

    assert "- name: Require the R2 state restore prerequisites" in text
    assert "- name: Stop after a failed pipeline" in text
    assert "if: ${{ steps.run.outputs.pipeline_exit != '0' }}" in text
    assert "if: ${{ steps.run.outputs.pipeline_exit == '0' && github.event.inputs.skip_publish != 'true' }}" in text
    assert (
        "if: ${{ success() && steps.run.outputs.pipeline_exit == '0' && env.RCLONE_CONFIG_R2_ACCESS_KEY_ID != '' }}"
        in text
    )
    assert not any(
        line.lstrip().startswith("rclone copy ") and "data/silver" in line and "--ignore-existing" in line
        for line in text.splitlines()
    )


def test_ci_runs_full_code_checks_unless_a_bot_changed_only_publishable_data() -> None:
    text = _workflow("ci.yml")

    assert "tools/ci_data_refresh.py" in text
    for job in ("lint", "deps", "firewall", "secrets", "typecheck", "test", "delivery"):
        assert f"  {job}:\n    needs: changes\n    if: needs.changes.outputs.data_refresh_only != 'true'" in text
    assert "  sql-contracts:\n    runs-on: ubuntu-latest" in text
