"""tools/source_health_report.py — read-only source-health canary over source_health.json.

READ-ONLY companion to ``tools/build_source_health.py``. That script GENERATES
``data/_meta/source_health.json`` at pipeline-end (it needs the live source
registry + bronze inputs present); this one only READS the committed JSON, so it
runs anywhere with zero third-party deps (stdlib only) — including a scheduled
GitHub Action that has no pipeline data and no project sync.

Why a separate read-only consumer (not ``build_source_health.py --strict``)
---------------------------------------------------------------------------
``build_source_health.py --strict`` re-derives health from the LIVE registry,
which needs the committed bronze inputs (CRO/Charities files are gitignored). Run
in CI it would flag every ``file_age`` source as missing and false-fail. So, just
like ``freshness_report.py`` mirrors ``check_freshness.py``, this report reads the
committed JSON the pipeline already wrote and gates on it.

What it alerts on — and why
---------------------------
The alert fires on any source whose status is ``failed`` in the last pipeline
run. For the ``file_age`` sources (CRO companies, Charities register, CRO
financial statements) ``failed`` means the held bronze went older than its
``stale_after_days`` policy — i.e. an automated poller (cro_poller) stopped, or
a manual drop (charities) is overdue. ``warning`` (e.g. "no threshold
configured") is reported as context but does NOT gate. ``skipped`` is normal: the
online ``index_poll`` / ``fixed_file`` sources are only HEAD-checked when the
generator ran with links enabled, and ``api_canary`` sources have no checker yet.

This is a *canary over the last run*, not a live probe: it tells you the health
the pipeline recorded when it last ran. The "did the pipeline stop running at
all" signal is covered separately by ``freshness_report.py`` (over freshness.json),
so this report deliberately does not re-gate on ``generated_at`` age.

Exit code:
    0  healthy — JSON well-formed and no source is ``failed``
    1  unhealthy — one or more sources ``failed``, OR json missing/malformed
A scheduled workflow turns exit 1 into a GitHub issue (.github/workflows/source_health.yml).

Usage:
    python tools/source_health_report.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SOURCE_HEALTH_JSON = _PROJECT_ROOT / "data" / "_meta" / "source_health.json"


def _report(json_path: Path = _SOURCE_HEALTH_JSON) -> int:
    """Print a source-health report; return 0 if healthy, 1 if any source failed
    (or the file is missing/malformed)."""
    if not json_path.exists():
        print(f"SOURCE HEALTH: ERROR — {json_path} not found.")
        print("  The pipeline writes it at the end of a run (tools/build_source_health.py).")
        return 1

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        print(f"SOURCE HEALTH: ERROR — could not read source_health.json: {exc}")
        return 1

    generated_at = payload.get("generated_at", "?")
    summary = payload.get("summary") or {}
    sources = payload.get("sources") or []

    print(f"SOURCE HEALTH REPORT  (as of {datetime.now(UTC).date().isoformat()} UTC)")
    print(f"  generated_at: {generated_at}  links_checked={payload.get('links_checked')}")
    print(
        "  summary: "
        f"{summary.get('sources_ok', 0)} ok / "
        f"{summary.get('sources_warning', 0)} warn / "
        f"{summary.get('sources_failed', 0)} failed / "
        f"{summary.get('sources_skipped', 0)} skipped "
        f"({summary.get('sources_checked', len(sources))} checked)"
    )

    failed = [s for s in sources if s.get("status") == "failed"]
    warnings = [s for s in sources if s.get("status") == "warning"]

    if warnings:
        print(f"  warnings (context only, not gating): {len(warnings)}")
        for s in warnings:
            print(f"    WARN  {s.get('source_id', '?')}: {s.get('detail', '')}")

    if failed:
        print(f"\nFAILED SOURCE(S) ({len(failed)}):")
        for s in failed:
            print(f"  FAILED {s.get('source_id', '?')} [{s.get('check_type', '?')}]: {s.get('detail', '')}")
        print(
            "\nA 'failed' file_age source means the held data went past its staleness policy "
            "(an automated poller stopped, or a manual drop is overdue). Refresh that source "
            "and re-run the pipeline so source_health.json is regenerated."
        )
        return 1

    print("\nOK: no sources failed in the last pipeline run.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only source-health canary over source_health.json.")
    parser.parse_args(argv)
    return _report()


if __name__ == "__main__":
    sys.exit(main())
