"""The single home for cross-tool cadence / freshness constants.

Before this module the same values were copied by hand across the monitoring
tools — `GRACE = 2.0` sat in both `freshness_status.py` and
`migration/check_source_cadence.py`, the cadence-label→days map lived only in
`migration/build_source_cadence.py`, and the freshness-age budget in
`freshness_report.py`. One import point stops a third copy drifting.

Stdlib-only, no imports — safe to pull from any tool (operational or migration).

What INTENTIONALLY lives elsewhere (do NOT fold these in):
  * **Per-source staleness** (`stale_after_days`) — belongs in each extractor's
    own config (ipas_sources.py, cro_poller.py, …) and flows into the generated
    registry. That IS the source of truth per source; centralising it here would
    invert the generated-registry design.
  * **Per-lane `--cadence-hours`** in the refresh workflows (.github/workflows/*.yml)
    and `freshness_status.LANES` — a lane's beat cadence is CI/runner config, not a
    shared Python constant, so it stays with the lane. (A YAML value can't import
    this module anyway.)
"""

from __future__ import annotations

# A signal is only LATE / OVERDUE once it exceeds cadence * GRACE — this absorbs a
# run that lands a little behind schedule (laptop asleep, Actions queue) without a
# false alarm. Used by freshness_status (per lane) and check_source_cadence (per source).
GRACE = 2.0

# A source whose health check fails continuously this long is presumed gone, not
# merely hiccuping — the BROKEN → TAKEN_DOWN boundary in check_source_cadence.
TAKEN_DOWN_AFTER_DAYS = 14

# Default max age (days) of the pipeline's freshness.json before freshness_report
# flags "the pipeline may have stopped". Overridable via --max-age-days /
# FRESHNESS_MAX_AGE_DAYS. ~weekly refresh + buffer.
FRESHNESS_MAX_AGE_DAYS = 14

# Cadence label → days, for seeding source_cadence.csv guesses. 0 == one-off /
# never-due (STATIC). Curated rows may override the day count directly.
CADENCE_DAYS: dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "fortnightly": 14,
    "monthly": 30,
    "quarterly": 90,
    "triannual": 120,
    "biannual": 182,
    "annual": 365,
    "one_off": 0,
    "ad_hoc": 0,
    "review": 0,
}
