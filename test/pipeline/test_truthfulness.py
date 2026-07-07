"""Data-truthfulness regression guards — voting history + attendance↔payment.

These lock in the 2026-06-25 truthfulness audit so a future pipeline change can't
silently corrupt the figures the app displays:

  * voting history — internal integrity (no inflated turnout, no conflated
    member-vote vs division-result, denominator == real division count);
  * attendance ↔ payment — the independent TAA payment record corroborates the
    attendance record (recipients verify attendance; no clawback anomalies);
  * an OPTIONAL external check against the Oireachtas open-data API that SKIPS
    (never fails) when offline — matching the project's no-network test rule.

All pure-data checks read the gold parquet directly (same as the other pipeline
tests). Thresholds are ratios / floors, not brittle exact counts, so a data
refresh doesn't turn them red unless something is genuinely wrong.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
DV = (ROOT / "data/gold/parquet/current_dail_vote_history.parquet").as_posix()
PSA = (ROOT / "data/gold/parquet/payments_full_psa.parquet").as_posix()
ATT = (ROOT / "data/gold/parquet/attendance_by_td_year.parquet").as_posix()
PART = (ROOT / "data/gold/parquet/participation_member_year.parquet").as_posix()

_DIVISION_DATE = "2025-06-18"  # a fixed, settled sitting day for the external check


@pytest.fixture(scope="module")
def con():
    return duckdb.connect()


def _scalar(con, sql):
    return con.execute(sql).fetchone()[0]


# ── voting history — internal integrity (no network) ──────────────────────────


def test_no_duplicate_member_division_rows(con):
    """A second row for the same (member, division) would inflate or contradict
    a member's vote count and the turnout denominator."""
    dup = _scalar(
        con,
        f"SELECT count(*) FROM (SELECT unique_member_code, vote_id, count(*) c "
        f"FROM read_parquet('{DV}') WHERE unique_member_code IS NOT NULL GROUP BY 1,2 HAVING c>1)",
    )
    assert dup == 0, f"{dup} duplicate (member, division) pairs"


def test_no_member_votes_more_than_total_divisions(con):
    """An impossible count (voted in more divisions than were held) = a join/dedup bug."""
    bad = _scalar(
        con,
        f"""WITH tot AS (SELECT year(date::DATE) y, count(DISTINCT vote_id) td FROM read_parquet('{DV}') GROUP BY 1),
            m AS (SELECT year(date::DATE) y, full_name, count(DISTINCT vote_id) v
                  FROM read_parquet('{DV}') WHERE full_name IS NOT NULL GROUP BY 1,2)
            SELECT count(*) FROM m JOIN tot USING(y) WHERE m.v > tot.td""",
    )
    assert bad == 0, f"{bad} members voted in more divisions than exist"


def test_member_vote_and_division_result_not_conflated(con):
    """vote_type is the MEMBER's own vote; vote_outcome is the DIVISION result.
    Conflating them would mislabel how a member voted."""
    vt = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT vote_type FROM read_parquet('{DV}') WHERE vote_type IS NOT NULL"
        ).fetchall()
    }
    vo = {
        r[0].strip()
        for r in con.execute(
            f"SELECT DISTINCT vote_outcome FROM read_parquet('{DV}') WHERE vote_outcome IS NOT NULL"
        ).fetchall()
    }
    assert vt <= {"Voted Yes", "Voted No", "Abstained"}, f"unexpected vote_type values: {vt}"
    assert vo <= {"Carried", "Lost", "_", ""}, f"unexpected vote_outcome values: {vo}"


def test_turnout_denominator_equals_real_division_count(con):
    """The turnout 'X of Y' denominator must be the real distinct-division count,
    not an inflated row count."""
    real = _scalar(con, f"SELECT count(DISTINCT vote_id) FROM read_parquet('{DV}') WHERE year(date::DATE)=2025")
    shown = con.execute(
        f"SELECT DISTINCT total_divisions FROM read_parquet('{PART}') WHERE house='Dáil' AND year=2025"
    ).fetchall()
    assert len(shown) == 1 and int(shown[0][0]) == real, f"denominator {[s[0] for s in shown]} != real {real}"


# ── attendance ↔ payment reconciliation (no network) ──────────────────────────


def test_taa_recipients_appear_in_attendance_record(con):
    """You must verify attendance to be paid the Travel & Accommodation Allowance,
    so (almost) every 2024 TAA recipient must be in the attendance record."""
    n_taa, in_both = con.execute(
        f"""WITH taa AS (SELECT DISTINCT unique_member_code FROM read_parquet('{PSA}')
                         WHERE payment_kind='TAA' AND year(date_paid)=2024 AND unique_member_code IS NOT NULL),
                att AS (SELECT DISTINCT unique_member_code FROM read_parquet('{ATT}') WHERE year=2024)
            SELECT (SELECT count(*) FROM taa),
                   (SELECT count(*) FROM (SELECT * FROM taa INTERSECT SELECT * FROM att))""",
    ).fetchone()
    assert n_taa > 0
    assert in_both / n_taa >= 0.95, f"only {in_both}/{n_taa} TAA recipients are in the attendance record"


def test_full_attendance_members_paid_across_the_year(con):
    """A member who attended the full threshold (>=120 days) and receives TAA was
    paid across most of the year — the payment tracks attendance, not a stub."""
    short = _scalar(
        con,
        f"""WITH taa AS (SELECT unique_member_code, count(*) n_pay FROM read_parquet('{PSA}')
                         WHERE payment_kind='TAA' AND year(date_paid)=2024 AND unique_member_code IS NOT NULL GROUP BY 1)
            SELECT count(*) FROM read_parquet('{ATT}') a JOIN taa t USING(unique_member_code)
            WHERE a.year=2024 AND a.total_days >= 120 AND t.n_pay < 6""",
    )
    assert short == 0, f"{short} full-attendance members have <6 TAA payments (payment not tracking attendance)"


def test_no_negative_taa_amounts(con):
    """TAA line items are payments, never clawbacks — a negative would be a parse error."""
    assert _scalar(con, f"SELECT count(*) FROM read_parquet('{PSA}') WHERE payment_kind='TAA' AND amount < 0") == 0


# ── external validation — official Oireachtas API (skips when offline) ─────────


def test_dail_divisions_match_official_api(con):
    """Our division tallies must equal the official Oireachtas open-data API for a
    fixed settled sitting day. SKIPS (never fails) when the API is unreachable, so
    CI stays green offline — it's a confidence check, not a gate."""
    requests = pytest.importorskip("requests")
    ua = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
    try:
        resp = requests.get(
            "https://api.oireachtas.ie/v1/divisions",
            params={"date_start": _DIVISION_DATE, "date_end": _DIVISION_DATE, "chamber": "dail", "limit": 50},
            headers={"User-Agent": ua},
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
    except Exception as exc:  # noqa: BLE001 — offline / API down = skip, not fail
        pytest.skip(f"Oireachtas API unavailable: {exc}")
    if not results:
        pytest.skip("Oireachtas API returned no divisions for the fixed date")

    official = {}
    for item in results:
        d = item["division"]
        vid = d["uri"].split("/")[-1].replace("vote_", f"{_DIVISION_DATE}_")
        t = d["tallies"]
        official[vid] = (t["taVotes"]["tally"], t["nilVotes"]["tally"], d["outcome"])

    ours = con.execute(
        f"""SELECT vote_id,
                   sum(CASE WHEN vote_type='Voted Yes' THEN 1 ELSE 0 END) AS y,
                   sum(CASE WHEN vote_type='Voted No' THEN 1 ELSE 0 END) AS n,
                   any_value(vote_outcome) AS o
            FROM read_parquet('{DV}') WHERE date::DATE = DATE '{_DIVISION_DATE}' AND full_name IS NOT NULL
            GROUP BY vote_id""",
    ).fetchall()
    assert ours, f"no local divisions for {_DIVISION_DATE} — vote data missing?"
    for vid, y, n, outcome in ours:
        assert vid in official, f"{vid} not in official API"
        assert official[vid] == (int(y), int(n), outcome), (
            f"{vid}: ours {int(y)}/{int(n)}/{outcome} != official {official[vid]}"
        )
