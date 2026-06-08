"""judiciary_bench_extract.py — promote the validated judiciary sandbox to gold.

The "green core" of the Judiciary feature (plan: doc/judiciary_feature_sources_claude_plan.md;
memory: project_judiciary_feature_validation). Reads the validated sandbox datasets in
``data/sandbox/judiciary/`` (pulled + pressure-tested 2026-06-04 by
pipeline_sandbox/persist_judiciary_data.py) and emits three analysis-ready GOLD tables.

This is the PRE-PROMOTION CLEANUP the plan calls for:
  * drop the ``body=='Courts'`` junk bucket (kept upstream as ``is_real_court=False``);
  * normalise judge names to a stable join key (``judge_key``) with a small alias table
    (data/_meta/judiciary_name_aliases.csv — Liz/Elizabeth diminutive, Gabett/Gabbett typo);
  * resolve ex-officio cross-listings (court presidents listed under up to 3 courts) to one
    substantive seat per judge for the bench/identity grain, faithfully keeping the raw
    seat count available;
  * flag the one impossible "elevation" (a name-collision HC->District) for manual review
    rather than silently trusting it.

The heavy name-matching was already validated in the sandbox join
(judiciary_appointment_roster_join, ~97% effective); this extractor reuses that result and
keys everything for the SQL views — it does NOT re-invent the join.

OUTPUTS (committed runtime set):
  data/gold/parquet/judiciary_appointments.parquet      one row per appointee per appointment event
  data/gold/parquet/judiciary_bench.parquet             one row per sitting judge (identity grain)
  data/gold/parquet/judiciary_nominations.parquet       one row per gov.ie nomination (vacancy context)
  data/gold/parquet/judiciary_courts_clearance.parquet  clearance facts (jurisdiction×area×category×year)
  data/gold/parquet/judiciary_courts_waiting.parquet    waiting-time lists (latest two years)
  data/gold/parquet/judiciary_courthouses.parquet       active geocoded courthouses (venue map)
META:
  data/_meta/judiciary_bench_coverage.json

The SQL views (sql_views/judiciary_roster.sql / judiciary_appointments.sql /
judiciary_profile.sql) shape + classify off these; the Streamlit page reads the views only.

PRIVACY / SCOPE: appointment, current office, court, rank, official assignment, and salary
BAND ONLY. No performance/bias/conduct/ranking. Conduct stats stay aggregate-only and are NOT
joined here. See the plan's "Privacy / Legal Safety Rules".

Run:
  ./.venv/Scripts/python.exe extractors/judiciary_bench_extract.py
"""

from __future__ import annotations

import calendar
import contextlib
import json
import logging
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import DATA_DIR, GOLD_PARQUET_DIR  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.0.0"
SANDBOX_DIR = DATA_DIR / "sandbox" / "judiciary"
META_DIR = DATA_DIR / "_meta"
ALIAS_CSV = META_DIR / "judiciary_name_aliases.csv"
COVERAGE_PATH = META_DIR / "judiciary_bench_coverage.json"

ROSTER_SOURCE_URL = "https://www.courts.ie/judges"
ROSTER_SNAPSHOT_DATE = "2026-05-26"  # "Published at" on the cached Courts Service roster
GOVIE_SOURCE_URL = "https://www.gov.ie/en/publication/judicial-appointments/"
SALARY_SOURCE = "SI 323/2021 (Judicial remuneration)"

# Constitutional seniority — the natural reading order for courts.
COURT_RANK = {
    "Supreme Court": 1,
    "Court of Appeal": 2,
    "High Court": 3,
    "Circuit Court": 4,
    "District Court": 5,
}
# Ordinary-judge salary band by court (SI 323/2021). Court of Appeal has no separate
# ordinary band in the order, so it is deliberately left unset rather than inferred.
SALARY_BY_COURT = {
    "Supreme Court": 232060,
    "High Court": 218748,
    "Circuit Court": 165421,
    "District Court": 147961,
}
SALARY_OFFICE_BY_COURT = {
    "Supreme Court": "Ordinary Judge, Supreme Court",
    "High Court": "Ordinary Judge, High Court",
    "Circuit Court": "Judge, Circuit Court",
    "District Court": "Judge, District Court",
}

_HONORIFICS = (
    "the hon",
    "mr justice",
    "ms justice",
    "mrs justice",
    "madam justice",
    "his honour judge",
    "her honour judge",
    "his honour",
    "her honour",
    "justice",
    "judge",
    "president",
)
# Tokens that are never part of a name: Irish notice suffixes (", AS"/", AG") and
# bare middle initials (single letters). Dropping both lets "Michael G. MacGrath",
# "Michael MacGrath, AS" and roster "Michael MacGrath" collapse to one key.
_STOP_TOKENS = {"as", "ag"}


# ───────────────────────────────────────────────────────── name normalisation
def _load_aliases() -> dict[str, str]:
    if not ALIAS_CSV.exists():
        return {}
    df = pd.read_csv(ALIAS_CSV)
    return {str(a).strip().lower(): str(c).strip().lower() for a, c in zip(df["alias"], df["canonical"], strict=False)}


def normalise_key(name: str, aliases: dict[str, str]) -> str:
    """Stable join key for a judge name: strip accents/honorifics/punctuation,
    lowercase, apply the alias map per token, preserve order (NO letter-sort —
    that over-collides distinct people). 'BRIAN O'SHEA' / 'Brian O'Shea' -> 'brian oshea'."""
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = unicodedata.normalize("NFD", str(name))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # drop accents
    s = s.lower()
    for h in _HONORIFICS:
        s = s.replace(h, " ")
    s = "".join(c if c.isalnum() or c.isspace() else " " for c in s)  # punctuation -> space
    tokens = [
        aliases.get(t, t) for t in s.split() if len(t) > 1 and t not in _STOP_TOKENS
    ]  # drop initials + notice suffixes
    return " ".join(tokens).strip()


def _iris_url(issue_date, pdf: str | None) -> str | None:
    """Reconstruct the Iris Oifigiúil archive URL for an appointment notice."""
    try:
        d = pd.to_datetime(issue_date)
    except Exception:  # noqa: BLE001
        return None
    fname = str(pdf) if pdf and str(pdf) != "nan" else f"IR{d.strftime('%d%m%y')}.pdf"
    month = calendar.month_name[d.month].lower()
    return f"https://irisoifigiuil.ie/archive/{d.year}/{month}/{fname}"


def _write(df: pd.DataFrame, name: str) -> Path:
    out = GOLD_PARQUET_DIR / f"{name}.parquet"
    save_parquet(df, out)
    logger.info("wrote %s (%d rows, %d cols)", out.name, len(df), len(df.columns))
    return out


# ───────────────────────────────────────────────────────── build: appointments
def build_appointments(spine: pd.DataFrame, join: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    """Event grain: one row per appointee per appointment notice (real courts only)."""
    rows = []
    real = spine[spine["is_real_court"]]
    for r in real.itertuples():
        names = [n.strip() for n in str(r.appointee).split(";") if n and n.strip() and n.strip().lower() != "none"]
        for nm in names:
            rows.append(
                {
                    "judge_key": normalise_key(nm, aliases),
                    "appointee": nm,
                    "issue_date": str(r.issue_date),
                    "appointed_court": r.court,
                    "role": r.role,
                    "appointing_authority": r.appointing_authority,
                    "notice_ref": None if r.notice_ref is None else str(r.notice_ref),
                    "source_url": _iris_url(r.issue_date, r.iris_source_pdf),
                }
            )
    ev = pd.DataFrame(rows)

    # Reuse the validated sandbox match for current_court + status (matched/elevated/unmatched).
    j = join.copy()
    j["issue_date"] = j["appointed_date"].astype(str)
    j = j[["appointee", "issue_date", "current_court", "status"]]
    ev = ev.merge(j, on=["appointee", "issue_date"], how="left")
    ev["status"] = ev["status"].fillna("unmatched")

    ar = ev["appointed_court"].map(COURT_RANK)
    cr = ev["current_court"].map(COURT_RANK)
    ev["is_elevation"] = ev["status"].eq("elevated")
    ev["elevated_to"] = ev["current_court"].where(ev["is_elevation"])
    # An "elevation" to a more junior court is impossible -> name-collision artefact, flag it.
    ev["requires_manual_review"] = ev["is_elevation"] & cr.notna() & ar.notna() & (cr > ar)
    return ev.sort_values(["issue_date", "appointed_court", "appointee"]).reset_index(drop=True)


# ───────────────────────────────────────────────────────── build: bench identity
def build_bench(
    roster: pd.DataFrame,
    appts: pd.DataFrame,
    salaries_present: set[str],
    hc: pd.DataFrame,
    aliases: dict[str, str],
) -> pd.DataFrame:
    ros = roster.copy()
    ros["judge_key"] = ros["judge_name"].map(lambda n: normalise_key(n, aliases))
    ros["court_rank"] = ros["court"].map(COURT_RANK).fillna(99).astype(int)

    # Resolve ex-officio cross-listings: one row per judge_key, prefer the substantive
    # (non ex-officio) seat, then the most senior court listed.
    ros = ros.sort_values(["judge_key", "is_ex_officio_or_multi", "court_rank"])
    raw_seats = ros.groupby("judge_key").size().rename("seat_count")
    ident = ros.drop_duplicates("judge_key", keep="first").merge(raw_seats, on="judge_key")

    # HC specialist-list assignment (Hilary Term 2026) by key.
    hc = hc.copy()
    hc["judge_key"] = hc["judge"].map(lambda n: normalise_key(n, aliases))
    hc_map = hc.drop_duplicates("judge_key").set_index("judge_key")
    ident["assignment"] = ident["judge_key"].map(hc_map["assignment"]) if len(hc_map) else None
    ident["assignment_term"] = ident["judge_key"].map(hc_map["term"]) if len(hc_map) else None

    # Appointment spine rollup per judge.
    a = appts.sort_values("issue_date")
    spine_rows = []
    for key, g in a.groupby("judge_key"):
        path = []
        for c in list(g["appointed_court"]) + [
            g["current_court"].dropna().iloc[-1] if g["current_court"].notna().any() else None
        ]:
            if c and (not path or path[-1] != c):
                path.append(c)
        spine_rows.append(
            {
                "judge_key": key,
                "first_appointed_date": g["issue_date"].iloc[0],
                "first_appointing_authority": g["appointing_authority"].iloc[0],
                "appointed_court": g["appointed_court"].iloc[0],
                "is_elevation": bool(g["is_elevation"].any()),
                "elevation_path": " → ".join(path) if len(path) > 1 else None,
                "appt_source_url": g["source_url"].dropna().iloc[0] if g["source_url"].notna().any() else None,
                "appt_review": bool(g["requires_manual_review"].any()),
            }
        )
    sp = pd.DataFrame(spine_rows)
    ident = ident.merge(sp, on="judge_key", how="left")
    ident["has_spine"] = ident["judge_key"].isin(set(appts["judge_key"]))

    # Salary band — ordinary-judge band by court; suppressed for ex-officio/president
    # seats (their premium can't be attributed to a named person from the roster alone).
    def _salary(row):
        if row["is_ex_officio_or_multi"]:
            return (None, "President / ex-officio (premium not attributed)")
        band = SALARY_BY_COURT.get(row["court"])
        return (band, SALARY_OFFICE_BY_COURT.get(row["court"]) if band else None)

    ident[["salary_band_eur", "salary_office"]] = ident.apply(lambda r: pd.Series(_salary(r)), axis=1)
    ident["salary_source"] = SALARY_SOURCE

    ident["requires_manual_review"] = ident["appt_review"].fillna(False).astype(bool)
    ident["current_court"] = ident["court"]  # the roster is the current state of record
    ident["source_url"] = ROSTER_SOURCE_URL
    ident["source_published_at"] = ROSTER_SNAPSHOT_DATE

    cols = [
        "judge_key",
        "judge_name",
        "court",
        "current_court",
        "court_rank",
        "is_ex_officio_or_multi",
        "seat_count",
        "salary_band_eur",
        "salary_office",
        "salary_source",
        "assignment",
        "assignment_term",
        "has_spine",
        "first_appointed_date",
        "first_appointing_authority",
        "appointed_court",
        "is_elevation",
        "elevation_path",
        "requires_manual_review",
        "appt_source_url",
        "source_url",
        "source_published_at",
    ]
    return ident[cols].sort_values(["court_rank", "judge_name"]).reset_index(drop=True)


# ───────────────────────────────────────────────────────── build: nominations
def build_nominations(nom: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    n = nom.copy()
    n["judge_key"] = n["nominee"].map(lambda x: normalise_key(x, aliases))
    n["source_name"] = "gov.ie nomination announcement"
    n["source_url"] = GOVIE_SOURCE_URL
    cols = [
        "announce_date",
        "nominee",
        "judge_key",
        "target_court",
        "prior_career",
        "vacancy_cause",
        "predecessor",
        "source_name",
        "source_url",
    ]
    return n[cols].sort_values(["announce_date", "target_court", "nominee"]).reset_index(drop=True)


# ──────────────────────────────────────────── build: the courts (system health)
# Three faithful promotions of the Courts Service "system health" datasets. NO
# metric is computed here — clearance_pct and the aggregation to court×year are the
# SQL view's job (logic firewall). This step only fixes source artefacts that would
# otherwise fragment a court or render as mojibake:
#   * clearance JURISDICTION casing ("Court Of Appeal" criminal-appeals table vs
#     "Court of Appeal" civil table are the SAME court — collapse to one canonical
#     spelling so the view groups them; AREA_OF_LAW still separates the streams);
#   * waiting-times labels carry PDF-extraction mojibake (U+FFFD replacement char) —
#     the only reliably-fixable artefact (U+FFFD -> apostrophe) is repaired, the rest
#     is left verbatim rather than guessed at.
CLEARANCE_SOURCE_URL = "https://data.courts.ie"
WAITING_SOURCE_URL = (
    "https://www.courts.ie/annual-report"  # Courts Service Annual Report 2024, Waiting Times pp.135–140
)
COURTHOUSE_SOURCE_URL = "https://data.courts.ie/files/court-offices/court-offices.csv"

# Source casing collisions -> one canonical court name (constitutional spelling).
_COURT_CANON = {"Court Of Appeal": "Court of Appeal"}


def build_courts_clearance(clr: pd.DataFrame) -> pd.DataFrame:
    """Faithful clearance facts at source grain (jurisdiction × area × category × year).
    Casing normalised so the view can group a court; counts untouched. The view computes
    clearance_pct = resolved/incoming (which legitimately exceeds 100% when a court clears
    backlog — that is a real signal, never capped here or downstream)."""
    c = clr.copy()
    c["jurisdiction"] = c["JURISDICTION"].replace(_COURT_CANON)
    out = pd.DataFrame(
        {
            "jurisdiction": c["jurisdiction"],
            "area_of_law": c["AREA_OF_LAW"],
            "year": c["YEAR"].astype(int),
            "category": c["CATEGORY"],
            "incoming": c["INCOMING"].astype(int),
            "resolved": c["RESOLVED"],
            "source_name": "Courts Service annual statistics",
            "source_url": CLEARANCE_SOURCE_URL,
        }
    )
    return out.sort_values(["year", "jurisdiction", "area_of_law", "category"]).reset_index(drop=True)


def build_courts_waiting(wt: pd.DataFrame) -> pd.DataFrame:
    """Waiting-time lists as published (latest two years side by side), VERBATIM.
    The source PDF carries ligature-extraction artefacts on the deeper matter-type rows
    ('certfii ed', U+FFFD where an apostrophe/en-dash stood). These are NOT guessed at
    here — U+FFFD is ambiguous (apostrophe vs en-dash) and silently 'fixing' it would
    corrupt the text. The clean, headline rows are the named venues (Dublin…Limerick);
    the page curates to those, but gold keeps every published row faithfully. The
    is_clean_label flag marks rows free of the known artefacts for that curation."""
    w = wt.copy()
    label = w["matter_or_venue"].astype(str)
    is_clean = ~label.str.contains(r"[�]|fii|\bfi\s", regex=True, na=False)
    out = pd.DataFrame(
        {
            "page": w["page"].astype(int),
            "matter_or_venue": w["matter_or_venue"],
            "wait_2024": w["wait_2024"],
            "wait_2023": w["wait_2023"],
            "is_clean_label": is_clean.values,
            "source_name": "Courts Service Annual Report 2024 (Waiting Times)",
            "source_url": WAITING_SOURCE_URL,
        }
    )
    return out.reset_index(drop=True)


def build_courthouses(ch: pd.DataFrame) -> pd.DataFrame:
    """Active, geocoded courthouses for the venue map (lat/lon + place metadata)."""
    h = ch[ch["active_status"] == "active"].copy()
    out = pd.DataFrame(
        {
            "court_house": h["court_house"],
            "address": h["court_house_address"],
            "eircode": h["court_house_eircode"],
            "region": h["region"],
            "county": h["county"],
            "circuit": h["circuit"],
            "latitude": h["latitude"],
            "longitude": h["longitude"],
            "source_name": "Courts Service court-office register",
            "source_url": COURTHOUSE_SOURCE_URL,
        }
    )
    return out.dropna(subset=["latitude", "longitude"]).sort_values("court_house").reset_index(drop=True)


# ───────────────────────────────────────────────────────────────────── main
def main() -> int:
    setup_standalone_logging("judiciary_bench_extract")
    aliases = _load_aliases()

    spine = pd.read_parquet(SANDBOX_DIR / "judicial_appointments_spine.parquet")
    roster = pd.read_parquet(SANDBOX_DIR / "judiciary_current_roster.parquet")
    join = pd.read_parquet(SANDBOX_DIR / "judiciary_appointment_roster_join.parquet")
    nom = pd.read_parquet(SANDBOX_DIR / "judicial_nominations_govie.parquet")
    hc = pd.read_parquet(SANDBOX_DIR / "judiciary_hc_assignments.parquet")
    salaries = pd.read_parquet(SANDBOX_DIR / "judicial_salaries.parquet")
    salaries_present = set(salaries["office"])
    clearance_raw = pd.read_parquet(SANDBOX_DIR / "courts_clearance.parquet")
    waiting_raw = pd.read_parquet(SANDBOX_DIR / "courts_waiting_times.parquet")
    courthouses_raw = pd.read_parquet(SANDBOX_DIR / "courthouses.parquet")

    appts = build_appointments(spine, join, aliases)
    bench = build_bench(roster, appts, salaries_present, hc, aliases)
    noms = build_nominations(nom, aliases)
    clearance = build_courts_clearance(clearance_raw)
    waiting = build_courts_waiting(waiting_raw)
    courthouses = build_courthouses(courthouses_raw)

    _write(appts, "judiciary_appointments")
    _write(bench, "judiciary_bench")
    _write(noms, "judiciary_nominations")
    _write(clearance, "judiciary_courts_clearance")
    _write(waiting, "judiciary_courts_waiting")
    _write(courthouses, "judiciary_courthouses")

    coverage = {
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "appointments_events": int(len(appts)),
        "appointments_real_court_notices": int(spine["is_real_court"].sum()),
        "appointments_junk_dropped": int((~spine["is_real_court"]).sum()),
        "bench_judges": int(len(bench)),
        "bench_with_spine": int(bench["has_spine"].sum()),
        "bench_pre_2016_gap": int((~bench["has_spine"]).sum()),
        "elevations": int(appts["is_elevation"].sum()),
        "flagged_for_review": int(bench["requires_manual_review"].sum()),
        "nominations": int(len(noms)),
        "courts_clearance_rows": int(len(clearance)),
        "courts_clearance_years": [int(clearance["year"].min()), int(clearance["year"].max())],
        "courts_waiting_rows": int(len(waiting)),
        "courthouses": int(len(courthouses)),
        "salary_bands_available": sorted(salaries_present),
        "sources": {
            "appointments": "Iris Oifigiúil (public_appointments.parquet, appointment_type=='judicial')",
            "roster": ROSTER_SOURCE_URL,
            "nominations": GOVIE_SOURCE_URL,
            "salaries": SALARY_SOURCE,
            "courts_clearance": CLEARANCE_SOURCE_URL,
            "courts_waiting": WAITING_SOURCE_URL,
            "courthouses": COURTHOUSE_SOURCE_URL,
        },
    }
    META_DIR.mkdir(parents=True, exist_ok=True)
    COVERAGE_PATH.write_text(json.dumps(coverage, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("coverage: %s", json.dumps(coverage, ensure_ascii=False))

    print(
        f"bench={len(bench)} (spine={coverage['bench_with_spine']}, "
        f"gap={coverage['bench_pre_2016_gap']}) | appts={len(appts)} "
        f"(elev={coverage['elevations']}, review={coverage['flagged_for_review']}) | noms={len(noms)} | "
        f"clearance={len(clearance)} | waiting={len(waiting)} | courthouses={len(courthouses)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
