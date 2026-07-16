"""judiciary_bench_extract.py — promote the validated judiciary sandbox to gold.

The "green core" of the Judiciary feature (memory: project_judiciary_feature_validation;
sources documented in data/sandbox/judiciary/README.md). Reads the validated sandbox datasets in
``data/sandbox/judiciary/`` (pulled + pressure-tested 2026-06-04 by
extractors/persist_judiciary_data.py) and emits three analysis-ready GOLD tables.

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

The SQL views (sql_views/judiciary/judiciary_roster.sql / judiciary_appointments.sql /
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
import math
import sys
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote_plus

import polars as pl

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
WAITING_CONTEXT_CSV = META_DIR / "courts_waiting_context.csv"  # hand-curated section headings
COVERAGE_PATH = META_DIR / "judiciary_bench_coverage.json"

ROSTER_SOURCE_URL = "https://www.courts.ie/judges"
ROSTER_SNAPSHOT_DATE = "2026-05-26"  # "Published at" on the cached Courts Service roster
# The old gov.ie listing page (/en/publication/judicial-appointments/) 404s
# since the 2025 gov.ie restructure; nomination rows now carry per-nominee
# search URLs (see build_nominations) and the coverage manifest points at the
# same search scoped to the topic.
GOVIE_SEARCH_URL = "https://www.gov.ie/en/search/?q="
GOVIE_SOURCE_URL = GOVIE_SEARCH_URL + "judicial+appointment"
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
    df = pl.read_csv(ALIAS_CSV)
    return {
        str(a).strip().lower(): str(c).strip().lower()
        for a, c in zip(df["alias"].to_list(), df["canonical"].to_list(), strict=False)
    }


def normalise_key(name: str, aliases: dict[str, str]) -> str:
    """Stable join key for a judge name: strip accents/honorifics/punctuation,
    lowercase, apply the alias map per token, preserve order (NO letter-sort —
    that over-collides distinct people).

    NOTE: punctuation (incl. the apostrophe) becomes a SPACE and single-letter tokens
    are then dropped, so "Brian O'Shea" -> "brian shea" (the bare "o" is discarded), not
    "brian oshea". This is self-consistent across every source that runs through this
    function, so the joins hold (verified: 0 key collisions on the bench, 0 unmatched
    diary rows). It does mean an alias whose canonical is written "oshea" would not
    match, and a source spelling a name without the apostrophe ("OShea" -> "oshea")
    would diverge — change the apostrophe handling here only with a coordinated re-promote
    of all judiciary parquets + the diary map (the key format would shift)."""
    if name is None or (isinstance(name, float) and math.isnan(name)):
        return ""
    s = unicodedata.normalize("NFD", str(name))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")  # drop accents
    s = s.lower()
    for h in _HONORIFICS:
        s = s.replace(h, " ")
    s = "".join(c if c.isalnum() or c.isspace() else " " for c in s)  # punctuation -> space
    raw_tokens = [
        aliases.get(t, t) for t in s.split() if len(t) > 1 and t not in _STOP_TOKENS
    ]  # drop initials + notice suffixes
    # Merge a standalone "mac"/"mc" token into the one that follows it: some sources
    # (the Legal Diary's OpenView text in particular) render Mac/Mc surnames with a
    # space ("MAC GRATH", "MC DONALD") where the canonical spelling has none
    # ("macgrath", "mcdonald") -- without this, the two never key-match. Additive only:
    # no roster judge_key currently contains a split mac/mc token (verified 2026-07-13),
    # so this cannot change an existing key, only let previously-unmatched sources join.
    tokens: list[str] = []
    i = 0
    while i < len(raw_tokens):
        t = raw_tokens[i]
        if t in ("mac", "mc") and i + 1 < len(raw_tokens):
            tokens.append(t + raw_tokens[i + 1])
            i += 2
        else:
            tokens.append(t)
            i += 1
    return " ".join(tokens).strip()


def _keyed(names: list, aliases: dict[str, str]) -> pl.Series:
    """normalise_key over a column's values (nulls -> "" exactly like the row-wise call)."""
    return pl.Series([normalise_key(n, aliases) for n in names], dtype=pl.String)


def _iris_url(issue_date, pdf: str | None) -> str | None:
    """Reconstruct the Iris Oifigiúil archive URL for an appointment notice."""
    try:
        d = datetime.fromisoformat(str(issue_date))
    except Exception:  # noqa: BLE001
        return None
    fname = str(pdf) if pdf and str(pdf) != "nan" else f"IR{d.strftime('%d%m%y')}.pdf"
    month = calendar.month_name[d.month].lower()
    return f"https://irisoifigiuil.ie/archive/{d.year}/{month}/{fname}"


def _write(df: pl.DataFrame, name: str) -> Path:
    out = GOLD_PARQUET_DIR / f"{name}.parquet"
    save_parquet(df, out)
    logger.info("wrote %s (%d rows, %d cols)", out.name, len(df), len(df.columns))
    return out


# ───────────────────────────────────────────────────────── build: appointments
_EVENT_SCHEMA = {
    "judge_key": pl.String,
    "appointee": pl.String,
    "issue_date": pl.String,
    "appointed_court": pl.String,
    "role": pl.String,
    "appointing_authority": pl.String,
    "notice_ref": pl.String,
    "source_url": pl.String,
}


def build_appointments(spine: pl.DataFrame, join: pl.DataFrame, aliases: dict[str, str]) -> pl.DataFrame:
    """Event grain: one row per appointee per appointment notice (real courts only)."""
    rows = []
    real = spine.filter(pl.col("is_real_court"))
    for r in real.iter_rows(named=True):
        names = [n.strip() for n in str(r["appointee"]).split(";") if n and n.strip() and n.strip().lower() != "none"]
        for nm in names:
            rows.append(
                {
                    "judge_key": normalise_key(nm, aliases),
                    "appointee": nm,
                    "issue_date": str(r["issue_date"]),
                    "appointed_court": r["court"],
                    "role": r["role"],
                    "appointing_authority": r["appointing_authority"],
                    "notice_ref": None if r["notice_ref"] is None else str(r["notice_ref"]),
                    "source_url": _iris_url(r["issue_date"], r["iris_source_pdf"]),
                }
            )
    ev = pl.DataFrame(rows, schema=_EVENT_SCHEMA)

    # Reuse the validated sandbox match for current_court + status (matched/elevated/unmatched).
    j = join.select(
        pl.col("appointee").cast(pl.String),
        pl.col("appointed_date").cast(pl.String).alias("issue_date"),
        pl.col("current_court").cast(pl.String),
        pl.col("status").cast(pl.String),
    )
    ev = ev.join(j, on=["appointee", "issue_date"], how="left", maintain_order="left")
    ev = ev.with_columns(pl.col("status").fill_null("unmatched"))

    ar = pl.col("appointed_court").replace_strict(COURT_RANK, default=None, return_dtype=pl.Int64)
    cr = pl.col("current_court").replace_strict(COURT_RANK, default=None, return_dtype=pl.Int64)
    ev = ev.with_columns(pl.col("status").eq("elevated").alias("is_elevation"))
    ev = ev.with_columns(
        pl.when(pl.col("is_elevation")).then(pl.col("current_court")).otherwise(None).alias("elevated_to"),
        # An "elevation" to a more junior court is impossible -> name-collision artefact, flag it.
        (pl.col("is_elevation") & cr.is_not_null() & ar.is_not_null() & (cr > ar))
        .fill_null(False)
        .alias("requires_manual_review"),
    )
    return ev.sort(["issue_date", "appointed_court", "appointee"])


# ───────────────────────────────────────────────────────── build: bench identity
def build_bench(
    roster: pl.DataFrame,
    appts: pl.DataFrame,
    salaries_present: set[str],
    hc: pl.DataFrame,
    aliases: dict[str, str],
) -> pl.DataFrame:
    ros = roster.with_columns(
        _keyed(roster["judge_name"].to_list(), aliases).alias("judge_key"),
        pl.col("court").replace_strict(COURT_RANK, default=None, return_dtype=pl.Int64)
        .fill_null(99)
        .alias("court_rank"),
    )

    # Resolve ex-officio cross-listings: one row per judge_key, prefer the substantive
    # (non ex-officio) seat, then the most senior court listed.
    ros = ros.sort(["judge_key", "is_ex_officio_or_multi", "court_rank"])
    raw_seats = ros.group_by("judge_key").len().select("judge_key", pl.col("len").cast(pl.Int64).alias("seat_count"))
    ident = ros.unique(subset=["judge_key"], keep="first", maintain_order=True).join(
        raw_seats, on="judge_key", how="inner", maintain_order="left"
    )

    # HC specialist-list assignment (Hilary Term 2026) by key.
    hc = hc.with_columns(_keyed(hc["judge"].to_list(), aliases).alias("judge_key"))
    hc_map = hc.unique(subset=["judge_key"], keep="first", maintain_order=True).select(
        "judge_key",
        pl.col("assignment").cast(pl.String),
        pl.col("term").cast(pl.String).alias("assignment_term"),
    )
    ident = ident.join(hc_map, on="judge_key", how="left", maintain_order="left")

    # Appointment spine rollup per judge.
    a = appts.sort("issue_date")
    spine_rows = []
    for (key,), g in a.group_by("judge_key", maintain_order=True):
        current = g["current_court"].drop_nulls()
        sources = g["source_url"].drop_nulls()
        path = []
        for c in list(g["appointed_court"]) + [current[-1] if len(current) else None]:
            if c and (not path or path[-1] != c):
                path.append(c)
        spine_rows.append(
            {
                "judge_key": key,
                "first_appointed_date": g["issue_date"][0],
                "first_appointing_authority": g["appointing_authority"][0],
                "appointed_court": g["appointed_court"][0],
                "is_elevation": bool(g["is_elevation"].any()),
                "elevation_path": " → ".join(path) if len(path) > 1 else None,
                "appt_source_url": sources[0] if len(sources) else None,
                "appt_review": bool(g["requires_manual_review"].any()),
            }
        )
    sp = pl.DataFrame(
        spine_rows,
        schema={
            "judge_key": pl.String,
            "first_appointed_date": pl.String,
            "first_appointing_authority": pl.String,
            "appointed_court": pl.String,
            "is_elevation": pl.Boolean,
            "elevation_path": pl.String,
            "appt_source_url": pl.String,
            "appt_review": pl.Boolean,
        },
    )
    ident = ident.join(sp, on="judge_key", how="left", maintain_order="left")
    ident = ident.with_columns(pl.col("judge_key").is_in(appts["judge_key"].to_list()).alias("has_spine"))

    # Salary band — ordinary-judge band by court; suppressed for ex-officio/president
    # seats (their premium can't be attributed to a named person from the roster alone).
    band = pl.col("court").replace_strict(SALARY_BY_COURT, default=None, return_dtype=pl.Int64)
    office = pl.col("court").replace_strict(SALARY_OFFICE_BY_COURT, default=None, return_dtype=pl.String)
    ident = ident.with_columns(
        pl.when(pl.col("is_ex_officio_or_multi"))
        .then(None)
        .otherwise(band)
        .cast(pl.Float64)  # matches the historical pandas gold schema (None-promoted float)
        .alias("salary_band_eur"),
        pl.when(pl.col("is_ex_officio_or_multi"))
        .then(pl.lit("President / ex-officio (premium not attributed)"))
        .when(band.is_not_null())
        .then(office)
        .otherwise(None)
        .alias("salary_office"),
        pl.lit(SALARY_SOURCE).alias("salary_source"),
    )

    ident = ident.with_columns(
        pl.col("appt_review").fill_null(False).cast(pl.Boolean).alias("requires_manual_review"),
        pl.col("court").alias("current_court"),  # the roster is the current state of record
        pl.lit(ROSTER_SOURCE_URL).alias("source_url"),
        pl.lit(ROSTER_SNAPSHOT_DATE).alias("source_published_at"),
    )

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
    return ident.select(cols).sort(["court_rank", "judge_name"])


# ───────────────────────────────────────────────────────── build: nominations
def build_nominations(nom: pl.DataFrame, aliases: dict[str, str]) -> pl.DataFrame:
    # gov.ie retired the single judicial-appointments listing page (the old
    # /en/publication/judicial-appointments/ URL now 404s) and per-announcement
    # URLs were never captured in the sandbox scrape. A nominee-scoped gov.ie
    # search is the closest stable, per-row link to the announcement itself.
    urls = [
        GOVIE_SEARCH_URL + quote_plus(f"{nominee} {court}")
        for nominee, court in zip(
            nom["nominee"].fill_null("").to_list(), nom["target_court"].fill_null("").to_list(), strict=True
        )
    ]
    n = nom.with_columns(
        _keyed(nom["nominee"].to_list(), aliases).alias("judge_key"),
        pl.lit("gov.ie nomination announcement").alias("source_name"),
        pl.Series("source_url", urls, dtype=pl.String),
    )
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
    return n.select(cols).sort(["announce_date", "target_court", "nominee"])


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


def build_courts_clearance(clr: pl.DataFrame) -> pl.DataFrame:
    """Faithful clearance facts at source grain (jurisdiction × area × category × year).
    Casing normalised so the view can group a court; counts untouched. The view computes
    clearance_pct = resolved/incoming (which legitimately exceeds 100% when a court clears
    backlog — that is a real signal, never capped here or downstream)."""
    out = clr.select(
        pl.col("JURISDICTION").replace(_COURT_CANON).alias("jurisdiction"),
        pl.col("AREA_OF_LAW").alias("area_of_law"),
        pl.col("YEAR").cast(pl.Int64).alias("year"),
        pl.col("CATEGORY").alias("category"),
        pl.col("INCOMING").cast(pl.Int64).alias("incoming"),
        # RESOLVED carries nulls (unpublished cells) -> Float64, matching the historical
        # pandas gold schema (NaN-promoted float); values are untouched.
        pl.col("RESOLVED").cast(pl.Float64).alias("resolved"),
        pl.lit("Courts Service annual statistics").alias("source_name"),
        pl.lit(CLEARANCE_SOURCE_URL).alias("source_url"),
    )
    return out.sort(["year", "jurisdiction", "area_of_law", "category"])


def build_courts_waiting(wt: pl.DataFrame) -> pl.DataFrame:
    """Waiting-time lists as published (latest two years side by side), VERBATIM.
    The source PDF carries ligature-extraction artefacts on the deeper matter-type rows
    ('certfii ed', U+FFFD where an apostrophe/en-dash stood). These are NOT guessed at
    here — U+FFFD is ambiguous (apostrophe vs en-dash) and silently 'fixing' it would
    corrupt the text. The clean, headline rows are the named venues (Dublin…Limerick);
    the page curates to those, but gold keeps every published row faithfully. The
    is_clean_label flag marks rows free of the known artefacts for that curation.

    SECTION CONTEXT (jurisdiction + list_context): the table extraction dropped the
    report's section headings ("High Court: Possession", "Court of Appeal - Civil"…),
    leaving labels like "Full hearing" 4x with no court attached. The headings are
    restored from the hand-curated data/_meta/courts_waiting_context.csv (transcribed
    from the Annual Report pp.135-140 — never inferred), keyed by (page, row order)
    and VERIFIED against the published label so source drift fails loudly instead of
    mislabelling a court. The CSV also carries the Central Criminal Court rows
    (seq_in_page < 0) the regex extraction missed — that table publishes bare week
    numbers ("44") rather than "44 weeks" strings."""
    is_clean = ~pl.col("matter_or_venue").cast(pl.String).str.contains(r"[�]|fii|\bfi\s").fill_null(False)
    out = wt.select(
        pl.col("page").cast(pl.Int64),
        pl.col("matter_or_venue"),
        pl.col("wait_2024"),
        pl.col("wait_2023"),
        is_clean.alias("is_clean_label"),
        pl.lit("Courts Service Annual Report 2024 (Waiting Times)").alias("source_name"),
        pl.lit(WAITING_SOURCE_URL).alias("source_url"),
    ).with_columns(pl.int_range(pl.len()).over("page").alias("seq_in_page"))

    ctx = pl.read_csv(WAITING_CONTEXT_CSV, schema_overrides={"wait_2024": pl.String, "wait_2023": pl.String})
    mapped = ctx.filter(pl.col("seq_in_page") >= 0)
    out = out.join(
        mapped.select("page", "seq_in_page", "match_prefix", "jurisdiction", "list_context"),
        on=["page", "seq_in_page"],
        how="left",
        maintain_order="left",
    )
    # drift guard: a context row must agree with the published label it claims to describe
    bad = [
        {k: r[k] for k in ("page", "seq_in_page", "matter_or_venue", "match_prefix")}
        for r in out.iter_rows(named=True)
        if r["match_prefix"] is not None
        and not str(r["matter_or_venue"]).casefold().startswith(str(r["match_prefix"]).casefold())
    ]
    if bad:
        raise ValueError(
            "courts_waiting_context.csv no longer matches the extracted waiting-time rows "
            f"(re-curate it): {bad}"
        )
    out = out.drop("match_prefix")

    supplements = ctx.filter(pl.col("seq_in_page") < 0)
    if supplements.height:
        extra = supplements.select(
            pl.col("page").cast(pl.Int64),
            pl.col("match_prefix").alias("matter_or_venue"),
            pl.col("wait_2024"),
            pl.col("wait_2023"),
            pl.lit(True).alias("is_clean_label"),
            pl.lit("Courts Service Annual Report 2024 (Waiting Times)").alias("source_name"),
            pl.lit(WAITING_SOURCE_URL).alias("source_url"),
            pl.col("seq_in_page").cast(pl.Int64),
            pl.col("jurisdiction"),
            pl.col("list_context"),
        )
        out = pl.concat([out, extra], how="vertical")
    return out


def build_courthouses(ch: pl.DataFrame) -> pl.DataFrame:
    """Active, geocoded courthouses for the venue map (lat/lon + place metadata)."""
    out = ch.filter(pl.col("active_status") == "active").select(
        pl.col("court_house"),
        pl.col("court_house_address").alias("address"),
        pl.col("court_house_eircode").alias("eircode"),
        pl.col("region"),
        pl.col("county"),
        pl.col("circuit"),
        pl.col("latitude"),
        pl.col("longitude"),
        pl.lit("Courts Service court-office register").alias("source_name"),
        pl.lit(COURTHOUSE_SOURCE_URL).alias("source_url"),
    )
    out = out.filter(
        pl.col("latitude").is_not_null()
        & pl.col("latitude").is_not_nan()
        & pl.col("longitude").is_not_null()
        & pl.col("longitude").is_not_nan()
    )
    return out.sort("court_house")


# ───────────────────────────────────────────────────────────────────── main
def main() -> int:
    setup_standalone_logging("judiciary_bench_extract")
    aliases = _load_aliases()

    spine = pl.read_parquet(SANDBOX_DIR / "judicial_appointments_spine.parquet")
    roster = pl.read_parquet(SANDBOX_DIR / "judiciary_current_roster.parquet")
    join = pl.read_parquet(SANDBOX_DIR / "judiciary_appointment_roster_join.parquet")
    nom = pl.read_parquet(SANDBOX_DIR / "judicial_nominations_govie.parquet")
    hc = pl.read_parquet(SANDBOX_DIR / "judiciary_hc_assignments.parquet")
    salaries = pl.read_parquet(SANDBOX_DIR / "judicial_salaries.parquet")
    salaries_present = set(salaries["office"].to_list())
    clearance_raw = pl.read_parquet(SANDBOX_DIR / "courts_clearance.parquet")
    waiting_raw = pl.read_parquet(SANDBOX_DIR / "courts_waiting_times.parquet")
    courthouses_raw = pl.read_parquet(SANDBOX_DIR / "courthouses.parquet")

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
