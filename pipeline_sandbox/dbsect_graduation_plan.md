# dbsect / debates — graduation & implementation plan

**Status:** ready to implement. Supersedes the *to-do* sections of
`dbsect_harvest_migration.md` and `dbsect_integration_plan.md` — those
predate the `services/` wiring that already exists (see "Current state").

**Audience:** an implementing Claude session. Every step gives exact files,
line anchors, and full content for new files.

---

## Context

Debate data is the record of what each TD actually *said* on the floor.
The Oireachtas exposes it in three layers:

1. **dbsect index** — every debate section ever referenced by a bill,
   question, or vote. A worklist. (`dbsect_harvest.py`)
2. **day-window listings** — per sitting day, every section with its
   speech/speaker counts, parent section, bill link. Structure, no text.
   (`dbsect_listings_flatten.py` → `v_debate_listings`)
3. **AKN XML** — the actual `<speech by="#TD">` contributions. The only
   layer with the words. (Stage 2 — Part B below.)

This plan delivers **Part A** (layers 1–2) as one low-risk PR, then
specifies Part B/C/D (the speech content, app, enrichment) as the roadmap.

---

## Current state — what is already wired

A previous session built the `services/` half. Verified present:

- `services/oireachtas_api_main.py` — `_load_debates_worklist()` reads
  `dbsect_index.parquet`; **STEP 5** fetches day-window listings via
  `run_member_scenario("debates_listings", …)`.
- `services/urls.py` — `build_debates_day_urls()` implemented.
- `services/storage.py` — `debates_listings` scenario →
  `DEBATES_LISTINGS_DIR / "debates_listings_results.json"`.
- `services/dail_config.py` — `DEBATES_DIR`, `DEBATES_LISTINGS_DIR`,
  `AKN_DIR` declared.
- `sql_views/v_debate_listings.sql` — written, reads
  `data/silver/parquet/debate_listings.parquet`.

**The gap:** `dbsect_harvest.py` and `dbsect_listings_flatten.py` are
still in `pipeline_sandbox/` and never run. So `dbsect_index.parquet` is
stale (last produced by a manual run), STEP 5 silently no-ops on an empty
worklist, `debate_listings.parquet` is never produced, and
`v_debate_listings.sql` fails to load against a missing file.

**Ordering constraint** (drives the whole design): the harvester must run
*inside* the "Members API" pipeline step — after the legislation/
questions/votes JSONs are fetched (`oireachtas_api_main` STEPs 3–4) and
before the debate-listings fetch (STEP 5).

> Note on the sandbox rule: the project convention is "new work →
> `pipeline_sandbox/`, don't touch `pipeline.py`". This plan deliberately
> *graduates* validated sandbox work into production — that is the
> sanctioned exception, and the user has explicitly requested it.

> Note on the rewrite: the sandbox scripts flatten nested JSON with
> hand-rolled Python loops. The project's established idiom is
> `pandas.json_normalize` with `record_path` + `meta`
> ([legislation.py:53-58](../legislation.py#L53-L58) flattens
> `bill.debates[]` exactly this way). Both scripts below are **rewritten**
> to that idiom — fewer functions, no data-processing loops, dtype/output
> consistent with the sibling flattener `legislation.py`. The cost is one
> rewrite-equivalence check (gate A8.1b).

---

# PART A — implementable PR

Five files touched, two new. Fully additive: no existing output file is
deleted, no existing step is removed.

## A0. Baseline capture — run this BEFORE any edit

The regression check (gate A8.9) needs a "before" snapshot. Run this on
a clean checkout *first* and keep `dbsect_baseline.json`:

```bash
python pipeline.py 2>&1 | tee dbsect_baseline_pipeline.log
python -c "
import json, glob, duckdb
snap = {}

# (a) silver + gold output row counts
snap['outputs'] = {}
for p in sorted(glob.glob('data/silver/parquet/*.parquet')
                 + glob.glob('data/gold/parquet/*.parquet')):
    try:
        snap['outputs'][p] = duckdb.execute(
            f\"SELECT COUNT(*) FROM read_parquet('{p}')\").fetchone()[0]
    except Exception as e:
        snap['outputs'][p] = f'ERR {e}'

# (b) SQL view load OK/fail split
ok = fail = 0
for f in sorted(glob.glob('sql_views/*.sql')):
    try:
        con = duckdb.connect()
        con.execute(open(f, encoding='utf-8', errors='ignore').read())
        ok += 1
    except Exception:
        fail += 1
snap['sql_views'] = {'ok': ok, 'fail': fail}

json.dump(snap, open('dbsect_baseline.json', 'w'), indent=2)
print('baseline written:', snap['sql_views'], len(snap['outputs']), 'outputs')
"
```

Also note from `dbsect_baseline_pipeline.log` the list of steps under
`Succeeded (.../...)` — that is the "before" succeeded set.

## A1. New file — `services/dbsect_harvest.py`

Graduated from `pipeline_sandbox/dbsect_harvest.py`, **rewritten** to the
project flattening idiom: `pandas.json_normalize` with `record_path` +
`meta`, exactly as `legislation.py` flattens `bill.debates[]` into
`debates.parquet`. This removes the sandbox version's three nested Python
loops and hand-built `dict` rows; vectorised `pandas` string ops replace
the per-row `_chamber_short` / `_norm_dbsect` helpers. `pandas` (not
`polars`) is used deliberately — it matches `legislation.py`, the sibling
flattener writing to the same `silver/parquet/` directory.

It reads `legislation_results.json` (the per-TD file), **not** the
unscoped file — see "Deferred enhancements".

Full file content:

```python
"""
services/dbsect_harvest.py

Local-only harvester. Flattens the bronze JSON already fetched for
legislation, questions, and votes into a deduplicated index of every
distinct dbsect_* identifier seen, with its provenance.

No API calls. Reads bronze JSON, writes one silver parquet:
  data/silver/parquet/dbsect_index.parquet

Schema (one row per (debate_section_id, source, source_key)):
  debate_section_id : str   e.g. 'dbsect_12'
  source            : str   'bill' | 'question' | 'vote'
  source_key        : str   bill_id | question_uri | vote_id
  date              : str   ISO date string, nullable
  chamber           : str   'dail' | 'seanad' | ''
  debate_uri        : str   raw debate.uri, nullable
  debate_title      : str   showAs text, nullable

Flattening uses pandas.json_normalize (record_path + meta), mirroring
legislation.py's silver flattener — the project idiom for turning nested
Oireachtas bronze JSON into tabular silver. dbsect ids are per-day, not
global: composite identity is (date, chamber, debate_section_id);
downstream joins must respect that.

Graduated from pipeline_sandbox/dbsect_harvest.py. Called by
services/oireachtas_api_main.main() (STEP 4.5).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from services.dail_config import (
    LEGISLATION_DIR,
    QUESTIONS_DIR,
    SILVER_PARQUET_DIR,
    VOTES_DIR,
)

logger = logging.getLogger(__name__)

_LEG_JSON = LEGISLATION_DIR / "legislation_results.json"
_QUE_JSON = QUESTIONS_DIR / "questions_results.json"
_VOT_JSON = VOTES_DIR / "votes_results.json"
_OUT = SILVER_PARQUET_DIR / "dbsect_index.parquet"

_SCHEMA = [
    "debate_section_id", "source", "source_key",
    "date", "chamber", "debate_uri", "debate_title",
]


def _records(path: Path) -> list[dict]:
    """Load a bronze results JSON and concatenate every page's `results`
    into one flat list of records. The page-concat is the only loop —
    it mirrors legislation.py; the flattening itself is json_normalize."""
    if not path.exists():
        logger.warning("dbsect_harvest: %s not found — skipping", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [r for page in raw for r in (page.get("results") or [])]


def _chamber_from_uri(uri: pd.Series) -> pd.Series:
    """Vectorised 'dail'/'seanad'/'' extraction from a house/chamber URI.

    Both observed URI shapes — `.../house/dail` and `.../house/dail/34`
    — carry the chamber as the segment right after `house/`. Committee
    URIs have no `house/<chamber>` segment and resolve to ''."""
    return (
        uri.astype("string")
        .str.extract(r"house/(dail|seanad)(?:/|$)", expand=False)
        .fillna("")
    )


def _norm_dbsect(col: pd.Series) -> pd.Series:
    """Vectorised: ensure every id carries the `dbsect_` prefix; blanks → NA."""
    s = col.astype("string").str.strip()
    s = s.mask(s == "", pd.NA)
    return s.where(s.isna() | s.str.startswith("dbsect_"), "dbsect_" + s)


def harvest_bills(records: list[dict]) -> pd.DataFrame:
    """One row per bill→debate edge. record_path/meta mirror legislation.py."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(
        records,
        record_path=["bill", "debates"],
        meta=[["bill", "billYear"], ["bill", "billNo"]],
        errors="ignore",
    )
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    df = df.dropna(subset=["bill.billYear", "bill.billNo"])
    return pd.DataFrame({
        "debate_section_id": _norm_dbsect(df["debateSectionId"]),
        "source": "bill",
        "source_key": (df["bill.billYear"].astype("Int64").astype("string")
                       + "_" + df["bill.billNo"].astype("Int64").astype("string")),
        "date": df["date"].astype("string"),
        "chamber": _chamber_from_uri(df["chamber.uri"]),
        "debate_uri": df["uri"].astype("string"),
        "debate_title": df["showAs"].astype("string"),
    })


def harvest_questions(records: list[dict]) -> pd.DataFrame:
    """One row per question. debateSection is a single object, not an
    array — a flat json_normalize (no record_path) is the right call."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(records, errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    return pd.DataFrame({
        "debate_section_id": _norm_dbsect(df["question.debateSection.debateSectionId"]),
        "source": "question",
        "source_key": df["question.uri"].astype("string"),
        "date": df["question.date"].astype("string"),
        "chamber": _chamber_from_uri(df["question.house.uri"]),
        "debate_uri": df["question.debateSection.uri"].astype("string"),
        "debate_title": df["question.debateSection.showAs"].astype("string"),
    })


def harvest_votes(records: list[dict]) -> pd.DataFrame:
    """One row per division. division.debate.debateSection is the bare
    dbsect id string."""
    if not records:
        return pd.DataFrame(columns=_SCHEMA)
    df = pd.json_normalize(records, errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=_SCHEMA)
    return pd.DataFrame({
        "debate_section_id": _norm_dbsect(df["division.debate.debateSection"]),
        "source": "vote",
        "source_key": df["division.voteId"].astype("string"),
        "date": df["division.date"].astype("string"),
        "chamber": _chamber_from_uri(df["division.chamber.uri"]),
        "debate_uri": df["division.debate.uri"].astype("string"),
        "debate_title": df["division.debate.showAs"].astype("string"),
    })


def harvest_dbsect_index() -> int:
    """Harvest dbsect identifiers from bronze JSON into dbsect_index.parquet.

    Returns the number of rows written (0 if no bronze JSON was found).
    """
    logger.info("dbsect_harvest: legislation=%s", _LEG_JSON)
    logger.info("dbsect_harvest: questions  =%s", _QUE_JSON)
    logger.info("dbsect_harvest: votes      =%s", _VOT_JSON)

    df = pd.concat(
        [
            harvest_bills(_records(_LEG_JSON)),
            harvest_questions(_records(_QUE_JSON)),
            harvest_votes(_records(_VOT_JSON)),
        ],
        ignore_index=True,
    )
    df = df.dropna(subset=["debate_section_id"])
    df = df.drop_duplicates(subset=["debate_section_id", "source", "source_key"])

    if df.empty:
        logger.warning(
            "dbsect_harvest: no rows harvested — bronze fetches may not have run"
        )
        return 0

    counts = df.groupby("source")["debate_section_id"].agg(
        rows="size", distinct_dbsect="nunique"
    )
    logger.info("dbsect_harvest: counts by source\n%s", counts)
    logger.info("dbsect_harvest: distinct dbsect total=%d",
                df["debate_section_id"].nunique())

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_OUT, index=False)
    logger.info("dbsect_harvest: wrote %s (%d rows)", _OUT, len(df))
    return len(df)


if __name__ == "__main__":
    from services.logging_setup import setup_logging
    setup_logging()
    harvest_dbsect_index()
```

Notes for the implementer:
- The bill `record_path=["bill","debates"]` + `meta` call is copied
  field-for-field from `legislation.py` — proven against the same bronze.
- The harvester accesses debate-level columns (`date`, `uri`, `showAs`,
  `chamber.uri`, `debateSectionId`) directly, exactly as `legislation.py`
  does — these are core fields always present once any debate exists.
  The `if df.empty` guard covers the no-debates case.

## A2. Edit — `services/dail_config.py`

The graduated harvester needs `SILVER_PARQUET_DIR`; `dail_config.py` is
bronze-only today. Add the silver constants after the bronze block
(after the `AKN_DIR` line, before `LOG_DIR`):

```python
SILVER_DIR = DATA_DIR / "silver"
SILVER_PARQUET_DIR = SILVER_DIR / "parquet"
```

And add both to the directory-creation loop (`for path in [ … ]`):

```python
    SILVER_DIR,
    SILVER_PARQUET_DIR,
```

## A3. Edit — `services/oireachtas_api_main.py`

Insert the harvester call between STEP 4 (votes) and STEP 5 (debate
listings). Two edits:

**(a)** Add to the imports at the top of the file:

```python
from services.dbsect_harvest import harvest_dbsect_index
```

**(b)** In `main()`, immediately after `run_votes(overwrite=overwrite_votes)`
and *before* the `STEP 5` logger lines, insert:

```python
    logger.info("=" * 70)
    logger.info("STEP 4.5: Harvesting dbsect index from bronze")
    logger.info("=" * 70)
    # Contain a harvester failure: a malformed bronze JSON must not abort
    # the whole Members API step (and with it STEP 5). Degrade to "no
    # debate listings this run" — matches the DAIL-163 continue-past-
    # failure philosophy in pipeline.py.
    try:
        n_dbsect = harvest_dbsect_index()
        logger.info(f"dbsect index harvested: {n_dbsect} rows")
    except Exception as e:
        logger.error(
            f"dbsect harvest failed (debate listings will be skipped): {e}"
        )
```

This makes STEP 5's `_load_debates_worklist()` see a fresh
`dbsect_index.parquet`. The `_load_debates_worklist()` warning comment
about "run pipeline_sandbox/dbsect_harvest.py first" is now stale —
update it to reference STEP 4.5.

> Why the `try/except`: `harvest_dbsect_index()` returns 0 gracefully on
> *missing* bronze files, but an unexpected *exception* (malformed JSON,
> pandas error) called bare here would propagate out of `main()`, fail
> the entire "Members API" pipeline step, and STEP 5 would never run.
> Containing it limits the blast radius to the debate feature alone.

## A4. New file — `dbsect_listings_flatten.py` (project root)

Graduated from `pipeline_sandbox/dbsect_listings_flatten.py`,
**rewritten** to the same `json_normalize` idiom. The sandbox version's
`flatten_listings` loop and row-level helpers (`_bill_ref`,
`_akn_xml_url`, `_debate_url_web`, `_chamber_short`) collapse into one
two-level `json_normalize` call (`record_path=["debateSections"]` +
record-level `meta`) and vectorised `pandas` string ops.

> Caveat — unlike the harvester, this script cannot be validated yet:
> `debates_listings_results.json` does not exist until STEP 5 first
> runs. Two structural unknowns, both handled defensively:
> 1. Each `debateSections[]` element may be wrapped
>    (`{"debateSection": {…}}`) or flat (`{…}`) — the sandbox author
>    hedged with `s.get("debateSection") or s`. The rewrite strips a
>    leading `debateSection.` from normalised column names (one
>    vectorised op), which handles either shape.
> 2. Optional keys (`bill`, `parentDebateSection`, `formats.xml`) are
>    omitted by `json_normalize` when no record carries them — the
>    `_col()` accessor returns an all-NA Series for absent columns.
> Confirm both on the first real bronze file (gate A8.6).

Full file content:

```python
"""
dbsect_listings_flatten.py

Flattens the bronze day-window debate-listing JSON into one silver
parquet row per (date, chamber, debate_section_id) — Stage 1 of the
debates integration (see pipeline_sandbox/dbsect_integration_plan.md §3).
Structural only: no AKN fetch, no speech parsing, no member resolution.

Input  (read-only):
  data/bronze/debates/listings/debates_listings_results.json
Output:
  data/silver/parquet/debate_listings.parquet

Flattening uses pandas.json_normalize (record_path + meta), the project
idiom shared with legislation.py and services/dbsect_harvest.py.
Composite identity is (date, chamber, debate_section_id); dbsect_2
recurs every sitting day, so never join on debate_section_id alone.

Run standalone (after the debates_listings scenario has produced bronze):
  python dbsect_listings_flatten.py
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from config import BRONZE_DIR, SILVER_PARQUET_DIR

logger = logging.getLogger(__name__)

_BRONZE = BRONZE_DIR / "debates" / "listings" / "debates_listings_results.json"
_OUT = SILVER_PARQUET_DIR / "debate_listings.parquet"

_AKN_BASE = "https://data.oireachtas.ie/akn/ie/debateRecord"
_WEB_BASE = "https://www.oireachtas.ie/en/debates/debate"


def _debate_records(path: Path) -> list[dict]:
    """Load bronze and return a flat list of debateRecord objects across
    all day-window pages. The page-concat is the only loop."""
    if not path.exists():
        logger.warning("dbsect_listings_flatten: %s not found", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return [
        r["debateRecord"]
        for page in raw
        for r in (page.get("results") or [])
        if isinstance(r.get("debateRecord"), dict)
    ]


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Return df[name] as a string Series, or an all-NA string Series if
    the column is absent — json_normalize omits columns whose key never
    appears in any record."""
    if name in df.columns:
        return df[name].astype("string")
    return pd.Series(pd.NA, index=df.index, dtype="string")


def flatten_listings(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(
        records,
        record_path=["debateSections"],
        meta=["date", ["house", "houseCode"], ["house", "chamberType"],
              ["chamber", "uri"]],
        errors="ignore",
    )
    if df.empty:
        return pd.DataFrame()

    # debateSections elements may be wrapped ({"debateSection": {...}}) or
    # flat ({...}); strip the prefix so both shapes yield the same columns.
    df.columns = df.columns.str.replace(r"^debateSection\.", "", regex=True)

    # chamber: prefer house.houseCode, fall back to the last chamber.uri
    # segment; committee records resolve to '' and are dropped below.
    house_code = _col(df, "house.houseCode")
    uri_tail = (_col(df, "chamber.uri")
                .str.rstrip("/").str.rsplit("/", n=1).str[-1])
    chamber = house_code.where(house_code.isin(["dail", "seanad"]), uri_tail)
    chamber = chamber.where(chamber.isin(["dail", "seanad"]), "")
    chamber = chamber.mask(_col(df, "house.chamberType") == "committee", "")

    dbsect = _col(df, "debateSectionId")
    date = _col(df, "date")

    # bill_ref '<year>_<no>' from bill.uri, falling back to bill.event.uri.
    m_uri = _col(df, "bill.uri").str.extract(r"/bill/(\d+)/(\d+)")
    m_evt = _col(df, "bill.event.uri").str.extract(r"/bill/(\d+)/(\d+)")
    bill_ref = (m_uri[0].fillna(m_evt[0]) + "_" + m_uri[1].fillna(m_evt[1]))

    constructed_akn = (_AKN_BASE + "/" + chamber + "/" + date
                       + "/debate/mul@/" + dbsect + ".xml")
    akn = _col(df, "formats.xml.uri").fillna(constructed_akn)

    out = pd.DataFrame({
        "debate_section_id": dbsect,
        "date": date,
        "chamber": chamber,
        "parent_section_id": _col(df, "parentDebateSection.debateSectionId"),
        "parent_section_title": _col(df, "parentDebateSection.showAs"),
        "bill_ref": bill_ref,
        "debate_type": _col(df, "debateType"),
        "speaker_count": pd.to_numeric(
            _col(df, "counts.speakerCount"), errors="coerce").fillna(0).astype(int),
        "speech_count": pd.to_numeric(
            _col(df, "counts.speechCount"), errors="coerce").fillna(0).astype(int),
        "akn_xml_url": akn,
        "debate_url_web": _WEB_BASE + "/" + chamber + "/" + date + "/" + dbsect + "/",
        "show_as": _col(df, "showAs"),
    })
    out = out[(out["chamber"] != "") & out["debate_section_id"].notna()]
    return out.drop_duplicates(subset=["date", "chamber", "debate_section_id"])


def run() -> int:
    df = flatten_listings(_debate_records(_BRONZE))
    if df.empty:
        logger.warning("dbsect_listings_flatten: no rows — run the "
                        "debates_listings scenario first")
        return 0
    logger.info("dbsect_listings_flatten: rows=%d distinct_dbsect=%d distinct_dates=%d",
                len(df), df["debate_section_id"].nunique(), df["date"].nunique())
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_OUT, index=False)
    logger.info("dbsect_listings_flatten: wrote %s", _OUT)
    return len(df)


if __name__ == "__main__":
    run()
```

## A5. Edit — `pipeline.py`

`dbsect_listings_flatten.py` runs *after* the whole "Members API" step
(it consumes `debates_listings_results.json`, which STEP 5 of the API
main produces). Add one entry to `STEPS`, immediately after the
`("Members API", "dummy_value")` line:

```python
    ("Flatten debate listings", "dbsect_listings_flatten.py"),
```

No other `pipeline.py` change. It runs as a `subprocess` like every
other script step; a failure is caught by the existing per-step
`try/except` and reported in the run summary.

## A6. SQL views

- `sql_views/v_debate_listings.sql` — **no change**. It already reads
  `debate_listings.parquet`; once A4/A5 produce that file, the view
  loads cleanly (this fixes one of the 9 currently-failing view loads).
- `sql_views/legislation_debates.sql` — **OPTIONAL, defer**. The
  `v_debate_listings.sql` header says it will eventually replace the
  bill-only `debates.parquet`. Leaving both views in place is harmless
  and keeps Part A's blast radius minimal. Do the refactor in a separate
  PR once `v_debate_listings` has been stable for a refresh cycle.

## A7. Sandbox cleanup (after the PR merges and gates pass)

- Delete `pipeline_sandbox/dbsect_harvest.py` and
  `pipeline_sandbox/dbsect_listings_flatten.py` — superseded by the
  graduated copies. **Do this only after gate A8.1b confirms the
  rewrite is equivalent** — the sandbox harvester is the equivalence
  oracle, so keep it until then.
- Keep `dbsect_harvest_migration.md`, `dbsect_integration_plan.md`,
  `dbsect_probe_findings.md`, and this file as history.

## A8. Validation gates — run before declaring Part A done

```bash
# 1. Harvester runs and is deterministic
python -m services.dbsect_harvest
python -m services.dbsect_harvest        # 2nd run
#   → both runs log identical "distinct dbsect total".

# 1b. REWRITE-EQUIVALENCE — the json_normalize rewrite must produce the
#     same index as the sandbox original on the same bronze. Run the old
#     sandbox harvester, snapshot its output, run the new one, diff.
python pipeline_sandbox/dbsect_harvest.py
python -c "import shutil; shutil.copy('data/silver/parquet/dbsect_index.parquet','dbsect_index_old.parquet')"
python -m services.dbsect_harvest
python -c "
import pandas as pd
key = ['debate_section_id','source','source_key']
old = pd.read_parquet('dbsect_index_old.parquet')
new = pd.read_parquet('data/silver/parquet/dbsect_index.parquet')
o = set(map(tuple, old[key].fillna('').astype(str).values))
n = set(map(tuple, new[key].fillna('').astype(str).values))
print('rows old/new:', len(old), len(new))
print('only in old:', len(o - n), '| only in new:', len(n - o))
assert o == n, 'REWRITE CHANGED THE INDEX — investigate before merging'
print('EQUIVALENT')
"
#   → 'EQUIVALENT'. Then delete dbsect_index_old.parquet.

# 2. No null/blank debate_section_id
python -c "import duckdb; print(duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('data/silver/parquet/dbsect_index.parquet') WHERE debate_section_id IS NULL OR debate_section_id=''\").fetchone())"
#   → (0,)

# 3. Gate 4 (reframed): per-TD bill-slice dbsects are a SUBSET of the
#    unscoped bill-debate dbsects in debates.parquet. (The old doc said
#    "equal"; that is now obsolete — legislation.py reads the unscoped
#    feed (~1,621 bills) while the harvester reads the per-TD feed
#    (~642 bills); per-TD bills are a strict subset.)
python -c "
import duckdb
h = set(r[0] for r in duckdb.execute(\"SELECT DISTINCT debate_section_id FROM read_parquet('data/silver/parquet/dbsect_index.parquet') WHERE source='bill'\").fetchall())
d = set(r[0] for r in duckdb.execute(\"SELECT DISTINCT 'dbsect_'||replace(debateSectionId,'dbsect_','') FROM read_parquet('data/silver/parquet/debates.parquet')\").fetchall())
print('harvester bill dbsects not in debates.parquet:', sorted(h - d))   # → []
"

# 4. Full API main produces a non-empty debates worklist + listings JSON
python -c "from services.oireachtas_api_main import main; main()"
#   → logs "dbsect index harvested: N rows" (N>0) and STEP 5 builds
#     ~700 day-window URLs instead of 0.

# 5. Listings flatten produces the parquet
python dbsect_listings_flatten.py
#   → writes data/silver/parquet/debate_listings.parquet

# 6. v_debate_listings loads and is composite-unique; spot-check the
#    wrapped/flat debateSection shape resolved correctly (non-null
#    debate_section_id, sane speaker_count/speech_count).
python -c "
import duckdb
duckdb.execute(open('sql_views/v_debate_listings.sql').read())
print('rows:', duckdb.sql('SELECT COUNT(*) FROM v_debate_listings').fetchone())
print('null dbsect:', duckdb.sql('SELECT COUNT(*) FROM v_debate_listings WHERE debate_section_id IS NULL').fetchone())
print('dupe (date,chamber,dbsect):', duckdb.sql('SELECT COUNT(*) FROM (SELECT debate_date,chamber,debate_section_id,COUNT(*) n FROM v_debate_listings GROUP BY 1,2,3 HAVING n>1)').fetchone())
"
#   → null dbsect 0, dupe count 0

# 7. Spot-check: pick 3 dbsect ids, open the built oireachtas.ie URL,
#    confirm the page title matches the stored debate_title / show_as.

# 8. Full pipeline smoke
python pipeline.py 2>&1 | tee dbsect_after_pipeline.log
#   → "Flatten debate listings" appears in the succeeded list.

# 9. REGRESSION DIFF — the existing ETL must not be broken. Compares the
#    post-implementation state against the A0 baseline. Asserts:
#      - every step that succeeded before still succeeds (the new step
#        may be added; none may be lost),
#      - no existing output parquet has a LOWER row count than before
#        (equal or higher is fine — Part A only adds data),
#      - the SQL view layer is no worse — fail count must not increase
#        (it should drop by 1: v_debate_listings now loads).
python -c "
import json, glob, re, duckdb
base = json.load(open('dbsect_baseline.json'))

# (a) outputs — no regression in row counts of pre-existing files
bad = []
for p, before in base['outputs'].items():
    try:
        after = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('{p}')\").fetchone()[0]
    except Exception as e:
        after = f'ERR {e}'
    if isinstance(before, int) and isinstance(after, int) and after < before:
        bad.append(f'{p}: {before} -> {after}')
    elif isinstance(before, int) and not isinstance(after, int):
        bad.append(f'{p}: {before} -> {after}')
print('output regressions:', bad or 'none')

# (b) SQL views — fail count must not rise
ok = fail = 0
for f in sorted(glob.glob('sql_views/*.sql')):
    try:
        con = duckdb.connect()
        con.execute(open(f, encoding='utf-8', errors='ignore').read())
        ok += 1
    except Exception:
        fail += 1
print(f\"sql views before: {base['sql_views']}  after: {{'ok': {ok}, 'fail': {fail}}}\")
assert fail <= base['sql_views']['fail'], 'SQL view layer regressed'
assert not bad, 'an existing output regressed — investigate before merging'
print('REGRESSION CHECK PASSED')
"
#    Then diff the succeeded-step lists by eye:
#      grep -A40 'Succeeded' dbsect_baseline_pipeline.log
#      grep -A40 'Succeeded' dbsect_after_pipeline.log
#    Every step in the baseline list must still be in the after list.
```

If gate 1, 1b, 2, 6, or 9 fails, do not merge — gate 9 specifically
guards the existing ETL; a failure there means Part A broke something
upstream of the debate work.

## A9. Tests (optional — the test step is commented out in `pipeline.py`)

If the test suite is being maintained, add to `test/test_silver_parquet.py`
a row-count assert for `dbsect_index.parquet` (~123k rows) and
`debate_listings.parquet`, and to `test/test_sql_views.py` a smoke
`SELECT` against `v_debate_listings`.

---

# PART B — Stage 2: the speech content (roadmap)

Part A delivers *structure* — which sections existed, their speech/
speaker counts, their bill/question/vote links. It does **not** deliver
the words. Those live only in Akoma Ntoso (AKN) XML, one file per debate
section. Stage 2 is a separate initiative; specified here so Part A is
built with the right seams.

```
debate_listings.parquet (akn_xml_url column)
  → services/akn_fetcher.py     → bronze/debates/akn/<chamber>/<date>/<dbsect>.xml
  → debate_speech_parse.py      → silver/parquet/speeches.parquet
  → debate_speech_resolve.py    → silver/parquet/speech_member_link.parquet
  → debate_aggregate.py         → gold/parquet/member_debates.parquet
```

**New files & responsibilities:**

| File | Does | Key constraints |
|---|---|---|
| `services/akn_fetcher.py` | Fetch each `akn_xml_url`, cache-by-existence (`if path.exists(): skip`). | **Mandatory** browser `User-Agent` + `Referer: https://www.oireachtas.ie/` headers — AKN returns 403 without them (proven in `dbsect_probe_findings.md`). Throttle ~250 ms. First run ≈ 3,008 calls ≈ 12 min, paid once (debates are immutable). |
| `debate_speech_parse.py` | One `lxml` pass per XML → one row per `<speech>`. | Store `char_count` + ~200-char `first_words`, **not** full text — keeps silver small, avoids re-hosting an attributable corpus. Row schema: `debate_section_id, date, chamber, speech_index, speaker_token, paragraph_count, char_count, first_words`. |
| `debate_speech_resolve.py` | Map `<speech by="#FirstnameLastname">` token → `unique_member_code`. | Two-pass: exact slug match vs `flattened_members`, then fuzzy surname + chamber + sitting-date fallback (reuse `normalise_join_key`). Unmatched (1–3%, mostly fada) → `speech_member_link_unresolved.parquet`, surfaced never dropped. |
| `debate_aggregate.py` | Roll up to `gold/parquet/member_debates.parquet`, one row per (member, debate_section). | Filter `writtens` sections — they carry a *minister's* reply, not the questioning TD's speech; do not credit them as TD contributions. |

`member_debates.parquet` schema: `unique_member_code, year,
debate_section_id, date, chamber, debate_title, parent_section_title,
bill_ref, debate_type, speech_count, char_count_total,
oireachtas_debate_url`.

**Deployment / storage split** — only the gold table ships to Streamlit
Cloud:

| Layer | Path | On Cloud? |
|---|---|---|
| bronze AKN XML | `bronze/debates/akn/**` | no — build machine only |
| silver speeches / member_link | `silver/parquet/` | no |
| **gold `member_debates.parquet`** | `gold/parquet/` | **yes (<1 MB)** |

CI: persist the AKN XML pool in an orphan `xml-cache` branch (durable,
free, survives runner rotation).

Pipeline wiring (Stage 2): `akn_fetcher` is called inside
`oireachtas_api_main` after STEP 5; `debate_speech_parse`,
`debate_speech_resolve`, `debate_aggregate` become three new
`pipeline.py` STEPS after "Flatten debate listings".

---

# PART C — App integration

No new navigation page — debate data lands where the user already is.

**New SQL views:**
- `sql_views/v_member_debates.sql` — `member_debates.parquet` keyed on
  `unique_member_code`. For the TD page.
- `sql_views/v_member_scrutiny.sql` — `member_debates ⋈ sponsors` on
  `bill_ref`: "for each bill a TD sponsored, did they speak in its
  debate sections?" Pure SQL inner join, no new fetch.

**Pages** (obey memory rules: no `st.dataframe` on primary views, year
pills, two-stage flow):
- `utility/pages_code/member_overview.py` — a "Debate contributions"
  card section: year-pill filter, one card per debate (title, chamber,
  speech count, char-count), click → external `oireachtas.ie` link.
- `utility/pages_code/legislation.py` — a bill-page card driven by
  `v_member_scrutiny`: "this bill was debated N times; its sponsor
  spoke in M of them."

**Metric discipline:** rank contributions by `char_count_total`, never
`speech_count` — 200 procedural interjections must not outrank 5
substantive speeches.

---

# PART D — Enrichment with other datasets

Every join below is cheap SQL on data already in gold once Part B lands.
This is what turns a transcript into journalism.

| Cross | Question answered | Join key |
|---|---|---|
| Debate × Votes | Did the TD *speak* on the section they then *voted* on, or vote silently? | `(date, chamber, debate_section_id)` |
| Debate × Sponsorship | Do TDs show up to debate the bills they sponsor? | `bill_ref` |
| Debate × Questions | Did a TD's written question roll into a debate they then spoke in? | `member_debates.parent_section_id ⋈ dbsect_index` (source=question) |
| Debate × Attendance | High attendance + low contribution = "present but silent." | `(unique_member_code, year)` |
| Debate × Interests / Lobbying | **Highest-value:** did a TD speak on a topic where they hold a declared interest, or were lobbied by an org with a stake? Conflict-of-interest surfacing. | topic ⋈ `member_interests` / `most_lobbied_politicians` |

The interests/lobbying cross needs a **topic layer** on debates — the
`parent_section_title` is a crude proxy; real value needs keyword/NLP
classification of debate titles. That is a genuinely new capability:
treat it as Stage 3, out of scope for Parts A–C.

---

# Deferred enhancements (not in Part A)

- **Repoint the bill harvest at `legislation_results_unscoped.json`.**
  Would lift bill→debate coverage from ~642 to ~1,621 bills. *Not done in
  Part A* because the unscoped JSON has a different on-disk shape
  (`{"results": [[bill,…],…]}`) than the per-TD file (a flat list of
  page objects), so `_records()` / `harvest_bills` would need shape-
  handling and re-verification. Low payoff: the bill slice is ~80
  dbsects vs the ~3,007 from questions, so it is <3% of total debate
  coverage. Do it only alongside a shape-check, as its own change.
- `legislation_debates.sql` refactor onto `v_debate_listings` (A6).
- Stage 2 / 3 (Parts B, D-topic-layer).

---

# Risks & rollback

| Risk | Mitigation |
|---|---|
| The json_normalize rewrite changes harvester behaviour vs the sandbox original. | Gate A8.1b diffs old vs new output on identical bronze and asserts set-equality before merge. |
| First run of STEP 5 suddenly issues ~700 API calls (was 0). | Expected & polite (~30 s via the existing `fetch_all` pool). Documented here so it is not mistaken for a fault. |
| `overwrite_debates_listings = False` means new sitting days are not refetched once the JSON exists. | Same limitation as every other scenario; out of scope. Flip the flag or add incremental refresh later. |
| Harvester reads a bronze JSON that did not refresh. | `_records` logs a warning and returns `[]`; `harvest_dbsect_index()` returns 0; STEP 5 no-ops gracefully. No crash. |
| Listings `debateSections[]` wrapped/flat shape unknown until first bronze. | The `debateSection.`-prefix strip + `_col()` accessor handle both; gate A8.6 confirms on first real data. |
| `v_debate_listings` references a missing parquet before A5 runs. | Pre-existing — the SQL loader already catches per-file load failures (40 OK / 9 fail today). A5 fixes it. |

**Rollback:** revert the `pipeline.py` and `oireachtas_api_main.py`
edits; `rm data/silver/parquet/dbsect_index.parquet
data/silver/parquet/debate_listings.parquet`. No upstream data mutated,
no existing parquet schema changed, no API contract altered.
