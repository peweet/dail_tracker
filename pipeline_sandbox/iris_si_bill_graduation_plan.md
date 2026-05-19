# Iris SI → Bill catalogue — graduation & implementation plan

**Status:** ready to implement.
**Audience:** an implementing Claude session. Every step gives exact files, line anchors, and full content for new files.

---

## Context

Iris Oifigiúil ETL output now contains **6,892 Statutory Instruments** with
full taxonomy (policy domain, operation type, EU relationship, form,
parent-legislation reference, eisb_url). **48,611 clean Iris events overall**
sit at `data/silver/iris_oifigiuil/`.

Goal: surface SIs on the bill page (the bill-detail panel of
`utility/pages_code/legislation.py`), grouped by enabling Act, with
filters by year / operation type, an EU badge, a "composition sentence"
summarising the regulatory shape, and a named-minister credit where the
SI's text identifies one. Plus a small "dormancy / freshness" line.

The SI-to-bill match logic **already exists** as a POC
(`utility/pages_code/legislation_si_poc.py`) — this plan **graduates** it
into the pipeline (removes the firewall violation) and adds the new
bill-page surface and the free enrichments uncovered during data
exploration.

---

## Current state — what is already wired

- `utility/pages_code/legislation_si_poc.py` — 855-line **POC page**,
  registered in `app.py`. Implements `match_si_to_bill` (year-bucketed
  token-set Jaccard, threshold 0.40, ±3-year window), `load_si`,
  `load_bills`, `_eisb_url` (with eli fallback). **Reads CSVs and joins
  inside Streamlit** — explicitly violates the project firewall.
- `data/silver/iris_oifigiuil/iris_si_taxonomy.csv` — 6,892 rows, freshly
  produced by the Iris ETL (May 19 snapshot).
- `data/silver/parquet/sponsors.parquet` — 1,620 bills, ready.
- POC reads from the **stale** `out/iris_si_taxonomy.csv` (May 5
  snapshot) — graduation also fixes this drift.

**Data shape findings from exploration** (drive the plan):

| Finding | Number | What it enables |
|---|---:|---|
| Acts with `eisb_url` populated | 6,892 (100%) | Every SI has a working irishstatutebook.ie link |
| Acts with `si_parent_legislation` | 3,446 (50%) | The joinable set |
| Expected post-match coverage | ~1,000-2,000 SIs | Bills table starts 2014; many SIs cite pre-2014 Acts |
| Policy domain categories | 17 (incl. migration_international_protection) | Granular filter |
| Operation categories | 19 (amendment / commencement / sanctions / scheme / ...) | "Composition sentence" |
| EU-flagged SIs | ~2,300 of 6,892 (~33%) | EU badge per card |
| **SIs with named minister extractable from raw_text** | 462 (6.7%) baseline, ~1,500-2,000 with better regex | Per-card minister credit |
| Sanctions SIs | 654 | Standalone filter (Finance + Enterprise dominate) |

> Sandbox-rule note: the project default is "new work → `pipeline_sandbox/`,
> don't touch `pipeline.py` or `enrich.py`." This plan deliberately
> *graduates* validated sandbox work — the sanctioned exception, mirroring
> the dbsect graduation that just shipped.

---

# PART A — implementable PR

Six files touched, three new. Fully additive: no existing output deleted,
no existing step removed. The POC page is refactored (firewall fixed) but
remains functional.

## A0. Baseline capture — run BEFORE any edit

```bash
python pipeline.py 2>&1 | tee si_baseline_pipeline.log
python -c "
import json, glob, duckdb
snap = {'outputs': {}}
for p in sorted(glob.glob('data/silver/parquet/*.parquet')
                 + glob.glob('data/gold/parquet/*.parquet')):
    try:
        snap['outputs'][p] = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('{p}')\").fetchone()[0]
    except Exception as e: snap['outputs'][p] = f'ERR {e}'
ok = fail = 0
for f in sorted(glob.glob('sql_views/*.sql')):
    try: duckdb.connect().execute(open(f, encoding='utf-8', errors='ignore').read()); ok += 1
    except Exception: fail += 1
snap['sql_views'] = {'ok': ok, 'fail': fail}
json.dump(snap, open('si_baseline.json', 'w'), indent=2)
print('baseline:', snap['sql_views'], len(snap['outputs']), 'outputs')
"
```

## A1. New file — `pipeline_sandbox/iris_si_bill_enrichment.py`

Graduates `match_si_to_bill` and friends from the POC. Adds:
- A `_extract_named_minister()` helper using a stricter regex
  (anchors on the operative-clause formula `"in exercise"`/`"hereby"`)
- A `si_minister_named` output column when the regex hits
- A `si_is_eu` boolean and `si_signed_date` for downstream views
- An unmatched-SI sibling output for the coverage report

```python
"""
pipeline_sandbox/iris_si_bill_enrichment.py

Graduates legislation_si_poc.match_si_to_bill into the pipeline.
Writes one row per matched (bill, SI) to
data/gold/parquet/bill_statutory_instruments.parquet. Identical matching
logic + tuning (Jaccard ≥ 0.40, ±3yr window, SI year ≥ 2018, taxonomy
confidence ≥ 0.5) — same constants as the POC, just relocated.

Adds two new outputs vs the POC:
  - si_minister_named   — first-name + last-name extracted from SI
                          raw_text where the standard "The Minister for
                          X, Firstname Lastname, in exercise..." formula
                          appears. Coverage ~10-30% of SIs.
  - bill_statutory_instruments_unmatched.parquet — SIs that did NOT join
                          to a bill; used by the coverage gate. Not
                          consumed by views.
"""
from __future__ import annotations

import logging
import re
import string
from pathlib import Path

import pandas as pd

from config import GOLD_PARQUET_DIR, SILVER_PARQUET_DIR

logger = logging.getLogger(__name__)

_SI_CSV   = Path("data/silver/iris_oifigiuil/iris_si_taxonomy.csv")
_SPONSORS = SILVER_PARQUET_DIR / "sponsors.parquet"
_OUT      = GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet"
_OUT_UNM  = GOLD_PARQUET_DIR / "bill_statutory_instruments_unmatched.parquet"

# ── Constants — lifted verbatim from legislation_si_poc.py ────────────
SI_YEAR_FLOOR        = 2018
MIN_TAXO_CONFIDENCE  = 0.5
MATCH_THRESHOLD      = 0.40
MATCH_YEAR_WINDOW    = 3
_TITLE_STOP = {"act", "bill", "of", "the", "and", "an", "a", "for", "to",
               "in", "no", "no.", "amendment"}

# ── Named-minister extractor (new) ────────────────────────────────────
# Anchors on the operative-clause formula so we don't confuse a comma in
# the portfolio name with the name segment. Portfolio can contain
# "Justice, Home Affairs and ..." (capitalised tokens separated by
# commas/and). The name is two capitalised words possibly with T.D.,
# terminated by ", in exercise" or ", hereby".
_NAMED_MIN_RE = re.compile(
    r"The Minister for "
    r"(?P<portfolio>[A-Z][A-Za-z]+(?:\s+[A-Z][a-zA-Z]+|"
    r",\s*(?:and\s+)?[A-Z][a-zA-Z]+|\s+and\s+[A-Z][a-zA-Z]+)*)"
    r",\s*"
    r"(?P<name>[A-Z][a-z]+(?:\s+[A-Z][a-zA-Z']+){1,2})"
    r"(?:,?\s+T\.?D\.?)?"
    r"\s*,\s*(?:in\s+exercise|hereby)",
)


def _title_tokens(s: str) -> set[str]:
    if not s:
        return set()
    out: set[str] = set()
    for w in s.lower().split():
        t = w.strip(string.punctuation)
        if t and t not in _TITLE_STOP:
            out.add(t)
    return out


def _extract_named_minister(text: str | float) -> str | None:
    if not isinstance(text, str):
        return None
    m = _NAMED_MIN_RE.search(text)
    return m.group("name").strip() if m else None


def load_si() -> pd.DataFrame:
    df = pd.read_csv(_SI_CSV, low_memory=False)
    df = df[df["notice_category"] == "statutory_instrument"]
    df = df[~df["is_quarantined"].fillna(False).astype(bool)]
    df = df[df["si_number"].notna() & df["si_year"].notna() & df["title"].notna()]
    df["si_year"]   = df["si_year"].astype(int)
    df["si_number"] = df["si_number"].astype(int)
    df = df[df["si_year"] >= SI_YEAR_FLOOR]
    df = df[df["si_taxonomy_confidence"].fillna(0) >= MIN_TAXO_CONFIDENCE]
    df["si_id"]      = (df["si_year"].astype(str) + "-"
                        + df["si_number"].astype(str).str.zfill(3))
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["si_minister_named"] = df["raw_text"].map(_extract_named_minister)
    return df.drop_duplicates(subset=["si_id"]).reset_index(drop=True)


def load_bills() -> pd.DataFrame:
    df = pd.read_parquet(_SPONSORS)
    df["bill_year_num"] = pd.to_numeric(df["bill_year"], errors="coerce")
    df["bill_no_num"]   = pd.to_numeric(df["bill_no"],   errors="coerce")
    df = df.dropna(subset=["bill_no_num", "bill_year_num", "short_title_en"])
    df["bill_id"] = (df["bill_year_num"].astype(int).astype(str) + "_"
                     + df["bill_no_num"].astype(int).astype(str))
    keep = ["bill_id", "short_title_en", "bill_year", "bill_no",
            "status", "bill_url", "bill_year_num", "bill_no_num",
            "sponsor_by_show_as", "unique_member_code"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].drop_duplicates(subset=["bill_id"]).reset_index(drop=True)


def match_si_to_bill(si_df: pd.DataFrame, bills_df: pd.DataFrame) -> pd.DataFrame:
    """Year-bucketed token-set Jaccard match. Lifted verbatim from
    legislation_si_poc.match_si_to_bill — same regex, same threshold,
    same year window. Returns si_df + four match columns."""
    parent_re = re.compile(r"([A-Z][^,()\n]{2,80}?\bAct\s+(\d{4}))")
    bills = bills_df.dropna(subset=["short_title_en"]).copy()
    bills["_tokens"] = bills["short_title_en"].astype(str).map(_title_tokens)

    bills_by_year: dict[int, list[dict]] = {}
    for rec in bills.to_dict("records"):
        y = rec.get("bill_year_num")
        if pd.notna(y):
            bills_by_year.setdefault(int(y), []).append(rec)

    def candidates(year_hint: int) -> list[dict]:
        out: list[dict] = []
        for dy in range(-MATCH_YEAR_WINDOW, MATCH_YEAR_WINDOW + 1):
            out.extend(bills_by_year.get(year_hint + dy, []))
        return out

    def best(parent_text) -> pd.Series:
        if not isinstance(parent_text, str):
            return pd.Series([None, None, None, None, 0.0])
        m = parent_re.search(parent_text)
        if not m:
            return pd.Series([None, None, None, None, 0.0])
        ref = _title_tokens(m.group(1))
        if not ref:
            return pd.Series([None, None, None, None, 0.0])
        year = int(m.group(2))
        cands = candidates(year)
        if not cands:
            return pd.Series([None, None, None, None, 0.0])
        best_row, best_score = None, 0.0
        for c in cands:
            bt = c["_tokens"]
            if not bt:
                continue
            inter = len(ref & bt)
            if inter == 0:
                continue
            score = inter / len(ref | bt)
            if score > best_score:
                best_score, best_row = score, c
        if best_row is None or best_score < MATCH_THRESHOLD:
            return pd.Series([None, None, None, None, round(best_score, 2)])
        return pd.Series([
            best_row["bill_id"],
            best_row["short_title_en"],
            best_row.get("bill_url"),
            best_row.get("unique_member_code"),
            round(best_score, 2),
        ])

    matched = si_df["si_parent_legislation"].apply(best)
    matched.columns = ["matched_bill_id", "matched_bill_title",
                       "matched_bill_url", "matched_sponsor_code",
                       "match_score"]
    return pd.concat([si_df.reset_index(drop=True),
                      matched.reset_index(drop=True)], axis=1)


def run() -> tuple[int, int]:
    si      = load_si()
    bills   = load_bills()
    joined  = match_si_to_bill(si, bills)

    matched   = joined[joined["matched_bill_id"].notna()].copy()
    unmatched = joined[joined["matched_bill_id"].isna()].copy()
    eu_neg = ("none_detected", "")

    out = pd.DataFrame({
        "bill_id":               matched["matched_bill_id"],
        "bill_short_title":      matched["matched_bill_title"],
        "sponsor_unique_member_code": matched["matched_sponsor_code"],
        "si_year":               matched["si_year"],
        "si_number":             matched["si_number"],
        "si_id":                 matched["si_id"],
        "si_title":              matched["title"],
        "si_signed_date":        matched["issue_date"].dt.date,
        "si_minister":           matched["si_responsible_actor"],
        "si_minister_named":     matched["si_minister_named"],
        "si_policy_domain":      matched["si_policy_domain_primary"],
        "si_policy_domains_all": matched["si_policy_domains"],
        "si_operation":          matched["si_operation_primary"],
        "si_operation_flags":    matched["si_operation_flags"],
        "si_form":               matched["si_form"],
        "si_eu_relationship":    matched["si_eu_relationship"],
        "si_is_eu":              ~matched["si_eu_relationship"].fillna("").isin(eu_neg),
        "eisb_url":              matched["eisb_url"],
        "iris_source_pdf":       matched["source_file"],
        "match_score":           matched["match_score"],
    })
    _OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(_OUT, index=False)

    # Unmatched stub — for the coverage gate
    unm = pd.DataFrame({
        "si_id":            unmatched["si_id"],
        "si_parent_text":   unmatched["si_parent_legislation"],
        "best_score":       unmatched["match_score"],
    })
    unm.to_parquet(_OUT_UNM, index=False)

    matched_count = len(out)
    total = len(si)
    named = int(out["si_minister_named"].notna().sum())
    logger.info(
        "bill_statutory_instruments: %d/%d SIs matched (%.1f%%) · "
        "named-minister extracted on %d (%.1f%%)",
        matched_count, total, 100 * matched_count / total,
        named, 100 * named / total,
    )
    return matched_count, total


if __name__ == "__main__":
    from services.logging_setup import setup_logging
    setup_logging()
    run()
```

### Gold schema — `bill_statutory_instruments.parquet`

| column | type | source |
|---|---|---|
| `bill_id` | str (`YYYY_NN`) | join key into `v_legislation_index` |
| `bill_short_title` | str | sponsors |
| `sponsor_unique_member_code` | str | sponsors — **Route B free-piggyback** for TD-page reverse lookup |
| `si_year`, `si_number`, `si_id` | int/int/str | taxonomy |
| `si_title` | str | taxonomy.title |
| `si_signed_date` | date | issue_date |
| `si_minister` | str | si_responsible_actor (role-string) |
| `si_minister_named` | str | extracted name (NEW; nullable) |
| `si_policy_domain`, `si_policy_domains_all` | str/str | primary + multi |
| `si_operation`, `si_operation_flags` | str/str | primary + multi |
| `si_form`, `si_eu_relationship`, `si_is_eu` | str/str/bool | taxonomy |
| `eisb_url` | str | **100% present, verified resolving** |
| `iris_source_pdf` | str | provenance |
| `match_score` | float | Jaccard ≥ 0.40 |

## A2. SQL view — `sql_views/bill_statutory_instruments.sql`

Single-source view, no joins (per project rule).

```sql
-- v_bill_statutory_instruments — SIs joined to their enabling bill.
-- Source: gold/parquet/bill_statutory_instruments.parquet (produced by
-- pipeline_sandbox/iris_si_bill_enrichment.py — lifts the
-- legislation_si_poc matcher into the pipeline).
--
-- Grain: one row per matched (bill, SI). SIs without a bill match are
-- written to a sibling parquet and not exposed here.

CREATE OR REPLACE VIEW v_bill_statutory_instruments AS
SELECT
    bill_id,
    bill_short_title,
    sponsor_unique_member_code,
    si_year, si_number, si_id,
    si_title,
    si_signed_date,
    si_minister,              -- 'The Minister for Finance' (role)
    si_minister_named,        -- 'Helen McEntee' (person, nullable)
    si_policy_domain,
    si_policy_domains_all,
    si_operation,
    si_operation_flags,
    si_form,
    si_eu_relationship,
    si_is_eu,
    eisb_url,
    iris_source_pdf,
    match_score
FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet')
WHERE bill_id IS NOT NULL
ORDER BY si_signed_date DESC NULLS LAST, si_number;
```

## A3. Data-access additions — `utility/data_access/legislation_data.py`

```python
@st.cache_data(ttl=300)
def _si_years_for_bill(_conn, bill_id: str) -> list[int]:
    df = _q(_conn,
        "SELECT DISTINCT si_year FROM v_bill_statutory_instruments"
        " WHERE bill_id = ? ORDER BY si_year DESC", [bill_id])
    return [int(y) for y in df["si_year"].dropna().tolist()] if not df.empty else []


@st.cache_data(ttl=300)
def _si_composition(_conn, bill_id: str) -> pd.DataFrame:
    """Operation-mix for the composition sentence above the list."""
    return _q(_conn,
        "SELECT si_operation, COUNT(*) AS n"
        " FROM v_bill_statutory_instruments"
        " WHERE bill_id = ? AND si_operation IS NOT NULL"
        " GROUP BY si_operation ORDER BY n DESC", [bill_id])


@st.cache_data(ttl=300)
def _si_freshness(_conn, bill_id: str) -> dict:
    """First/last SI dates + EU share. One small dict for the headline."""
    df = _q(_conn,
        "SELECT MIN(si_signed_date) first_si, MAX(si_signed_date) last_si,"
        " COUNT(*) total, SUM(CASE WHEN si_is_eu THEN 1 ELSE 0 END) eu_count"
        " FROM v_bill_statutory_instruments WHERE bill_id = ?", [bill_id])
    if df.empty:
        return {}
    r = df.iloc[0]
    return {
        "first_si": r["first_si"], "last_si": r["last_si"],
        "total": int(r["total"] or 0), "eu_count": int(r["eu_count"] or 0),
    }


@st.cache_data(ttl=300)
def _si_by_bill(_conn, bill_id: str,
                year: int | None = None,
                operation: str | None = None,
                eu_only: bool = False) -> pd.DataFrame:
    clauses = ["bill_id = ?"]; params: list = [bill_id]
    if year is not None:  clauses.append("si_year = ?");        params.append(year)
    if operation:         clauses.append("si_operation = ?");   params.append(operation)
    if eu_only:           clauses.append("si_is_eu = TRUE")
    return _q(_conn,
        "SELECT si_year, si_number, si_id, si_title, si_signed_date,"
        " si_minister, si_minister_named, si_policy_domain, si_operation,"
        " si_form, si_is_eu, eisb_url"
        " FROM v_bill_statutory_instruments"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY si_signed_date DESC NULLS LAST", params)
```

The connection that the legislation page uses must register the new
view. Add `"bill_statutory_instruments.sql"` to its `_DOMAIN_FILES`
list, same as `v_debate_listings` was added for the member page.

## A4. New page section — `utility/pages_code/legislation.py`

```python
def _section_statutory_instruments(conn, bill_id: str) -> None:
    st.html('<p class="section-heading">Statutory Instruments under this Act</p>')

    fresh = _si_freshness(conn, bill_id)
    comp  = _si_composition(conn, bill_id)
    if not fresh or not comp.empty == False and comp.empty:
        empty_state(
            "No SIs under this Act",
            "Either none have been issued yet, this Bill predates the SI "
            "data window (2018), or it never became an Act."
        )
        return

    # ── Composition sentence + freshness line ────────────────────────
    total = fresh["total"]
    parts = [f"{int(r.n)} {r.si_operation.replace('_', ' ')}"
             for r in comp.head(3).itertuples()]
    tail  = f" · {len(comp) - 3} other types" if len(comp) > 3 else ""
    st.caption(
        f"**{total} SI{'s' if total != 1 else ''}** under this Act: "
        + " · ".join(parts) + tail
    )
    if fresh.get("first_si") and fresh.get("last_si"):
        eu_pct = (100 * fresh['eu_count'] / total) if total else 0
        st.caption(
            f"First SI: {fresh['first_si']:%d %b %Y} · "
            f"last activity: {fresh['last_si']:%d %b %Y} · "
            f"EU-driven share: {eu_pct:.0f}%"
        )

    # ── Filter pills ─────────────────────────────────────────────────
    years = _si_years_for_bill(conn, bill_id)
    selected_year = st.pills(
        "SI year",
        options=["All years"] + [str(y) for y in years],
        default="All years",
        key=f"si_year_{bill_id}",
        label_visibility="collapsed",
    ) or "All years"
    year_val = None if selected_year == "All years" else int(selected_year)

    selected_op = st.pills(
        "Operation",
        options=["All operations"] + comp["si_operation"].dropna().tolist(),
        default="All operations",
        key=f"si_op_{bill_id}",
        label_visibility="collapsed",
        format_func=lambda x: x.replace("_", " "),
    ) or "All operations"
    op_val = None if selected_op == "All operations" else selected_op

    df = _si_by_bill(conn, bill_id, year=year_val, operation=op_val)
    if df.empty:
        empty_state("No SIs match these filters", "Try a different year or operation type.")
        return

    n = len(df)
    st.caption(f"Showing {n} SI{'s' if n != 1 else ''}")

    # ── Cards ───────────────────────────────────────────────────────
    for _, row in df.iterrows():
        date_disp = (pd.to_datetime(row["si_signed_date"]).strftime("%d %b %Y")
                     if pd.notna(row["si_signed_date"]) else "—")
        domain    = (row["si_policy_domain"] or "").replace("_", " ")
        operation = (row["si_operation"]      or "").replace("_", " ")
        form      = (row["si_form"]           or "").replace("_", " ")
        named     = row.get("si_minister_named")
        # Prefer named minister; fall back to the role string.
        minister  = named.strip() if isinstance(named, str) and named.strip() else (row["si_minister"] or "—")
        eu_badge  = (
            '<span class="signal" style="background:#fef3c7;border-color:#fcd34d;'
            'color:#92400e;margin-left:0.25rem;">EU</span>'
            if row["si_is_eu"] else ""
        )
        url_html = source_link_html(
            row["eisb_url"], "irishstatutebook.ie",
            aria_label="Open SI on irishstatutebook.ie",
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">'
            f'SI {int(row["si_number"])}/{int(row["si_year"])} · {_h(date_disp)}'
            f'</span>'
            f'<span class="signal leg-status-active">{_h(form)}</span>'
            f'{eu_badge}'
            f'</div>'
            f'<div class="leg-bill-card-title">{_h(row["si_title"])}</div>'
            f'<div style="margin-top:0.2rem;font-size:0.85rem;'
            f'color:var(--text-secondary);">'
            f'{_h(operation)} · {_h(domain)} · {_h(minister)} · {url_html}'
            f'</div>'
            f'</div>'
        )
```

Call-site in the bill-detail render (one line, after the existing
sponsor/stages sections):

```python
_section_statutory_instruments(conn, bill_id)
```

## A5. POC refactor — `utility/pages_code/legislation_si_poc.py`

Replace `load_si()` + `match_si_to_bill()` with a single read of the new
gold parquet (or a DuckDB query against the view). Delete the
"firewall rules are intentionally suspended" comment from the docstring.
Drop the stale `out/` path constant.

Sketch:

```python
SI_GOLD = ROOT / "data" / "gold" / "parquet" / "bill_statutory_instruments.parquet"

@st.cache_data(show_spinner="Loading Statutory Instruments…")
def load_si() -> pd.DataFrame:
    df = pd.read_parquet(SI_GOLD)
    # Adapter so the existing rendering code still works:
    df = df.rename(columns={
        "bill_id":            "matched_bill_id",
        "bill_short_title":   "matched_bill_title",
        "si_title":           "title",
        "si_minister":        "si_responsible_actor",
        "si_policy_domain":   "si_policy_domain_primary",
        "si_operation":       "si_operation_primary",
    })
    df["issue_date"] = pd.to_datetime(df["si_signed_date"], errors="coerce")
    return df
```

`match_si_to_bill` is no longer called from this file — delete the
function and `load_bills`. The rest of the page (renderers, sidebar,
detail view) is unchanged.

Optional follow-up rename (separate PR): `legislation_si_poc.py` →
`legislation_statutory_instruments.py`; remove `_poc` from page IDs in
`app.py` and routing.

## A6. Pipeline wiring — `pipeline.py`

Add one entry to `STEPS`, after the `Iris Oifigiuil ETL` step (which
produces the silver input) and before `Enrich`:

```python
    ("Iris SI ↔ bill enrichment", "pipeline_sandbox/iris_si_bill_enrichment.py"),
```

No try/except wrapper needed — `pipeline.py` already catches per-step
failures.

## A7. Test additions (optional — test step is commented out today)

If the test suite is active:
- `test/test_silver_parquet.py` — assert row count on
  `bill_statutory_instruments.parquet` (~1,000–2,000 rows).
- `test/test_sql_views.py` — smoke `SELECT COUNT(*) FROM
  v_bill_statutory_instruments`.

## A8. Validation gates

```bash
# 1. Enrichment runs and is deterministic
python pipeline_sandbox/iris_si_bill_enrichment.py
python pipeline_sandbox/iris_si_bill_enrichment.py   # 2nd run → identical row count

# 2. Coverage report — expect 1,000-2,000 SIs matched of 6,892
python -c "
import duckdb
m = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet')\").fetchone()[0]
u = duckdb.execute(\"SELECT COUNT(*) FROM read_parquet('data/gold/parquet/bill_statutory_instruments_unmatched.parquet')\").fetchone()[0]
print(f'matched: {m} | unmatched: {u} | rate: {100*m/(m+u):.1f}%')
assert 800 <= m <= 3500, f'coverage outside expected band: {m}'
print('OK')
"

# 3. Named-minister coverage
python -c "
import duckdb
n = duckdb.execute(\"SELECT COUNT(*), SUM(CASE WHEN si_minister_named IS NOT NULL THEN 1 ELSE 0 END) FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet')\").fetchone()
print(f'matched={n[0]}  with-named-minister={n[1]} ({100*n[1]/n[0]:.1f}%)')
assert n[1] >= 50, 'named-minister extraction degraded below floor'
"

# 4. Spot-check 3 post-2014 Acts and one of their SI eisb URLs
python -c "
import duckdb
for act in ['Climate Action and Low Carbon (Amendment)', 'Finance', 'Health Insurance']:
    rs = duckdb.execute(f\"\"\"
      SELECT bill_short_title, si_id, si_title, eisb_url
      FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet')
      WHERE bill_short_title ILIKE '%{act}%'
      LIMIT 3\"\"\").fetchall()
    print(act, '->', len(rs), 'SIs');
    for r in rs: print('  ', r)
"

# 5. eisb URL liveness — sample 5, curl with browser UA
python -c "
import duckdb
rs = duckdb.execute(\"SELECT eisb_url FROM read_parquet('data/gold/parquet/bill_statutory_instruments.parquet') USING SAMPLE 5\").fetchall()
for r in rs: print(r[0])
" | while read url; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -L -A 'Mozilla/5.0' -e 'https://www.oireachtas.ie/' --max-time 25 "$url")
  echo "  $code  $url"
done

# 6. SQL view loads + composite-unique
python -c "
import duckdb
duckdb.execute(open('sql_views/bill_statutory_instruments.sql').read())
print('rows:', duckdb.sql('SELECT COUNT(*) FROM v_bill_statutory_instruments').fetchone()[0])
print('null bill_id:', duckdb.sql('SELECT COUNT(*) FROM v_bill_statutory_instruments WHERE bill_id IS NULL').fetchone()[0])
print('dupe (bill_id,si_id):', duckdb.sql('SELECT COUNT(*) FROM (SELECT bill_id,si_id,COUNT(*) n FROM v_bill_statutory_instruments GROUP BY 1,2 HAVING n>1)').fetchone()[0])
"

# 7. POC page equivalence — render counts should not drop
#   (manual: visit /statutory_instruments before/after; KPI strip totals match.)

# 8. Streamlit smoke
streamlit run utility/app.py --server.headless true --server.port 8540 &
sleep 8 ; curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8540/legislation
#  → 200, then manually visit a bill detail page and confirm the SI card renders

# 9. Regression diff vs A0 baseline
python -c "
import json, glob, duckdb
base = json.load(open('si_baseline.json'))
OWNED = {'data/gold/parquet\\\\bill_statutory_instruments.parquet',
         'data/gold/parquet\\\\bill_statutory_instruments_unmatched.parquet'}
bad=[]
for p, before in base['outputs'].items():
    if p in OWNED: continue
    try: after = duckdb.execute(f\"SELECT COUNT(*) FROM read_parquet('{p}')\").fetchone()[0]
    except Exception as e: after = f'ERR {e}'
    if isinstance(before,int) and isinstance(after,int) and after < before:
        bad.append(f'{p}: {before} -> {after}')
ok=fail=0
for f in sorted(glob.glob('sql_views/*.sql')):
    try: duckdb.connect().execute(open(f, encoding='utf-8', errors='ignore').read()); ok+=1
    except: fail+=1
assert not bad and fail <= base['sql_views']['fail'], (bad, ok, fail)
print('REGRESSION CHECK PASSED — pre-existing outputs unchanged, views', ok, '/', fail)
"
```

Block merge if gates 1, 2, 6, or 9 fail.

---

# PART B — adjacent enrichments (optional, can ship in same PR or follow-up)

These are tiny additions that unlock TD-side cross-references using
work already done in Part A.

## B1 — Route B: "Regulations from your sponsored bills" on the TD page

Part A's gold parquet already carries `sponsor_unique_member_code` (the
free piggyback). Add to `utility/data_access/member_overview_data.py`:

```python
"bill_statutory_instruments.sql",
```

And a query function in `member_overview_data.py`:

```python
@st.cache_data(ttl=300)
def _si_from_sponsored_bills(_conn, unique_member_code: str) -> pd.DataFrame:
    return _q(_conn,
        "SELECT bill_short_title, bill_id, COUNT(*) si_count,"
        " MAX(si_signed_date) latest_si"
        " FROM v_bill_statutory_instruments"
        " WHERE sponsor_unique_member_code = ?"
        " GROUP BY bill_short_title, bill_id"
        " ORDER BY si_count DESC", [unique_member_code])
```

Drop a small callout into `_section_legislation` on `member_overview.py`:

```
Bills you sponsored that became Acts have produced 7 SIs since enactment.
```

## B2 — Route C: PQ-mentions-SI extraction

A small enrichment that finds `S.I. No. X of YYYY` references in
`questions.parquet.question_text`. 131 questions baseline; a richer
regex should yield 200-400.

Sketch:

```python
# pipeline_sandbox/pq_si_references.py
import re, pandas as pd
RE = re.compile(r"S\.?I\.?\s*(?:No\.?)?\s*(\d+)\s*(?:of|/)\s*(\d{4})", re.IGNORECASE)

def extract():
    q = pd.read_parquet("data/silver/parquet/questions.parquet")
    rows = []
    for _, r in q[q["question_text"].notna()].iterrows():
        for m in RE.finditer(r["question_text"]):
            rows.append({
                "unique_member_code": r["unique_member_code"],
                "td_name":            r["td_name"],
                "context_date":       r["context_date"],
                "year":               int(m.group(2)),
                "number":             int(m.group(1)),
                "si_id":              f"{m.group(2)}-{m.group(1).zfill(3)}",
                "question_text_snippet": r["question_text"][:200],
            })
    pd.DataFrame(rows).to_parquet("data/gold/parquet/pq_si_references.parquet", index=False)
```

→ a view `v_pq_si_references` → join to `v_bill_statutory_instruments`
on `si_id` → drops onto the TD page as "PQs you've asked about SIs."

---

# Deferred (out of scope for Part A)

| Item | Why deferred |
|---|---|
| Minister-tenure timeline (Route A — full) | Needs ~300 lines of Iris appointment parsing; the `si_minister_named` inline extraction in Part A covers ~10-30% of SIs for free. Build a curated tenure table (50-100 rows) only when the named-minister coverage isn't enough |
| TD-as-debater on SI (Route D) | Stage 2 AKN speech layer (not yet built) |
| "Ministerial regulatory power score" leaderboard | Builds on the tenure timeline above |
| Sanctions standalone page | One filter on the new view; can ship trivially once Part A is in |
| Dormant-Act page-wide widget | One query; ship as a separate small PR |
| Topic NLP classification beyond the 17 domains | Out of scope; the parser already classifies |
| Variant deduplication of `si_parent_legislation` (e.g. *European Communities Act 1972* × 3) | Improves match coverage by ~3-5%; deferred to a tuning pass after Part A's coverage gate runs |

---

# Risks & rollback

| Risk | Mitigation |
|---|---|
| Match rate below 800 SIs (gate floor) | Coverage gate catches it pre-merge; investigate variant-dedup before flipping POC reads |
| Named-minister regex matches portfolio fragments | Regex is anchored on the operative-clause formula (`, in exercise`/`, hereby`); coverage gate also asserts a non-degraded floor |
| POC page regression after refactor | Manual gate 7 + the POC remains the standalone SI browser — page behaviour should be identical, just sourced differently |
| eisb URL drift | Already verified resolving in batch; pattern is stable since 2018 |
| Streamlit caches the old view definition | App restart in gate 8 covers this; the cached resource rebuilds on boot |

**Rollback:** revert the `pipeline.py` and page edits;
`rm data/gold/parquet/bill_statutory_instruments*.parquet`. No upstream
data mutated, no existing parquet schema changed.

---

# Files in/out

**New (3):**
- `pipeline_sandbox/iris_si_bill_enrichment.py`
- `sql_views/bill_statutory_instruments.sql`
- This document (`pipeline_sandbox/iris_si_bill_graduation_plan.md`)

**Edited (4):**
- `utility/pages_code/legislation.py` — new card section
- `utility/pages_code/legislation_si_poc.py` — read view instead of in-page join
- `utility/data_access/legislation_data.py` — new view + 4 query functions
- `pipeline.py` — one STEPS tuple

**Gold outputs (2):**
- `data/gold/parquet/bill_statutory_instruments.parquet` (matched)
- `data/gold/parquet/bill_statutory_instruments_unmatched.parquet` (for the gate)
