"""EXPERIMENTAL (tracked code, gitignored sandbox data) — pull EPA enforcement/compliance history per
licence from the EPA LEAP open-data API, for the accountability lens "public money → environmentally
non-compliant operators".

The capability register ([[epa_capability_register]]) shows who is LICENSED and what public money they
won/were paid. This adds the third axis: their COMPLIANCE record — incidents, complaints and
non-compliances the EPA logged against the licence. Joined together (see [[epa_accountability_view]]),
it surfaces firms taking public money while carrying a poor environmental-enforcement history — an
artefact nobody publishes joined-up.

Source: EPA LEAP API (https://data.epa.ie/leap, CC-BY-4.0, no auth). Two calls per licence:
  1. LicenceProfile/byregno      → licence_profile_id + organisation_name + county + status
  2. ComplianceList/compliancelist → every compliance record (type = Incident / Complaint /
                                     Non-Compliance / Site Visit / …) with status + date
We aggregate to a per-licence compliance burden (counts by type, open count, latest date).

SCOPE (logged, not silent): the crawl is restricted to the procurement-relevant universe — the whole
waste sector plus every firm that received public money — not all 1,272 licences. Unmatched non-waste
firms cannot tie to public money, so they are out of scope here (full crawl is a documented extension).

Polite + resumable: 0.3s between licences, results streamed to a JSONL checkpoint so a re-run skips
licences already pulled. Output parquet is rebuilt from the checkpoint at the end.

Outputs (gitignored):
  data/sandbox/epa_enforcement_checkpoint.jsonl   (resume log, one line per licence)
  data/sandbox/parquet/epa_enforcement.parquet
Run (capability register first): ./.venv/Scripts/python.exe pipeline_sandbox/epa_enforcement_pull.py
"""

from __future__ import annotations

import contextlib
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_sandbox._capability_join import name_norm  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger(__name__)

API = "https://data.epa.ie/leap"
UA = "dail-tracker-research/1.0 (civic data project; EPA LEAP open data CC-BY-4.0)"
DELAY_S = 0.3

LICENCES = ROOT / "data/sandbox/parquet/epa_licensed_facilities.parquet"
REGISTER = ROOT / "data/sandbox/parquet/epa_capability_register.parquet"
CHECKPOINT = ROOT / "data/sandbox/epa_enforcement_checkpoint.jsonl"
OUT = ROOT / "data/sandbox/parquet/epa_enforcement.parquet"

# compliance-record type -> bucket. The "bad" signals are incident/complaint/non_compliance; site_visit
# and returns are routine regulatory activity, counted separately so they don't inflate the burden.
_TYPE_BUCKET = {
    "incident": "incident",
    "complaint": "complaint",
    "non-compliance": "non_compliance",
    "non compliance": "non_compliance",
    "noncompliance": "non_compliance",
    "site visit": "site_visit",
    "compliance investigation": "investigation",
}


def _bucket(t: str) -> str:
    return _TYPE_BUCKET.get(str(t).strip().lower(), "other")


def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    return s


def _crawl_set() -> pd.DataFrame:
    """Unique (licence_number) for waste-sector licences ∪ licences of public-money firms."""
    lic = pd.read_parquet(LICENCES)[["licence_number", "licensee_name", "licence_class"]].copy()
    lic = lic[lic["licence_number"].notna()].drop_duplicates("licence_number")
    lic["nkey"] = name_norm(lic["licensee_name"])
    reg = pd.read_parquet(REGISTER)
    paid = reg[reg["cro_company_num"].notna() & reg["has_public_track_record"]]
    paid_keys = set(name_norm(paid["licensee_name"]))
    crawl = lic[(lic["licence_class"] == "waste") | (lic["nkey"].isin(paid_keys))].copy()
    crawl["in_scope_reason"] = crawl.apply(
        lambda r: "waste+publicmoney" if (r["licence_class"] == "waste" and r["nkey"] in paid_keys)
        else ("waste" if r["licence_class"] == "waste" else "publicmoney"),
        axis=1,
    )
    return crawl.reset_index(drop=True)


def _get(session: requests.Session, ep: str, **params) -> dict | list | None:
    try:
        r = session.get(API + ep, params=params, timeout=60)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("  %s %s failed: %s", ep, params, exc)
        return None


def _pull_one(session: requests.Session, licence_number: str) -> dict:
    rec: dict = {"licence_number": licence_number, "status": "no_profile"}
    prof = _get(session, "/api/v1/LicenceProfile/byregno", licence_reg_no=licence_number)
    if not isinstance(prof, dict) or not prof.get("licence_profile_id"):
        return rec
    pid = prof["licence_profile_id"]
    rec.update(
        status="ok",
        licence_profile_id=pid,
        organisation_name=prof.get("organisation_name"),
        county=prof.get("county"),
        town=prof.get("town"),
        active_licence_status=prof.get("active_licence_status"),
        sector=prof.get("active_licence_sector"),
        uww_priority_site=bool(prof.get("uww_priority_site")),
    )
    cl = _get(session, "/api/v1/ComplianceList/compliancelist", licence_profile_id=pid, per_page=2000)
    records = cl.get("list", []) if isinstance(cl, dict) else []
    buckets = {"incident": 0, "complaint": 0, "non_compliance": 0, "site_visit": 0, "investigation": 0, "other": 0}
    n_open = 0
    last_date = None
    for r in records:
        buckets[_bucket(r.get("type"))] += 1
        if str(r.get("status", "")).strip().lower() not in ("closed", "complete", "completed"):
            n_open += 1
        d = r.get("date")
        if d and (last_date is None or d > last_date):
            last_date = d
    rec.update(
        n_compliance_records=len(records),
        n_open=n_open,
        last_record_date=(last_date or "")[:10],
        **{f"n_{k}": v for k, v in buckets.items()},
    )
    return rec


def _load_done() -> set[str]:
    done = set()
    if CHECKPOINT.exists():
        for line in CHECKPOINT.read_text(encoding="utf-8").splitlines():
            with contextlib.suppress(Exception):
                done.add(json.loads(line)["licence_number"])
    return done


def pull() -> None:
    session = _new_session()
    crawl = _crawl_set()
    done = _load_done()
    todo = crawl[~crawl["licence_number"].isin(done)]
    log.info(
        "enforcement crawl: %d licences in scope (%s); %d already done; %d to pull",
        len(crawl),
        dict(crawl["in_scope_reason"].value_counts()),
        len(done),
        len(todo),
    )
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    reasons = dict(zip(crawl["licence_number"], crawl["in_scope_reason"], strict=True))
    with CHECKPOINT.open("a", encoding="utf-8") as fh:
        for i, lnum in enumerate(todo["licence_number"], 1):
            rec = _pull_one(session, lnum)
            rec["in_scope_reason"] = reasons.get(lnum)
            fh.write(json.dumps(rec, default=str) + "\n")
            fh.flush()
            if i % 50 == 0:
                log.info("  [%d/%d] %s -> %s", i, len(todo), lnum, rec.get("status"))
            time.sleep(DELAY_S)


def build_parquet() -> pd.DataFrame:
    rows = [json.loads(ln) for ln in CHECKPOINT.read_text(encoding="utf-8").splitlines() if ln.strip()]
    df = pd.DataFrame(rows).drop_duplicates("licence_number", keep="last")
    cnt_cols = [c for c in df.columns if c.startswith("n_")]
    df[cnt_cols] = df[cnt_cols].fillna(0).astype("int64")
    # the "bad" enforcement signal excludes routine site visits / investigations
    df["n_enforcement_events"] = df["n_incident"] + df["n_complaint"] + df["n_non_compliance"]
    save_parquet(df, OUT)
    return df


def main() -> None:
    setup_standalone_logging("epa_enforcement_pull")
    pull()
    df = build_parquet()
    ok = df[df["status"] == "ok"]
    log.info(
        "WROTE %s — %d licences | %d with a profile | %d with enforcement events | total events %d",
        OUT,
        len(df),
        len(ok),
        int((ok["n_enforcement_events"] > 0).sum()),
        int(ok["n_enforcement_events"].sum()),
    )


if __name__ == "__main__":
    main()
