"""EXPERIMENTAL (tracked code, gitignored sandbox data) — the SUPPLIER-CAPABILITY REGISTER.

Joins the NSAI certified-company pull ([[nsai_certified_companies_scrape]]) to the CRO company master
and to public payments, producing one row per CRO firm: cert portfolio + company identity/health +
public-payment track record. The supply-side answer to "who is provably qualified, and how do they
perform on public work" — a view neither the registers nor the payments data give alone.

Matching discipline (precision over recall — these feed civic transparency, so a wrong join is worse
than a miss):
  * KEY: the project-canonical company key — CRO's precomputed ``name_norm`` (built by
    [[name_norm]] / ``shared.name_norm.name_norm_expr``); the NSAI side is normalised by the IDENTICAL
    rule so the exact join lands.
  * AMBIGUITY: ``name_norm`` deliberately drops geo/legal fillers (IRELAND/HOLDINGS/GROUP/LTD…), so a
    key can map to several companies (e.g. "Toyota Ireland" & "Toyota Holdings" → TOYOTA). When it
    does, we pick the LIVE / most-recently-filing entity and flag ``ambiguous_name`` + lower
    confidence — never silently assert one.
  * FUZZY: only for the residual, and only when extremely likely — name score ≥98 (near-identical),
    OR 92–97 corroborated COLUMN-BY-COLUMN by the location field agreeing with the CRO address.
  * DISSOLVED: a match to a non-active CRO entity (dead shell while the operating company trades under
    a longer name) is flagged ``match_review_needed`` + confidence halved, excluded from headlines.
  * DEDUP: collapsed to one row per ``company_num`` (name variants merged) so ``public_eur`` is safe
    to sum.

Findings are LEADS TO INVESTIGATE, not conclusions (no-inference rule).

PUBLIC-MONEY TRACK RECORD — two tiers, NEVER summed together (the procurement honesty rail):
  * SPENT (``public_eur``) — money actually paid/committed, from ``procurement_payments_fact``
    (council + public-body payment/PO lines, safe-to-sum only).
  * AWARDED (``total_award_eur`` = ``etenders_award_eur`` + ``ted_award_eur``) — contract awards won,
    from the ``procurement_award_spend_link`` per-CRO spine (eTenders + TED, safe-to-sum only).
  These are DIFFERENT business processes (a ceiling won vs euros out the door). They are reported
  side-by-side and the ``spend_to_award_ratio`` relates them, but the two € columns are NEVER added.
  This closes the earlier gap where the register saw only the SPENT side and understated reach.

Outputs (gitignored):
  data/sandbox/parquet/nsai_capability_register.parquet
  data/sandbox/nsai_capability_register_summary.json
Run (scrape first): ./.venv/Scripts/python.exe pipeline_sandbox/nsai_capability_register.py
"""

from __future__ import annotations

import contextlib
import difflib
import json
import logging
import re
import sys
from pathlib import Path

import pandas as pd
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402
from shared.name_norm import name_norm_expr  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

log = logging.getLogger(__name__)

CERTS = ROOT / "data/sandbox/parquet/nsai_certified_companies.parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"
PAYMENTS = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
# per-CRO award+spend spine (eTenders + TED awards already CRO-keyed); see extractors/procurement_award_spend_link.py
AWARD_LINK = ROOT / "data/sandbox/parquet/procurement_award_spend_link.parquet"
OUT = ROOT / "data/sandbox/parquet/nsai_capability_register.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/nsai_capability_register_summary.json"

HEALTH_FLAGS = ["annual_return_overdue_flag", "accounts_overdue_flag", "recent_distress_flag"]
PUBLIC_BODY = re.compile(
    r"\b(council|comhairle|county council|city council|department|roinn|university|hse|garda|"
    r"oifig|udaras|iarnrod|an post|etb|credit union)\b",
    re.I,
)


def _name_norm(names: pd.Series) -> pd.Series:
    """Apply the canonical company name_norm rule (shared.name_norm) to a pandas Series."""
    out = pl.DataFrame({"n": names.astype(str).tolist()}).with_columns(name_norm_expr("n").alias("k"))["k"].to_list()
    return pd.Series(out, index=names.index)


def _toksort(s: str) -> str:
    return " ".join(sorted(s.split()))


def _score(a: str, b: str) -> int:
    return int(
        round(
            max(
                difflib.SequenceMatcher(None, a, b).ratio(),
                difflib.SequenceMatcher(None, _toksort(a), _toksort(b)).ratio(),
            )
            * 100
        )
    )


def _load_cro() -> pd.DataFrame:
    cro = pd.read_parquet(
        CRO,
        columns=[
            "company_num",
            "company_name",
            "name_norm",
            "company_status",
            "entity_age_years",
            "nace_v2_code",
            "last_ar_date",
            "company_reg_date",
            "company_address_1",
            "company_address_2",
            "company_address_3",
            "company_address_4",
            "eircode",
            *HEALTH_FLAGS,
        ],
    )
    cro["nkey"] = cro["name_norm"].fillna("")
    cro["active"] = cro["company_status"].str.contains("Normal", case=False, na=False)
    cro["addr"] = (
        cro[["company_address_1", "company_address_2", "company_address_3", "company_address_4", "eircode"]]
        .fillna("")
        .agg(" ".join, axis=1)
        .str.lower()
    )
    # live-preference ordering: active first, then most-recent annual return, then registration
    return cro.sort_values(["active", "last_ar_date", "company_reg_date"], ascending=[False, False, False])


def _location_agrees(nsai_loc: str, cro_addr: str) -> bool:
    words = [w for w in re.sub(r"[^a-z ]", " ", str(nsai_loc).lower()).split() if len(w) >= 4]
    return any(w in cro_addr for w in words) if words else False


def _load_award_spine() -> pd.DataFrame:
    """eTenders + TED contract awards per CRO firm (AWARDED tier — safe-to-sum, kept distinct from SPENT).

    The award_spend_link spine is keyed by a hybrid ``entity``; we keep only the CRO-keyed rows and
    re-key on ``cro_company_num`` (float, matching the register) so it joins the NSAI firms directly.
    """
    if not AWARD_LINK.exists():
        log.warning("award spine %s missing — awards columns will be empty", AWARD_LINK)
        return pd.DataFrame(columns=["cro_company_num"])
    asl = pd.read_parquet(
        AWARD_LINK,
        columns=[
            "company_num",
            "keyed_by_cro",
            "in_etenders",
            "in_ted",
            "total_award_eur",
            "etenders_award_eur",
            "ted_award_eur",
            "etenders_awards",
            "ted_awards",
            "spend_to_award_ratio",
        ],
    )
    asl = asl[asl["keyed_by_cro"].eq(True) & asl["company_num"].notna()].copy()
    asl["cro_company_num"] = asl["company_num"].astype("float64")
    asl = (
        asl.drop(columns=["company_num", "keyed_by_cro"])
        .sort_values("total_award_eur", ascending=False)
        .drop_duplicates("cro_company_num", keep="first")
        .rename(
            columns={
                "in_etenders": "won_etenders",
                "in_ted": "won_ted",
                "etenders_awards": "n_etenders_awards",
                "ted_awards": "n_ted_awards",
            }
        )
    )
    return asl


def build() -> pd.DataFrame:
    certs = pd.read_parquet(CERTS)
    cro = _load_cro()
    pay = pd.read_parquet(
        PAYMENTS, columns=["cro_company_num", "amount_eur", "value_safe_to_sum", "publisher_name", "year"]
    )

    # one row per NSAI firm: cert portfolio + a single (modal) location
    firms = (
        certs.groupby("company")
        .agg(
            nsai_location=("location", lambda x: x.mode().iloc[0] if len(x.mode()) else ""),
            certs=("standard_code", lambda x: sorted(set(x))),
            n_certs=("standard_code", "nunique"),
        )
        .reset_index()
        .rename(columns={"company": "nsai_company"})
    )
    firms = firms[firms["nsai_company"].str.strip().str.len() > 1].reset_index(drop=True)
    firms["nkey"] = _name_norm(firms["nsai_company"])
    firms = firms[firms["nkey"].str.len() >= 3].reset_index(drop=True)
    firms["dskey"] = firms["nkey"].str.replace(" ", "", regex=False)

    # CRO lookup structures (cro is live-preference ordered, so first-seen == preferred)
    cfields = [
        "company_num",
        "company_name",
        "company_status",
        "active",
        "addr",
        "entity_age_years",
        "nace_v2_code",
        *HEALTH_FLAGS,
    ]
    nkey_groups: dict[str, list[dict]] = {}
    block: dict[str, list[dict]] = {}
    for row in cro[["nkey", *cfields]].itertuples(index=False):
        r = dict(zip(["nkey", *cfields], row, strict=True))
        nkey_groups.setdefault(r["nkey"], []).append(r)
        dk = r["nkey"].replace(" ", "")
        if len(dk) >= 6:
            block.setdefault(dk[:4], []).append(r)

    rows = []
    for f in firms.itertuples():
        rec = {
            "match_method": None,
            "match_confidence": 0.0,
            "name_score": None,
            "location_agrees": None,
            "ambiguous_name": False,
        }
        chosen = None
        cands = nkey_groups.get(f.nkey, [])
        if cands:
            chosen = cands[0]  # live-preferred
            if len(cands) == 1:
                rec.update(match_method="exact_unique", match_confidence=1.0)
            else:
                rec.update(match_method="exact_ambiguous", match_confidence=0.70, ambiguous_name=True)
        elif len(f.dskey) >= 6:
            best, best_score = None, 0
            for cand in block.get(f.dskey[:4], []):
                sc = _score(f.nkey, cand["nkey"])
                if sc > best_score:
                    best, best_score = cand, sc
            if best:
                agrees = _location_agrees(f.nsai_location, best["addr"])
                rec["name_score"] = best_score
                rec["location_agrees"] = agrees
                if best_score >= 98:
                    chosen = best
                    rec.update(match_method="fuzzy_name", match_confidence=0.90)
                elif best_score >= 92 and agrees:
                    chosen = best
                    rec.update(match_method="fuzzy_name_loc", match_confidence=0.85)
        out = {
            "nsai_company": f.nsai_company,
            "nsai_location": f.nsai_location,
            "n_certs": f.n_certs,
            "certs": f.certs,
            "is_public_body": bool(PUBLIC_BODY.search(f.nsai_company)),
            **rec,
        }
        if chosen:
            out.update(
                {
                    "cro_company_num": chosen["company_num"],
                    "cro_name": chosen["company_name"],
                    "cro_status": chosen["company_status"],
                    "cro_active": bool(chosen["active"]),
                    "entity_age_years": chosen["entity_age_years"],
                    "nace_v2_code": chosen["nace_v2_code"],
                    **{flag: bool(chosen[flag]) for flag in HEALTH_FLAGS},
                }
            )
        rows.append(out)
    df = pd.DataFrame(rows)

    # public payments per CRO firm (safe-to-sum only)
    p = pay[pay["value_safe_to_sum"] & pay["cro_company_num"].notna()].copy()
    p["cro_company_num"] = p["cro_company_num"].astype("int64")
    agg = (
        p.groupby("cro_company_num")
        .agg(
            public_eur=("amount_eur", "sum"),
            n_payments=("amount_eur", "size"),
            n_publishers=("publisher_name", "nunique"),
            first_paid_year=("year", "min"),
            last_paid_year=("year", "max"),
        )
        .reset_index()
    )
    df = df.merge(agg, on="cro_company_num", how="left")

    # collapse to one row per CRO firm (merge name variants) so public_eur is safe to sum
    def _union(series):
        acc = set()
        for v in series:
            if isinstance(v, (list, tuple)):
                acc.update(v)
        return sorted(acc)

    matched = df[df["cro_company_num"].notna()].sort_values("match_confidence", ascending=False)
    unmatched = df[df["cro_company_num"].isna()].copy()
    scalar = {c: "first" for c in df.columns if c not in ("cro_company_num", "certs", "nsai_company")}
    scalar.update({"certs": _union, "nsai_company": "first"})
    collapsed = matched.groupby("cro_company_num", as_index=False).agg(scalar)
    collapsed["name_variants"] = matched.groupby("cro_company_num")["nsai_company"].nunique().values
    collapsed["n_certs"] = collapsed["certs"].map(len)
    unmatched["name_variants"] = 1
    df = pd.concat([collapsed, unmatched], ignore_index=True)

    # AWARDED tier — eTenders + TED contract awards won, joined per CRO firm (NEVER summed into SPENT)
    df = df.merge(_load_award_spine(), on="cro_company_num", how="left")
    for col in ("won_etenders", "won_ted"):
        df[col] = df[col].eq(True) if col in df else False
    df["won_public_tender"] = df["won_etenders"] | df["won_ted"]
    for col in ("total_award_eur", "etenders_award_eur", "ted_award_eur"):
        if col not in df:
            df[col] = pd.NA

    # derived flags + dissolved-match handling (.eq(True) treats NaN as False without dtype downcast)
    active = df["cro_active"].eq(True)
    overdue = df[HEALTH_FLAGS].eq(True).any(axis=1)
    df["not_good_standing"] = df["cro_company_num"].notna() & ((~active) | overdue)
    # public-money track record across BOTH tiers (paid OR won a tender), each tier kept separate
    df["received_public_money"] = df["public_eur"].notna()
    df["won_public_award"] = df["total_award_eur"].fillna(0).gt(0)
    df["has_public_track_record"] = df["received_public_money"] | df["won_public_award"]
    df["match_review_needed"] = df["cro_company_num"].notna() & (~active)
    df.loc[df["match_review_needed"], "match_confidence"] = (
        df.loc[df["match_review_needed"], "match_confidence"] * 0.5
    ).round(2)
    df["match_caveat"] = ""
    df.loc[df["match_review_needed"], "match_caveat"] = (
        "matched non-active CRO entity — verify vs live operating company"
    )
    df.loc[df["ambiguous_name"].fillna(False) & (df["match_caveat"] == ""), "match_caveat"] = (
        "name_norm maps to >1 company — live entity preferred"
    )
    return df


def _summary(df: pd.DataFrame) -> dict:
    matched = df[df["cro_company_num"].notna()]
    hi = matched[~matched["match_review_needed"] & (matched["match_confidence"] >= 0.85)]
    paid = hi[hi["received_public_money"]]
    won = hi[hi["won_public_award"]]
    track = hi[hi["has_public_track_record"]]
    leads = paid[paid["not_good_standing"]]
    return {
        "nsai_firms": int(len(df)),
        "matched_to_cro": int(len(matched)),
        "high_confidence_live": int(len(hi)),
        "review_needed_dissolved": int(matched["match_review_needed"].sum()),
        "match_methods": {k: int(v) for k, v in matched["match_method"].value_counts().items()},
        # SPENT tier (payments fact)
        "high_conf_received_public_money": int(len(paid)),
        "high_conf_public_eur_safe_sum": float(round(paid["public_eur"].sum(), 2)),
        # AWARDED tier (eTenders + TED) — reported separately, NEVER summed with SPENT
        "high_conf_won_public_award": int(len(won)),
        "high_conf_won_etenders": int(hi["won_etenders"].sum()),
        "high_conf_won_ted": int(hi["won_ted"].sum()),
        "high_conf_total_awarded_eur_safe_sum": float(round(won["total_award_eur"].sum(), 2)),
        "high_conf_etenders_awarded_eur": float(round(hi["etenders_award_eur"].fillna(0).sum(), 2)),
        "high_conf_ted_awarded_eur": float(round(hi["ted_award_eur"].fillna(0).sum(), 2)),
        # certified AND has any public track record (paid or won) — the keystone capability signal
        "high_conf_with_public_track_record": int(len(track)),
        "live_but_overdue_with_public_money": int(len(leads)),
        "live_but_overdue_public_eur": float(round(leads["public_eur"].sum(), 2)),
        "caveats": [
            "Findings are leads to investigate, not conclusions.",
            "SPENT (public_eur) and AWARDED (total_award_eur) are different tiers — NEVER add them.",
            "AWARDED = eTenders + TED contract awards (safe-to-sum); payments understate as before but "
            "awards now show tendered reach the payments fact misses.",
            "CRO status is point-in-time; dissolution after a legitimate payment is not wrongdoing.",
            "fuzzy_name_loc can admit rare false positives where name+county coincide (review bucket).",
        ],
    }


def main() -> None:
    setup_standalone_logging("nsai_capability_register")
    df = build()
    save_parquet(df, OUT)
    summary = _summary(df)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info(
        "WROTE %s — %d firms | matched %d | SPENT €%.0f (%d firms) | AWARDED €%.0f (%d firms) | track-record %d",
        OUT,
        len(df),
        summary["matched_to_cro"],
        summary["high_conf_public_eur_safe_sum"],
        summary["high_conf_received_public_money"],
        summary["high_conf_total_awarded_eur_safe_sum"],
        summary["high_conf_won_public_award"],
        summary["high_conf_with_public_track_record"],
    )
    log.info("summary: %s", json.dumps(summary["match_methods"]))


if __name__ == "__main__":
    main()
