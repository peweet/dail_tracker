"""Shared CRO-match + public-money enrichment for sandbox supplier-capability registers.

Extracted from the NSAI capability register so every credential register (NSAI certs, EPA licences,
future trade registers) reuses the SAME precision-first CRO join and the SAME never-sum-across-tiers
public-money model. The NSAI register [[nsai_capability_register]] is the reference implementation;
the EPA register [[epa_capability_register]] is the first consumer of this helper.

Three pieces:
  * ``match_to_cro(firms, name_col, location_col)`` — exact ``name_norm`` join (the project-canonical
    company key, [[name_norm]]), ambiguity-aware live-preference, and a gated fuzzy fallback that only
    fires when a name is near-identical OR is corroborated column-by-column by the location field.
  * ``attach_award_and_spend(df)`` — joins the SPENT side (``procurement_payments_fact``) and the
    AWARDED side (``procurement_award_spend_link``: eTenders + TED), kept in SEPARATE € columns that
    are NEVER added together, plus the standing/dissolved flags.
  * ``collapse_by_cro(df, list_cols)`` — merge name-variant rows to one row per CRO firm so the € is
    safe to sum.

Findings are LEADS TO INVESTIGATE, not conclusions (the project's no-inference rule).
"""

from __future__ import annotations

import difflib
import logging
import re
from pathlib import Path

import pandas as pd
import polars as pl

ROOT = Path(__file__).resolve().parents[1]

from shared.name_norm import name_norm_expr  # noqa: E402

log = logging.getLogger(__name__)

CRO = ROOT / "data/silver/cro/companies.parquet"
PAYMENTS = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
AWARD_LINK = ROOT / "data/sandbox/parquet/procurement_award_spend_link.parquet"

HEALTH_FLAGS = ["annual_return_overdue_flag", "accounts_overdue_flag", "recent_distress_flag"]
PUBLIC_BODY = re.compile(
    r"\b(council|comhairle|county council|city council|department|roinn|university|hse|garda|"
    r"oifig|udaras|iarnrod|an post|etb|credit union|uisce|irish water)\b",
    re.I,
)

# CRO identity columns carried onto a matched firm
_CRO_OUT = ["company_num", "company_name", "company_status", "active", "entity_age_years", "nace_v2_code"]


def name_norm(names: pd.Series) -> pd.Series:
    """Apply the canonical company name_norm rule (shared.name_norm) to a pandas Series."""
    out = (
        pl.DataFrame({"n": names.astype(str).tolist()})
        .with_columns(name_norm_expr("n").alias("k"))["k"]
        .to_list()
    )
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


def _location_agrees(loc: str, cro_addr: str) -> bool:
    words = [w for w in re.sub(r"[^a-z ]", " ", str(loc).lower()).split() if len(w) >= 4]
    return any(w in cro_addr for w in words) if words else False


def match_to_cro(firms: pd.DataFrame, name_col: str, location_col: str) -> pd.DataFrame:
    """Return ``firms`` with CRO identity + match-quality columns added (one row in, one row out).

    Precision over recall: a wrong join is worse than a miss (these feed civic transparency).
      exact_unique      name_norm hits exactly one CRO entity                    confidence 1.00
      exact_ambiguous   name_norm hits several → pick live, flag ambiguity       confidence 0.70
      fuzzy_name        de-spaced near-identical (score ≥ 98)                     confidence 0.90
      fuzzy_name_loc    score 92–97 AND the location field agrees with CRO addr  confidence 0.85
    """
    cro = _load_cro()
    firms = firms.copy()
    firms["nkey"] = name_norm(firms[name_col])
    firms = firms[firms["nkey"].str.len() >= 3].reset_index(drop=True)
    firms["dskey"] = firms["nkey"].str.replace(" ", "", regex=False)

    cfields = ["nkey", *_CRO_OUT, "addr", *HEALTH_FLAGS]
    nkey_groups: dict[str, list[dict]] = {}
    block: dict[str, list[dict]] = {}
    for row in cro[cfields].itertuples(index=False):
        r = dict(zip(cfields, row, strict=True))
        nkey_groups.setdefault(r["nkey"], []).append(r)
        dk = r["nkey"].replace(" ", "")
        if len(dk) >= 6:
            block.setdefault(dk[:4], []).append(r)

    out_rows = []
    for f in firms.itertuples():
        rec = {
            "match_method": None,
            "match_confidence": 0.0,
            "name_score": None,
            "location_agrees": None,
            "ambiguous_name": False,
        }
        loc = getattr(f, location_col, "") or ""
        chosen = None
        cands = nkey_groups.get(f.nkey, [])
        if cands:
            chosen = cands[0]  # live-preferred (cro is live-preference ordered)
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
                agrees = _location_agrees(loc, best["addr"])
                rec["name_score"] = best_score
                rec["location_agrees"] = agrees
                if best_score >= 98:
                    chosen = best
                    rec.update(match_method="fuzzy_name", match_confidence=0.90)
                elif best_score >= 92 and agrees:
                    chosen = best
                    rec.update(match_method="fuzzy_name_loc", match_confidence=0.85)
        out = {**rec}
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
        out_rows.append(out)
    joined = pd.concat([firms.reset_index(drop=True), pd.DataFrame(out_rows)], axis=1)
    return joined.drop(columns=["nkey", "dskey"])  # internal match keys, not for output


def attach_award_and_spend(df: pd.DataFrame) -> pd.DataFrame:
    """Join SPENT (payments fact) + AWARDED (eTenders/TED spine) per CRO firm; the two € tiers stay
    in separate columns and are NEVER summed. Adds standing/dissolved flags + confidence handling."""
    df = df.copy()

    # SPENT — money actually paid/committed (safe-to-sum only)
    pay = pd.read_parquet(
        PAYMENTS, columns=["cro_company_num", "amount_eur", "value_safe_to_sum", "publisher_name", "year"]
    )
    p = pay[pay["value_safe_to_sum"] & pay["cro_company_num"].notna()].copy()
    p["cro_company_num"] = p["cro_company_num"].astype("int64")
    spend = (
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
    spend["cro_company_num"] = spend["cro_company_num"].astype("float64")
    df = df.merge(spend, on="cro_company_num", how="left")

    # AWARDED — eTenders + TED contract awards won (safe-to-sum only)
    df = df.merge(_load_award_spine(), on="cro_company_num", how="left")
    for col in ("won_etenders", "won_ted"):
        df[col] = df[col].eq(True) if col in df else False
    df["won_public_tender"] = df["won_etenders"] | df["won_ted"]
    for col in ("total_award_eur", "etenders_award_eur", "ted_award_eur"):
        if col not in df:
            df[col] = pd.NA

    # standing + dissolved-match handling (.eq(True) treats NaN as False without dtype downcast)
    active = df["cro_active"].eq(True) if "cro_active" in df else pd.Series(False, index=df.index)
    overdue = df[HEALTH_FLAGS].eq(True).any(axis=1) if set(HEALTH_FLAGS) <= set(df.columns) else False
    has_cro = df["cro_company_num"].notna()
    df["not_good_standing"] = has_cro & ((~active) | overdue)
    df["received_public_money"] = df["public_eur"].notna()
    df["won_public_award"] = df["total_award_eur"].fillna(0).gt(0)
    df["has_public_track_record"] = df["received_public_money"] | df["won_public_award"]
    df["match_review_needed"] = has_cro & (~active)
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


def _load_award_spine() -> pd.DataFrame:
    """eTenders + TED contract awards per CRO firm (AWARDED tier — kept distinct from SPENT)."""
    if not AWARD_LINK.exists():
        log.warning("award spine %s missing — award columns will be empty", AWARD_LINK)
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
    return (
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


def collapse_by_cro(df: pd.DataFrame, list_cols: tuple[str, ...], name_col: str) -> pd.DataFrame:
    """Merge name-variant rows that resolved to the same CRO firm into one row, so € is safe to sum.

    ``list_cols`` are set-unioned; ``name_col`` (the register's display name) keeps the first variant
    and a ``name_variants`` count is added. Unmatched rows pass through untouched.
    """

    def _union(series):
        acc = set()
        for v in series:
            if isinstance(v, (list, tuple)):
                acc.update(v)
        return sorted(acc)

    matched = df[df["cro_company_num"].notna()].sort_values("match_confidence", ascending=False)
    unmatched = df[df["cro_company_num"].isna()].copy()
    scalar = {c: "first" for c in df.columns if c not in ("cro_company_num", *list_cols, name_col)}
    scalar.update({c: _union for c in list_cols})
    scalar[name_col] = "first"
    collapsed = matched.groupby("cro_company_num", as_index=False).agg(scalar)
    collapsed["name_variants"] = matched.groupby("cro_company_num")[name_col].nunique().values
    unmatched["name_variants"] = 1
    return pd.concat([collapsed, unmatched], ignore_index=True)
