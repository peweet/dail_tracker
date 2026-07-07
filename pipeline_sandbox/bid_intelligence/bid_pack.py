"""SANDBOX PROTOTYPE — automated bid-intelligence "market-research pack".

Status: EXPERIMENT. Lives entirely under ``pipeline_sandbox/``; it imports the
EXISTING production procurement query functions read-only and COMPOSES them — it
writes nothing to gold/silver, registers no view, and modifies no production file.
This is the engineering proof of the §5 MVP in
``doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`` (and the bid-intelligence assessment):
*when a relevant tender appears, assemble the historical pack a bid manager / QS
would otherwise build by hand.*

What it deliberately is NOT (and the graduation path must preserve):
  - NOT a bid-price calculator and NOT a win-probability model. The data cannot
    quote a job (no project size/area anywhere; 4.5x-15x intra-trade spread;
    framework ceilings 14x-79x above real awards — see the ``bid_signal`` docstring).
  - NOT a place where the three money grains are summed. eTenders awards, EU/TED
    awards, and public-body payments (SPENT vs COMMITTED) stay separate, labelled.
  - NOT a profiler of natural persons: sole-trader/individual awardees are excluded
    from the competitor + payment-evidence blocks and name-masked in the listing
    (the same posture as ``queries.procurement.awards_for_supplier``).

Graduation path (NOT done here — needs owner sign-off per the roadmap's user-domain
list): move ``build_bid_pack`` into ``dail_tracker_core/dossiers.py``, add a
``/v1/procurement/bid-pack`` route + an MCP tool, and wire a scheduled tender-feed
diff + delivery. None of that is started; this module only proves the composition.
"""

from __future__ import annotations

from typing import Any

import duckdb

from dail_tracker_core import serialize
from dail_tracker_core.queries import procurement as proc

# ── Standing caveat: restates the documented never-claim rules verbatim-in-spirit
# (DATA_LIMITATIONS §16-§17 + the bid_signal docstring). No NEW figure or wording
# decision about a money label is made here — those are owner-gated in the roadmap.
PACK_CAVEAT = (
    "HISTORICAL CONTEXT FOR A HUMAN BID/NO-BID AND QS REVIEW — NOT A BID PRICE. "
    "This pack reports what the public record shows (comparable awards, buyer history, "
    "active firms, competition climate, value benchmarks and actual payment evidence). "
    "It does NOT calculate a profitable or winning bid price and does not predict who "
    "will win. Award values are contract ceilings/estimates at the point of award, NOT "
    "money paid; framework/DPS values are multi-year ceilings (only value_safe_to_sum "
    "rows total). The three money grains — eTenders awards, EU/TED awards, and "
    "public-body payments (SPENT vs COMMITTED) — are DIFFERENT grains and are never "
    "summed or blended. There is no project size/area (m2/GFA) in the data, so two "
    "awards in the same category can differ purely by scale; treat every benchmark as "
    "context, never a quote. Single-bid / incumbency figures are recorded facts, often "
    "wholly legitimate — never a verdict."
)

_INDIVIDUAL_CLASS = "sole_trader_or_individual"
_NAME_WITHHELD = "(individual — name withheld)"


def _trade_code(cpv_code: str | None) -> str | None:
    """The 4-digit CPV trade group behind an 8-digit CPV code — the bid_signal key.

    Verified against the corpus: cpv ``72000000`` -> trade ``7200`` (one bid_signal row).
    """
    if not cpv_code:
        return None
    digits = "".join(ch for ch in str(cpv_code) if ch.isdigit())
    return digits[:4] if len(digits) >= 4 else None


def _mask_award_row(rec: dict[str, Any]) -> dict[str, Any]:
    """Disclose that an award happened but withhold a natural person's name — never
    compose an individual's identifiable history (mirrors the production page posture)."""
    if rec.get("supplier_class") == _INDIVIDUAL_CLASS:
        masked = dict(rec)
        masked["supplier"] = _NAME_WITHHELD
        masked["supplier_norm"] = None
        return masked
    return rec


def build_bid_pack(
    conn: duckdb.DuckDBPyConnection,
    *,
    cpv_code: str | None = None,
    buyer: str | None = None,
    max_comparables: int = 50,
    max_firms: int = 12,
    max_evidence_firms: int = 3,
) -> dict[str, Any]:
    """Assemble the historical bid-intelligence pack for a tender context (a CPV
    category and/or a contracting authority). Composition of EXISTING procurement
    views only — no new figure is derived, no price is quoted. At least one of
    ``cpv_code`` / ``buyer`` is required.

    Returns a structured dict whose blocks each carry their own grain note, plus a
    standing ``caveat``. Empty/unavailable blocks are reported honestly, never faked.
    """
    if not cpv_code and not buyer:
        return {"error": "pass a cpv_code and/or a buyer (contracting authority) to build a pack"}

    trade = _trade_code(cpv_code)
    pack: dict[str, Any] = {
        "context": {"cpv_code": cpv_code, "buyer": buyer, "trade_code": trade},
        "caveat": PACK_CAVEAT,
    }

    # 1. Comparable awards (the spine). With BOTH a CPV and a buyer we want the realistic
    #    "this buyer buying this category" set — fetch the buyer's awards (which carry
    #    cpv_code) and filter to the category. CPV-only / buyer-only use the direct view.
    if cpv_code and buyer:
        comp_res = proc.awards_for_authority(conn, buyer)
        spine = "buyer_x_cpv"
    elif cpv_code:
        comp_res = proc.awards_for_cpv(conn, cpv_code)
        spine = "cpv"
    else:
        comp_res = proc.awards_for_authority(conn, buyer)
        spine = "buyer"
    if not comp_res.ok:
        pack["comparable_awards"] = {"unavailable": comp_res.unavailable_reason}
        comparables: list[dict[str, Any]] = []
    else:
        recs = serialize.to_records(comp_res.data)
        if cpv_code and buyer:
            recs = [r for r in recs if r.get("cpv_code") == cpv_code]
        comparables = [_mask_award_row(r) for r in recs]
        pack["comparable_awards"] = {
            "spine": spine,
            "n_total": len(comparables),
            "awards": comparables[:max_comparables],
            "note": "AWARD grain: ceilings/estimates at award, not money paid; only value_safe_to_sum rows total.",
        }

    # 2. Category value benchmark (median / IQR award value) — real figures, AWARD grain.
    if cpv_code:
        cpv_res = proc.cpv_summary(conn, limit=None)
        if cpv_res.ok and not cpv_res.is_empty:
            row = cpv_res.data.loc[cpv_res.data["cpv_code"] == cpv_code]
            pack["category_benchmark"] = serialize.first_record(row)

    # 3. Market signal (bid_signal: award band + ceiling band + median bids + single-bid % + SME win %).
    if trade:
        sig = proc.bid_signal(conn, trade_code=trade)
        if sig.ok and not sig.is_empty:
            pack["market_signal"] = serialize.first_record(sig.data)
            pack["market_signal_note"] = (
                "Award band EXCLUDES framework ceilings (carried separately); median_bids / single_bid_pct "
                "from TED 2024+ where reported; sme_win_pct = SME share of awards. Signals, never a price."
            )

    # 4. Buyer history — award side (reliable, same corpus) + payment side (name-keyed, may not match).
    if buyer:
        auth_res = proc.authority_summary(conn, limit=None)
        if auth_res.ok and not auth_res.is_empty:
            row = auth_res.data.loc[auth_res.data["contracting_authority"] == buyer]
            pack["buyer_awards"] = serialize.first_record(row)
        prof = proc.payments_publisher_profile(conn, buyer)
        if prof.ok and not prof.is_empty and serialize.value(prof.data.iloc[0]["publisher_name"]) is not None:
            pack["buyer_payments"] = serialize.first_record(prof.data)
        else:
            pack["buyer_payments"] = {
                "note": "No public-body payment register row matches this buyer name. Buyer names differ "
                "across the award and payment registers (no shared key), so absence here is a name-key gap, "
                "not proof the buyer makes no payments.",
            }

    # 5. Active firms in this context (the competitor set) — derived from the comparable awards,
    #    COMPANIES ONLY (individuals excluded — never profile a natural person).
    firms: dict[str, dict[str, Any]] = {}
    for r in comparables:
        norm = r.get("supplier_norm")
        if not norm or r.get("supplier_class") == _INDIVIDUAL_CLASS:
            continue
        f = firms.setdefault(norm, {"supplier": r.get("supplier"), "supplier_norm": norm, "n_awards": 0})
        f["n_awards"] += 1
    active = sorted(firms.values(), key=lambda x: x["n_awards"], reverse=True)
    pack["active_firms"] = {
        "n_total": len(active),
        "firms": active[:max_firms],
        "note": "Firms appearing on award rows in this context (counts only — award counts are the "
        "trustworthy metric; ceilings distort value shares). Companies only; individuals excluded.",
    }

    # 6. Payment evidence for the leading incumbent(s) — what the State actually PAID them
    #    (the differentiator). SPENT/COMMITTED carried separately, never summed.
    evidence: list[dict[str, Any]] = []
    for f in active[:max_evidence_firms]:
        pe = proc.payments_for_supplier(conn, f["supplier_norm"])
        if pe.ok and not pe.is_empty:
            evidence.append(
                {
                    "supplier": f["supplier"],
                    "supplier_norm": f["supplier_norm"],
                    "payment_footprint": serialize.to_records(pe.data),  # one row per realisation_tier
                }
            )
    pack["incumbent_payment_evidence"] = {
        "firms": evidence,
        "note": "PAYMENT grain: paid (SPENT) and ordered (COMMITTED) are different lifecycle stages, never "
        "added; an indicative floor across mixed VAT bases; never summed with award ceilings.",
    }

    # 7. Re-bid radar — contracts in this context whose ADVERTISED term ends soon (future opportunity).
    exp = proc.expiring_contracts_etenders(conn, months_ahead=24, limit=None)
    if exp.ok and not exp.is_empty:
        rows = serialize.to_records(exp.data)
        if cpv_code:
            rows = [r for r in rows if r.get("cpv_code") == cpv_code]
        elif buyer:
            rows = [r for r in rows if r.get("buyer_name") == buyer]
        pack["rebid_radar"] = {
            "n": len(rows),
            "contracts": rows[:max_comparables],
            "note": "Advertised term end (award date + duration) — a term, not a verified end event; "
            "renewals may extend it. Frameworks excluded by the view.",
        }

    return pack
