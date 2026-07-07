"""Caveat / provenance metadata — the single source of truth, in CORE.

Caveats used to live as strings inside the Streamlit pages (sometimes duplicated
across a SQL-view header, a page comment, and the rendered copy). They belong with
the data, not the interface: the same claim must ride on every surface that serves
a figure — the Streamlit page, the JSON API, and any future file-based "dossier
pack". This module holds the canonical text so each interface RENDERS it but none
OWNS it.

Each constant is a typed caveat *family* (causation / coverage / money-grain /
method-confidence / legal-state). The text is moved VERBATIM from where it was
already approved (the dossier composition layer, the data-coverage scope guard, and
the relevant page copy) — provenance wording is the user's domain and is never
invented here. New surfaces attach the matching constant rather than re-phrasing.

The dossier helpers import these names; ``serialize.envelope(..., caveat=...)`` lets
list endpoints carry one in ``head``.
"""

from __future__ import annotations

# ── Cross-reference: votes × Register of Members' Interests ───────────────────
INTERESTS = (
    "Register of Members' Interests covers 2020–2025 only; divisions before 2020 "
    "have no interests counterpart and match nothing. 'landlord'/'property' use the "
    "derived flags; 'director'/'shareholder' use the declared interest_category. "
    "held_in_vote_year=true means the interest was declared in the vote's own year."
)

# ── Procurement × lobbying co-occurrence ──────────────────────────────────────
# Mirrors data/_meta/procurement_lobbying_overlap_coverage.json.
PROC_LOBBY = (
    "Co-occurrence by ENTITY only: each company appears on BOTH the public-procurement "
    "award register and the lobbying register. NOT evidence that lobbying influenced any "
    "contract — there is no shared key linking a specific lobby to a specific award. "
    "Exact normalised-name matching undercounts (subsidiary / trading-name variants are "
    "missed). awarded_value_safe_eur is a per-supplier total carried on each of that "
    "supplier's lobby entities — never sum it across the nested lobby_entities."
)

# ── Procurement competition (single-bidder signal) ────────────────────────────
COMPETITION = (
    "single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count, from TED 2024+ award "
    "notices — each contract PART counted once (the honest lot-level rate; an earlier "
    "notice-level reading over-stated multi-lot buyers). A FACTUAL competition signal, NEVER "
    "a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke "
    "research equipment, genuine urgency (research universities legitimately single-source a "
    "lot). It is the EU Single Market Scoreboard's procurement-integrity indicator: a prompt "
    "to look, not evidence of wrongdoing. Rank only buyers with a healthy n_lots_with_bidcount "
    "(min_lots default 40); small samples are noisy. Coverage is 2024+ only (the eForms era "
    "carries bid counts)."
)

# ── Procurement awards (the AWARD-CEILING grain) ──────────────────────────────
# Awards are the contracted ceiling, NOT what was spent. Lifted from the procurement
# query docstrings ("Award CEILINGS, not realised spend — use public_body_payments
# for what was paid") + the 3-money-grain master rule.
PROCUREMENT_AWARDS = (
    "eTenders/TED AWARD ceilings — the contracted maximum, NOT realised spend; for what "
    "was actually paid use public-body payments. Only the sum-safe value column is addable "
    "(framework/DPS ceilings and estimates are not), and award values are NEVER summed with "
    "the payments or T&A-allowance grains."
)

# ── Public-body payments (realised SPEND grain) ───────────────────────────────
PUBPAY = (
    "sum-safe spend only; never add to procurement AWARD values (different grain). "
    "VAT basis varies by publisher and is unconfirmed for most (only HSE/Tusla are "
    "documented incl-VAT), so cross-publisher totals mix VAT bases — see "
    "data/_meta/procurement_payments_vat_matrix.json for the per-publisher basis."
)

# ── Lobbying — revolving-door individual (DPO) ────────────────────────────────
DPO = "Co-occurrence on the public lobbying register only — NOT evidence of improper influence."

# ── Corporate notices (Iris Oifigiúil gazette) ────────────────────────────────
CORP_NOTICE = (
    "corporate notices only (no individuals); a wind-up/receivership is a legal-status fact, "
    "not a verdict — and Members' Voluntary Liquidation is a SOLVENT wind-up, not distress"
)
CORP_REPEAT = (
    "regulatory provenance only — not a verdict; exact normalised name match (may miss aliases); "
    "solvent MVLs excluded from the distress count"
)
CORP_RECEIVER = (
    "whole-corpus rankings (filter-independent, precomputed gold); an appointer/operator named on a "
    "receivership notice is a public-record fact, not a verdict on any company or director"
)

# ── Ministerial diaries (who ministers meet) ──────────────────────────────────
DIARY = (
    "from ministers' own published diaries — access, not influence; self-curated, non-exhaustive and "
    "quarterly-in-arrears; a diary meeting is not a lobbying return and co-occurrence implies no causation"
)

# ── Money-grain master rule (the data-coverage scope guard) ───────────────────
MONEY_GRAINS = (
    "procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different "
    "value grains — NEVER sum across them"
)

# ── Local-government council money (revenue / capital / PO / payment) ─────────
# Lifted from the council pages: each council figure stands alone beside the national
# benchmark, never apportioned, never summed across measures or stages.
COUNCIL_MONEY = (
    "Council revenue, capital, purchase orders and payments are different stages of council "
    "money and are never added together; each council's figure is its own reported amount, "
    "shown beside the national benchmark — not apportioned, never summed across measures."
)

# ── NOAC performance indicators (council scorecard) ───────────────────────────
NOAC_SCORECARD = (
    "Each indicator is the council's own reported figure shown beside the national median "
    "benchmark — not apportioned, never summed across measures "
    "(NOAC Performance Indicator Report 2024)."
)

# ── Asylum / Ukraine accommodation spend ──────────────────────────────────────
ACCOMMODATION_SPEND = (
    "Drawn from the published over-€20,000 purchase-order registers (committed spend, by year "
    "and provider); a realised-spend grain — never added to procurement award ceilings or to "
    "other money grains."
)

# ── CSO general-government finance (the public-spend denominator) ──────────────
GOV_FINANCE = (
    "CSO general-government revenue / expenditure / balance (national-accounts aggregates, "
    "GFA01) — the authoritative denominator for 'share of total public spend'; never summed "
    "with transaction-level award or payment registers."
)

# ── Organisation 360° dossier — cross-register entity co-occurrence ────────────
# RESERVED for the planned organisation dossier (see doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md).
# This is the app's highest false-causation surface (money + influence + distress co-located on
# one entity), so the rail is mandatory. Same family as PROC_LOBBY / DIARY, generalised.
ENTITY_COOCCURRENCE = (
    "Cross-register links (procurement, lobbying, ministerial diaries, corporate notices) are "
    "co-occurrence by ENTITY only — the SAME organisation appears on each register. NOT evidence "
    "that one caused another: there is no key linking a specific lobby or meeting to a specific "
    "contract. Exact normalised-name / CRO matching undercounts (subsidiary and trading-name "
    "variants are missed), and any cross-register fusion below an exact match tier is suppressed, "
    "not guessed. No individuals — sole traders / natural persons are excluded."
)

# ── Attendance participation & TAA ────────────────────────────────────────────
ATTENDANCE = (
    "Turnout is divisions voted in ÷ divisions held; office-holders (ministers, chairs, party "
    "leaders) are flagged, not hidden, because not voting can be their role — a low rate is "
    "context, not a verdict. TAA figures are the statutory 120-day Travel & Accommodation "
    "Allowance basis and exclude office-holders (not paid TAA on the attendance basis)."
)
