"""Election Finance — unified Irish general-election political-finance hub.

One page for the whole 2024 general-election money story, drawn from THREE separate
official SIPO records that used to be scattered across two pages:

  * Donations      — what donors declared giving to parties (> €1,500).
  * Party spending — each party's national-agent spend ON its candidates (Part 3).
  * Candidates     — each candidate's OWN Expenses Statement, down to Part-5 lines.

A fourth tab adds GE2020 party national-agent spending — a SEPARATE election, OCR'd
from scanned returns, never summed with the 2024 figures.

A tab strip (?view=overview|donations|party|candidates|ge2020) routes between them;
drill params (?dparty / ?eparty / ?cand / ?gparty) take precedence and open a detail
view. The page title is "Election Finance"; ?view=ge2020 swaps the hero to a GE2020
banner. (Module/function still named election_2024 — url_path is unchanged.)

Layout / controls / HTML only — NO business logic. Every figure comes from the
v_sipo_* views via the three sipo_*_data data-access wrappers (which forbid parquet
reads / GROUP BY / pandas aggregation). The combined per-party "full picture" card
reads v_sipo_ge2024_party_finance, where the cross-return JOIN lives in the pipeline.

⚠ NEVER SUM ACROSS THE THREE STREAMS — they are different grains and party-agent
spend overlaps candidate spend (two views of the same campaigning, different
returns). The money map states this in copy and the view-layer keeps them apart.

No-inference posture throughout: OCR-derived rows carry a "verify · SIPO PDF" mark;
coverage is shown as "M of 607" so the gap is explicit; donor home addresses are
never shown; spending is never asserted to imply influence.
"""

from __future__ import annotations

from html import escape as _h
from urllib.parse import quote

import pandas as pd
import streamlit as st

from data_access.sipo_candidate_data import (
    fetch_by_category,
    fetch_candidate,
    fetch_filed_unquantified,
    fetch_line_items,
    fetch_party_finance,
    fetch_ranked,
    fetch_top_details,
    fetch_totals,
)
from data_access.sipo_donations_data import (
    fetch_donations_by_party,
    fetch_donations_totals,
    fetch_party_donors,
)
from data_access.sipo_expenses_data import (
    fetch_expenses_by_party,
    fetch_expenses_totals,
    fetch_ge2020_by_party,
    fetch_ge2020_party_categories,
    fetch_ge2020_party_items,
    fetch_ge2020_party_overall,
    fetch_ge2020_totals,
    fetch_party_candidates,
    fetch_party_national_categories,
    fetch_party_national_items,
    fetch_party_national_overall,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    page_error_boundary,
    party_colour,
    text_search_mask,
    totals_strip,
)
from ui.entity_links import member_profile_url
from ui.source_pdfs import provenance_expander

# Total per-candidate expense statements in the SIPO GE2024 corpus (the OCR work-list).
# Shown so the coverage gap is explicit, never implied-complete.
_CORPUS_TOTAL = 607

_CAT_ORDER = ["5A", "5B", "5C", "5D", "5E", "5F", "5G", "5H"]
_STAT_CAP = 58_350.0  # statutory 5-seat candidate limit — a per-candidate category cell
# above this is an OCR decimal-loss artefact (only ever on non-reconciling statements).

# Stream colours — also used by the .e24-bar.{in,agent,cand} CSS classes.
_C_IN = "#2e7d6b"
_C_AGENT = "#3a6ea5"
_C_CAND = "#8a5a9e"

_TABS = [
    ("overview", "Overview"),
    ("donations", "Donations"),
    ("party", "Party spending"),
    ("candidates", "Candidates"),
    ("ge2020", "Election 2020"),
]

_DON_CAVEAT = (
    "Source: Standards in Public Office Commission, 2024 election donation "
    "statements. Figures are OCR-read from the official scanned returns; rows "
    "marked “to verify” should be checked against the source PDF. Donations are a "
    "matter of public record; nothing here implies influence or wrongdoing."
)
_EXP_CAVEAT = (
    "Source: Standards in Public Office Commission, 2024 National-Agent election "
    "expenses statements. Figures are OCR-read from the official scanned returns; "
    "rows marked “to verify” should be checked against the source PDF. This is "
    "per-candidate national-agent spend, not a party's total campaign outlay; "
    "nothing here implies influence or wrongdoing."
)
_CAND_CAVEAT = (
    "Source: Standards in Public Office Commission — individual candidates' 2024 "
    "general-election Expenses Statements. Figures are OCR-read from the official "
    "scanned returns; rows marked “verify” should be checked against the source PDF. "
    "Spending here is what a candidate spent campaigning — nothing implies influence "
    "or wrongdoing."
)
_GE2020_CAVEAT = (
    "Source: Standards in Public Office Commission, 2020 National-Agent election "
    "expenses statements. Figures are OCR-read from the official scanned returns; the "
    "printed party total is the headline, and parties marked “verify” are where the "
    "OCR'd line items don't sum to that total — check those against the source PDF. "
    "This is party-level national-agent spend, never added to the 2024 figures, and "
    "nothing here implies influence or wrongdoing."
)


# ── shared candidate-tier CSS (category bars + line-item rows) ────────────────────

_ESP_CSS = """
<style>
.esp-cats{display:flex;flex-direction:column;gap:0.35rem;margin:0.4rem 0 0.2rem}
.esp-catrow{display:grid;grid-template-columns:13rem 1fr 6rem;align-items:center;gap:0.6rem;font-size:0.85rem}
.esp-catlabel{color:var(--text-meta)}
.esp-cattrack{background:#eee;border-radius:4px;height:0.62rem;overflow:hidden}
.esp-catbar{display:block;height:100%;background:#3a6ea5}
.esp-catval{text-align:right;font-variant-numeric:tabular-nums}
.esp-items{display:flex;flex-direction:column;gap:0.2rem;margin-top:0.4rem}
.esp-irow{display:grid;grid-template-columns:8.5rem 1fr 6rem;gap:0.6rem;align-items:baseline;
  padding:0.32rem 0.55rem;background:#ffffff;border:1px solid rgba(0,0,0,0.07);border-radius:5px;font-size:0.88rem}
.esp-icat{color:var(--text-meta);font-size:0.78rem}
.esp-ivendor{font-weight:500}
.esp-icost{text-align:right;font-variant-numeric:tabular-nums}
</style>
"""


def _category_bars(by_cat: pd.DataFrame) -> str:
    """Simple proportional bars for the 8 statutory categories."""
    if by_cat.empty:
        return ""
    mx = float(by_cat["total_spend"].max() or 1)
    rows = []
    for _, c in by_cat.iterrows():
        label = _h(str(c["category_label"] or c["category"]))
        total = float(c["total_spend"] or 0)
        pct = max(2.0, total / mx * 100)
        rows.append(
            f'<div class="esp-catrow">'
            f'<span class="esp-catlabel">{_h(str(c["category"]))} · {label}</span>'
            f'<span class="esp-cattrack"><span class="esp-catbar" style="width:{pct:.0f}%"></span></span>'
            f'<span class="esp-catval">€{total:,.0f}</span>'
            "</div>"
        )
    return f'<div class="esp-cats">{"".join(rows)}</div>'


# ── page head + tab strip ─────────────────────────────────────────────────────────


def _render_head(view: str = "") -> None:
    if view == "ge2020":
        hero_banner(
            kicker="POLITICAL FINANCE · GENERAL ELECTION 2020",
            title="Election 2020",
            dek=(
                "What each party's national agent spent at the 2020 general election, "
                "OCR-read from the official scanned SIPO returns. Party-level national-agent "
                "spend only — not donations or per-candidate returns — and never added to "
                "the 2024 figures."
            ),
        )
    else:
        hero_banner(
            kicker="POLITICAL FINANCE · GENERAL ELECTIONS",
            title="Election Finance",
            dek=(
                "The money behind recent Irish general elections. The 2024 picture in full — "
                "what donors gave to parties, what party agents spent on candidates, and what "
                "each candidate spent themselves — plus party spending from 2020 on its own "
                "tab. Separate official returns at different grains, never a single ledger."
            ),
        )
    glossary_strip(
        [
            ("SIPO", "Standards in Public Office Commission"),
            ("TD", "Teachta Dála (member of the Dáil)"),
        ]
    )


def _tab_strip(active: str) -> None:
    chips = []
    for slug, label in _TABS:
        cls = "e24-tab active" if slug == active else "e24-tab"
        chips.append(f'<a class="{cls}" href="?view={slug}" target="_self">{_h(label)}</a>')
    st.html(f'<div class="e24-tabs">{"".join(chips)}</div>')


# ── Overview tab: the money map + per-party "full picture" cards ──────────────────


def _tier_html(stripe: str, lbl: str, amt: float, meta: str, grain: str) -> str:
    return (
        f'<div class="e24-tier" style="--e24-stripe:{stripe}">'
        f'<span class="lbl">{_h(lbl)}</span>'
        f'<span class="amt">€{amt:,.0f}</span>'
        f'<span class="meta">{_h(meta)}</span>'
        f'<span class="grain">{_h(grain)}</span>'
        "</div>"
    )


def _money_map() -> None:
    don = fetch_donations_totals()
    exp = fetch_expenses_totals()
    cand = fetch_totals()
    loaded = cand["candidates"]

    t1 = _tier_html(
        _C_IN,
        "↓ Donated to parties",
        float(don["total"] or 0),
        f"{don['parties']} parties · {don['donations']} donations",
        "declared receipts over €1,500",
    )
    t2 = _tier_html(
        _C_AGENT,
        "→ Party agents' spend",
        float(exp["total"] or 0),
        f"{exp['candidates']} candidates · {exp['parties']} parties",
        "national-agent returns (Part 3)",
    )
    t3 = _tier_html(
        _C_CAND,
        "→ Candidates' own spend",
        float(cand["total"] or 0),
        f"{loaded} of {_CORPUS_TOTAL} statements",
        "each candidate's own return",
    )
    st.html(f'<div class="e24-map">{t1}<div class="e24-arrow">→</div>{t2}<div class="e24-arrow">→</div>{t3}</div>')
    st.html(
        '<div class="e24-nosum">⚠ Three separate official records at different grains — '
        "never add them together. Donations are money <em>received</em>; the other two are "
        "money <em>spent</em>. Party-agent spend and candidates' own spend are two views of "
        "the same campaigning from different returns (they overlap). Candidate coverage is "
        "incremental as returns are processed.</div>"
    )


def _safe_max(s: pd.Series) -> float:
    return float(s.max()) if s.notna().any() else 1.0


def _party_finance_card(row: pd.Series, mx: dict[str, float]) -> str:
    party = str(row["party"])
    stripe = party_colour(party)
    streams = [
        ("in", "↓ Donated in", row["donated_in_eur"], mx["in"], f"?dparty={quote(party)}"),
        ("agent", "→ Agent spend", row["agent_spend_eur"], mx["agent"], f"?eparty={quote(party)}"),
        ("cand", "→ Candidate spend", row["candidate_spend_eur"], mx["cand"], "?view=candidates"),
    ]
    srows = []
    for cls, lbl, val, mxv, href in streams:
        if pd.notna(val) and val:
            v = float(val)
            pct = max(2.0, v / mxv * 100) if mxv else 0
            bar = f'<span class="e24-track"><span class="e24-bar {cls}" style="width:{pct:.0f}%"></span></span>'
            sv = f'<span class="sv">€{v:,.0f}</span>'
        else:
            bar = '<span class="e24-track"></span>'
            sv = '<span class="sv none">—</span>'
        srows.append(
            f'<a class="e24-stream-lbl" href="{href}" target="_self">'
            f'<span class="e24-stream"><span class="sl">{_h(lbl)} →</span>{bar}{sv}</span></a>'
        )
    return (
        f'<div class="e24-pcard" style="--e24-stripe:{stripe}">'
        f'<div class="phead"><span class="sw"></span><h3>{_h(party)}</h3></div>'
        f'<div class="e24-streams">{"".join(srows)}</div>'
        "</div>"
    )


def _render_overview() -> None:
    st.caption(
        "The whole 2024 election money picture in one place — follow it from donations "
        "received, to party-agent spend, to each candidate's own return."
    )
    _money_map()

    pf = fetch_party_finance()
    if pf.empty:
        empty_state(
            "No party-level returns loaded",
            "Party donation and spending returns appear here once filed and processed.",
        )
        return

    st.markdown("#### By party · the full picture")
    st.html(
        '<div class="e24-legend">'
        f'<span class="lk"><span class="dot" style="background:{_C_IN}"></span>Donated in</span>'
        f'<span class="lk"><span class="dot" style="background:{_C_AGENT}"></span>Party-agent spend on candidates</span>'
        f'<span class="lk"><span class="dot" style="background:{_C_CAND}"></span>Candidates'
        "&#39; own spend</span></div>"
    )
    mx = {
        "in": _safe_max(pf["donated_in_eur"]),
        "agent": _safe_max(pf["agent_spend_eur"]),
        "cand": _safe_max(pf["candidate_spend_eur"]),
    }
    cards = "".join(_party_finance_card(r, mx) for _, r in pf.iterrows())
    st.html(f'<div class="don-grid">{cards}</div>')
    st.caption(
        "Bars are scaled within each stream, so a party's three bars are NOT comparable "
        "to each other — only to the same-coloured bar on other parties. Streams are "
        "different records and are never added together. “—” means no return in that "
        "stream, never zero."
    )


# ── Donations tab + drill ─────────────────────────────────────────────────────────


def _don_party_card(row: pd.Series) -> str:
    party = str(row["party"])
    stripe = party_colour(party)
    total = float(row["total_value"] or 0)
    n = int(row["donation_count"] or 0)
    vc = int(row["verify_count"] or 0)
    verify = f'<span class="don-vmark">{vc} to verify</span>' if vc else ""
    return (
        f'<a class="don-card" href="?dparty={quote(party)}" target="_self" '
        f'style="--don-stripe:{stripe}">'
        f'<span class="don-dir">↑ received</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(party)}</h3></div>'
        f'<div class="don-amount">€{total:,.0f}</div>'
        f'<div class="don-sub">{n} donation{"" if n == 1 else "s"}</div>'
        f'<div class="don-cardfoot"><span class="go">View donors →</span>{verify}</div>'
        "</a>"
    )


def _render_party_donor_list(party: str) -> None:
    if back_button("← All parties", key="don_back"):
        st.query_params.pop("dparty", None)
        st.rerun()
    st.markdown(f"#### {_h(party)} · donations received 2024")
    donors = fetch_party_donors(party)
    if donors.empty:
        empty_state(
            "No donations on record",
            f"{party} declared no donations above the €1,500 threshold for 2024.",
        )
        return
    stripe = party_colour(party)
    rows: list[str] = []
    for _, d in donors.iterrows():
        amt = float(d["value_eur"] or 0)
        date = _h(str(d["date_received_raw"] or "—"))
        method = _h(str(d["nature"] or "")[:24])
        vmark = (
            f'<span class="don-vmark">verify · SIPO p.{int(d["source_page"])}</span>' if bool(d["needs_verify"]) else ""
        )
        rows.append(
            f'<div class="don-rrow"><span class="dn">{_h(str(d["donor_name"]))}</span>'
            f'<span class="dt">{date}</span><span class="mt">{method}</span>'
            f'<span class="da">€{amt:,.0f}</span>{vmark}</div>'
        )
    st.html(f'<div class="don-receipts" style="--don-stripe:{stripe}">{"".join(rows)}</div>')
    st.caption("Donor name, amount, date and method are the public record. Home addresses are never shown.")


def _render_donations_tab() -> None:
    st.caption(
        "Donations over €1,500 that parties declared to the Standards in Public Office "
        "Commission for 2024. Donor names and amounts are the public record; home "
        "addresses are not shown."
    )
    totals = fetch_donations_totals()
    totals_strip(
        [
            (f"€{totals['total']:,.0f}", "declared (> €1,500)"),
            (str(totals["parties"]), "parties"),
            (str(totals["donations"]), "donations"),
        ]
    )
    by_party = fetch_donations_by_party()
    if by_party.empty:
        empty_state(
            "No donations on record",
            "No declared party donations are loaded for this election yet.",
        )
        return
    cards = "".join(_don_party_card(r) for _, r in by_party.iterrows())
    st.html(f'<div class="don-grid">{cards}</div>')
    st.caption(_DON_CAVEAT)


# ── Party-spending tab + drill (national-agent expenses) ──────────────────────────


def _exp_party_card(row: pd.Series) -> str:
    party = str(row["party"])
    stripe = party_colour(party)
    total = float(row["total_expenditure"] or 0)
    n = int(row["candidate_count"] or 0)
    exc = int(row["excluded_count"] or 0)
    note = f'<span class="don-vmark">{exc} to verify</span>' if exc else ""
    return (
        f'<a class="don-card" href="?eparty={quote(party)}" target="_self" '
        f'style="--don-stripe:{stripe}">'
        f'<span class="don-dir">↓ spent on candidates</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(party)}</h3></div>'
        f'<div class="don-amount">€{total:,.0f}</div>'
        f'<div class="don-sub">{n} candidate{"" if n == 1 else "s"}</div>'
        f'<div class="don-cardfoot"><span class="go">View candidates →</span>{note}</div>'
        "</a>"
    )


def _national_category_bars(cats: pd.DataFrame) -> str:
    """Proportional bars for the 8 Part-4 statutory headings. `category_total_eur` is
    the printed official figure; a heading whose itemised lines don't sum to it carries
    an 'items partial' mark (the total stays trustworthy, the line list under-captures)."""
    if cats.empty:
        return ""
    mx = float(cats["category_total_eur"].max() or 1)
    rows = []
    for _, c in cats.iterrows():
        sec = _h(str(c["section"]))
        label = _h(str(c["category_label"]))
        total = float(c["category_total_eur"] or 0)
        pct = max(2.0, total / mx * 100) if mx else 0
        mark = "" if bool(c["reconciles"]) else '<span class="don-vmark">items partial</span>'
        rows.append(
            f'<div class="esp-catrow"><span class="esp-catlabel">{sec} · {label} {mark}</span>'
            f'<span class="esp-cattrack"><span class="esp-catbar" style="width:{pct:.0f}%"></span></span>'
            f'<span class="esp-catval">€{total:,.0f}</span></div>'
        )
    return f'<div class="esp-cats">{"".join(rows)}</div>'


def _render_party_national_spend(party: str) -> None:
    """Part-4: the party's own itemised central campaign spend (category bars + line
    items). Incremental coverage — shown only where the Part-4 pages have been OCR'd."""
    overall = fetch_party_national_overall(party)
    if overall is None:
        st.caption(
            "Itemised national-agent spend (Part 4 of the return) has not been processed "
            f"for {party} yet — extraction is ongoing."
        )
        return

    totals_strip([(f"€{overall:,.0f}", "national-agent spend · itemised (Part 4)")])
    cats = fetch_party_national_categories(party)
    if not cats.empty:
        st.markdown("##### Where the party spent · by category")
        st.html(_national_category_bars(cats))

    items = fetch_party_national_items(party)
    if not items.empty:
        with st.expander(f"All {len(items)} national-agent line items"):
            rows = []
            for _, it in items.iterrows():
                sec = _h(str(it["section"] or ""))
                desc = _h(str(it["item_description"])) if pd.notna(it["item_description"]) else "—"
                cost = float(it["cost_eur"] or 0)
                vmark = "" if bool(it["is_verified"]) else '<span class="don-vmark">verify</span>'
                rows.append(
                    f'<div class="esp-irow"><span class="esp-icat">{sec}</span>'
                    f'<span class="esp-ivendor">{desc} {vmark}</span>'
                    f'<span class="esp-icost">€{cost:,.2f}</span></div>'
                )
            st.html(f'<div class="esp-items">{"".join(rows)}</div>')
            st.caption(
                "“Expenditure item” is the agent's free-text entry — a mix of supplier "
                "names and item descriptions, not a verified vendor list."
            )
    st.caption(
        "Part-4 itemised expenditure by the party's national agent (its central campaign "
        "outlay), from the “Expenses Review” page of the SIPO return. Category totals are "
        "the printed official figures; headings marked “items partial” are where the "
        "itemised OCR under-captures the total."
    )


def _render_party_candidate_list(party: str) -> None:
    if back_button("← All parties", key="exp_back"):
        st.query_params.pop("eparty", None)
        st.rerun()
    st.markdown(f"#### {_h(party)} · 2024 election spending")

    # Part 4 — what the party actually spent centrally, itemised (incremental coverage).
    _render_party_national_spend(party)

    # Part 3 — how the national agent apportioned a limit-bound amount to each candidate.
    st.markdown("##### Apportioned to each candidate · Part 3")
    cands = fetch_party_candidates(party)
    if cands.empty:
        empty_state(
            "No candidate apportionment on record",
            f"{party} has no Part-3 per-candidate expenditure loaded.",
        )
        return
    stripe = party_colour(party)
    rows: list[str] = []
    for _, c in cands.iterrows():
        flag = str(c["flag"])
        name = _h(str(c["candidate_name"] or "—"))
        const = _h(str(c["constituency"] or "—"))
        page = int(c["source_page"]) if pd.notna(c["source_page"]) else 0
        cap = float(c["statutory_limit_eur"]) if pd.notna(c["statutory_limit_eur"]) else None
        if flag in ("over_limit_verify", "no_amount"):
            # decimal-loss / missing amount — never show the bad magnitude
            amt_html = '<span class="da">—</span>'
            meta = f'<span class="don-vmark">verify · SIPO p.{page}</span>'
        else:
            amt = float(c["expenditure_eur"] or 0)
            amt_html = f'<span class="da">€{amt:,.0f}</span>'
            if not bool(c["is_verified"]):
                meta = f'<span class="don-vmark">verify · SIPO p.{page}</span>'
            elif cap and amt:
                # Tier-1 context: how much of the statutory cap this candidate used.
                meta = f'<span class="dt">{amt / cap * 100:.0f}% of €{cap:,.0f} cap</span>'
            else:
                meta = ""
        rows.append(
            f'<div class="don-rrow"><span class="dn">{name}</span><span class="mt">{const}</span>{amt_html}{meta}</div>'
        )
    st.html(f'<div class="don-receipts" style="--don-stripe:{stripe}">{"".join(rows)}</div>')
    st.caption(
        "Part 3 apportions a limit-bound amount to each candidate (not the same as the "
        "Part-4 central spend above — the two are different parts of one return and are "
        "not added together). “% of cap” is spend against that candidate's statutory limit."
    )


def _render_expenses_tab() -> None:
    st.caption(
        "What each party's national agent spent at the 2024 general election. The card "
        "total is the Part-3 amount apportioned across candidates; open a party to see its "
        "Part-4 itemised central spend by category (Advertising, Posters, …) where loaded."
    )
    totals = fetch_expenses_totals()
    totals_strip(
        [
            (f"€{totals['total']:,.0f}", "spent on candidates"),
            (str(totals["parties"]), "parties"),
            (str(totals["candidates"]), "candidates"),
        ]
    )
    by_party = fetch_expenses_by_party()
    if by_party.empty:
        empty_state(
            "No expenses on record",
            "No party election-expense returns are loaded for this election yet.",
        )
        return
    cards = "".join(_exp_party_card(r) for _, r in by_party.iterrows())
    st.html(f'<div class="don-grid">{cards}</div>')
    st.caption(_EXP_CAVEAT)


# ── GE2020 tab + drill (national-agent party spending, a SEPARATE election) ────────


def _ge2020_party_card(row: pd.Series) -> str:
    party = str(row["party"])
    stripe = party_colour(party)
    total = float(row["overall_total_eur"] or 0)
    note = "" if bool(row["reconciles"]) else '<span class="don-vmark">verify · SIPO PDF</span>'
    return (
        f'<a class="don-card" href="?gparty={quote(party)}" target="_self" '
        f'style="--don-stripe:{stripe}">'
        f'<span class="don-dir">↓ national-agent spend · 2020</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(party)}</h3></div>'
        f'<div class="don-amount">€{total:,.0f}</div>'
        f'<div class="don-sub">printed party total</div>'
        f'<div class="don-cardfoot"><span class="go">View breakdown →</span>{note}</div>'
        "</a>"
    )


def _render_ge2020_party(party: str) -> None:
    if back_button("← All parties", key="ge2020_back"):
        st.query_params.pop("gparty", None)
        st.rerun()
    st.markdown(f"#### {_h(party)} · 2020 national-agent spending")

    overall = fetch_ge2020_party_overall(party)
    if overall is None or overall["total"] is None:
        empty_state(
            "No national-agent return loaded",
            f"{party} has no processed 2020 national-agent expenses statement.",
        )
        return

    totals_strip([(f"€{float(overall['total']):,.0f}", "national-agent spend · printed total")])
    if not overall["reconciles"]:
        pg = f" (p.{overall['source_page']})" if overall["source_page"] else ""
        st.caption(
            f"⚠ {party}'s OCR'd line items don't sum to the printed total — verify against the "
            f"official SIPO PDF{pg} before using this figure."
        )

    cats = fetch_ge2020_party_categories(party)
    if not cats.empty:
        st.markdown("##### Where the party spent · by category")
        st.html(_national_category_bars(cats))

    items = fetch_ge2020_party_items(party)
    if not items.empty:
        with st.expander(f"All {len(items)} national-agent line items"):
            rows = []
            for _, it in items.iterrows():
                sec = _h(str(it["section"] or ""))
                desc = _h(str(it["item_description"])) if pd.notna(it["item_description"]) else "—"
                cost = float(it["cost_eur"] or 0)
                vmark = "" if bool(it["is_verified"]) else '<span class="don-vmark">verify</span>'
                rows.append(
                    f'<div class="esp-irow"><span class="esp-icat">{sec}</span>'
                    f'<span class="esp-ivendor">{desc} {vmark}</span>'
                    f'<span class="esp-icost">€{cost:,.2f}</span></div>'
                )
            st.html(f'<div class="esp-items">{"".join(rows)}</div>')
            st.caption(
                "“Expenditure item” is the agent's free-text entry — a mix of supplier "
                "names and item descriptions, not a verified vendor list."
            )
    st.caption(_GE2020_CAVEAT)


def _render_ge2020_tab() -> None:
    st.caption(
        "What each party's national agent spent at the 2020 general election — the 2020 "
        "counterpart of the Party-spending tab. Party-level totals only (no per-candidate "
        "apportionment); open a party to see its itemised spend by category."
    )
    totals = fetch_ge2020_totals()
    strip = [
        (f"{totals['parties']}", "parties filed"),
        (f"€{totals['total_reconciling']:,.0f}", "spend · reconciling parties"),
    ]
    if totals["unreconciled_parties"]:
        strip.append((f"{totals['unreconciled_parties']}", "to verify"))
    totals_strip(strip)

    by_party = fetch_ge2020_by_party()
    if by_party.empty:
        empty_state(
            "No 2020 returns loaded",
            "Party national-agent expenses for the 2020 general election appear here once processed.",
        )
        return
    cards = "".join(_ge2020_party_card(r) for _, r in by_party.iterrows())
    st.html(f'<div class="don-grid">{cards}</div>')
    st.caption(
        "The headline sums the parties whose figures reconcile; parties marked “verify” are "
        "shown individually but kept out of that total because their OCR'd figure needs "
        "checking. " + _GE2020_CAVEAT
    )


# ── Candidates tab + drill (per-candidate own statements) ─────────────────────────


def _cand_card(row: pd.Series, rank: int) -> str:
    name = str(row["candidate_name"] or "—")
    const = str(row["constituency_name"] or "—")
    party = row["party"]
    stripe = party_colour(str(party)) if pd.notna(party) and party else "rgba(0,0,0,0.14)"
    total = float(row["total_spend_eur"] or 0)
    ptxt = _h(str(party)) if pd.notna(party) and party else "Unknown party"
    td = '<span class="don-vmark" style="background:#eef;color:#224">TD</span>' if bool(row["is_elected_td"]) else ""
    verify = '<span class="don-vmark">verify · SIPO PDF</span>' if bool(row["needs_verify"]) else ""
    return (
        f'<a class="don-card" href="?cand={quote(name)}" target="_self" '
        f'style="--don-stripe:{stripe}">'
        f'<span class="don-dir">#{rank} · spent campaigning</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(name)}</h3></div>'
        f'<div class="don-amount">€{total:,.0f}</div>'
        f'<div class="don-sub">{_h(const)} · {ptxt}</div>'
        f'<div class="don-cardfoot"><span class="go">View breakdown →</span>{td}{verify}</div>'
        "</a>"
    )


_UNQUANT_NOTE = {
    "no_total_declared": "no total on form",
    "figures_unreadable": "scan unreadable",
}


def _unquantified_card(row: pd.Series) -> str:
    """A FILED candidate with no trustworthy total — searchable, links to the official PDF,
    and shows NO amount (a corrupt magnitude is never displayed; a blank is never asserted
    to be €0). Whole card is the PDF link where we have one, else a plain card."""
    name = str(row["candidate_name"] or "—")
    const = str(row["constituency_name"] or "—")
    party = row["party"]
    stripe = party_colour(str(party)) if pd.notna(party) and party else "rgba(0,0,0,0.14)"
    ptxt = _h(str(party)) if pd.notna(party) and party else "Unknown party"
    note = _UNQUANT_NOTE.get(str(row["filed_status"]), "amount not available")
    td = '<span class="don-vmark" style="background:#eef;color:#224">TD</span>' if bool(row["is_elected_td"]) else ""
    pdf = row["source_pdf_url"]
    inner = (
        f'<span class="don-dir">filed · amount not available</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(name)}</h3></div>'
        f'<div class="don-amount" style="font-size:1rem;color:var(--text-meta)">—</div>'
        f'<div class="don-sub">{_h(const)} · {ptxt}</div>'
        f'<div class="don-cardfoot"><span class="go">'
        + ("Open statement (PDF) ↗" if pd.notna(pdf) and pdf else "Statement on file")
        + f'</span><span class="don-vmark">{_h(note)}</span>{td}</div>'
    )
    if pd.notna(pdf) and pdf:
        return (
            f'<a class="don-card" href="{_h(str(pdf))}" target="_blank" rel="noopener" '
            f'style="--don-stripe:{stripe}">{inner}</a>'
        )
    return f'<div class="don-card" style="--don-stripe:{stripe}">{inner}</div>'


def _render_candidate(name: str) -> None:
    if back_button("← All candidates", key="esp_back"):
        st.query_params.pop("cand", None)
        st.rerun()

    cand = fetch_candidate(name)
    if cand is None:
        empty_state("Candidate not found", f"No election-expenses statement is loaded for {name}.")
        return

    party = cand["party"]
    ptxt = _h(str(party)) if pd.notna(party) and party else "Unknown party"
    const = _h(str(cand["constituency_name"] or "—"))
    total = float(cand["total_spend_eur"] or 0)
    stripe = party_colour(str(party)) if pd.notna(party) and party else "rgba(0,0,0,0.14)"

    st.markdown(f"### {_h(name)}")
    badges = [f"{const}", ptxt, f"€{total:,.0f} total"]
    if bool(cand["is_elected_td"]):
        badges.append("Elected TD")
    totals_strip(
        [
            (f"€{total:,.0f}", "total spend"),
            (f"€{float(cand['spend_not_public_eur'] or 0):,.0f}", "not public funds"),
            (f"€{float(cand['spend_public_eur'] or 0):,.0f}", "public funds"),
        ]
    )

    # Cross-link to the canonical member profile (the roster join enabled this).
    code = cand["unique_member_code"]
    if pd.notna(code) and code:
        href = member_profile_url(str(code))
        st.markdown(
            f'<a class="dt-member-link" href="{_h(href)}" target="_self">View TD profile ↗</a>', unsafe_allow_html=True
        )

    # Category split from the candidate's own grid row. ONLY shown when the statement
    # reconciled — on a non-reconciling statement the per-category cells can carry an
    # OCR decimal-loss value (e.g. a €29,544 "posters" cell on a €6,358 total), which
    # would render a wildly misleading bar. We suppress rather than guess (no-inference).
    reconciled = bool(cand["reconciles"])
    cat_rows = [
        {"category": cat, "category_label": cat, "total_spend": float(v)}
        for cat in _CAT_ORDER
        if (v := cand.get(f"cat_{cat}_eur")) is not None and pd.notna(v) and 0 < v <= _STAT_CAP
    ]
    if reconciled and cat_rows:
        st.markdown("#### Where the money went · by category")
        st.html(_category_bars(pd.DataFrame(cat_rows)))
    elif cat_rows:
        st.markdown("#### Where the money went · by category")
        st.caption(
            "Category breakdown omitted — this statement's figures did not reconcile "
            "on OCR, so the per-category split is unreliable. See the line items below "
            "and verify against the source PDF."
        )

    # Part-5 line items (the Grealish -> Galway Advertiser detail).
    items = fetch_line_items(name)
    st.markdown("#### Line items")
    if items.empty:
        empty_state("No line items parsed", "This statement's itemised Part-5 lines are not available yet.")
    else:
        rows = []
        for _, it in items.iterrows():
            cat = _h(str(it["category_label"] or it["category"] or ""))
            detail = _h(str(it["detail"])) if pd.notna(it["detail"]) else "—"
            cost = float(it["cost_eur"] or 0)
            rows.append(
                f'<div class="esp-irow"><span class="esp-icat">{cat}</span>'
                f'<span class="esp-ivendor">{detail}</span>'
                f'<span class="esp-icost">€{cost:,.2f}</span></div>'
            )
        st.html(
            f'<div class="esp-items" style="border-left:3px solid {stripe};padding-left:0.5rem">{"".join(rows)}</div>'
        )
        st.caption(
            "“Detail” is the candidate's free-text entry — a mix of supplier names "
            "(e.g. Galway Advertiser) and item descriptions (e.g. Posters), and is "
            "captured for only some lines. The items shown are an indicative, partial "
            "view and may not sum to the headline total — verify against the source PDF."
        )

    if bool(cand["needs_verify"]):
        st.caption("⚠ This statement's totals did not reconcile cleanly on OCR — verify against the source PDF.")
    pdf = cand["source_pdf_url"]
    if pd.notna(pdf) and pdf:
        st.markdown(f"[Official SIPO statement (PDF) ↗]({_h(str(pdf))})")


def _render_candidates_tab() -> None:
    totals = fetch_totals()
    loaded = totals["candidates"]
    unq = fetch_filed_unquantified()
    filed = loaded + len(unq)
    totals_strip(
        [
            (f"{filed}", f"of {_CORPUS_TOTAL} filed"),
            (f"{loaded}", "with a usable total"),
            (f"€{totals['total']:,.0f}", "total spend"),
            (f"€{totals['median']:,.0f}", "median candidate"),
            (str(totals["elected"]), "elected TDs"),
        ]
    )
    if filed < _CORPUS_TOTAL:
        st.caption(f"Coverage: {filed} of {_CORPUS_TOTAL} candidate statements processed (extraction is ongoing).")

    ranked = fetch_ranked()
    if ranked.empty and unq.empty:
        empty_state("No candidate expenses loaded", "No per-candidate election-expense statements are available yet.")
        return

    # Display-only filters (row selection, not aggregation) — applied to BOTH the ranked
    # spenders and the filed-without-a-total list, so a search finds any filed candidate.
    parties = ["All parties"] + sorted(
        {str(p) for df in (ranked, unq) for p in df["party"].dropna().tolist() if str(p).strip()}
    )
    fcol, scol = st.columns([1, 2])
    sel_party = fcol.selectbox("Party", parties, key="esp_party")
    search = (
        scol.text_input(
            "Search candidate",
            key="esp_search",
            placeholder="e.g. Grealish",
            help=f"Searches all {len(ranked) + len(unq)} filed candidate statements by name.",
        )
        .strip()
        .lower()
    )

    def _apply(df: pd.DataFrame) -> pd.DataFrame:
        if sel_party != "All parties":
            df = df[df["party"] == sel_party]
        if search:
            df = df[text_search_mask(df, search, ["candidate_name"])]
        return df

    filtered = sel_party != "All parties" or bool(search)

    # Ranked spenders (the rows with a trustworthy total). fetch_ranked has no row cap,
    # so any of them is findable by name — the 120 cap below is display-only.
    view = _apply(ranked)
    n = len(view)
    heading = "Matching candidates" if filtered else "Top campaign spenders"
    st.markdown(f"#### {heading} · {n} candidate{'' if n == 1 else 's'}")
    if view.empty:
        st.caption(
            "No candidates with a usable total match — see the “also filed” list below."
            if filtered
            else "No ranked candidates loaded yet."
        )
    else:
        cards = "".join(_cand_card(r, i + 1) for i, (_, r) in enumerate(view.head(120).iterrows()))
        st.html(f'<div class="don-grid">{cards}</div>')
        if n > 120:
            st.caption(f"Showing the top 120 of {n} by spend. Use the filters above to narrow.")
    st.caption(_CAND_CAVEAT)

    # Filed-but-unquantified: searchable, no amount shown, each links to the official PDF.
    uq_view = _apply(unq)
    if not uq_view.empty:
        st.markdown(f"#### Also filed · no usable total · {len(uq_view)} candidate{'' if len(uq_view) == 1 else 's'}")
        with st.expander("Show these candidates", expanded=filtered):
            cards = "".join(_unquantified_card(r) for _, r in uq_view.head(150).iterrows())
            st.html(f'<div class="don-grid">{cards}</div>')
            st.caption(
                "These candidates filed a 2024 Election Expenses Statement, but its total "
                "could not be read reliably from the scan (the total cell was blank, or the "
                "only figure was a corrupt OCR magnitude). No amount is shown rather than a "
                "guessed one — open the official SIPO statement to read the real figures."
            )

    # Category breakdown across all loaded candidates.
    by_cat = fetch_by_category()
    if not by_cat.empty:
        st.markdown("#### Where campaign money goes · by category")
        st.html(_category_bars(by_cat))

    # Top spend-detail lines (suppliers + descriptions).
    top = fetch_top_details(20)
    if not top.empty:
        st.markdown("#### Most common spend lines")
        rows = []
        for _, d in top.iterrows():
            detail = _h(str(d["detail"]))
            total = float(d["total_spend"] or 0)
            nc = int(d["candidate_count"] or 0)
            rows.append(
                f'<div class="esp-irow"><span class="esp-icat">{nc} candidate'
                f'{"" if nc == 1 else "s"}</span><span class="esp-ivendor">{detail}</span>'
                f'<span class="esp-icost">€{total:,.0f}</span></div>'
            )
        st.html(f'<div class="esp-items">{"".join(rows)}</div>')
        st.caption(
            "A mix of supplier names and item descriptions as written on the returns — "
            "not a verified vendor list, and a partial view (a free-text detail is "
            "captured for only some line items). Indicative, not exhaustive."
        )


# ── provenance (whole-page) ───────────────────────────────────────────────────────


def _render_provenance(view: str = "") -> None:
    if view == "ge2020":
        provenance_expander(
            sections=[
                "**What this is.** Each party's National-Agent election-expenses return for the "
                "2020 general election — party-level national-agent spend, itemised by statutory "
                "category. A different election and a different return from the 2024 figures on "
                "the other tabs; the two are never added together.",
                "**OCR-derived.** Values are read from the official scanned SIPO PDFs. The "
                "printed party total is the headline; where the itemised lines don't sum to it "
                "the party is marked “verify”, and decimal-loss mis-reads are flagged rather "
                "than shown as fact.",
                "**No inference.** Spending is a matter of public record — nothing here implies "
                "influence or wrongdoing.",
            ],
            source_caption="Source: Standards in Public Office Commission — GE2020 National-Agent "
            "election-expenses statements.",
        )
        return
    provenance_expander(
        sections=[
            "**What this is.** The complete 2024 general-election money picture from three "
            "separate SIPO records: party donation statements (money received), each party's "
            "National-Agent election-expenses return (Part 3 apportions a limit-bound amount to "
            "each candidate; Part 4 itemises the agent's own central spend by category), and "
            "each candidate's own Election Expenses Statement (itemised to Part-5 lines).",
            "**Different grains — never summed.** Donations are receipts; agent spend and "
            "candidate spend are two views of campaign spending from different returns and "
            "overlap. The figures are shown side by side, never added into one total.",
            "**OCR-derived.** Values are read from the official scanned PDFs. Rows that did not "
            "reconcile carry a “verify” mark; decimal-loss mis-reads are excluded, never shown "
            "as a number. Candidate extraction is incremental, so coverage grows over time.",
            "**No inference.** Spending and donations are matters of public record — nothing "
            "here implies influence or wrongdoing. Donor home addresses are never shown. Party "
            "is the registry party for elected TDs and the declared party otherwise; where it "
            "could not be determined it is shown as unknown, never guessed.",
        ],
        source_caption="Source: Standards in Public Office Commission — GE2024 party donation "
        "statements, National-Agent election-expenses statements, and individual candidate "
        "Election Expenses Statements.",
    )


# ── entry point ───────────────────────────────────────────────────────────────────


@page_error_boundary
def election_2024_page() -> None:
    inject_css()
    hide_sidebar()
    st.html(_ESP_CSS)

    qp = st.query_params
    # Drill params win — a card click is a fresh navigation into a detail view,
    # independent of the active tab. Each drill's own back button clears its param.
    if qp.get("cand"):
        _render_head()
        _render_candidate(qp.get("cand"))
        return
    if qp.get("dparty"):
        _render_head()
        _render_party_donor_list(qp.get("dparty"))
        return
    if qp.get("eparty"):
        _render_head()
        _render_party_candidate_list(qp.get("eparty"))
        return
    if qp.get("gparty"):
        _render_head("ge2020")
        _render_ge2020_party(qp.get("gparty"))
        return

    view = qp.get("view") or "overview"
    if view not in {slug for slug, _ in _TABS}:
        view = "overview"
    _render_head("ge2020" if view == "ge2020" else "")
    _tab_strip(view)

    if view == "donations":
        _render_donations_tab()
    elif view == "party":
        _render_expenses_tab()
    elif view == "candidates":
        _render_candidates_tab()
    elif view == "ge2020":
        _render_ge2020_tab()
    else:
        _render_overview()

    _render_provenance(view)
