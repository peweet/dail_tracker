"""
TD Payments — payments.py

Retrieval-only Streamlit page. All parsing, aggregation, and ranking live in
sql_views/payments_*.sql (pipeline layer). All data access functions live in
utility/data_access/payments_data.py.

This file: layout, controls, HTML card rendering, and navigation only.
No groupby, merge, pivot, or metric definitions here.

Genuinely-open pipeline gaps (verified against sql_views/ on 2026-06-04):

TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL on v_payments_sources
    (v_payments_sources exposes source_url; the per-year official PDF link is not yet
    surfaced as a dedicated column)

SHIPPED — tokens cleared 2026-06-04: canonical unique_member_code AND party_name +
constituency are now present on v_payments_member_detail (verified in sql_views/), so
cross-page member links and party/constituency display work without an in-page lookup.
"""

from __future__ import annotations

import datetime as _dt
import re
import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.payments_data import (
    fetch_alltime_ranking,
    fetch_filter_options,
    fetch_payments_summary,
    fetch_since_2020_summary,
    fetch_year_ranking,
)
from data_access.sipo_donations_data import (
    fetch_donations_by_party,
    fetch_donations_totals,
    fetch_party_donors,
)
from data_access.sipo_expenses_data import (
    fetch_expenses_by_party,
    fetch_expenses_totals,
    fetch_party_candidates,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    clean_meta,
    clickable_card_link,
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    member_jump_panel,
    page_error_boundary,
    party_colour,
    ranked_member_card,
    totals_strip,
)
from data_access.identity_resolver import resolve_member_code
from ui.components import member_moved_callout
from ui.entity_links import member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import PAYMENTS, provenance_expander

from config import NOTABLE_TDS, TAA_BAND_TABLE, TAA_DEDUCTIONS_NOTE

# ── Constants ──────────────────────────────────────────────────────────────────

_CAVEAT = (
    "Parliamentary Standard Allowance (PSA) payments cover the cost of carrying out "
    "parliamentary duties. The amount a TD receives is primarily determined by their "
    "TAA distance band — the measured road distance from their normal place of residence "
    "to Leinster House. A higher total does not imply wrongdoing; it reflects living "
    "farther from Dublin. Totals shown here cover both the TAA-banded travel allowance "
    "and the vouched Public Representation Allowance (PRA), including the ministerial "
    "PRA rate where applicable. Data sourced from official Oireachtas payment records."
)

_QUARANTINE_NOTE = (
    "**Data quality notice:** A small number of payment rows fall outside the expected "
    "schema (malformed cells, illegible amounts) and are excluded from this view. The "
    "excluded count is published in `payments_full_psa_quarantine.parquet` and is "
    "typically under 1% of all rows."
)


def _flip_name(raw: str) -> str:
    """'Collins, Michael' → 'Michael Collins'. Pass-through if no comma."""
    if ", " in raw:
        last, first = raw.split(", ", 1)
        return f"{first.strip()} {last.strip()}"
    return raw


def _clean_taa_label(raw: str) -> tuple[str, bool]:
    """Strip the internal '(unmapped)' / '(unknown)' parentheticals from
    TAA band labels so citizens don't see system jargon. Returns
    ``(clean_label, is_unmapped)`` — the second flag drives a small
    caveat pill on the card so users know the distance band isn't
    derived from the current registry (P1-6 audit fix)."""
    is_unmapped = bool(re.search(r"\((?:unmapped|unknown)\)", raw))
    clean = re.sub(r"\s*\((?:unmapped|unknown)\)\s*$", "", raw).strip() or raw
    return clean, is_unmapped


def _pay_card_html(row: pd.Series) -> str:
    """Member name card for the payments ranked list.

    Data ships names "Last, First" (sortable but unidiomatic) and TAA labels
    with "(unmapped)" / "(unknown)" parentheticals (internal pipeline
    metadata). Both are normalised here for display. Unmapped bands carry a
    small caveat glyph + tooltip so the uncertainty is visible without dev
    jargon; mapped bands stay clean.
    """
    name = _flip_name(str(row.get("member_name", "—")))
    pos = str(row.get("position", "Deputy"))
    party = str(row.get("party_name", "") or "")
    constit = str(row.get("constituency", "") or "")
    taa_label, taa_unmapped = _clean_taa_label(str(row.get("taa_band_label", "—")))
    taa = _h(taa_label)
    count = int(row.get("payment_count", 0) or 0)
    total_str = f"€{float(row.get('total_paid', 0) or 0):,.0f}"
    taa_pill_cls = "pay-taa-pill pay-taa-pill-unmapped" if taa_unmapped else "pay-taa-pill"
    taa_caveat = (
        '<span class="pay-taa-caveat" title="Distance band not mapped in the '
        'current registry — verified using the recorded TAA value instead.">?</span>'
        if taa_unmapped
        else ""
    )
    pills = (
        f'<span class="{taa_pill_cls}">{taa}{taa_caveat}</span>'
        f'<span class="pay-count-pill-accent">{count} payments</span>'
    )
    badge = (
        f'<div class="pay-total-badge">'
        f'<span class="pay-total-badge-num">{total_str}</span>'
        f'<span class="pay-total-badge-lbl">total</span>'
        f"</div>"
    )
    return ranked_member_card(
        name=name,
        meta=clean_meta(party, constit) or pos,
        rank=int(row.get("rank_high", 0)),
        pills_html=pills,
        badge_html=badge,
    )


# ── Provenance footer ──────────────────────────────────────────────────────────


def _render_provenance(summary: pd.Series, year: int | None = None, house: str = "Dáil") -> None:
    first_year = summary.get("first_year", "2020")
    last_year = summary.get("last_year", "—")
    year_str = str(year) if year else None
    # PAYMENTS is a curated list of Dáil PSA source PDFs; don't link them under a
    # Seanad view. TODO: add a Seanad PSA-PDF list to ui/source_pdfs.
    links = [] if house == "Seanad" else [(lbl, url) for lbl, url in PAYMENTS if not year_str or year_str in lbl]
    caveat = _CAVEAT if house != "Seanad" else _CAVEAT.replace("a TD receives", "a Senator receives")
    provenance_expander(
        sections=[
            caveat,
            "**TAA distance bands**\n\n" + TAA_BAND_TABLE,
            TAA_DEDUCTIONS_NOTE,
            _QUARANTINE_NOTE,
        ],
        source_caption=(
            f"Data: Oireachtas Parliamentary Standard Allowance records · {first_year}–{last_year}"
            + (f" · Showing {year}" if year else "")
        ),
        pdf_links=links,
    )


# ── Rankings view (all-time since 2020) ───────────────────────────────────────


def _render_rankings(since_2020: dict, summary: pd.Series, house: str, term: str, terms: str) -> None:
    total = since_2020["total"]
    members = since_2020["members"]
    avg = since_2020["avg_per_td"]

    totals_strip(
        [
            (f"€{total:,.0f}", "Total since 2020"),
            (f"{members:,}", f"{terms} with payments"),
            (f"€{avg:,.0f}", f"Avg per {term} since 2020"),
        ]
    )

    alltime = fetch_alltime_ranking(house)
    if alltime.empty:
        empty_state(
            "All-time rankings not yet available",
            "v_payments_alltime_ranking returned no rows. Re-run the pipeline if you expect data here.",
        )
        _render_provenance(summary, house=house)
        return

    st.caption(f"All-time rankings · since 2020 · {len(alltime)} members")

    # The new view exposes the same columns the year-view card already
    # reads, with two name aliases: total_paid_since_2020 → total_paid and
    # payment_count_since_2020 → payment_count. Aliasing here keeps the
    # _pay_card_html helper unchanged.
    top_10 = alltime.head(10).copy()
    next_10 = alltime.iloc[10:20].copy()
    for chunk in (top_10, next_10):
        chunk["total_paid"] = chunk["total_paid_since_2020"]
        chunk["payment_count"] = chunk["payment_count_since_2020"]

    col_l, col_r = st.columns(2)
    for col, chunk in ((col_l, top_10), (col_r, next_10)):
        with col:
            cards: list[str] = []
            for _, row in chunk.iterrows():
                # unique_member_code comes straight from the view (pipeline
                # join), so we don't need to round-trip through a name lookup
                # against v_member_registry — payment names are "Last, First"
                # and accent-stripped, which never exact-matches the registry.
                raw_name = str(row["member_name"])
                display_name = _flip_name(raw_name)
                code = str(row.get("unique_member_code", "") or "").strip() or resolve_member_code(raw_name)
                inner = _pay_card_html(row)
                if code:
                    cards.append(
                        clickable_card_link(
                            href=member_profile_url(code, section="payments"),
                            inner_html=inner,
                            aria_label=f"View {display_name}'s payments profile",
                        )
                    )
                else:
                    cards.append(inner)
            st.html("\n".join(cards))

    _render_provenance(summary, house=house)


# ── Stage 1 — Primary ranked view ─────────────────────────────────────────────


def _render_primary(year_options: list[str], summary: pd.Series, house: str, term: str, terms: str) -> None:
    # Hero + glossary are rendered by the caller (payments_page) so the
    # main-panel member jump can sit between them and the view controls.
    all_views = ["Rankings"] + year_options
    # Default to the most-recent COMPLETED year, not the current YTD year
    # (audit P1-1). year_options is sorted DESC, so skip the first option
    # if it matches the current calendar year.
    current_year_str = str(_dt.date.today().year)
    default_year = year_options[1] if len(year_options) > 1 and year_options[0] == current_year_str else year_options[0]
    selected_view = (
        st.segmented_control(
            "View",
            all_views,
            default=default_year,
            key="pay_view",
            label_visibility="collapsed",
        )
        or default_year
    )

    since_2020 = fetch_since_2020_summary(house)

    if selected_view == "Rankings":
        _render_rankings(since_2020, summary, house, term, terms)
        return

    selected_year = int(selected_view)
    ranking = fetch_year_ranking(selected_year, house)

    if ranking.empty:
        empty_state(
            "No payment data for this year",
            "The selected year has no records in the current dataset.",
        )
        _render_provenance(summary, selected_year, house)
        return

    total_yr = float(ranking.iloc[0]["year_total_paid"])
    yr_count = int(ranking.iloc[0]["year_member_count"])
    avg_yr = float(ranking.iloc[0]["year_avg_per_td"])

    totals_strip(
        [
            (f"€{total_yr:,.0f}", f"Total · {selected_year}"),
            (f"€{avg_yr:,.0f}", f"Avg per {term} · {selected_year}"),
        ]
    )

    st.caption(f"Ranked by total PSA received · {selected_year} · {yr_count} {terms.lower()}")

    top_10 = ranking.head(10)
    next_10 = ranking.iloc[10:20]

    col_l, col_r = st.columns(2)
    for col, chunk in ((col_l, top_10), (col_r, next_10)):
        with col:
            cards: list[str] = []
            for _, row in chunk.iterrows():
                name = str(row["member_name"])
                # Pipeline ships unique_member_code on v_payments_yearly_evolution
                # for ~97% of TDs; name-based resolver is the last-resort fallback
                # (and is broken for the "Last, First" + accent-stripped format
                # the payments parquet uses — every card was non-clickable before).
                code = str(row.get("unique_member_code", "") or "").strip() or resolve_member_code(name)
                if code:
                    cards.append(
                        clickable_card_link(
                            href=member_profile_url(code, section="payments"),
                            inner_html=_pay_card_html(row),
                            aria_label=f"View {_flip_name(name)}'s payments profile",
                        )
                    )
                else:
                    # Member not in v_member_registry — render unwrapped.
                    cards.append(_pay_card_html(row))
            st.html("\n".join(cards))

    export_df = ranking[
        ["rank_high", "member_name", "position", "taa_band_label", "total_paid", "payment_count"]
    ].copy()
    export_df.columns = ["Rank", "Member", "Position", "TAA Band", "Total Paid (€)", "Payments"]
    export_button(
        export_df,
        f"Download {selected_year} payments CSV",
        f"{house.lower()}_payments_{selected_year}.csv",
        key="pay_export_primary",
    )

    _render_provenance(summary, selected_year, house)


# ── Entry point ────────────────────────────────────────────────────────────────


# ── Party donations lens (SIPO political finance, GE2024) ───────────────────────

_DON_CAVEAT = (
    "Source: Standards in Public Office Commission, 2024 election donation "
    "statements. Figures are OCR-read from the official scanned returns; rows "
    "marked “to verify” should be checked against the source PDF. Donations are a "
    "matter of public record; nothing here implies influence or wrongdoing."
)


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
            f'<span class="don-vmark">verify · SIPO p.{int(d["source_page"])}</span>'
            if bool(d["needs_verify"])
            else ""
        )
        rows.append(
            f'<div class="don-rrow"><span class="dn">{_h(str(d["donor_name"]))}</span>'
            f'<span class="dt">{date}</span><span class="mt">{method}</span>'
            f'<span class="da">€{amt:,.0f}</span>{vmark}</div>'
        )
    st.html(f'<div class="don-receipts" style="--don-stripe:{stripe}">{"".join(rows)}</div>')
    st.caption(
        "Donor name, amount, date and method are the public record. "
        "Home addresses are never shown."
    )


def _render_party_donations() -> None:
    hero_banner(
        kicker="POLITICAL FINANCE · GENERAL ELECTION 2024",
        title="Party Donations",
        dek=(
            "Donations over €1,500 that parties declared to the Standards in Public "
            "Office Commission for 2024. Donor names and amounts are the public "
            "record; home addresses are not shown."
        ),
    )
    selected = st.query_params.get("dparty")
    if selected:
        _render_party_donor_list(selected)
        return
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


# ── Party election-expenses lens (SIPO political finance, GE2024) ────────────────
# Companion to the donations lens; reuses the .don-* card styling. Shows the
# national-agent "expenditure on the candidate" column (Part 3) — money SPENT, not
# received. It is per-candidate-allocated spend, so it UNDER-counts parties that book
# spend centrally (Sinn Féin); it is not a total campaign outlay. Decimal-loss OCR
# mis-reads (flag over_limit_verify) are quarantined by the view and shown as
# "verify · SIPO p.N", never as a number.

_EXP_CAVEAT = (
    "Source: Standards in Public Office Commission, 2024 National-Agent election "
    "expenses statements. Figures are OCR-read from the official scanned returns; "
    "rows marked “to verify” should be checked against the source PDF. This is "
    "per-candidate national-agent spend, not a party's total campaign outlay; "
    "nothing here implies influence or wrongdoing."
)


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


def _render_party_candidate_list(party: str) -> None:
    if back_button("← All parties", key="exp_back"):
        st.query_params.pop("eparty", None)
        st.rerun()
    st.markdown(f"#### {_h(party)} · national-agent spend on candidates 2024")
    cands = fetch_party_candidates(party)
    if cands.empty:
        empty_state(
            "No candidate expenses on record",
            f"{party} has no national-agent candidate expenditure loaded.",
        )
        return
    stripe = party_colour(party)
    rows: list[str] = []
    for _, c in cands.iterrows():
        flag = str(c["flag"])
        name = _h(str(c["candidate_name"] or "—"))
        const = _h(str(c["constituency"] or "—"))
        page = int(c["source_page"]) if pd.notna(c["source_page"]) else 0
        if flag in ("over_limit_verify", "no_amount"):
            # decimal-loss / missing amount — never show the bad magnitude
            amt_html = '<span class="da">—</span>'
            vmark = f'<span class="don-vmark">verify · SIPO p.{page}</span>'
        else:
            amt = float(c["expenditure_eur"] or 0)
            amt_html = f'<span class="da">€{amt:,.0f}</span>'
            vmark = (
                f'<span class="don-vmark">verify · SIPO p.{page}</span>'
                if not bool(c["is_verified"])
                else ""
            )
        rows.append(
            f'<div class="don-rrow"><span class="dn">{name}</span>'
            f'<span class="mt">{const}</span>{amt_html}{vmark}</div>'
        )
    st.html(f'<div class="don-receipts" style="--don-stripe:{stripe}">{"".join(rows)}</div>')
    st.caption(
        "National-agent expenditure on each candidate (Part 3 of the SIPO return). "
        "Amounts flagged “verify” are OCR reads to check against the source PDF."
    )


def _render_party_expenses() -> None:
    hero_banner(
        kicker="POLITICAL FINANCE · GENERAL ELECTION 2024",
        title="Election Expenses",
        dek=(
            "What each party's national agent spent on its candidates at the 2024 "
            "general election, from the returns filed with the Standards in Public "
            "Office Commission. This is per-candidate spend — parties also spend "
            "centrally, which is not counted here."
        ),
    )
    selected = st.query_params.get("eparty")
    if selected:
        _render_party_candidate_list(selected)
        return
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


@page_error_boundary
def payments_page() -> None:
    inject_css()
    hide_sidebar()

    # ── Lens: per-member parliamentary payments vs SIPO party donations ─────────
    # Donations are party-level (GE2024), so they get their own lens rather than
    # threading through the per-member, chamber-scoped payments flow.
    lens = (
        st.segmented_control(
            "View",
            options=["Member payments", "Party donations", "Election expenses"],
            default="Member payments",
            key="pay_lens",
            label_visibility="collapsed",
        )
        or "Member payments"
    )
    if lens == "Party donations":
        _render_party_donations()
        return
    if lens == "Election expenses":
        _render_party_expenses()
        return

    summary = fetch_payments_summary()

    # House scope — Dáil (default) or Seanad. The payments views UNION both
    # chambers; the rankings/totals are house-partitioned in the view layer
    # (Senators rank among Senators), and this picker scopes the page to one.
    house = (
        st.segmented_control(
            "Chamber",
            options=["Dáil", "Seanad"],
            default="Dáil",
            key="pay_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    is_seanad = house == "Seanad"
    term, terms = ("Senator", "Senators") if is_seanad else ("TD", "TDs")

    opts = fetch_filter_options(house)
    year_options = opts.get("years", [])
    if not year_options:
        st.error(
            "No payment data available. "
            "Ensure sql_views/payments_*.sql are present and the DuckDB connection is loaded."
        )
        return

    hero_banner(
        kicker="PUBLIC SPENDING · PARLIAMENTARY ALLOWANCES",
        title=f"{term} Payments",
        dek=f"Parliamentary Standard Allowance (PSA): the official record of payments to {house} members.",
    )
    glossary_strip(
        [
            (term, "Seanadóir, a member of the Seanad (Senate)" if is_seanad else "Teachta Dála, a member of the Dáil"),
            ("TAA", "Travel & Accommodation Allowance, reimbursed mileage and overnight stays"),
            ("PRA", "Public Representation Allowance, an unvouched flat allowance for constituency work"),
            ("PSA", "Parliamentary Standard Allowance, the umbrella term for TAA plus PRA"),
        ]
    )

    # ── Member jump (was the sidebar) ───────────────────────────────────────────
    picked = member_jump_panel(
        opts["members"],
        search_key_prefix="pay",
        session_key="selected_td_pay",
        label=f"Browse all {terms.lower()}",
        placeholder="e.g. Mary Lou McDonald" if not is_seanad else "e.g. Michael McDowell",
        notable=None if is_seanad else NOTABLE_TDS,
        chip_key_prefix="pay_notable",
    )
    if picked and st.session_state.get("selected_td_pay") != picked:
        st.session_state["selected_td_pay"] = picked
        st.rerun()

    # Legacy ?member=<name> URLs AND member-jump selections both redirect
    # to the canonical /member-overview?member=<code>#payments profile.
    # Shared helper resolves the real unique_member_code, scrubs state, and
    # calls st.stop() so the rankings page body doesn't render under the
    # callout (round-3 audit P0-3).
    qp_member = st.query_params.get("member")
    if qp_member:
        member_moved_callout(
            qp_member,
            section="payments",
            section_label="Per-TD payments",
            legacy_param="member",
            state_keys=("selected_td_pay",),
        )

    selected_td = st.session_state.get("selected_td_pay")
    if selected_td:
        member_moved_callout(
            selected_td,
            section="payments",
            section_label="Per-TD payments",
            state_keys=("selected_td_pay",),
        )

    _render_primary(year_options, summary, house, term, terms)
