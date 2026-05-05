"""
Lobbyist POC — register-enriched lobbying organisation index.

Proves the CRO × Charity × Lobbying integration end-to-end. Renders the
funding / status / age / state-adjacent enrichments from CRO/INTEGRATION_PLAN.md
§§ 6.1, 8.4, 9.2 on top of the existing lobbying gold leaderboard.

DATA — registered views only:
- v_experimental_lobbying_org_index_enriched
  (sql_views/lobbying_experimental_org_index_enriched.sql)
- v_experimental_charity_finance_timeseries
  (sql_views/lobbying_experimental_charity_finance_timeseries.sql)
- v_lobbying_contact_detail (existing — Stage 2 returns + lobbying.ie source URL)

UI:
- Stage 1: hero → stat strip → filter strip → clickable enrichment cards.
- Stage 2: identity strip → KPI grid → financial time series → returns
  table with lobbying.ie source link → CSV export → provenance.
- Match-method evidence is shown on every card so Tier B/C name-collision
  false positives (e.g. dissolved company sharing a chamber's name) are
  visible to the user, not buried.
- Cards with match_method != 'both_name_exact' get a dashed border per
  §8.4 (uncertainty indicator).
"""
from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.lobbying_data import (
    fetch_experimental_distinct_filters,
    fetch_experimental_finance_timeseries,
    fetch_experimental_org_index_enriched,
    fetch_experimental_org_index_summary,
    fetch_experimental_org_one,
    fetch_experimental_returns_for_org,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    hero_banner,
    render_stat_strip,
    sidebar_page_header,
    stat_item,
    todo_callout,
)
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander


# ── Display helpers ───────────────────────────────────────────────────────────

_QP_ORG = "lpoc_org"
PAGE_SIZE = 25

_STATUS_LABELS = {
    "active":       ("Active", "lpoc-status-active"),
    "in_distress":  ("In distress", "lpoc-status-in_distress"),
    "dead":         ("Dissolved / strike-off", "lpoc-status-dead"),
    "registered":   ("Registered charity", "lpoc-status-registered"),
    "deregistered": ("Deregistered charity", "lpoc-status-deregistered"),
    "unknown":      ("Status not in registers", "lpoc-status-unknown"),
}

_FUNDING_LABELS = {
    "state_funded":     ("State-funded", "lpoc-funding-state_funded"),
    "mostly_donations": ("Mostly donations", "lpoc-funding-mostly_donations"),
    "mostly_trading":   ("Mostly trading", "lpoc-funding-mostly_trading"),
    "mixed":            ("Mixed funding", "lpoc-funding-mixed"),
    "undisclosed":      ("Funding not disclosed", "lpoc-funding-undisclosed"),
}

_MATCH_LABELS = {
    "both_name_exact":    "Resolved on name in both registers",
    "charity_name_exact": "Charity register match (name)",
    "company_name_exact": "CRO register match (name)",
    "unmatched":          "No register match — name-only entity",
}

# Warning-flag IDs come from v_experimental_lobbying_org_index_enriched.
# Tier governs colour: red = strong concern, amber = warning, info = neutral.
# IDs are stable contracts with the SQL view — labels here are for humans only.
_FLAG_LABELS: dict[str, tuple[str, str, str]] = {
    # id -> (label, tier, tooltip)
    "lobbied_while_in_distress": (
        "Lobbied while in distress", "red",
        "This entity filed lobbying returns dated after its CRO status moved to "
        "Liquidation, Receivership, or Strike Off Listed.",
    ),
    "lobbied_while_extinct": (
        "Lobbied after dissolution", "red",
        "Lobbying returns are dated after the matched company's recorded "
        "dissolution date. Either the name match is wrong (worth checking) or "
        "the entity continued filing under a legally-extinct vehicle.",
    ),
    "annual_return_overdue": (
        "Annual return overdue", "red",
        "The CRO Next Annual Return Date (NARD) has passed but the company is "
        "still listed Normal — the s.725 strike-off pipeline.",
    ),
    "charity_insolvent_latest": (
        "Liabilities exceed assets", "red",
        "Latest annual report shows total liabilities greater than total assets.",
    ),
    "recent_distress": (
        "Recent status change", "amber",
        "CRO status moved to Liquidation / Receivership / Strike-Off / Dissolved "
        "within the last 12 months.",
    ),
    "accounts_overdue": (
        "Accounts overdue (>18m)", "amber",
        "Last accounts filed with CRO are more than 18 months old while the "
        "company is still listed Normal.",
    ),
    "no_registered_address": (
        "No registered address", "amber",
        "CRO has no registered office on file (placeholder address). Section 728 "
        "exposure.",
    ),
    "charity_filing_overdue": (
        "Charity filing overdue (>18m)", "amber",
        "Most recent annual report to the Charities Regulator is more than 18 "
        "months old.",
    ),
    "charity_deficit_latest": (
        "Reported a deficit", "amber",
        "Latest annual report shows a deficit (expenditure greater than income).",
    ),
    "recent_rename": (
        "Recently renamed", "info",
        "Current company name took effect within the last 24 months.",
    ),
    "invalid_reg_date": (
        "Registration date invalid", "info",
        "CRO record carries a registration date earlier than 1900 — usually a "
        "data-quality artefact on legacy entities.",
    ),
    "foreign_domicile": (
        "Foreign-domiciled charity", "info",
        "Country of establishment recorded by the Charities Regulator is "
        "outside Ireland.",
    ),
    "cro_undisclosed": (
        "Incorporated but no CRO number", "info",
        "Charity Governing Form indicates an incorporated body, but no CRO "
        "number is disclosed on the public register.",
    ),
}


def _pill(text: str, css_class: str) -> str:
    return f'<span class="lpoc-pill {css_class}">{_h(text)}</span>'


def _fmt_currency_eur_short(amount: float | None) -> str:
    if amount is None or pd.isna(amount):
        return ""
    if amount >= 1_000_000_000:
        return f"€{amount / 1_000_000_000:.1f}bn"
    if amount >= 1_000_000:
        return f"€{amount / 1_000_000:.1f}m"
    if amount >= 1_000:
        return f"€{amount / 1_000:.0f}k"
    return f"€{amount:,.0f}"


def _fmt_signed_currency(amount: float | None) -> str:
    if amount is None or pd.isna(amount):
        return ""
    pretty = _fmt_currency_eur_short(abs(amount))
    return f"−{pretty}" if amount < 0 else f"+{pretty}"


def _fmt_pct(share: float | None) -> str:
    if share is None or pd.isna(share):
        return ""
    return f"{share * 100:.0f}%"


def _funding_pill(row: pd.Series) -> str:
    profile = row.get("funding_profile")
    if not profile or pd.isna(profile):
        return ""
    label, css = _FUNDING_LABELS.get(profile, _FUNDING_LABELS["undisclosed"])
    gov_share = row.get("gov_funded_share_latest")
    text = f"{label} {int(round(gov_share * 100))}%" if pd.notna(gov_share) else label
    return _pill(text, css)


def _status_pill(row: pd.Series) -> str:
    status = row.get("status") or "unknown"
    label, css = _STATUS_LABELS.get(status, _STATUS_LABELS["unknown"])
    return _pill(label, css)


def _age_pill(row: pd.Series) -> str:
    yrs = row.get("entity_age_years")
    if pd.isna(yrs) or yrs is None:
        return ""
    return _pill(f"{int(yrs)} yrs on register", "lpoc-metric")


def _income_pill(row: pd.Series) -> str:
    amt = row.get("gross_income_latest_eur")
    if amt is None or pd.isna(amt):
        return ""
    return _pill(_fmt_currency_eur_short(amt), "lpoc-metric")


def _employees_pill(row: pd.Series) -> str:
    band = row.get("employees_band_latest")
    if not band or pd.isna(band):
        return ""
    return _pill(f"{band} employees", "lpoc-metric")


def _state_adjacent_pill(row: pd.Series) -> str:
    if row.get("state_adjacent_flag") is True:
        return _pill("State-adjacent body", "lpoc-state-adjacent")
    return ""


def _newly_incorporated_pill(row: pd.Series) -> str:
    if row.get("newly_incorporated_flag") is True:
        return _pill("Newly incorporated", "lpoc-newly-incorporated")
    return ""


def _match_evidence_pill(row: pd.Series) -> str:
    method = row.get("match_method") or "unmatched"
    label = _MATCH_LABELS.get(method, method)
    return _pill(label, "lpoc-match-evidence")


_TIER_ORDER = {"red": 0, "amber": 1, "info": 2}


def _flag_list(row: pd.Series) -> list[str]:
    raw = row.get("flags")
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        try:
            if pd.isna(raw):
                return []
        except (TypeError, ValueError):
            pass
        items = list(raw) if hasattr(raw, "__iter__") else []
    return [str(f) for f in items if f]


def _flag_label_for(flag_id: str, row: pd.Series) -> tuple[str, str, str]:
    """Resolve label/tier/tooltip for a flag, with optional row-aware enrichment.

    Most flags use the static label from _FLAG_LABELS. `foreign_domicile` is
    augmented with the actual country (e.g. "Foreign-domiciled — United
    Kingdom") so the user can read where the entity is registered without
    drilling further.
    """
    label, tier, tooltip = _FLAG_LABELS.get(
        flag_id, (flag_id.replace("_", " ").capitalize(), "info", "")
    )
    if flag_id == "foreign_domicile":
        country = row.get("country_established")
        if country and pd.notna(country):
            country_str = str(country).strip()
            if country_str:
                label = f"Foreign-domiciled — {country_str}"
                tooltip = (
                    f"Country of establishment recorded by the Charities "
                    f"Regulator: {country_str}."
                )
    return label, tier, tooltip


def _flag_pill(flag_id: str, row: pd.Series) -> str:
    label, tier, tooltip = _flag_label_for(flag_id, row)
    css = f"lpoc-flag lpoc-flag-{tier}"
    title_attr = f' title="{_h(tooltip)}"' if tooltip else ""
    return (
        f'<span class="lpoc-pill {css}"{title_attr}>'
        f'<span class="lpoc-flag-dot" aria-hidden="true"></span>{_h(label)}'
        f'</span>'
    )


def _flag_pills_html(row: pd.Series) -> str:
    flags = _flag_list(row)
    if not flags:
        return ""
    flags_sorted = sorted(
        flags,
        key=lambda f: (_TIER_ORDER.get(_FLAG_LABELS.get(f, ("", "info", ""))[1], 9), f),
    )
    return "".join(_flag_pill(f, row) for f in flags_sorted)


def _build_card_html(row: pd.Series, rank: int, *, clickable: bool) -> str:
    is_low_conf = row.get("match_method") in (
        None, "unmatched", "company_name_exact", "charity_name_exact",
    )
    flags_html = _flag_pills_html(row)
    classes = "lpoc-card"
    if is_low_conf:
        classes += " lpoc-card-low-confidence"
    if any(_FLAG_LABELS.get(f, ("", "info", ""))[1] == "red" for f in _flag_list(row)):
        classes += " lpoc-card-flagged"
    if clickable:
        classes += " lpoc-card-clickable"

    pieces = [
        _newly_incorporated_pill(row),
        _state_adjacent_pill(row),
        _status_pill(row),
        _funding_pill(row),
        _employees_pill(row),
        _income_pill(row),
        _age_pill(row),
        _pill(f"{int(row['return_count'])} returns", "lpoc-metric"),
        _pill(f"{int(row['politicians_targeted'])} politicians", "lpoc-metric"),
        _match_evidence_pill(row),
    ]
    pills_html = "".join(p for p in pieces if p)

    sector = row.get("sector_label")
    sector_meta = f" · {_h(str(sector))}" if sector and pd.notna(sector) else ""
    period_meta = ""
    fp = row.get("first_period")
    lp = row.get("last_period")
    if fp and pd.notna(fp) and lp and pd.notna(lp):
        period_meta = f" · {_h(str(fp))} → {_h(str(lp))}"

    flags_row = (
        f'<div class="lpoc-card-row lpoc-flags-row">{flags_html}</div>'
        if flags_html else ""
    )
    return (
        f'<div class="{classes}">'
        f'  <div class="lpoc-card-row">'
        f'    <span class="lpoc-rank">#{rank}</span>'
        f'    <span class="lpoc-name">{_h(row["lobbyist_name"])}</span>'
        f'    <span class="lpoc-meta">{sector_meta}{period_meta}</span>'
        f'  </div>'
        f'  <div class="lpoc-card-row">{pills_html}</div>'
        f'  {flags_row}'
        f'</div>'
    )


def _render_clickable_card(row: pd.Series, rank: int) -> None:
    """Stage 1 card — full-card link via query param. Whole card clickable."""
    href = f"?{_QP_ORG}={quote(row['lobbyist_name'])}"
    inner = _build_card_html(row, rank, clickable=True)
    aria = f"View detail for {row['lobbyist_name']}"
    st.html(
        f'<div class="dt-card-link-wrap" style="margin-bottom:0.5rem">'
        f'<a class="dt-card-link" href="{_h(href)}" target="_self" '
        f'aria-label="{_h(aria)}"></a>'
        f'{inner}'
        f'</div>'
    )


# ── Stage 1: ranked enriched index ────────────────────────────────────────────

def _summary_row(summary: pd.DataFrame) -> tuple[int, int, int, int, int, int, int]:
    if summary.empty:
        return (0, 0, 0, 0, 0, 0, 0)
    r = summary.iloc[0]
    return (
        int(r.get("total_orgs", 0) or 0),
        int(r.get("matched_orgs", 0) or 0),
        int(r.get("matched_both", 0) or 0),
        int(r.get("state_adjacent_orgs", 0) or 0),
        int(r.get("newly_incorporated_orgs", 0) or 0),
        int(r.get("state_funded_orgs", 0) or 0),
        int(r.get("flagged_orgs", 0) or 0),
    )


def _filter_strip() -> tuple[str | None, str | None, bool, bool]:
    filters = fetch_experimental_distinct_filters()
    statuses = filters["status"]
    profiles = filters["funding_profile"]

    cols = st.columns([2, 2, 2, 2, 1])
    with cols[0]:
        status_choices = ["(any)", *statuses]
        status_sel = st.selectbox(
            "Status",
            options=status_choices,
            key="lpoc_status",
            index=0,
            format_func=lambda v: "Any status" if v == "(any)" else _STATUS_LABELS.get(v, (v, ""))[0],
        )
    with cols[1]:
        profile_choices = ["(any)", *profiles]
        profile_sel = st.selectbox(
            "Funding profile",
            options=profile_choices,
            key="lpoc_funding",
            index=0,
            format_func=lambda v: "Any profile" if v == "(any)" else _FUNDING_LABELS.get(v, (v, ""))[0],
        )
    with cols[2]:
        exclude_state = st.toggle(
            "Hide state-adjacent bodies (HSE, hospitals, etc.)",
            value=False,
            key="lpoc_exclude_state",
            help=(
                "State-adjacent bodies have ≥80% government income and >€100m gross "
                "income. They dwarf civil-society NGOs by income — toggle off to "
                "compare like-for-like."
            ),
        )
    with cols[3]:
        flagged_only = st.toggle(
            "Flagged only",
            value=False,
            key="lpoc_flagged_only",
            help=(
                "Show only organisations with at least one warning flag — "
                "lobbied while in distress, overdue filings, deficits, etc."
            ),
        )
    with cols[4]:
        if st.button("Reset", key="lpoc_reset"):
            for k in (
                "lpoc_status", "lpoc_funding", "lpoc_exclude_state",
                "lpoc_flagged_only", "lpoc_page",
            ):
                st.session_state.pop(k, None)
            st.rerun()

    return (
        None if status_sel == "(any)" else status_sel,
        None if profile_sel == "(any)" else profile_sel,
        bool(exclude_state),
        bool(flagged_only),
    )


def _render_stage1() -> None:
    hero_banner(
        kicker="Lobbying · Proof of Concept",
        title="Lobbyist enrichment — register-resolved",
        dek=(
            "Each lobbying organisation is matched to the CRO companies register "
            "and the Public Register of Charities. Cards are clickable — open one "
            "to see its financial history and every return it has filed with the "
            "lobbying.ie register."
        ),
        badges=["Experimental", "Sources: CRO + Charities Regulator + lobbying.ie"],
    )

    summary = fetch_experimental_org_index_summary()
    total, matched, matched_both, state_adj, newly, state_funded, flagged = _summary_row(summary)

    if total == 0:
        empty_state(
            "No data available",
            "Run pipeline_sandbox/cro_normalise.py, charity_normalise.py, and "
            "charity_resolved.py to produce the silver parquets the experimental "
            "view depends on.",
        )
        return

    matched_pct = (matched / total * 100) if total else 0
    both_pct = (matched_both / total * 100) if total else 0
    render_stat_strip(
        stat_item(f"{total:,}", "Lobbying organisations"),
        stat_item(f"{matched_pct:.0f}%", "Resolved to a register"),
        stat_item(f"{both_pct:.0f}%", "Matched in both registers"),
        stat_item(f"{state_funded:,}", "State-funded charities"),
        stat_item(f"{state_adj:,}", "State-adjacent bodies"),
        stat_item(f"{newly:,}", "Newly incorporated"),
        stat_item(f"{flagged:,}", "With warning flags"),
    )

    st.divider()

    status, funding, exclude_state, flagged_only = _filter_strip()
    df = fetch_experimental_org_index_enriched(
        status=status,
        funding_profile=funding,
        exclude_state_adjacent=exclude_state,
        flagged_only=flagged_only,
    )

    if df.empty:
        empty_state(
            "No organisations match the current filters",
            "Try clearing the status or funding filters, or include state-adjacent "
            "bodies if you want to see HSE-class entities.",
        )
        return

    if matched_pct < 50:
        todo_callout(
            "Tier B/C name-match coverage is below 50% of organisations. The "
            "manual override CSV (data/_meta/lobbyist_name_overrides.csv) is the "
            "next layer per CRO/INTEGRATION_PLAN.md §11 — top-200 should be "
            "hand-pinned before this leaves the POC stage."
        )

    st.markdown(f"**Showing {len(df):,} organisations** — click a card to open its profile")

    page = st.session_state.setdefault("lpoc_page", 0)
    total_pages = max(1, (len(df) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = min(page, total_pages - 1)
    start = page * PAGE_SIZE
    page_df = df.iloc[start:start + PAGE_SIZE]

    for offset, (_, row) in enumerate(page_df.iterrows()):
        _render_clickable_card(row, rank=start + offset + 1)

    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    if nav_left.button("← Previous", key="lpoc_prev", disabled=page <= 0):
        st.session_state.lpoc_page = page - 1
        st.rerun()
    nav_mid.markdown(
        f"<div style='text-align:center;font-size:0.85rem;color:var(--text-meta)'>"
        f"Page {page + 1} of {total_pages}</div>",
        unsafe_allow_html=True,
    )
    if nav_right.button("Next →", key="lpoc_next", disabled=page >= total_pages - 1):
        st.session_state.lpoc_page = page + 1
        st.rerun()

    export_button(
        df,
        label="Download enriched index (CSV)",
        filename="lobbyist_enrichment_poc.csv",
        key="lpoc_csv_export",
    )

    _render_provenance_footer()


# ── Stage 2: organisation detail ──────────────────────────────────────────────

def _identity_strip(row: pd.Series) -> None:
    name = str(row["lobbyist_name"])
    sector = row.get("sector_label")
    fp, lp = row.get("first_period"), row.get("last_period")
    chips = []
    if pd.notna(row.get("rcn")):
        chips.append(_pill(f"RCN {int(row['rcn'])}", "lpoc-metric"))
    if pd.notna(row.get("company_num")):
        chips.append(_pill(f"CRO #{int(row['company_num'])}", "lpoc-metric"))
    chips.append(_match_evidence_pill(row))
    chips.append(_status_pill(row))
    if row.get("state_adjacent_flag") is True:
        chips.append(_state_adjacent_pill(row))
    if row.get("newly_incorporated_flag") is True:
        chips.append(_newly_incorporated_pill(row))

    meta_parts = []
    if sector and pd.notna(sector):
        meta_parts.append(_h(str(sector)))
    if fp and lp and pd.notna(fp) and pd.notna(lp):
        meta_parts.append(f"Filing since {_h(str(fp))} · last {_h(str(lp))}")
    meta = " · ".join(meta_parts)

    flags_html = _flag_pills_html(row)
    flags_row = (
        f'<div class="lpoc-card-row lpoc-flags-row" style="flex-basis:100%">'
        f'{flags_html}</div>'
        if flags_html else ""
    )
    st.html(
        f'<div class="lpoc-id-strip">'
        f'  <span class="lpoc-id-strip-name">{_h(name)}</span>'
        f'  <span class="lpoc-id-strip-meta">{meta}</span>'
        f'  <div class="lpoc-card-row" style="flex-basis:100%">{"".join(chips)}</div>'
        f'  {flags_row}'
        f'</div>'
    )


def _kpi(num_html: str, label: str, *, negative: bool = False) -> str:
    cls = "lpoc-kpi-num lpoc-kpi-num-negative" if negative else "lpoc-kpi-num"
    return (
        f'<div class="lpoc-kpi">'
        f'<div class="{cls}">{num_html}</div>'
        f'<div class="lpoc-kpi-lbl">{_h(label)}</div>'
        f'</div>'
    )


def _kpi_grid(row: pd.Series) -> None:
    items = [
        _kpi(f"{int(row['return_count']):,}", "Lobbying returns"),
        _kpi(f"{int(row['politicians_targeted']):,}", "Politicians targeted"),
        _kpi(f"{int(row['distinct_policy_areas']):,}", "Distinct policy areas"),
    ]
    if pd.notna(row.get("gross_income_latest_eur")):
        items.append(_kpi(_fmt_currency_eur_short(row["gross_income_latest_eur"]), "Latest reported income"))
    if pd.notna(row.get("gov_funded_share_latest")):
        items.append(_kpi(_fmt_pct(row["gov_funded_share_latest"]), "Government-funded share"))
    if row.get("funding_profile") and pd.notna(row.get("funding_profile")):
        label = _FUNDING_LABELS.get(row["funding_profile"], (row["funding_profile"], ""))[0]
        items.append(_kpi(_h(label), "Funding profile"))
    if row.get("employees_band_latest") and pd.notna(row.get("employees_band_latest")):
        items.append(_kpi(_h(str(row["employees_band_latest"])), "Employees (band)"))
    if pd.notna(row.get("entity_age_years")):
        items.append(_kpi(f"{int(row['entity_age_years'])} yrs", "Years on register"))

    st.html(f'<div class="lpoc-kpi-grid">{"".join(items)}</div>')


def _render_finance_section(rcn: int) -> None:
    df = fetch_experimental_finance_timeseries(rcn)
    if df.empty:
        st.html('<p class="lpoc-section-sub">No annual reports filed by this charity yet.</p>')
        return

    st.html(
        f'<p class="lpoc-section-sub">Self-reported annual returns to the '
        f'Charities Regulator (RCN {rcn}). All amounts in EUR; <em>Number of '
        f'employees</em> is an ordinal text band, never a numeric headcount. '
        f'Most-recent year may show partial data — coverage typically lags 12–24 '
        f'months.</p>'
    )

    if df["gross_income"].notna().any():
        chart_df = df[["period_year", "gross_income"]].dropna().sort_values("period_year")
        chart_df = chart_df.set_index("period_year")
        st.markdown("**Gross income over time**")
        st.bar_chart(chart_df, height=140)

    display = df.copy()
    display["surplus_deficit_str"] = display["surplus_deficit"].apply(_fmt_signed_currency)
    display["gov_pct"] = display["gov_share"].apply(_fmt_pct)
    money_cols = [
        "gross_income", "gross_expenditure", "gov_eur", "other_public_bodies_eur",
        "donations_eur", "trading_eur", "philanthropic_eur", "other_income_eur",
        "bequests_eur", "cash_at_hand", "total_assets", "total_liabilities", "net_assets",
    ]
    for c in money_cols:
        display[c] = display[c].apply(_fmt_currency_eur_short)

    column_order = [
        "period_year", "gross_income", "gross_expenditure", "surplus_deficit_str",
        "gov_pct", "gov_eur", "other_public_bodies_eur", "donations_eur", "trading_eur",
        "philanthropic_eur", "other_income_eur", "bequests_eur",
        "total_assets", "total_liabilities", "net_assets", "cash_at_hand",
        "employees_band", "volunteers_band",
    ]
    column_labels = {
        "period_year": "Year", "gross_income": "Gross income", "gross_expenditure": "Gross expenditure",
        "surplus_deficit_str": "Surplus / (deficit)", "gov_pct": "Gov %", "gov_eur": "Govt / LA",
        "other_public_bodies_eur": "Other public bodies", "donations_eur": "Donations",
        "trading_eur": "Trading", "philanthropic_eur": "Philanthropic", "other_income_eur": "Other income",
        "bequests_eur": "Bequests", "total_assets": "Total assets",
        "total_liabilities": "Total liabilities", "net_assets": "Net assets", "cash_at_hand": "Cash",
        "employees_band": "Employees", "volunteers_band": "Volunteers",
    }
    st.dataframe(
        display[column_order].rename(columns=column_labels),
        hide_index=True,
        width="stretch",
    )

    export_button(
        df,
        label="Download financial history (CSV)",
        filename=f"charity_{int(rcn)}_finance.csv",
        key="lpoc_finance_csv",
    )


def _render_returns_section(org_name: str) -> None:
    df = fetch_experimental_returns_for_org(org_name)
    if df.empty:
        empty_state(
            "No returns filed",
            "This organisation appears in the leaderboard but no per-return rows "
            "are available — most likely a contract-detail view filter.",
        )
        return

    st.html(
        f'<p class="lpoc-section-sub">{len(df):,} return{"s" if len(df) != 1 else ""} '
        f'filed under this lobbyist name on lobbying.ie. Click a row\'s '
        f'<em>Source ↗</em> link to open the original filing on the Standards '
        f'in Public Office Commission register.</p>'
    )

    display = df[["period_start_date", "member_name", "public_policy_area", "source_url"]].copy()
    display.columns = ["Period start", "Politician", "Policy area", "Source"]

    st.dataframe(
        display,
        hide_index=True,
        width="stretch",
        column_config={
            "Source": st.column_config.LinkColumn(
                "Source",
                help="Open the return on lobbying.ie",
                display_text="lobbying.ie ↗",
            ),
        },
    )

    export_button(
        df,
        label="Download returns (CSV)",
        filename=f"lobbying_returns_{org_name.replace(' ', '_')}.csv",
        key="lpoc_returns_csv",
    )


def _render_stage2(org_name: str) -> None:
    one = fetch_experimental_org_one(org_name)
    if one.empty:
        empty_state(
            "Organisation not found",
            f'No row in the enriched index matches "{org_name}". It may have been '
            f'renamed or filtered out of the gold leaderboard.',
        )
        if back_button("← Back to index", key="lpoc_back_notfound"):
            st.query_params.pop(_QP_ORG, None)
            st.rerun()
        return

    if back_button("← Back to enriched index", key="lpoc_back"):
        st.query_params.pop(_QP_ORG, None)
        st.rerun()

    row = one.iloc[0]

    _identity_strip(row)
    _kpi_grid(row)

    if pd.notna(row.get("rcn")):
        st.html('<h3 class="lpoc-section-h">Financial history (Charities Regulator)</h3>')
        _render_finance_section(int(row["rcn"]))
    else:
        st.html(
            f'<p class="lpoc-section-sub"><em>No charity-register match — '
            f'financial history is not available for this organisation. '
            f'See the enriched-index page for match-method evidence.</em></p>'
        )

    st.html('<h3 class="lpoc-section-h">Lobbying returns filed</h3>')
    _render_returns_section(org_name)

    _render_provenance_footer()


# ── Provenance (shared across stages) ─────────────────────────────────────────

def _render_provenance_footer() -> None:
    provenance_expander(
        sections=[
            (
                "**Lobbying activity** — Standards in Public Office Commission "
                "register at [lobbying.ie](https://www.lobbying.ie/). Org leaderboard "
                "is built by the existing pipeline; this POC adds the register layer."
            ),
            (
                "**Companies register** — Companies Registration Office bulk download "
                "(snapshot 2026-05-04). Status, type, NACE code, incorporation date, "
                "address and Eircode."
            ),
            (
                "**Charities register** — Charities Regulator Public Register and "
                "Annual Reports (effective 2026-04-26). Income breakdowns, governance, "
                "trustees and CRO-number cross-link."
            ),
            (
                "**Match method** — exact name match after stripping legal suffixes "
                "(LIMITED, LTD, DAC, PLC, CLG, COMPANY LIMITED BY GUARANTEE, …). "
                "Fuzzy matches and manual overrides are tracked in "
                "`data/_meta/lobbyist_name_overrides.csv` per "
                "CRO/INTEGRATION_PLAN.md §11 — not yet wired in."
            ),
            (
                "**Caveats** — *Number of Employees* is a self-reported text band "
                "(`1–9`, `10–19` … `5000+`); never used as a numeric metric. "
                "*Funding share* uses the latest filed annual return per charity, "
                "which lags by up to two years. State-adjacent bodies (HSE, hospitals, "
                "Tusla, Pobal) appear as charities and are flagged separately."
            ),
            (
                "**Warning flags** — derived in pipeline_sandbox/cro_normalise.py "
                "and pipeline_sandbox/charity_normalise.py, then composited by "
                "sql_views/lobbying_experimental_org_index_enriched.sql. Three "
                "tiers — **red** (lobbied while in distress / after dissolution; "
                "annual return overdue under s.725 CA 2014; liabilities exceed "
                "assets), **amber** (recent CRO status change, accounts overdue "
                ">18 months, no registered address, charity filing overdue >18 "
                "months, reported deficit), and **info** (recently renamed, "
                "invalid CRO registration date, foreign-domiciled charity, "
                "incorporated charity without a CRO number). A flag indicates a "
                "data signal worth checking, not a verdict — name-based register "
                "matches can be wrong, especially for `company_name_exact` rows."
            ),
        ],
        source_caption=(
            "Sources: lobbying.ie · CRO bulk register · Charities Regulator Public "
            "Register & Annual Reports."
        ),
    )


# ── Page entry point ──────────────────────────────────────────────────────────

def lobbyist_poc_page() -> None:
    inject_css()
    sidebar_page_header("Lobbyist POC")

    selected_org = st.query_params.get(_QP_ORG)
    if selected_org:
        _render_stage2(selected_org)
    else:
        _render_stage1()
