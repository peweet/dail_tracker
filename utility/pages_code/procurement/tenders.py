from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_expiring_contracts_result,
    fetch_expiring_contracts_stats_result,
    fetch_expiring_etenders_result,
    fetch_live_tender_sectors_result,
    fetch_live_tenders_result,
    fetch_live_tenders_stats_result,
)
from ui.components import (
    empty_state,
    fmt_civic_date,
    paginate,
    pagination_controls,
)
from ui.entity_links import source_link_html

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _TOP,
    _LIVE_PAGE,
    _eur,
    _buyer_link,
    _card,
)


# Human label for how the estimated end date was derived (carried from the view; the
# basis is part of the fact being presented, not a UI judgement).
_END_BASIS_LABEL = {
    "explicit_end_date": "end date on the notice",
    "start_plus_duration": "start date + advertised duration",
    "conclusion_plus_duration": "signed date + advertised duration",
}


def _expiring_contracts_caveat(s) -> None:
    """The section's headline, built from the corpus summary rather than asserted in prose.

    The framework count belongs in the headline because it changes what the list MEANS for a
    smaller firm: a single-winner contract ending is one competition on one date, whereas a
    multi-supplier framework is competed at every call-off across its whole life. Conditional on
    n_frameworks so the clause disappears rather than reading "0 of them" if the corpus ever has
    none.
    """
    n_frameworks = _n(s.get("n_frameworks"))
    fw_line = (
        f" {n_frameworks:,} of them are multi-supplier frameworks — several appointed suppliers "
        "competing at each call-off, rather than one winner holding the whole contract."
        if n_frameworks
        else ""
    )
    st.html(
        '<div class="pr-caveat"><strong>Advertised contract terms — when current contracts are due to end.</strong> '
        f"{_n(s.get('n_with_estimate')):,} TED award notices state a contract term (an explicit end date on "
        f"{_n(s.get('n_explicit')):,} of them; otherwise the signed/start date plus the advertised duration). "
        "These are the terms <em>as advertised on the award notice</em> — a contract can end early or run "
        f"longer through renewal options, which are shown separately and never folded in.{fw_line}</div>"
    )


def _framework_pill(r) -> str | None:
    """The multi-supplier-framework pill, or None for a single-winner contract.

    The framework flag was selected by the query and reaching this frame all along, but was
    never rendered — so the one field telling a smaller firm "several suppliers hold this, and
    it is competed at each call-off" was invisible. n_winners qualifies it with the number
    actually appointed on the notice.
    """
    if not _truthy(getattr(r, "is_multi_supplier_framework", None)):
        return None
    n_win = getattr(r, "n_winners", None)
    appointed = ""
    if n_win is not None and not pd.isna(n_win) and int(n_win) > 1:
        appointed = f" ({int(n_win)} suppliers)"
    return f'<span class="pr-pill pr-pill-cro">multi-supplier framework{appointed}</span>'


def _render_expiring_contracts() -> None:
    stats_res = fetch_expiring_contracts_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "Contract-term data isn't available right now",
            "The advertised-term view couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]
    _expiring_contracts_caveat(s)
    win_col, kind_col = st.columns([1.4, 1], vertical_alignment="center")
    with win_col:
        window = st.segmented_control(
            "Ending within",
            ["6 months", "12 months", "24 months"],
            default="12 months",
            key="pr_expiring_window",
            label_visibility="collapsed",
        )
    with kind_col:
        # Filter, not a separate lens: the framework rows belong to this same advertised-term
        # register and are ordered by the same end date, so splitting them out would duplicate
        # the list rather than narrow it.
        kind = st.segmented_control(
            "Contract kind",
            ["All contracts", "Frameworks only"],
            default="All contracts",
            key="pr_expiring_kind",
            label_visibility="collapsed",
        )
    months = int((window or "12 months").split()[0])
    frameworks_only = kind == "Frameworks only"
    res = fetch_expiring_contracts_result(months_ahead=months, limit=_TOP, frameworks_only=frameworks_only)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        if frameworks_only:
            empty_state(
                "No frameworks ending in this window",
                f"No multi-supplier framework has an advertised term ending within {months} months. "
                "Try a wider window, or switch back to all contracts.",
            )
        else:
            empty_state("No contracts in this window", "No advertised term ends in the selected period.")
        return
    kind_label = "multi-supplier frameworks" if frameworks_only else "contracts"
    st.caption(
        f"{len(df):,} {kind_label} whose advertised term ends within {months} months, soonest first. "
        "Values are award/ceiling figures shown for context — never totals. "
        "Use each Source notice link to open the authoritative TED record."
    )
    cards = []
    for r in df.itertuples():
        meta_parts = [_esc(_coalesce(getattr(r, "cpv_division", None)))]
        winners = _coalesce(getattr(r, "winners_display", None))
        if winners:
            meta_parts.append(_esc(winners))
        dur = getattr(r, "contract_duration_months", None)
        if dur is not None and not pd.isna(dur):
            dur_i = int(dur) if float(dur).is_integer() else dur
            meta_parts.append(f"{dur_i}-month term")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        end = _coalesce(getattr(r, "contract_end_date_est", None))
        if end:
            pills.append(f'<span class="pr-pill pr-pill-val">ends {fmt_civic_date(end)}</span>')
        ev = _eur(getattr(r, "award_value_eur", None))
        if ev != "—":
            vkind = _coalesce(getattr(r, "value_kind", None))
            pills.append(
                f'<span class="pr-pill">{ev}{" ceiling" if vkind == "framework_or_dps_ceiling" else ""}</span>'
            )
        fw_pill = _framework_pill(r)
        if fw_pill:
            pills.append(fw_pill)
        renew = getattr(r, "renewal_max", None)
        if renew is not None and not pd.isna(renew) and int(renew) > 0:
            pills.append(f'<span class="pr-pill pr-pill-lob">up to {int(renew)} renewals</span>')
        basis = _END_BASIS_LABEL.get(_coalesce(getattr(r, "contract_end_basis", None)))
        if basis:
            pills.append(f'<span class="pr-pill">{basis}</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        url = _coalesce(getattr(r, "notice_url", None))
        source = source_link_html(
            url,
            "Source notice",
            aria_label=f"Open the EU award notice from {buyer or 'this buyer'} on TED",
        )
        name_html = f"<span>{_buyer_link(buyer)}</span>"
        if source:
            name_html += f'<span class="pr-sub">{source}</span>'
        cards.append(_card(name_html, meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal award '
        'notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>), eForms '
        "contract-term fields (BT-36/BT-145/BT-536/BT-537). The end date is the advertised term, not a verified "
        "event; ~36% of award notices state a term. Winner names follow the published notice; sole traders and "
        "individuals are not shown.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# National (eTenders) forward lenses — the sub-EU-threshold mass TED can't see.
# Rendered ABOVE the TED lens in each "Open right now" segment as its own register,
# never value-merged with TED (two registers, never summed). Snapshot-based, so it
# carries an explicit freshness line + staleness guard.
# ──────────────────────────────────────────────────────────────────────────────
def _national_freshness_html(retrieved) -> str:
    """Point-in-time freshness line for the live national snapshot. Display-only — formats
    the snapshot stamp and flags it when older than 3 days (a stale open-tenders list whose
    deadlines have passed would mislead; this is the one real risk of a scraped snapshot)."""
    ts = pd.to_datetime(retrieved, errors="coerce", utc=True)
    if pd.isna(ts):
        return ""
    age_days = (pd.Timestamp.now(tz="UTC") - ts).days
    stale = (
        ' <strong class="pr-cap-stale">— this snapshot may be out of date; '
        "some deadlines below may already have passed.</strong>"
        if age_days > 3
        else ""
    )
    return f'<p class="pr-cap">National opportunities as of {fmt_civic_date(ts)}.{stale}</p>'


def _national_sector_facet(within_days: int | None) -> str | None:
    """Sector (CPV division) facet for the national open-tenders list. Returns the chosen sector,
    or None when 'All sectors' is picked OR the snapshot has no CPV yet (the sectors query is
    unavailable pre-enrichment, so the facet is simply omitted — the date filter still works)."""
    res = fetch_live_tender_sectors_result(within_days)
    if not res.ok or res.data.empty:
        return None
    counts = {str(r.sector): int(r.n) for r in res.data.itertuples()}
    options = ["All sectors", *counts.keys()]
    # The OPTION VALUE is the raw sector (stable); the count is shown via format_func only. Storing
    # the count in the value broke things: the counts change with the date window, so a previously
    # chosen "Construction (45)" was no longer in the new options and Streamlit raised on an
    # out-of-range selectbox value — blanking the page. Also guard a sector that vanishes entirely
    # from the new window by resetting the stored value before the widget reads it.
    if st.session_state.get("pr_live_sector") not in options:
        st.session_state["pr_live_sector"] = "All sectors"
    choice = st.selectbox(
        "Sector (CPV division)",
        options,
        key="pr_live_sector",
        format_func=lambda s: s if s == "All sectors" else f"{s} ({counts.get(s, 0):,})",
    )
    return None if choice == "All sectors" else choice


def _render_national_open_tenders() -> None:
    """Open NATIONAL tenders (etenders.gov.ie), PLANNED tier, soonest-closing first. A separate
    register from TED above — sub-EU-threshold opportunities (schools, councils, water schemes)."""
    stats_res = fetch_live_tenders_stats_result()
    if not stats_res.ok or stats_res.data.empty or _n(stats_res.data.iloc[0].get("n_open")) == 0:
        # Silent absence is honest here: the snapshot may simply not be polled yet. Show a quiet
        # note rather than an error so the TED lens below still reads as the primary content.
        st.html(
            '<div class="pr-foot"><strong>National (eTenders) live tenders:</strong> no current snapshot '
            "loaded. The national opportunities feed is refreshed separately from the EU-journal data above.</div>"
        )
        return
    s = stats_res.data.iloc[0]
    # The data's horizon: the furthest submission deadline in the open set. Surfacing it (and
    # making "All open" the default + reachable window) answers "project to the furthest date" —
    # the list was never capped at 30 days, but the largest pill was, which read as a cap.
    last_closing = s.get("last_closing")
    horizon = ""
    if not pd.isna(last_closing):
        horizon = f" The furthest deadline currently open is <strong>{fmt_civic_date(last_closing)}</strong>."
    st.html(
        '<div class="pr-caveat"><strong>National opportunities — open right now on eTenders.</strong> '
        f"{_n(s.get('n_open')):,} tenders currently accepting bids from {_n(s.get('n_buyers')):,} Irish public "
        f"buyers ({_n(s.get('closing_within_14d')):,} close within 14 days).{horizon} The sub-EU-threshold national "
        "picture the EU-journal feed above cannot show. The estimated value shown is a <em>buyer estimate "
        "recorded before any award</em>: never a contract value, never a payment, and never summed.</div>"
    )
    st.html(_national_freshness_html(s.get("retrieved_utc")))
    # Forward DATE facet: narrow to soonest-closing windows, or "All open" to project to the
    # furthest deadline in the data. The national eTenders snapshot carries a CPV division only
    # after the detail-page enrichment (added below when present); the TED lens always has one.
    max_days = _n(s.get("max_days"))
    windows = ["All open", "7 days", "14 days", "30 days", "90 days"]
    if max_days > 90:
        windows.append("180 days")
    window = st.segmented_control(
        "Closing within",
        windows,
        default="All open",
        key="pr_live_window",
        label_visibility="collapsed",
    )
    sel = window or "All open"
    within_days = None if sel == "All open" else int(sel.split()[0])
    # Sector facet — only when the snapshot carries a CPV division (post-enrichment). Degrades
    # silently to date-only on an un-enriched snapshot (the column is simply absent).
    sector = _national_sector_facet(within_days)
    # Fetch the FULL open set (limit=None), not a 60-row cap — the cap made the list read as if it
    # "stopped" at whatever date the 60th soonest-closing tender happened to close (≈ July 2026).
    # The view is already ordered soonest-closing first; pagination below walks the whole horizon.
    res = fetch_live_tenders_result(limit=None, within_days=within_days, sector=sector)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        if sector:
            empty_state("No national tenders in that sector", f"No open national tender in “{sector}” for this window.")
        elif within_days is not None:
            empty_state(
                "No national tenders closing that soon",
                f"No open national tender closes within {within_days} days. Try a wider window.",
            )
        else:
            empty_state("No open national tenders", "The national live-tender view returned no rows.")
        return
    window_label = "soonest-closing" if within_days is None else f"closing within {within_days} days"
    sector_label = f" in {sector}" if sector else ""
    total = len(df)
    st.caption(
        f"{total:,} {window_label} national tenders{sector_label}, numbered by how soon they close. "
        "Estimated value is a pre-award buyer estimate — not an award and not a payment. "
        "Click a tender to open it on eTenders."
    )
    # Same paginate + pagination_controls "click bar" the supplier and award lists use, so the
    # reader can page through every open tender instead of the list ending at an arbitrary date.
    # The page counter is namespaced by the active filter so changing the window/sector starts at
    # page 1 (instead of stranding the reader on a page that no longer exists in the smaller set).
    pg_key = f"pr_live_{within_days if within_days is not None else 'all'}_{sector or 'all'}"
    page_idx = paginate(total, key_prefix=pg_key, page_size=_LIVE_PAGE)
    page = df.iloc[page_idx * _LIVE_PAGE : (page_idx + 1) * _LIVE_PAGE]
    cards = []
    for offset, r in enumerate(page.itertuples()):
        rank = page_idx * _LIVE_PAGE + offset + 1  # global rank (soonest-closing first) — the numbered list
        meta_parts = [_esc(_coalesce(getattr(r, "procedure", None)))]
        dl = _coalesce(getattr(r, "submission_deadline", None))
        if dl:
            meta_parts.append(f"closes {fmt_civic_date(dl)}")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        days = getattr(r, "days_to_deadline", None)
        if days is not None and not pd.isna(days):
            d = int(days)
            label = "closes today" if d == 0 else f"{d} day{'s' if d != 1 else ''} left"
            cls = "pr-pill pr-pill-lob" if d <= 14 else "pr-pill"
            pills.append(f'<span class="{cls}">{label}</span>')
        ev = _eur(getattr(r, "estimated_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{ev} est. value</span>')
        buyer = _coalesce(getattr(r, "buyer", None))
        title = _coalesce(getattr(r, "title", None))
        name_html = f"<span>{_buyer_link(buyer)}</span>"
        if title:
            name_html += f'<span class="pr-sub">{_esc(title)}</span>'
        url = _coalesce(getattr(r, "detail_url", None))
        source = source_link_html(
            url,
            "Source notice",
            aria_label=f"Open the national tender from {buyer or 'this buyer'} on eTenders",
        )
        if source:
            name_html += f'<span class="pr-sub">{source}</span>'
        cards.append(_card(name_html, meta, pills, rank=rank))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div class="pr-sp-md"></div>')
    pagination_controls(
        total,
        key_prefix=pg_key,
        page_sizes=(_LIVE_PAGE,),
        default_page_size=_LIVE_PAGE,
        label="tenders",
    )
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders — the national public-procurement platform '
        '(<a href="https://www.etenders.gov.ie" target="_blank" rel="noopener">etenders.gov.ie ↗</a>), live '
        "request-for-tender notices captured as a point-in-time snapshot. Open opportunities only; estimated "
        "values are pre-award buyer estimates — never awards or payments, and never summed.</div>"
    )


def _render_national_expiring() -> None:
    """NATIONAL (eTenders) contracts whose advertised term is due to end — the re-tender pipeline,
    reconstructed from award date + advertised duration. A term, never a verified end event."""
    window = st.segmented_control(
        "National contracts ending within",
        ["12 months", "24 months", "36 months"],
        default="24 months",
        key="pr_expiring_etenders_window",
        label_visibility="collapsed",
    )
    months = int((window or "24 months").split()[0])
    res = fetch_expiring_etenders_result(months_ahead=months, limit=_TOP)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        st.html(
            '<div class="pr-foot"><strong>National (eTenders) contract terms:</strong> no national contracts '
            "with an advertised term ending in this window, or the award corpus isn't loaded.</div>"
        )
        return
    st.html(
        '<div class="pr-caveat"><strong>National contract terms due to end — the re-tender pipeline.</strong> '
        "Reconstructed from each national award's date plus its <em>advertised duration</em> — the term as "
        "stated, not a verified end event; contracts can end early or run longer through renewals (not folded "
        "in). Frameworks are excluded. The value shown is an award/ceiling figure for context — never summed.</div>"
    )
    st.caption(
        f"{len(df):,} national contracts whose advertised term ends within {months} months, soonest first. "
        "Advertised terms only; values are award/ceiling figures, never totals."
    )
    cards = []
    for r in df.itertuples():
        meta_parts = [_esc(_coalesce(getattr(r, "spend_category", None), getattr(r, "cpv_code", None)))]
        winner = _coalesce(getattr(r, "winner_display", None))
        if winner:
            meta_parts.append(_esc(winner))
        dur = getattr(r, "duration_months", None)
        if dur is not None and not pd.isna(dur):
            meta_parts.append(f"{int(dur)}-month term")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        end = _coalesce(getattr(r, "est_end_date", None))
        if end:
            pills.append(f'<span class="pr-pill pr-pill-val">ends {fmt_civic_date(end)}</span>')
        ev = _eur(getattr(r, "award_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill">{ev} award value</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        contract = _coalesce(getattr(r, "contract_name", None))
        name_html = f"<span>{_buyer_link(buyer)}</span>"
        if contract:
            name_html += f'<span class="pr-sub">{_esc(contract)}</span>'
        cards.append(_card(name_html, meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders national award notices (estimated end = award '
        "date + advertised contract duration). The end date is the advertised term, not a verified event; ~43% "
        "of national awards state a duration, and frameworks / dynamic purchasing systems (DPS) are excluded. "
        "Sole-trader and individual winner "
        "names are not shown; the contract itself stays listed as public record.</div>"
    )
