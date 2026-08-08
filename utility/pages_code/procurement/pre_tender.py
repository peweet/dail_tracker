from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_pre_tender_areas_result,
    fetch_pre_tender_leads_result,
    fetch_pre_tender_sectors_result,
    fetch_pre_tender_stages_result,
    fetch_pre_tender_summary_result,
)
from ui.components import (
    empty_state,
    paginate,
    pagination_controls,
)
from ui.entity_links import source_link_html

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import fmt_civic_date
from ui.format import to_int as _n

from ._shared import _card


_PRE_TENDER_PAGE = 24  # cards per page (multiple of 3 for the grid)


# Reader-facing label for each normalised stage, ordered closest-to-tender first by the
# corpus's own stage_display_order. The label describes WHERE THE PROJECT HAD REACHED at the
# observation date — never where it is now, which nothing in this lane verifies.
_STAGE_LABEL = {
    "tender_preparation": "Tender documents being prepared",
    "approved_to_tender": "Approved to go to tender",
    "procurement_or_delivery_check": "At procurement or delivery",
    "approved_to_procure": "Approved to procure",
    "procurement_planned": "Procurement planned",
    "detailed_design": "Detailed design",
    "developed_design": "Developed design",
    "statutory_approval_part8": "Part 8 statutory approval",
    "planning_or_procurement": "Planning or procurement",
    "planning": "Planning",
    "approved_works": "Works approved",
    "capital_budget_approved": "Capital budget approved",
    "funded_not_started": "Funded, not started",
    "preliminary_design": "Preliminary design",
    "early_design": "Early design",
    "business_case": "Business case",
    "approval_gate": "At an approval gate",
    "consultancy_authorized": "Consultancy authorised",
    "active_delivery": "In construction",
}

# How old the observation is, from the corpus's own snapshot_freshness banding. This is the
# section's load-bearing honesty signal: an observation older than a year may already have
# gone to tender and been awarded, and the reader has to see that on the card itself.
_FRESHNESS = {
    "recent_180_days": ("pr-pill", "reported within 6 months"),
    "ageing_181_to_365_days": ("pr-pill", "reported 6–12 months ago"),
    "stale_over_365_days": ("pr-pill pr-pill-lob", "reported over a year ago"),
}


def _stage_label(stage) -> str:
    """Human label for a normalised stage, falling back to the underscored key made readable
    so a stage added upstream still renders as words rather than disappearing."""
    key = _coalesce(stage)
    if not key:
        return ""
    return _STAGE_LABEL.get(key, key.replace("_", " ").capitalize())


# The Department's approval gates, latest first. Only the furthest one reached is shown: five
# date pills on one card would bury the observation date, which is the fact that makes the rest
# of the card safe to read. Null on every corpus that publishes no gate dates.
_GATE_LABELS = (
    ("stage_4_start_date", "Stage 4 (construction)"),
    ("stage_3_start_date", "Stage 3 (tender documents)"),
    ("stage_2b_start_date", "Stage 2b (detailed design)"),
    ("stage_2a_start_date", "Stage 2a (developed design)"),
    ("stage_1_start_date", "Stage 1 (initial design)"),
)


def _as_date(val):
    """A date from a published cell, or None when the cell held wording rather than a date."""
    if val is None or (not isinstance(val, str) and pd.isna(val)):
        return None
    parsed = pd.to_datetime(val, errors="coerce")
    return None if pd.isna(parsed) else parsed


def _site_pills(r, today) -> list[str]:
    """On-site status and furthest approval gate, from the Department's published stage dates.

    These columns are why the promotion joins the snapshot table back in: a start-on-site date in
    the past means the main contract is let, so the row is a live building site rather than a lead.
    Rendering it is what stops the section presenting the two as the same thing.
    """
    pills: list[str] = []
    on_site = _as_date(getattr(r, "start_on_site_date", None))
    if on_site is not None:
        if on_site.date() <= today:
            pills.append(
                f'<span class="pr-pill pr-pill-cro">on site since {_esc(fmt_civic_date(on_site))} '
                "— already advertised and awarded</span>"
            )
        else:
            pills.append(f'<span class="pr-pill">on site due {_esc(fmt_civic_date(on_site))}</span>')
    else:
        # Wording the Department wrote where a date was expected ("TBC"), kept verbatim.
        note = _coalesce(getattr(r, "start_on_site_note", None))
        if note:
            pills.append(f'<span class="pr-pill">on site: {_esc(note)}</span>')
    for column, label in _GATE_LABELS:
        gate = _as_date(getattr(r, column, None))
        if gate is not None:
            pills.append(f'<span class="pr-pill">{label} from {_esc(fmt_civic_date(gate))}</span>')
            break
    return pills


def _facet(res, value_col: str, label: str, key: str, all_label: str) -> str | None:
    """One 'All X / value (n)' dropdown built from a facet QueryResult. Returns the chosen value,
    or None for the all-inclusive default (and when the facet has no rows to offer).

    The OPTION VALUE is the raw key and the count is applied through format_func only — storing
    the count in the value is what broke the live-tender sector facet: counts move with the other
    filters, so a previously chosen "Kildare (38)" vanished from the new options and Streamlit
    raised on an out-of-range selectbox value. A value that disappears entirely from the new
    option list is reset before the widget reads it, for the same reason.
    """
    if not res.ok or res.data.empty:
        return None
    counts = {str(getattr(r, value_col)): int(r.n) for r in res.data.itertuples()}
    options = [all_label, *counts.keys()]
    if st.session_state.get(key) not in options:
        st.session_state[key] = all_label
    choice = st.selectbox(
        label,
        options,
        key=key,
        format_func=lambda v: v if v == all_label else f"{v} ({counts.get(v, 0):,})",
    )
    return None if choice == all_label else choice


def _pre_tender_caveat(s) -> None:
    """The section's headline, built from the corpus summary rather than asserted in prose.

    Every number here is read off the summary row. The verification sentence is conditional on
    n_status_verified because the claim it makes — that nothing has been checked against the live
    tender register — must stay true if that ever changes upstream.
    """
    n_leads = _n(s.get("n_leads"))
    n_areas = _n(s.get("n_areas"))
    n_sources = _n(s.get("n_sources"))
    n_verified = _n(s.get("n_status_verified"))
    n_stale = _n(s.get("n_stale"))
    n_undated = _n(s.get("n_undated"))
    n_past = _n(s.get("n_past_tender"))
    newest = s.get("newest_observation")
    span = f" The most recent was reported on {fmt_civic_date(newest)}." if not pd.isna(newest) else ""
    if n_verified == 0:
        verification = (
            "<strong>None of these has been checked against the live tender register.</strong> "
            "A project may since have gone to tender, been awarded, or been shelved"
        )
    else:
        verification = (
            f"<strong>{n_verified:,} of {n_leads:,} have been checked against the live tender "
            "register</strong>; the rest have not, and may since have gone to tender, been awarded, "
            "or been shelved"
        )
    # Every count below is read off the summary row, including the two that qualify the section's
    # own framing. The undated clause is what stops "n reported over a year ago" from being read
    # as an age statement about the whole corpus when some rows carry no usable date at all.
    if n_stale and n_undated:
        stale_note = f" {n_stale:,} were reported more than a year ago, and {n_undated:,} carry no usable date."
    elif n_stale:
        stale_note = f" {n_stale:,} of them were reported more than a year ago."
    elif n_undated:
        stale_note = f" {n_undated:,} carry no usable date."
    else:
        stale_note = ""
    # The lane is named for what most of it holds, not all of it: some observations record a
    # project that had ALREADY reached construction when it was reported, so the blanket claim
    # "not been advertised" would be false for them. State the count rather than soften the claim.
    if n_past:
        headline = (
            "<strong>Before the tender — public projects a body has described, most of them not yet "
            f"advertised.</strong> {n_past:,} of these had already reached construction when they "
            "were reported, and are marked as such."
        )
    else:
        headline = (
            "<strong>Before the tender — public projects a body has described, "
            "but which have not been advertised.</strong>"
        )
    st.html(
        f'<div class="pr-caveat">{headline} '
        f"{n_leads:,} dated observations across {n_areas:,} areas, taken from {n_sources:,} published "
        f"sources — most of them answers given to the Dáil.{span} "
        f"{verification} — this is a record of what was said on a date, not a live opportunity list."
        f"{stale_note} Any amount shown is the source's own wording and is never parsed or added up.</div>"
    )


def _render_pre_tender() -> None:
    """Pre-notice observations: what a public body has SAID about a project it intends to build,
    before any tender exists. The earliest lane on the page and a separate, non-money grain — it is
    never value-merged with the open-tender, award or payment registers (see caveats.PRE_TENDER)."""
    summary_res = fetch_pre_tender_summary_result()
    if not summary_res.ok or summary_res.data.empty or _n(summary_res.data.iloc[0].get("n_leads")) == 0:
        empty_state(
            "Pre-tender observations aren't available right now",
            "The pre-tender view couldn't be loaded, or holds no observations — a source/pipeline "
            "issue, not an empty result.",
        )
        return
    _pre_tender_caveat(summary_res.data.iloc[0])

    # Facets are mutually aware: each option list is counted under the OTHER two filters, so a
    # combination that would return nothing is never offered. Read in the order a reader narrows —
    # what kind of work, how far along, where.
    sector_col, stage_col, area_col = st.columns(3)
    with sector_col:
        sector = _facet(
            fetch_pre_tender_sectors_result(area=None, stage=None), "sector", "Sector", "pr_pt_sector", "All sectors"
        )
    with stage_col:
        stage_res = fetch_pre_tender_stages_result(area=None, sector=sector)
        stage_labels = (
            {str(r.stage): _stage_label(r.stage) for r in stage_res.data.itertuples()}
            if stage_res.ok and not stage_res.data.empty
            else {}
        )
        stage = _facet(stage_res, "stage", "Stage reached", "pr_pt_stage", "Any stage")
    with area_col:
        area = _facet(
            fetch_pre_tender_areas_result(sector=sector, stage=stage), "area", "Area", "pr_pt_area", "All areas"
        )

    res = fetch_pre_tender_leads_result(area=area, sector=sector, stage=stage)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state(
            "No observations match those filters",
            "No pre-tender observation matches this combination. Widen one of the three filters.",
        )
        return

    total = len(df)
    narrowing = " · ".join(p for p in (sector, stage_labels.get(stage or "", stage), area) if p)
    st.caption(
        f"{total:,} observations{f' — {narrowing}' if narrowing else ''}, closest to tender first. "
        "Each card shows the stage the project had reached on the date it was reported, in the "
        "source's own words. Open the source to read the original answer."
    )

    pg_key = f"pr_pt_{sector or 'all'}_{stage or 'all'}_{area or 'all'}"
    page_idx = paginate(total, key_prefix=pg_key, page_size=_PRE_TENDER_PAGE)
    page = df.iloc[page_idx * _PRE_TENDER_PAGE : (page_idx + 1) * _PRE_TENDER_PAGE]
    today = pd.Timestamp.today().date()
    cards = []
    for r in page.itertuples():
        # The observation date leads the meta line: it is the single fact that makes the rest of
        # the card safe to read, so it is never demoted to a pill or a footnote.
        src_date = _coalesce(getattr(r, "source_date", None))
        meta_parts = [f"as reported {fmt_civic_date(src_date)}"] if src_date else ["date not stated"]
        for col in ("buyer_or_sponsor", "reporting_area"):
            val = _coalesce(getattr(r, col, None))
            if val:
                meta_parts.append(_esc(val))
        meta = " · ".join(meta_parts)

        pills = []
        stage_txt = _stage_label(getattr(r, "normalized_stage", None))
        if stage_txt:
            pills.append(f'<span class="pr-pill pr-pill-val">{_esc(stage_txt)}</span>')
        published = _coalesce(getattr(r, "published_stage", None))
        if published and published.lower() != stage_txt.lower():
            # The source's own stage wording, kept beside the normalised label because the two
            # vocabularies genuinely differ: the Department's "Stage 4" is construction, while a
            # council's "Stage 4" is nothing of the sort.
            pills.append(f'<span class="pr-pill">source: “{_esc(published)}”</span>')
        fresh_cls, fresh_txt = _FRESHNESS.get(_coalesce(getattr(r, "snapshot_freshness", None)), ("", ""))
        if fresh_txt:
            pills.append(f'<span class="{fresh_cls}">{fresh_txt}</span>')
        pills.extend(_site_pills(r, today))
        amount = _coalesce(getattr(r, "amount_text", None))
        if amount:
            # Source wording, rendered verbatim and labelled as such — never parsed into a number,
            # never totalled, never compared with an award or a payment.
            pills.append(f'<span class="pr-pill">source wording: {_esc(amount)}</span>')

        project = _coalesce(getattr(r, "project_name", None))
        name_html = f"<span>{_esc(project) if project else '—'}</span>"
        roll = _coalesce(getattr(r, "school_roll_number", None))
        if roll:
            name_html += f'<span class="pr-sub">Roll number {_esc(roll)}</span>'
        work = _coalesce(getattr(r, "likely_work_package", None))
        if work:
            name_html += f'<span class="pr-sub">{_esc(work)}</span>'
        source = source_link_html(
            _coalesce(getattr(r, "source_url", None)),
            "Read the source",
            aria_label=f"Open the published source for {project or 'this observation'}",
        )
        if source:
            name_html += f'<span class="pr-sub">{source}</span>'
        cards.append(_card(name_html, meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div class="pr-sp-md"></div>')
    pagination_controls(
        total,
        key_prefix=pg_key,
        page_sizes=(_PRE_TENDER_PAGE,),
        default_page_size=_PRE_TENDER_PAGE,
        label="observations",
    )
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> published answers to Dáil questions, council '
        "and semi-state reports, gathered as dated observations "
        '(<a href="https://data.oireachtas.ie" target="_blank" rel="noopener">data.oireachtas.ie ↗</a>). '
        "A pre-tender observation is not a tender, an award, a payment or a budget, and is never added "
        "to any of them. Stages are the source's own; nothing here has been confirmed against the live "
        "tender register, so check the source before acting on any row.</div>"
    )
