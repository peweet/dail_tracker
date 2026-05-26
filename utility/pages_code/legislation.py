import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import BILL_STATUS_CSS
from data_access.legislation_data import (
    fetch_all_statuses,
    fetch_bill_debates,
    fetch_bill_detail,
    fetch_bill_pdfs,
    fetch_bill_timeline,
    fetch_legislation_index_filtered,
    fetch_pre2014_act_detail,
    fetch_si_by_bill,
    fetch_si_composition,
    fetch_si_freshness,
    fetch_si_years_for_bill,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    evidence_heading,
    hero_banner,
    page_error_boundary,
    paginate,
    pagination_controls,
    render_stat_strip,
    sidebar_date_range,
    sidebar_page_header,
    stat_item,
)
from ui.entity_links import bill_detail_url, source_link_html
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander

# ── Helpers ────────────────────────────────────────────────────────────────────


def _status_badge_class(status: str) -> str:
    key = status.lower() if status else ""
    for k, cls in BILL_STATUS_CSS.items():
        if k in key:
            return f"signal {cls}"
    return "signal leg-status-active"


def _fmt_date(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


# ── Stage 1 — legislation index ────────────────────────────────────────────────


def _render_legislation_index(
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    title_search: str | None = None,
) -> None:
    # ── Hero ──────────────────────────────────────────────────────────────────
    hero_banner(
        kicker="Bills · Oireachtas",
        title="Bills Before the Oireachtas",
        dek=(
            "Track where each Bill stands in the legislative journey — "
            "from First Reading in the Dáil through the Seanad to Presidential signature. "
            "Select a phase below, then click any bill to open the full record."
        ),
    )

    # ── Fetch ─────────────────────────────────────────────────────────────────
    df = fetch_legislation_index_filtered(
        start_date=start_date,
        end_date=end_date,
        status=status,
        title_search=title_search,
    )

    if df.empty:
        st.html('<p class="section-heading">0 bills</p>')
        empty_state(
            "No bills found",
            "No bills match the current filters. Try widening the date range or clearing the status filter.",
        )
        return

    # ── Phase grouping ────────────────────────────────────────────────────────
    dail_df = df[df["bill_phase"] == "dail"]
    seanad_df = df[df["bill_phase"] == "seanad"]
    enacted_df = df[df["bill_phase"] == "enacted"]

    # ── Pipeline strip ────────────────────────────────────────────────────────
    st.html(
        f"""
        <div class="leg-pipeline-strip">
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(dail_df)}</div>
                <div class="leg-pipeline-label">Dáil Stages</div>
                <div class="leg-pipeline-sub">stages 1–5 · before the Dáil</div>
            </div>
            <div class="leg-pipeline-sep">→</div>
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(seanad_df)}</div>
                <div class="leg-pipeline-label">Seanad Stages</div>
                <div class="leg-pipeline-sub">stages 6–10 · before the Seanad</div>
            </div>
            <div class="leg-pipeline-sep">→</div>
            <div class="leg-pipeline-card">
                <div class="leg-pipeline-num">{len(enacted_df)}</div>
                <div class="leg-pipeline-label">Enacted</div>
                <div class="leg-pipeline-sub">stage 11 · signed into law</div>
            </div>
        </div>
        """
    )

    # ── Phase selector ────────────────────────────────────────────────────────
    phase_opts = {
        f"All ({len(df)})": df,
        f"Dáil Stages ({len(dail_df)})": dail_df,
        f"Seanad Stages ({len(seanad_df)})": seanad_df,
        f"Enacted ({len(enacted_df)})": enacted_df,
    }
    _phase_keys = list(phase_opts.keys())
    phase_sel = (
        st.segmented_control(
            "Filter by phase",
            _phase_keys,
            default=_phase_keys[0],
            label_visibility="collapsed",
            key="leg_phase_radio",
        )
        or _phase_keys[0]
    )
    view_df = phase_opts[phase_sel]

    if view_df.empty:
        empty_state("No bills in this phase", "No bills in this phase match the current filters.")
        return

    total = len(view_df)
    st.html(f'<p class="section-heading">{total:,} bill{"s" if total != 1 else ""}</p>')

    # ── Pagination state (controls rendered below the cards) ──────────────────
    LEG_PAGE_SIZE = 10
    page_idx = paginate(total, key_prefix="leg_index", page_size=LEG_PAGE_SIZE)
    visible_df = view_df.iloc[page_idx * LEG_PAGE_SIZE : (page_idx + 1) * LEG_PAGE_SIZE]

    # ── Bill card list — entire card is the click target via
    # clickable_card_link (URL-based navigation: ?bill=<bill_id>). The
    # inner Oireachtas ↗ link remains independently clickable thanks to
    # the stretched-link CSS pattern in shared_css.py. ──────────────────
    cards: list[str] = []
    for _, row in visible_df.iterrows():
        status = row.get("bill_status", "—") or "—"
        status_cls = _status_badge_class(status)
        date_str = _fmt_date(row.get("introduced_date"))
        title = row.get("bill_title", "—") or "—"
        sponsor_raw = row.get("sponsor")
        sponsor = sponsor_raw if isinstance(sponsor_raw, str) and sponsor_raw.strip() and sponsor_raw.strip() != "—" else ""
        stage = row.get("current_stage", "—") or "—"
        # Drop the leading em-dash on cards with no sponsor (e.g. older
        # enacted bills with NULL sponsor in the API) — used to render
        # "— · Enacted · Oireachtas ↗" on all 525 Enacted bills.
        meta_text = f"{sponsor} · {stage}" if sponsor else stage
        url = row.get("oireachtas_url") or ""
        link_html = source_link_html(
            url,
            "Oireachtas",
            aria_label="Open this bill on oireachtas.ie",
        )

        card_html = (
            f'<div class="leg-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="{status_cls}">{html.escape(status)}</span>'
            f'<span class="leg-bill-card-date">{html.escape(date_str)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{html.escape(title)}</div>'
            f'<div class="leg-bill-card-footer">'
            f'<span class="leg-bill-card-meta">{html.escape(meta_text)}</span>'
            f"{link_html}"
            f"</div>"
            f"</div>"
        )
        cards.append(
            clickable_card_link(
                href=bill_detail_url(str(row["bill_id"])),
                inner_html=card_html,
                aria_label=f"Open {title}",
            )
        )
    st.html("\n".join(cards))

    # ── Pagination controls (below the cards) ─────────────────────────────────
    pagination_controls(
        total,
        key_prefix="leg_index",
        page_sizes=(LEG_PAGE_SIZE,),
        default_page_size=LEG_PAGE_SIZE,
        label="bills",
    )

    # ── Export ────────────────────────────────────────────────────────────────
    export_cols = [
        c
        for c in [
            "bill_title",
            "sponsor",
            "bill_type",
            "bill_status",
            "introduced_date",
            "current_stage",
            "oireachtas_url",
        ]
        if c in view_df.columns
    ]
    export_button(view_df[export_cols], "Export current view as CSV", "legislation_filtered.csv", "leg_csv_export")

    # ── Provenance ────────────────────────────────────────────────────────────
    provenance_expander(
        sections=[
            "**Source:** [Houses of the Oireachtas Open Data API](https://api.oireachtas.ie)\n\n"
            "**Dataset:** All Bills introduced to the Oireachtas (Private Members' and Government Bills).\n\n"
            "**Bill phases:** Dáil stages (1–5) → Seanad stages (6–10) → Enacted (stage 11). "
            "[How a Bill Becomes Law](https://www.oireachtas.ie/en/how-parliament-works/legislation/how-a-bill-becomes-law/)\n\n"
            "**Stage information** reflects the most recent stage recorded in the API at time of extract.",
        ],
        source_caption="Data: Houses of the Oireachtas Open Data API",
    )


# ── Stage 2 — bill detail ──────────────────────────────────────────────────────


def _render_stage_timeline(timeline_df: pd.DataFrame) -> None:
    if timeline_df.empty:
        empty_state("Timeline not available", "Stage timeline data is not available for this bill.")
        return

    # Stage group thresholds — immutable Oireachtas legislative procedure.
    # Source: Houses of the Oireachtas, "How a Bill becomes Law"
    # https://www.oireachtas.ie/en/how-parliament-works/legislation/how-a-bill-becomes-law/
    _STAGE_GROUPS = {
        2: "Dáil stages",  # stages 2–5: Second Stage through Report/Final Stage
        6: "Seanad stages",  # stages 6–10: same progression in the Seanad
        11: "Presidential signature",
    }
    _seen_groups: set[int] = set()

    rows_html = ""
    for _, r in timeline_df.iterrows():
        num = r.get("stage_number")
        num_val = int(num) if pd.notna(num) else None

        if num_val is not None:
            for threshold, group_label in sorted(_STAGE_GROUPS.items()):
                if num_val >= threshold and threshold not in _seen_groups:
                    rows_html += f'<div class="leg-stage-group">{html.escape(group_label)}</div>'
                    _seen_groups.add(threshold)

        current_cls = "leg-stage-current" if r.get("is_current_stage") else ""
        date_str = _fmt_date(r.get("stage_date"))
        num_str = str(num_val) if num_val is not None else "·"
        # NaN (float) is truthy in Python, so `r.get("x") or ""` doesn't
        # guard against missing CSV cells — must check for str explicitly.
        _ch = r.get("chamber")
        chamber = _ch if isinstance(_ch, str) and _ch else ""
        _sn = r.get("stage_name")
        label = html.escape(_sn if isinstance(_sn, str) and _sn else "—")
        if chamber:
            label = f"{label} <span class='leg-stage-chamber'>· {html.escape(chamber)}</span>"

        rows_html += (
            f'<div class="leg-stage-row {current_cls}">'
            f'<span class="leg-stage-num">{html.escape(num_str)}</span>'
            f'<span class="leg-stage-label">{label}</span>'
            f'<span class="leg-stage-date">{html.escape(date_str)}</span>'
            f"</div>"
        )

    st.html(f'<div class="leg-stage-list">{rows_html}</div>')


def _render_debates(debates_df: pd.DataFrame) -> None:
    if debates_df.empty:
        empty_state("No debates found", "No debate records found for this bill.")
        return

    rows_html = ""
    for _, r in debates_df.iterrows():
        date_str = _fmt_date(r.get("debate_date"))
        title = r.get("debate_title", "—") or "—"
        url = r.get("debate_url") or ""
        chamber = r.get("chamber") or ""

        title_html = (
            f'<a class="leg-debate-title" href="{html.escape(url, quote=True)}" target="_blank">{html.escape(title)}</a>'
            if url
            else f'<span class="leg-debate-title-plain">{html.escape(title)}</span>'
        )
        rows_html += (
            f'<div class="leg-debate-row">'
            f'<span class="leg-debate-date">{html.escape(date_str)}</span>'
            f"{title_html}"
            f'<span class="leg-debate-chamber">{html.escape(chamber)}</span>'
            f"</div>"
        )

    st.html(f'<div class="leg-debate-list">{rows_html}</div>')


_PDF_GROUP_LABELS: dict[str, str] = {
    "version": "Bill text",
    "related_doc": "Explanatory documents",
    "amendment": "Amendment lists",
}

_PDF_SUBTYPE_LABELS: dict[str, str] = {
    # related_doc subtypes
    "memo": "Explanatory Memorandum",
    "digest": "Bills Digest",
    "gluais": "Gluais",
    "errata": "Erratum",
    # amendment subtypes
    "numberedList": "Numbered List",
    "creamList": "Cream List",
}


def _section_bill_pdfs(bill_id: str) -> None:
    """Oireachtas-issued PDFs for this bill: text versions, explanatory
    documents, and amendment lists. Sourced from v_legislation_pdfs
    (union of versions / related_docs / bill_amendments)."""
    st.html('<p class="section-heading">Documents</p>')

    pdfs_df = fetch_bill_pdfs(bill_id)
    if pdfs_df.empty:
        empty_state(
            "No documents published",
            "No Oireachtas PDFs are available for this bill. Pre-2014 Acts "
            "and bills withdrawn at very early stages often have none.",
        )
        return

    rendered_groups: list[str] = []
    for category, label in _PDF_GROUP_LABELS.items():
        grp = pdfs_df[pdfs_df["pdf_category"] == category]
        if grp.empty:
            continue
        rendered_groups.append(_pdf_group_html(label, grp))

    st.html('<div class="leg-doc-section">' + "".join(rendered_groups) + "</div>")


def _pdf_group_html(group_label: str, grp_df: pd.DataFrame) -> str:
    """Render one PDF subsection (e.g. 'Bill text') as a stack of source-cards."""
    cards = ""
    for _, r in grp_df.iterrows():
        label_text = r.get("pdf_label") or _PDF_SUBTYPE_LABELS.get(r.get("pdf_subtype") or "", "") or "Document"
        date_str = _fmt_date(r.get("pdf_date"))
        lang = r.get("pdf_lang") or ""
        meta_bits: list[str] = []
        if date_str and date_str != "—":
            meta_bits.append(date_str)
        if lang and lang != "eng":
            meta_bits.append(lang.upper())
        meta_str = " · ".join(meta_bits)

        link_html = source_link_html(
            r.get("pdf_url") or "",
            "Open PDF",
            aria_label=f"Open {label_text} PDF in a new tab",
        )

        cards += (
            '<div class="leg-source-card">'
            f'<div class="leg-source-label">{html.escape(label_text)}'
            + (f' <span class="leg-source-meta">· {html.escape(meta_str)}</span>' if meta_str else "")
            + "</div>"
            f"{link_html}"
            "</div>"
        )
    return (
        f'<div class="leg-doc-group-label">{html.escape(group_label)} '
        f'<span class="leg-doc-group-count">({len(grp_df)})</span></div>'
        f"{cards}"
    )


def _section_statutory_instruments(bill_id: str) -> None:
    """Statutory Instruments issued under this Act. Sourced from
    v_bill_statutory_instruments (Iris SI taxonomy joined to enabling
    bills via the lifted POC matcher)."""
    st.html('<p class="section-heading">Statutory Instruments under this Act</p>')

    fresh = fetch_si_freshness(bill_id)
    comp = fetch_si_composition(bill_id)
    if not fresh or comp.empty:
        empty_state(
            "No SIs under this Act",
            "Either none have been issued yet, this Bill predates the SI "
            "data window (2018), or it never became an Act.",
        )
        return

    # Composition sentence
    total = fresh["total"]
    parts = [f"{int(r.n)} {str(r.si_operation).replace('_', ' ')}" for r in comp.head(3).itertuples()]
    tail = f" · {len(comp) - 3} other types" if len(comp) > 3 else ""
    st.caption(f"**{total} SI{'s' if total != 1 else ''}** under this Act: " + " · ".join(parts) + tail)
    if fresh.get("first_si") is not None and fresh.get("last_si") is not None:
        eu_pct = (100 * fresh["eu_count"] / total) if total else 0
        st.caption(
            f"First SI: {pd.Timestamp(fresh['first_si']):%d %b %Y} · "
            f"last activity: {pd.Timestamp(fresh['last_si']):%d %b %Y} · "
            f"EU-driven share: {eu_pct:.0f}%"
        )

    # Year pills
    years = fetch_si_years_for_bill(bill_id)
    selected_year = (
        st.pills(
            "SI year",
            options=["All years"] + [str(y) for y in years],
            default="All years",
            key=f"si_year_{bill_id}",
            label_visibility="collapsed",
        )
        or "All years"
    )
    year_val = None if selected_year == "All years" else int(selected_year)

    # Operation pills (driven by the composition above)
    selected_op = (
        st.pills(
            "Operation",
            options=["All operations"] + comp["si_operation"].dropna().tolist(),
            default="All operations",
            key=f"si_op_{bill_id}",
            label_visibility="collapsed",
            format_func=lambda x: str(x).replace("_", " "),
        )
        or "All operations"
    )
    op_val = None if selected_op == "All operations" else selected_op

    df = fetch_si_by_bill(bill_id, year=year_val, operation=op_val)
    if df.empty:
        empty_state("No SIs match these filters", "Try a different year or operation type.")
        return

    n = len(df)
    st.caption(f"Showing {n} SI{'s' if n != 1 else ''}")

    for _, row in df.iterrows():
        date_disp = (
            pd.to_datetime(row["si_signed_date"]).strftime("%d %b %Y") if pd.notna(row["si_signed_date"]) else "—"
        )
        domain = str(row["si_policy_domain"] or "").replace("_", " ")
        operation = str(row["si_operation"] or "").replace("_", " ")
        form = str(row["si_form"] or "").replace("_", " ")
        named = row.get("si_minister_named")
        role = row.get("si_minister")
        if isinstance(named, str) and named.strip():
            minister = named.strip()
        elif isinstance(role, str) and role.strip():
            minister = role.strip()
        else:
            minister = "—"
        eu_badge = (
            '<span class="signal signal-eu">EU</span>'
            if bool(row.get("si_is_eu"))
            else ""
        )
        url = row.get("eisb_url") or ""
        url_html = (
            source_link_html(
                url,
                "irishstatutebook.ie",
                aria_label="Open SI on irishstatutebook.ie",
            )
            if isinstance(url, str) and url.startswith("http")
            else ""
        )

        st.html(
            f'<div class="leg-bill-card leg-si-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">'
            f"SI {int(row['si_number'])}/{int(row['si_year'])} · {html.escape(date_disp)}"
            f"</span>"
            f'<span class="signal leg-status-active">{html.escape(form)}</span>'
            f"{eu_badge}"
            f"</div>"
            f'<div class="leg-bill-card-title">{html.escape(str(row["si_title"]))}</div>'
            f'<div class="leg-si-meta">'
            f"{html.escape(operation)} · {html.escape(domain)} · "
            f"{html.escape(minister)} · {url_html}"
            f"</div>"
            f"</div>"
        )


def _render_pre2014_act_detail(bill_id: str) -> None:
    """Minimal detail view for synthetic 'act_<year>_<slug>' IDs — pre-2014
    Acts that aren't in sponsors.parquet. Renders a small hero + the SI
    section only (no timeline/debates/sponsor — those don't exist)."""
    info = fetch_pre2014_act_detail(bill_id)
    if not info:
        st.warning(f"Act '{bill_id}' not found in the pre-2014 Acts table.")
        return

    title = info["act_short_title"]
    year = info["act_year"]
    domain = (info["policy_domain"] or "").replace("_", " ")

    badges = '<span class="signal signal-neutral">Pre-2014 Act</span>'
    if domain:
        badges += f' <span class="signal signal-dark">{html.escape(domain)}</span>'

    st.html(
        f"""
        <div class="leg-bill-identity">
            <div class="leg-bill-badges">{badges}</div>
            <div class="leg-bill-title">{html.escape(title)}</div>
            <div class="leg-bill-ref">Enacted {year}</div>
            <p class="leg-long-title leg-pre2014-long-title">
                Primary Act predates the Oireachtas bills database (2014).
                Statutory Instruments made under it are listed below;
                stage timeline and Oireachtas debates are not available
                via this surface.
            </p>
        </div>
        """
    )
    st.divider()
    _section_statutory_instruments(bill_id)


def _render_bill_detail(bill_id: str) -> None:
    # ── Back navigation ───────────────────────────────────────────────────────
    if back_button("← Back to Legislation Index", key="leg"):
        st.session_state.pop("leg_selected_bill_id", None)
        st.query_params.clear()
        st.rerun()

    # Synthetic pre-2014 Act IDs ('act_YYYY_slug') don't live in
    # v_legislation_index — render the minimal "Act page" instead of the
    # full bill flow.
    if isinstance(bill_id, str) and bill_id.startswith("act_"):
        _render_pre2014_act_detail(bill_id)
        return

    # ── Load data ─────────────────────────────────────────────────────────────
    detail_df = fetch_bill_detail(bill_id)
    timeline_df = fetch_bill_timeline(bill_id)
    debates_df = fetch_bill_debates(bill_id)

    if detail_df.empty:
        st.warning(f"Bill '{bill_id}' not found. It may have been removed or filtered out.")
        return

    row = detail_df.iloc[0]

    # ── Bill identity strip ───────────────────────────────────────────────────
    status = row.get("bill_status", "—") or "—"
    bill_type = row.get("bill_type", "—") or "—"
    current_house = row.get("current_house") or ""
    status_cls = _status_badge_class(status)

    badges_html = f'<span class="{status_cls}">{html.escape(status)}</span>'
    if bill_type and bill_type != "—":
        badges_html += f' <span class="signal signal-neutral">{html.escape(bill_type)}</span>'
    if current_house:
        badges_html += f' <span class="signal signal-dark">{html.escape(current_house)}</span>'

    bill_title = row.get("bill_title", "—") or "—"
    bill_no = row.get("bill_no", "")
    bill_year = row.get("bill_year", "")
    oireachtas_url = row.get("oireachtas_url") or ""
    ref = f"Bill {html.escape(str(bill_no))} of {html.escape(str(bill_year))}" if bill_no and bill_year else ""

    link_html = source_link_html(
        oireachtas_url,
        "View Bill on Oireachtas.ie",
        aria_label="Open this bill on oireachtas.ie",
    )

    # Round-3 audit P1-B fix: Oireachtas API ships `long_title` with raw
    # <p>...</p> wrappers (and occasionally <i>, <em>, etc). Previously the
    # whole string was html.escape()d and the tags rendered as literal
    # "<p>...</p>" text on screen. Strip tags first, then escape the result.
    import re as _re
    long_title_raw = (row.get("long_title") or "").strip()
    long_title_clean = _re.sub(r"<[^>]+>", "", long_title_raw).strip()
    long_title_html = (
        f'<p class="leg-long-title leg-long-title-tight">{html.escape(long_title_clean)}</p>'
        if long_title_clean
        else ""
    )

    st.html(
        f"""
        <div class="leg-bill-identity">
            <div class="leg-bill-badges">{badges_html}</div>
            <div class="leg-bill-title">{html.escape(bill_title)}</div>
            {f'<div class="leg-bill-ref">{ref}</div>' if ref else ""}
            {long_title_html}
            {link_html}
        </div>
        """
    )

    # ── Stat strip ────────────────────────────────────────────────────────────
    introduced = _fmt_date(row.get("introduced_date"))
    sponsor = row.get("sponsor", "—") or "—"
    current_stage = row.get("current_stage") or "—"
    method = row.get("method") or "—"

    render_stat_strip(
        stat_item(introduced, "Introduced"),
        stat_item(sponsor, "Sponsor"),
        stat_item(current_stage, "Current stage"),
        stat_item(method, "Method"),
    )

    # ── Two-column detail ─────────────────────────────────────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        evidence_heading("Stage Timeline")
        _render_stage_timeline(timeline_df)

    with col_right:
        debate_label = f"Debates ({len(debates_df)})" if not debates_df.empty else "Debates"
        evidence_heading(debate_label)
        _render_debates(debates_df)

    # ── CSV export of this bill's timeline ────────────────────────────────────
    if not timeline_df.empty:
        export_button(
            timeline_df, "Export stage timeline as CSV", f"bill_{bill_no}_{bill_year}_timeline.csv", "leg_timeline_csv"
        )

    # ── Documents (Oireachtas PDFs: versions, memos, amendments) ──────────────
    st.divider()
    _section_bill_pdfs(bill_id)

    # ── Statutory Instruments under this Act ──────────────────────────────────
    st.divider()
    _section_statutory_instruments(bill_id)

    # ── Provenance ────────────────────────────────────────────────────────────
    last_updated = row.get("last_updated") or "—"
    source = row.get("source") or "—"
    provenance_expander(
        sections=[
            f"**Bill ID:** {bill_id}",
            f"**Source:** {source}  ·  **Last updated (API):** {last_updated}",
            "**Data origin:** Houses of the Oireachtas Open Data API.",
        ]
    )


# ── Entry point ────────────────────────────────────────────────────────────────


@page_error_boundary
def legislation_page() -> None:
    inject_css()

    # URL-driven entry: ?bill=<bill_id> opens the detail view (mirrors the
    # member_overview ?member=… pattern). Session state is the source of
    # truth thereafter so back-button + reruns stay consistent.
    url_bill = st.query_params.get("bill")
    if url_bill:
        st.session_state["leg_selected_bill_id"] = url_bill

    selected_bill_id: str | None = st.session_state.get("leg_selected_bill_id")

    # ── Sidebar ───────────────────────────────────────────────────────────────
    start_date: str | None = None
    end_date: str | None = None
    status_param: str | None = None
    search_param: str | None = None

    with st.sidebar:
        sidebar_page_header("Legislation")

        if selected_bill_id:
            st.html('<div class="page-subtitle">Bill detail</div>')
        else:
            st.html('<div class="page-subtitle">Source: Oireachtas Open Data API</div>')
            st.divider()

            start_date, end_date = sidebar_date_range(
                "Introduced between",
                key="leg_date_range",
                empty_default=True,
            )

            st.html('<p class="sidebar-label">Status</p>')
            statuses = fetch_all_statuses()
            status_sel = st.selectbox(
                "Status",
                ["All"] + statuses,
                label_visibility="collapsed",
                key="leg_status_filter",
            )
            status_param = status_sel if status_sel != "All" else None

            st.html('<p class="sidebar-label">Search title</p>')
            title_search = st.text_input(
                "Search title",
                placeholder="e.g. Housing, Health, Education…",
                label_visibility="collapsed",
                key="leg_title_search",
            )
            search_param = title_search.strip() or None

    # ── Main content ──────────────────────────────────────────────────────────
    if selected_bill_id:
        _render_bill_detail(selected_bill_id)
    else:
        _render_legislation_index(
            start_date=start_date,
            end_date=end_date,
            status=status_param,
            title_search=search_param,
        )
