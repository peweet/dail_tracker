from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_call_offs_for_supplier_result,
    fetch_dependency_for_supplier_result,
    fetch_incumbency_for_supplier_result,
    fetch_epa_compliance_result,
    fetch_single_bid_baseline_result,
    fetch_supplier_single_bid_result,
    fetch_supplier_summary_result,
    fetch_ted_awards_by_year_result,
    fetch_ted_competition_stats_result,
    fetch_ted_corpus_stats_result,
    fetch_ted_for_supplier_result,
    fetch_ted_notices_for_supplier_result,
    fetch_ted_supplier_summary_result,
    fetch_ted_tender_sectors_result,
    fetch_ted_tenders_result,
    fetch_ted_tenders_stats_result,
)
from ui.entity_links import (
    company_profile_url,
    source_link_html,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    fmt_civic_date,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _TOP,
    _eur,
    _eur_scale,
    _awards_word,
    _authority_link,
    _buyer_link,
    _ted_winner_href,
    _yr_axis,
    _return_to_browse,
    _card,
)




# ──────────────────────────────────────────────────────────────────────────────
# Tab: EU-level awards (TED) — a SEPARATE register, never summed with eTenders.
# ──────────────────────────────────────────────────────────────────────────────
def _ted_value_pill(val) -> str:
    """Sum-safe TED value pill; omitted when the firm has no summable value (all its EU
    notices are framework ceilings) so the card shows the trustworthy count instead of '—'."""
    if _eur(val) == "—":
        return ""
    return f'<span class="pr-pill pr-pill-val">{_eur(val)} awarded (EU)</span>'


def _ted_competition_strip() -> None:
    """Neutral competition-intensity facts from the eForms award notices: how many received
    only one tender, ran without an open call, or were awarded on lowest price alone. Framed
    strictly as disclosure (no-inference rule) — never 'uncompetitive'/'rigged' as a verdict."""
    res = fetch_ted_competition_stats_result()
    if not res.ok or res.data.empty:
        return
    s = res.data.iloc[0]
    with_t = _n(s.get("notices_with_tenders"))
    single = _n(s.get("single_bid_notices"))
    if not with_t and not _n(s.get("uncompetitive_notices")):
        return
    parts = []
    if with_t:
        parts.append(
            f"<strong>{single:,}</strong> of {with_t:,} award notices that report a tender count "
            f"received <strong>only one tender</strong> on at least one lot ({100 * single / with_t:.0f}%)"
        )
    unc = _n(s.get("uncompetitive_notices"))
    if unc:
        parts.append(f"<strong>{unc:,}</strong> were awarded without an open competitive call")
    po = _n(s.get("price_only_notices"))
    if po:
        parts.append(f"<strong>{po:,}</strong> were awarded on lowest price alone")
    st.html(
        '<p class="pr-cap"><strong>Competition signals (eForms, 2024+):</strong> '
        + "; ".join(parts)
        + ". These are factual disclosures recorded in the notices themselves — a single tender or "
        "a negotiated procedure is a matter of record, not evidence of wrongdoing. Competition detail "
        "is only recorded from 2024 (eForms); earlier years show winners and value only.</p>"
    )




def _render_ted() -> None:
    stats_res = fetch_ted_corpus_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "EU-level award data isn't available right now",
            "The TED register couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]

    show_pan_eu = st.toggle(
        "Include pan-EU research frameworks",
        value=False,
        key="pr_ted_paneu",
        help="375 notices (e.g. GÉANT) where Ireland is one of dozens of participants. Their "
        "vast shared ceilings are never summable, so this only changes the notice count.",
    )
    n_shown = _n(s.get("n_notices")) if show_pan_eu else _n(s.get("n_notices_ex_pan_eu"))
    span = f"{_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
    caption = (
        f"{n_shown:,} EU Official Journal award notices ({span}), from {_n(s.get('n_buyers')):,} "
        f"Irish public buyers. {_eur_scale(s.get('value_safe_eur'))} in summable awarded value — "
        "a different register from eTenders (some firms appear in both; the two are never added "
        "together)."
    )
    if show_pan_eu:
        caption += (
            f" Including the {_n(s.get('n_pan_eu')):,} pan-EU frameworks adds "
            f"{_eur_scale(s.get('pan_eu_ceiling_eur'))} of <em>shared</em> ceilings — a mirage like "
            "the €570bn headline, never real Irish spend."
        )
    else:
        caption += f" {_n(s.get('n_pan_eu')):,} pan-EU research frameworks are excluded from totals."
    st.html(f'<p class="pr-cap">{caption}</p>')

    _ted_competition_strip()

    # EU awards over time (2016–2026) — the payoff of the legacy backfill. Collapsed so the
    # winner ranking stays the first thing on the tab (matches the eTenders trend pattern).
    tr = fetch_ted_awards_by_year_result()
    if tr.ok and not tr.data.empty and len(tr.data) > 1:
        with st.expander("EU awards over time"):
            st.bar_chart(
                _yr_axis(tr.data), x="year", y="n_awards", x_label="Year", y_label="Awards", height=200, color="#9c5b2e"
            )

    res = fetch_ted_supplier_summary_result(limit=_TOP, order_by="awards")
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No TED winners", "The EU register loaded but returned no company-class winners.")
        return
    st.caption(
        f"Top {len(df):,} firms by number of EU award notices won. Value is awarded value, not spend. "
        "Click a firm to open its individual EU notices."
    )
    cards = []
    for i, r in enumerate(df.head(_TOP).itertuples(), start=1):
        meta = f"{_awards_word(_n(r.n_awards))} · {_n(r.n_buyers):,} buyer{'s' if _n(r.n_buyers) != 1 else ''}"
        cro = _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None))
        pills = [p for p in (_ted_value_pill(r.ted_value_safe_eur), cro) if p]
        inner = _card(f"<span>{_esc(r.winner_name)}</span>", meta, pills, rank=i)
        cards.append(
            clickable_card_link(
                href=_ted_winner_href(r.winner_join_norm),
                inner_html=inner,
                aria_label=f"View the EU award notices won by {r.winner_name}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, the EU '
        'Official Journal of public procurement (<a href="https://ted.europa.eu" target="_blank" '
        'rel="noopener">ted.europa.eu ↗</a>), winners matched to the Companies Registration Office. '
        "2024+ from the TED API; 2016–2023 winner detail recovered from the per-notice Official Journal "
        "XML (the API omits it for pre-2024 notices). Award notices, not payments; a separate register "
        "from the national eTenders data — never summed.</div>"
    )


def _cro_pill_from(company_num, status) -> str:
    """CRO chip from explicit num/status (TED rows expose these directly, not as a row attr)."""
    if not _truthy(company_num):
        return ""
    label = _esc(_coalesce(status) or "matched")
    return f'<span class="pr-pill pr-pill-cro">CRO: {label}</span>'


def _render_ted_supplier_panel(supplier_norm: str) -> None:
    """Cross-reference block on an eTenders supplier profile: the same firm's TED (EU-level)
    footprint, matched on the normalised name. Clearly a separate register — never added to
    the eTenders headline (honesty rail; 66% of TED winners also appear in eTenders)."""
    res = fetch_ted_for_supplier_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    n = _n(r.get("n_awards"))
    if n <= 0:
        return
    val = _eur(r.get("ted_value_safe_eur"))
    val_clause = f" worth {val} in summable awarded value" if val != "—" else ""
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Also in the EU register (TED)</div>'
        f'<div class="pr-ted-xref-b">This firm also won <strong>{n:,} EU Official Journal award '
        f"notice{'' if n == 1 else 's'}</strong>{val_clause}, from {_n(r.get('n_buyers')):,} buyers "
        "(2016–2026). A separate register — these are <em>not</em> added to the national total above.</div></div>"
    )

    # The conduit: route the reader to the AUTHORITATIVE notice. The tracker stores a thin
    # slice of each award (winner, buyer, a value-kind tag); the full deliverable, the real
    # framework ceiling and the award criteria live in the EU Official Journal notice itself.
    # The core query also rolls up CLOSELY-NAMED winners (shared brand stem) so a merged/
    # renamed entity's notices surface here; we keep exact-name and variant rows in SEPARATE,
    # labelled sections so a name match is never passed off as a verified same-company claim.
    # Display-only — every link points at the source; nothing here is computed or inferred.
    exact_html, variant_html, total = _ted_notices_sections(supplier_norm)
    if total:
        with st.expander(f"Open the {total:,} authoritative EU notice{'' if total == 1 else 's'} on TED ↗"):
            st.html(_TED_NOTICES_INTRO + exact_html)
            if variant_html:
                st.html(variant_html)


def _render_epa_credentials_panel(company_num) -> None:
    """Cross-register block on a company dossier: the firm's EPA environmental-licence portfolio and
    its EPA enforcement record (matched on CRO company_num). A SEPARATE public register — licences +
    compliance counts only, never juxtaposed with or added to the firm's money figures above.

    No-inference rails: counts are EPA regulatory records, not findings of wrongdoing; the panel says so
    inline, and an un-sampled firm is shown as 'not assessed' so a zero is never read as a clean record."""
    if not _truthy(company_num):
        return
    try:
        cnum = int(float(company_num))
    except (TypeError, ValueError):
        return
    res = fetch_epa_compliance_result(cnum)
    if not res.ok or res.data.empty:
        return  # firm holds no EPA licence (or isn't CRO-matched in the register) — silent absence
    r = res.data.iloc[0]
    n_lic = _n(r.get("n_licences"))
    if n_lic <= 0:
        return
    classes = _esc(r.get("licence_classes") or "")
    cls_clause = f" ({classes})" if classes else ""
    active_clause = (
        " — at least one currently active" if _truthy(r.get("any_active_licence")) else " — none currently active"
    )
    body = (
        f"This firm holds <strong>{n_lic:,} EPA environmental licence{'' if n_lic == 1 else 's'}</strong>"
        f"{cls_clause}{active_clause}. "
    )
    if _truthy(r.get("enforcement_crawled")):
        ev = _n(r.get("n_enforcement_events"))
        if ev > 0:
            inc, comp, nc, op = (
                _n(r.get("n_incident")),
                _n(r.get("n_complaint")),
                _n(r.get("n_non_compliance")),
                _n(r.get("n_open")),
            )
            last = _esc(str(r.get("last_record_date") or "")[:10])
            last_clause = f", most recent {last}" if last else ""
            body += (
                f"The EPA's enforcement record for {'this licence' if n_lic == 1 else 'these licences'} "
                f"shows <strong>{inc:,} incident{'' if inc == 1 else 's'}, {comp:,} complaint"
                f"{'' if comp == 1 else 's'} and {nc:,} non-compliance{'' if nc == 1 else 's'}</strong>, "
                f"of which <strong>{op:,} {'is' if op == 1 else 'are'} still open</strong>{last_clause}. "
                "These are entries in the EPA's regulatory record (incidents, complaints and "
                "non-compliances — a subset of all compliance activity) — <em>not</em> findings of "
                "wrongdoing, and they partly reflect a site's scale and how often it is inspected."
            )
        else:
            body += (
                "The EPA's enforcement record shows no logged incidents, complaints or non-compliances "
                "against these licences."
            )
    else:
        body += (
            "This firm's EPA enforcement record is <em>not assessed</em> here (its licences fall outside "
            "the compliance sample) — absence of counts does not mean a clean record."
        )
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Environmental licences (EPA)</div>'
        f'<div class="pr-ted-xref-b">{body} '
        '<a href="https://www.epa.ie/our-services/licensing/licencesearch/" target="_blank" '
        'rel="noopener">Check the EPA licence register ↗</a></div></div>'
    )


def _ted_notice_li(nr, *, show_name: bool) -> str:
    """One TED notice as a source-linked list item. ``show_name`` leads with the winner's own
    published name (used on variant rows so a name-based grouping is never hidden)."""
    url = _coalesce(getattr(nr, "notice_url", None))
    source = source_link_html(
        url,
        "Source notice",
        aria_label="Open this award notice on TED in a new tab",
    )
    if not source:
        return ""
    date = _coalesce(getattr(nr, "dispatch_date", None))[:10]
    buyer = _buyer_link(getattr(nr, "buyer_name", None))
    is_fw = _coalesce(getattr(nr, "value_kind", None)) == "framework_or_dps_ceiling"
    tag = "framework — shared ceiling, not a payment" if is_fw else "contract award"
    name_pre = f"<strong>{_esc(_coalesce(getattr(nr, 'winner_name', None)))}</strong> — " if show_name else ""
    return (
        f'<li class="pr-notice">{name_pre}{buyer} · {date} '
        f'<span class="pr-notice-tag">{tag}</span> · {source}</li>'
    )


_TED_NOTICES_INTRO = (
    '<p class="pr-cap">The tracker stores a thin slice of each award. Each notice below opens '
    "the full Official Journal record on TED — where the authority publishes what is actually "
    "being built, the real framework ceiling and the award criteria. The source, not our summary.</p>"
)


def _ted_notices_sections(supplier_norm: str) -> tuple[str, str, int]:
    """Build one winner's TED notice list, split into an exact-name ``<ul>`` and a labelled
    closely-named-variant section. Returns ``(exact_html, variant_html, total_count)`` —
    both blocks empty and total 0 when the firm has no linkable notices. Shared by the
    supplier-profile cross-reference panel and the EU-register winner drill-down so the
    name-match honesty copy can never drift between them."""
    notices_res = fetch_ted_notices_for_supplier_result(supplier_norm)
    ndf = notices_res.data if notices_res.ok else pd.DataFrame()
    exact_li = [
        li
        for nr in ndf.itertuples()
        if _truthy(getattr(nr, "is_exact_name", False))
        for li in (_ted_notice_li(nr, show_name=False),)
        if li
    ]
    variant_li = [
        li
        for nr in ndf.itertuples()
        if not _truthy(getattr(nr, "is_exact_name", False))
        for li in (_ted_notice_li(nr, show_name=True),)
        if li
    ]
    exact_html = f'<ul class="pr-notice-list">{"".join(exact_li)}</ul>' if exact_li else ""
    variant_html = ""
    if variant_li:
        variant_html = (
            '<p class="pr-cap" style="margin-top:0.8rem"><strong>Closely-named winners.</strong> '
            f"{len(variant_li):,} further notice{'' if len(variant_li) == 1 else 's'} won under a "
            "<em>similar</em> name (shared name stem — e.g. a renamed or merged company). Grouped by "
            "name only; these <em>may be different legal entities</em> — confirm via the CRO number on "
            "each notice before treating them as one firm.</p>"
            f'<ul class="pr-notice-list">{"".join(variant_li)}</ul>'
        )
    return exact_html, variant_html, len(exact_li) + len(variant_li)


def _render_ted_winner_profile(join_norm: str) -> None:
    """Drill-down for one EU-register (TED) winner: the firm's individual Official Journal
    award notices as line items, each linking to the authoritative source. The EU register's
    counterpart to the national supplier profile — reached from the TED winner ranking, and
    NOT gated on the firm appearing in the national eTenders register (most TED-only winners
    don't). A separate register, never summed with the national award totals."""
    if back_button("← Back to procurement", key="prtedwin"):
        _return_to_browse("wins", register="ted")

    res = fetch_ted_for_supplier_result(join_norm)
    row = res.data.iloc[0] if (res.ok and not res.data.empty) else None
    if row is None:
        if not res.ok:
            empty_state(
                "EU register isn't available right now",
                "The TED views couldn't be loaded — a source/pipeline issue, not an empty result.",
            )
        else:
            empty_state("EU winner not found", "That link didn't match a firm in the EU register. Use Back to return.")
        return

    name = _esc(_coalesce(row.get("winner_name"))) or "—"
    n_awards, n_buyers = _n(row.get("n_awards")), _n(row.get("n_buyers"))
    sub = f"{_awards_word(n_awards)} from {n_buyers:,} EU public buyer{'s' if n_buyers != 1 else ''} · 2016–2026"
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">EU REGISTER · TED</div>'
        f'<h1 class="pr-prof-name">{name}</h1><div class="pr-prof-sub">{sub}</div></div>'
    )
    pills = [
        p
        for p in (
            _ted_value_pill(row.get("ted_value_safe_eur")),
            _cro_pill_from(row.get("cro_company_num"), row.get("cro_company_status")),
        )
        if p
    ]
    if pills:
        st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    st.caption(
        "EU Official Journal (TED) award notices won by this firm — a separate register from the national "
        "eTenders data, and never summed with it. Award notices, not payments; framework rows are shared "
        "ceilings, not money paid."
    )
    exact_html, variant_html, total = _ted_notices_sections(join_norm)
    if not total:
        empty_state(
            "No linkable notices", "This firm is in the EU ranking but none of its notices carry a source link."
        )
    else:
        st.html(_TED_NOTICES_INTRO + exact_html)
        if variant_html:
            st.html(variant_html)

    # Same firm's lot-level competition context (TED 2024+), shown with its no-inference caveat.
    _render_supplier_competition_panel(join_norm)

    # If the firm is ALSO on the national register, route to its full cross-register dossier.
    sup = fetch_supplier_summary_result(limit=None)
    if sup.ok and not sup.data.empty and bool((sup.data["supplier_norm"] == join_norm).any()):
        st.html(
            f'<div style="margin:1rem 0"><a class="dt-entity-cta" href="{_esc(company_profile_url(join_norm))}" '
            'target="_self">See this firm’s full public-money dossier (national awards + payments) →</a></div>'
        )

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, the EU '
        'Official Journal of public procurement (<a href="https://ted.europa.eu" target="_blank" '
        'rel="noopener">ted.europa.eu ↗</a>), winners matched to the Companies Registration Office. '
        "Award notices, not payments; a separate register from the national eTenders data — never summed.</div>"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Supplier-profile context panels (shared by the in-page ?supplier= profile and the
# /company dossier). All pre-aggregated in the registered views; factual structure
# signals with their caveats — never verdicts (no-inference rule).
# ──────────────────────────────────────────────────────────────────────────────
def _render_supplier_competition_panel(supplier_norm: str) -> None:
    """Lot-level single-bid context for one firm (TED 2024+, sole-winner notices only)
    against the national baseline — OpenTender-style competition context on the profile.
    Omitted below 5 bid-counted lots (a rate over 2 lots would mislead)."""
    res = fetch_supplier_single_bid_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    lots, single = _n(r.get("n_lots_with_bidcount")), _n(r.get("n_single_bid_lots"))
    if lots < 5:
        return
    pct = r.get("single_bid_lot_pct")
    base_res = fetch_single_bid_baseline_result()
    base_clause = ""
    if base_res.ok and not base_res.data.empty:
        b = base_res.data.iloc[0].get("single_bid_lot_pct")
        if b is not None:
            base_clause = f" The national rate across all EU-notice lots is <strong>{float(b):g}%</strong>."
    excl = _n(r.get("n_multi_winner_notices_excluded"))
    excl_clause = (
        f" ({excl:,} multi-winner notice{'' if excl == 1 else 's'} excluded — their lot counts "
        "can't be attributed to one winner.)"
        if excl
        else ""
    )
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Competition context (EU notices, 2024+)</div>'
        f'<div class="pr-ted-xref-b">Of the <strong>{lots:,}</strong> contract lots this firm won outright '
        f"that report a bid count, <strong>{single:,}</strong> drew a single bid "
        f"(<strong>{float(pct):g}%</strong>).{base_clause} A single bid is recorded fact, often wholly "
        f"legitimate (a niche specialism, genuine urgency) — context to look at, never evidence of "
        f"wrongdoing.{excl_clause}</div></div>"
    )


def _render_supplier_relationships_panel(supplier_norm: str, *, cross_page: bool = False) -> None:
    """The firm's repeat buyers (distinct-years spans) + its top-buyer share — structure
    facts from the awards register. Central-purchasing buyers (OGP / EPS) are badged:
    a streak with them is repeated central-framework success, not a bilateral relationship.

    ``cross_page=True`` (the company dossier) makes each buyer name a cross-page link to
    its procurement dossier — closing the supplier↔buyer loop; on the Procurement page
    itself the links stay relative for a soft rerun."""
    inc = fetch_incumbency_for_supplier_result(supplier_norm)
    idf = inc.data if inc.ok else pd.DataFrame()
    idf = idf[idf["n_awards"] >= 2] if not idf.empty else idf
    dep = fetch_dependency_for_supplier_result(supplier_norm)
    drow = dep.data.iloc[0] if (dep.ok and not dep.data.empty) else None
    if idf.empty and drow is None:
        return

    parts = []
    if drow is not None and _n(drow.get("total_awards")) >= 5:
        share = drow.get("top_authority_share_pct")
        cp = (
            " — the Office of Government Procurement buys on behalf of the whole public service, "
            "so this reflects central frameworks, not one bilateral customer"
            if _truthy(drow.get("top_authority_is_central_purchasing"))
            else ""
        )
        parts.append(
            f"<strong>{_n(drow.get('awards_from_top_authority')):,}</strong> of its "
            f"<strong>{_n(drow.get('total_awards')):,}</strong> recorded awards "
            f"({float(share):g}%) came from "
            f"<strong>{_authority_link(drow.get('top_authority'), cross_page=cross_page)}</strong>{cp}."
        )
    if parts:
        st.html(
            '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Buyer relationships</div>'
            f'<div class="pr-ted-xref-b">{" ".join(parts)} A repeat relationship is a structure fact — '
            "durable incumbency is often the procurement system working (framework renewals, "
            "specialist capability).</div></div>"
        )
    if not idf.empty and len(idf) >= 1:
        rows = []
        for r in idf.itertuples():
            yrs = _n(r.n_distinct_years)
            span = (
                f"{_n(r.first_year)}–{_n(r.last_year)}"
                if _n(r.first_year) != _n(r.last_year)
                else str(_n(r.first_year))
            )
            badge = (
                ' <span class="pr-pill pr-pill-lob">central purchasing body</span>'
                if _truthy(r.authority_is_central_purchasing)
                else ""
            )
            rows.append(
                f'<div class="pr-award"><div class="pr-award-body">'
                f'<div class="pr-award-auth">{_authority_link(r.contracting_authority, cross_page=cross_page)}{badge}</div>'
                f'<div class="pr-award-meta">{_awards_word(_n(r.n_awards))} across '
                f"{yrs:,} year{'s' if yrs != 1 else ''} ({span})</div></div></div>"
            )
        with st.expander(f"Repeat buyers ({len(idf):,})"):
            st.html("".join(rows))


def _render_supplier_call_offs_panel(supplier_norm: str, *, cross_page: bool = False) -> None:
    """The firm's call-off awards (drawdowns under a framework/DPS) with the parent
    agreement named where its notice exists in the corpus — the framework nesting, made
    visible. An unresolved parent is disclosed, never hidden; a parent ceiling is context,
    never added to the call-off's own value. ``cross_page=True`` links each buyer name to
    its procurement dossier (company-dossier reuse)."""
    # Deferred: .profiles imports panels from this module, so this reverse edge is call-time.
    from .profiles import _award_value_html

    res = fetch_call_offs_for_supplier_result(supplier_norm)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        return
    resolved = df[df["parent_in_corpus"] == True]  # noqa: E712 — pandas mask
    n_unresolved = len(df) - len(resolved)
    with st.expander(f"Framework drawdowns ({len(df):,} call-offs)"):
        st.html(
            '<p class="pr-cap">These awards are <strong>call-offs</strong> — drawdowns under a '
            "framework or dynamic purchasing system. Where the parent agreement's notice is in the "
            "published corpus it is named below; its ceiling is the framework's spending limit, "
            "never this call-off's value and never added to it.</p>"
        )
        rows = []
        for r in resolved.head(15).itertuples():
            parent_bits = [f"under agreement {_esc(r.parent_agreement_id)}"]
            pv = _eur(getattr(r, "parent_value_eur", None))
            if pv != "—" and _coalesce(getattr(r, "parent_value_kind", None)) == "framework_or_dps_ceiling":
                parent_bits.append(f"ceiling {pv}")
            n_ps = _n(getattr(r, "parent_n_suppliers", None))
            if n_ps > 1:
                parent_bits.append(f"{n_ps:,} suppliers on the framework")
            rows.append(
                f'<div class="pr-award"><div class="pr-award-body">'
                f'<div class="pr-award-auth">{_authority_link(r.contracting_authority, cross_page=cross_page)}</div>'
                f'<div class="pr-award-meta">{fmt_civic_date(getattr(r, "award_date", None))} · '
                f"{' · '.join(parent_bits)}</div></div>{_award_value_html(r)}</div>"
            )
        if rows:
            st.html("".join(rows))
        if n_unresolved:
            st.html(
                f'<p class="pr-cap">{n_unresolved:,} further call-off{"" if n_unresolved == 1 else "s"} '
                "name a parent agreement whose own notice is <strong>not in the published corpus</strong> — "
                "a coverage gap in the source register, disclosed rather than hidden.</p>"
            )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Tender pipeline (TED cn-standard) — a THIRD grain (pre-award), never summed.
# ──────────────────────────────────────────────────────────────────────────────
def _render_ted_tenders() -> None:
    stats_res = fetch_ted_tenders_stats_result()
    if not stats_res.ok or stats_res.data.empty:
        empty_state(
            "Tender-pipeline data isn't available right now",
            "The TED competition-notice view couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    s = stats_res.data.iloc[0]
    span = f"{_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
    st.html(
        '<div class="pr-caveat"><strong>The tender pipeline — opportunities, not awards.</strong> '
        f"{_n(s.get('n_notices')):,} EU-journal <em>competition</em> notices ({span}) from "
        f"{_n(s.get('n_buyers')):,} Irish public buyers — what is being put out to tender. The estimated "
        "value shown is a <em>buyer estimate recorded before any award</em>: never a contract value, never "
        "a payment, and never added to the award or payment figures elsewhere on this page.</div>"
    )
    # Two facets, side by side: an open-by-deadline DATE gate, and a SECTOR (CPV division) filter.
    # Unlike the national feed above, TED notices carry a CPV code, so sector filtering is possible here.
    fcol1, fcol2 = st.columns([1, 1])
    with fcol1:
        only_open = st.toggle(
            "Only tenders still open by deadline",
            value=False,
            key="pr_ted_open",
            help=f"{_n(s.get('n_still_open')):,} of {_n(s.get('n_notices')):,} have a submission deadline still in the future.",
        )
    sector = None
    with fcol2:
        # Sector option list carries a per-division count (the "facet counts in parentheses"
        # convention); counts track the open toggle so they match the list below.
        sectors_res = fetch_ted_tender_sectors_result(only_open=only_open)
        sec_df = sectors_res.data if sectors_res.ok else pd.DataFrame()
        if not sec_df.empty:
            label_to_sector = {f"{r.sector} ({int(r.n):,})": r.sector for r in sec_df.itertuples()}
            choice = st.selectbox(
                "Sector (CPV division)",
                ["All sectors", *label_to_sector.keys()],
                index=0,
                key="pr_ted_sector",
            )
            sector = label_to_sector.get(choice)
    res = fetch_ted_tenders_result(only_open=only_open, limit=_TOP, sector=sector)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        if sector:
            empty_state("No tenders in that sector", f"No competition notice in “{sector}” for this filter.")
        else:
            empty_state(
                "No tenders", "No still-open competition notice." if only_open else "The view returned no rows."
            )
        return
    sector_label = f" in {sector}" if sector else ""
    st.caption(
        f"{len(df):,} most-recent competition notices{' still open' if only_open else ''}{sector_label}. "
        "Estimated value is a pre-award buyer estimate — not an award and not a payment. "
        "Click a notice to open the full tender on TED."
    )
    cards = []
    for r in df.head(_TOP).itertuples():
        meta_parts = [
            _esc(_coalesce(getattr(r, "cpv_division", None))),
            _esc(_coalesce(getattr(r, "procedure_type", None))),
        ]
        dl = _coalesce(getattr(r, "submission_deadline", None))
        if dl:
            meta_parts.append(f"deadline {fmt_civic_date(dl)}")
        meta = " · ".join(p for p in meta_parts if p)
        pills = []
        ev = _eur(getattr(r, "estimated_value_eur", None))
        if ev != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{ev} est. value</span>')
        if _truthy(getattr(r, "is_still_open", None)):
            pills.append('<span class="pr-pill pr-pill-lob">still open</span>')
        if _truthy(getattr(r, "is_uncompetitive_procedure", None)):
            pills.append('<span class="pr-pill pr-pill-lob">no open call</span>')
        buyer = _coalesce(getattr(r, "buyer_name", None))
        url = _coalesce(getattr(r, "notice_url", None))
        source = source_link_html(
            url,
            "Source notice",
            aria_label=f"Open the EU tender notice from {buyer or 'this buyer'} on TED",
        )
        name_html = f"<span>{_buyer_link(buyer)}</span>"
        if source:
            name_html += f'<span class="pr-sub">{source}</span>'
        cards.append(_card(name_html, meta, pills))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal '
        'competition notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>). '
        "Pre-award opportunities; estimated values are buyer estimates — never awards or payments, and never summed.</div>"
    )
