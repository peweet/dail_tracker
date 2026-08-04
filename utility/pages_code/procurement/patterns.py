from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    fetch_bid_signal_result,
    fetch_competition_by_cpv_result,
    fetch_dependency_top_result,
    fetch_entity_search_result,
    fetch_incumbency_top_result,
    fetch_new_entrants_result,
    fetch_quarter_profile_top_result,
    fetch_quarter_totals_result,
    fetch_sector_breadth_top_result,
    fetch_single_bid_baseline_result,
    fetch_single_bid_notices_for_cpv_result,
    fetch_supplier_concentration_result,
    fetch_supplier_summary_result,
)
from ui.entity_links import (
    company_link_html,
    company_profile_url,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    finding_lede,
    text_search_mask,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _eur,
    _awards_word,
    _supplier_href,
    _authority_href,
    _authority_link,
    _cpv_href,
    _single_bid_cpv_href,
    _paid_supplier_href,
    _yr_axis,
    _return_to_browse,
    _card,
    _value_pill,
)

from .payments import _paid_pill, _paid_publisher_href



def _entity_search_hero() -> None:
    """Search-first entry (USAspending lesson: people type a NAME first). One box over
    suppliers + public bodies + categories — the reader never needs to know which register
    answers their question. DISPLAY-ONLY name filter over the pre-built search corpus
    (v_procurement_entity_search); renders nothing until the user types."""
    res = fetch_entity_search_result()
    if not res.ok or res.data.empty:
        return
    q = st.text_input(
        "Search procurement",
        placeholder="Search a company, public body or category…",
        key="pr_hero_q",
        label_visibility="collapsed",
    )
    qs = (q or "").strip()
    if not qs:
        return
    df = res.data
    hits = df[text_search_mask(df, qs, ["display_name"])].head(12)
    if hits.empty:
        empty_state("No matches", "Try a shorter term — names are matched as published.")
        return
    # The two registers carry distinct kind labels so a reader can tell an award-ceiling result
    # (eTenders) from a realised-payment result (a body's own >€20k list) at a glance.
    kind_label = {
        "supplier": "COMPANY",
        "authority": "PUBLIC BODY",
        "cpv": "CATEGORY",
        "paid_supplier": "PAID CONTRACTOR",
        "paid_body": "PUBLIC BODY · PAYMENTS",
    }
    aria = {
        "supplier": "Open the public-money dossier of",
        "authority": "View the awards made by",
        "cpv": "View the awards in category",
        "paid_supplier": "Open the published payments of",
        "paid_body": "View the published payments of",
    }
    cards = []
    for r in hits.itertuples():
        kind = str(r.entity_kind)
        nc = _n(r.n_counterparties)
        is_paid = kind in ("paid_supplier", "paid_body")
        if is_paid:
            # Payments grain: lines, not awards. Tier (SPENT/COMMITTED) rides the row so the
            # money pill + deep-link are tier-correct (paid vs ordered, the right dossier).
            tier = (_coalesce(getattr(r, "paid_tier", None)) or "SPENT").upper()
            n = _n(r.n_records)
            meta = f"{n:,} published line{'s' if n != 1 else ''}"
            meta += (
                f" · {nc:,} public bod{'ies' if nc != 1 else 'y'}"
                if kind == "paid_supplier"
                else f" · {nc:,} supplier{'s' if nc != 1 else ''}"
            )
        else:
            meta = _awards_word(_n(r.n_records))
            meta += (
                f" · {nc:,} public bod{'ies' if nc != 1 else 'y'}"
                if kind == "supplier"
                else f" · {nc:,} supplier{'s' if nc != 1 else ''}"
            )
        pills = [f'<span class="pr-pill pr-pill-lob">{kind_label.get(kind, kind)}</span>']
        if _eur(r.awarded_value_safe_eur) != "—":
            pills.append(_value_pill(r.awarded_value_safe_eur))
        # Paid figure is a DIFFERENT grain (realised payments) — its own label, never merged.
        if is_paid and _eur(getattr(r, "paid_safe_eur", None)) != "—":
            pills.append(_paid_pill(r.paid_safe_eur, tier))
        elif kind == "supplier" and _eur(getattr(r, "paid_safe_eur", None)) != "—":
            pills.append(f'<span class="pr-pill pr-pill-val">{_eur(r.paid_safe_eur)} paid (where published)</span>')
        if _truthy(getattr(r, "on_lobbying_register", None)):
            pills.append('<span class="pr-pill pr-pill-lob">also on lobbying register</span>')
        if kind == "paid_supplier":
            href = _paid_supplier_href(r.url_key, tier)
        elif kind == "paid_body":
            href = _paid_publisher_href(r.url_key, tier)
        elif kind == "authority":
            href = _authority_href(r.url_key)
        elif kind == "cpv":
            href = _cpv_href(r.url_key)
        else:
            href = _supplier_href(r.url_key)
        inner = _card(f"<span>{_esc(r.display_name)}</span>", meta, pills)
        cards.append(clickable_card_link(href=href, inner_html=inner, aria_label=f"{aria[kind]} {r.display_name}"))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.caption(
        "Two registers in one search: an **award** (eTenders/TED) is a contract ceiling at the point "
        "of award; a **paid** result is a separate, later stage (a public body's own >€20k payment "
        "list). The same firm can appear under both — they are different stages and never added together."
    )


# ──────────────────────────────────────────────────────────────────────────────
# Tab: Patterns — factual structure signals from the derived views
# (doc/PROCUREMENT_NUGGETS.md). Every card is an observable shape in the public
# record with its caveat attached; prompts to look, never verdicts (no-inference).
# ──────────────────────────────────────────────────────────────────────────────
def _render_single_bid_cpv(cpv_division: str) -> None:
    """Drill-down for one market's single-bid award notices (reached from the Patterns single-bid
    card). Each notice opens the authoritative EU Official Journal record. A single bid is a
    recorded fact — often wholly legitimate (a niche market with few capable suppliers) — never
    presented as evidence of wrongdoing (no-inference rail)."""
    if back_button("← Back to procurement", key="prsbcpv"):
        _return_to_browse("patterns")

    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">SINGLE-BID NOTICES · EU NOTICES 2024+</div>'
        f'<h1 class="pr-prof-name">{_esc(cpv_division)}</h1>'
        '<div class="pr-prof-sub">Contract-award notices in this market that drew a single tender</div></div>'
    )

    res = fetch_single_bid_notices_for_cpv_result(cpv_division)
    if not res.ok:
        empty_state("Competition data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data
    if df.empty:
        empty_state(
            "No single-bid notices in this market",
            "No 2024+ EU award notice in this CPV division recorded a single tender.",
        )
        return
    st.caption(
        f"{len(df):,} EU Official Journal award notice{'s' if len(df) != 1 else ''} in “{_esc(cpv_division)}” that "
        "received a single tender (2024+ eForms, soonest first). A single bid is a matter of record — niche "
        "markets often have few capable suppliers — and is never, on its own, evidence of wrongdoing. "
        "Click a notice to open the authoritative record on TED."
    )
    cards = []
    for r in df.itertuples():
        date = _coalesce(getattr(r, "dispatch_date", None))[:10]
        winner = _esc(_coalesce(getattr(r, "winner_name", None))) or "—"
        meta_parts = [_esc(_coalesce(getattr(r, "buyer_name", None)))]
        if date:
            meta_parts.append(date)
        meta = " · ".join(p for p in meta_parts if p)
        pills = ['<span class="pr-pill pr-pill-lob">single bid</span>']
        if _coalesce(getattr(r, "value_kind", None)) == "framework_or_dps_ceiling":
            pills.append('<span class="pr-pill">framework ceiling</span>')
        inner = _card(f"<span>{winner}</span>", meta, pills)
        url = _coalesce(getattr(r, "notice_url", None))
        if url.startswith("http"):
            cards.append(
                clickable_card_link(
                    href=url,
                    inner_html=inner,
                    aria_label=f"Open the EU award notice won by {winner} on TED",
                    target="_blank",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> TED — Tenders Electronic Daily, EU Official Journal award '
        'notices (<a href="https://ted.europa.eu" target="_blank" rel="noopener">ted.europa.eu ↗</a>), eForms '
        "single-tender field (2024+). A single bid is recorded fact, not a verdict.</div>"
    )


def _render_patterns() -> None:
    st.html(
        '<div class="pr-caveat"><strong>Patterns are facts about the register, not findings about '
        "anyone.</strong> Each panel below shows a structure in the published record — how often one "
        "bid wins, how long the same firms keep winning, when orders are placed. Any of these shapes "
        "can be wholly legitimate; they are starting points for a reader's own questions.</div>"
    )

    # 1. Single-bid by market (lot-level, TED 2024+)
    comp = fetch_competition_by_cpv_result()
    if comp.ok and not comp.data.empty:
        base = fetch_single_bid_baseline_result()
        base_pct = (
            float(base.data.iloc[0].get("single_bid_lot_pct"))
            if base.ok and not base.data.empty and base.data.iloc[0].get("single_bid_lot_pct") is not None
            else None
        )
        st.html('<h2 class="pr-section-h">How often does one bid win, by market?</h2>')
        cap = (
            "Share of contract lots that drew a single bid, per category (EU award notices, 2024+; "
            "lots with a reported bid count)."
        )
        if base_pct is not None:
            cap += f" National rate: {base_pct:g}%."
        cap += " A single bid is often legitimate — niche markets have few capable suppliers."
        cap += " Click a market to see the individual single-bid notices inside it."
        st.caption(cap)
        cards = []
        for r in comp.data.head(12).itertuples():
            pct = r.single_bid_lot_pct
            meta = (
                f"{_n(r.n_single_bid_lots):,} of {_n(r.n_lots_with_bidcount):,} lots single-bid · "
                f"{_n(r.n_buyers):,} buyers"
            )
            pill = f'<span class="pr-pill pr-pill-val">{float(pct):g}% single-bid</span>' if pct is not None else ""
            inner = _card(f"<span>{_esc(r.cpv_division)}</span>", meta, [pill] if pill else [])
            cards.append(
                clickable_card_link(
                    href=_single_bid_cpv_href(r.cpv_division),
                    inner_html=inner,
                    aria_label=f"See the single-bid award notices in {r.cpv_division}",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 2. New entrants per year
    ne = fetch_new_entrants_result()
    if ne.ok and not ne.data.empty:
        shown = ne.data[ne.data["is_left_censored"] == False]  # noqa: E712 — pandas mask
        if len(shown) > 1:
            st.html('<h2 class="pr-section-h">Who gets in — first-time winners</h2>')
            first_y, last_y = _n(shown.iloc[0].get("year")), _n(shown.iloc[-1].get("year"))
            first_pct, last_pct = (
                shown.iloc[0].get("pct_awards_to_new_entrants"),
                shown.iloc[-1].get("pct_awards_to_new_entrants"),
            )
            st.caption(
                f"Share of each year's contract awards won by suppliers with no earlier award in the register: "
                f"{float(first_pct):g}% in {first_y} → {float(last_pct):g}% in {last_y}. A falling entry rate is a "
                "market shape — consistent with consolidation, central frameworks, or a maturing register; the "
                "register only began in 2013, so earlier years are not comparable and are omitted."
            )
            st.bar_chart(
                _yr_axis(shown),
                x="year",
                y="pct_awards_to_new_entrants",
                x_label="Year",
                y_label="New-entrant share (%)",
                height=200,
                color="#9c5b2e",
            )

    # 3. Longest-running relationships
    inc = fetch_incumbency_top_result()
    if inc.ok and not inc.data.empty:
        st.html('<h2 class="pr-section-h">The longest-running winners</h2>')
        st.caption(
            "Supplier–buyer pairs with awards in six or more different years. Durable incumbency is often "
            "the system working (framework renewals, specialist capability) — a record of persistence, not "
            "an accusation. Office of Government Procurement rows reflect central frameworks for the whole "
            "public service."
        )
        cards = []
        for r in inc.data.head(12).itertuples():
            yrs = _n(r.n_distinct_years)
            badge = (
                '<span class="pr-pill pr-pill-lob">central purchasing body</span>'
                if _truthy(r.authority_is_central_purchasing)
                else ""
            )
            supplier = company_link_html(r.supplier_norm, r.supplier, css_class="pr-auth-link")
            buyer = _authority_link(r.contracting_authority)
            name_html = (
                f"<span>{supplier}</span>"
                f'<span class="pr-sub">{_awards_word(_n(r.n_awards))} from {buyer}</span>'
            )
            meta = f"{_n(r.first_year)}–{_n(r.last_year)}"
            pills = [f'<span class="pr-pill pr-pill-val">{yrs} winning years</span>'] + ([badge] if badge else [])
            cards.append(_card(name_html, meta, pills))
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 4. One-buyer suppliers (central purchasing excluded in the query)
    dep = fetch_dependency_top_result()
    if dep.ok and not dep.data.empty:
        st.html('<h2 class="pr-section-h">Suppliers with one main buyer</h2>')
        st.caption(
            "Firms that won at least 80% of their recorded awards (10+) from a single public body. "
            "A specialist serving the one body that buys its specialism is the market working — this is "
            "a structure fact, not a risk score. Central purchasing bodies are excluded (winning via OGP "
            "frameworks is how the system is designed)."
        )
        cards = []
        for r in dep.data.head(12).itertuples():
            supplier = company_link_html(r.supplier_norm, r.supplier, css_class="pr-auth-link")
            buyer = _authority_link(r.top_authority)
            name_html = (
                f"<span>{supplier}</span>"
                f'<span class="pr-sub">{_n(r.awards_from_top_authority):,} of '
                f"{_n(r.total_awards):,} awards from {buyer}</span>"
            )
            pills = [f'<span class="pr-pill pr-pill-val">{float(r.top_authority_share_pct):g}% one buyer</span>']
            cards.append(_card(name_html, "", pills))
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 5. Year-end ordering shape (COMMITTED tier only)
    qt = fetch_quarter_totals_result()
    if qt.ok and len(qt.data) == 4:
        st.html('<h2 class="pr-section-h">When orders are placed</h2>')
        st.caption(
            "Purchase-order lines by quarter across all publishing bodies (ordered tier only — never mixed "
            "with payments). A year-end rise is a known public-finance seasonality; invoicing cycles, grant "
            "schedules and works seasons all contribute. The shape is the fact; the reason is not asserted."
        )
        st.bar_chart(
            qt.data, x="quarter", y="n_lines", x_label="Quarter", y_label="Order lines", height=200, color="#9c5b2e"
        )
        skew = fetch_quarter_profile_top_result()
        if skew.ok and not skew.data.empty:
            cards = []
            for r in skew.data.head(6).itertuples():
                meta = f"{_n(r.n_lines):,} of its order lines fall in Q4"
                pills = [f'<span class="pr-pill pr-pill-val">{float(r.pct_of_publisher_lines):g}% in Q4</span>']
                inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, pills)
                cards.append(
                    clickable_card_link(
                        href=_paid_publisher_href(r.publisher_name, "COMMITTED"),
                        inner_html=inner,
                        aria_label=f"View the suppliers ordered by {r.publisher_name}",
                    )
                )
            st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    # 6. Sector breadth (paid corpus)
    sb = fetch_sector_breadth_top_result()
    if sb.ok and not sb.data.empty:
        st.html('<h2 class="pr-section-h">Firms paid across the most of the State</h2>')
        st.caption(
            "Suppliers appearing in the published payment lists of bodies across the most public-service "
            "sectors (health, councils, justice, …) — reach, as published. Grouped by the published name; "
            "totals are the usual indicative floors, never audited sums."
        )
        cards = []
        for r in sb.data.head(6).itertuples():
            meta = f"{_n(r.n_sectors)} sectors · {_n(r.n_publishers):,} public bodies"
            pills = []
            if _eur(getattr(r, "paid_safe_eur", None)) != "—":
                pills.append(f'<span class="pr-pill pr-pill-val">{_eur(r.paid_safe_eur)} paid (floor)</span>')
            inner = _card(f"<span>{_esc(r.supplier_normalised)}</span>", meta, pills)
            # Forward edge: these cards were a dead-end though the row carries the
            # supplier_normalised key — link each to its canonical /company dossier.
            norm = _coalesce(getattr(r, "supplier_normalised", None))
            cards.append(
                clickable_card_link(
                    href=company_profile_url(str(norm)),
                    inner_html=inner,
                    aria_label=f"View company dossier for {r.supplier_normalised}",
                )
                if norm
                else inner
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')

    st.html(
        '<div class="pr-foot"><strong>Method:</strong> every panel reads a registered, documented view '
        "(doc/PROCUREMENT_NUGGETS.md) over the same published registers as the rest of this page — "
        "eTenders awards, EU Official Journal notices, and public bodies' own payment lists. Counts and "
        "shares only within one register and one grain; nothing here mixes award ceilings with payments.</div>"
    )


def _page_lede(stats) -> None:
    """The page's opening findings (findings-not-filters,
    doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md). DISPLAY-ONLY: the top-winner row,
    the concentration row and the corpus counts all arrive pre-aggregated from
    the registered views; this assembles sentences and renders."""
    sentences: list[str] = []

    top = fetch_supplier_summary_result(limit=1)
    min_y, max_y = _n(stats.get("min_year")), _n(stats.get("max_year"))
    span = f"{min_y}–{max_y}" if min_y and max_y else "recent years"
    if top.ok and not top.data.empty:
        t = top.data.iloc[0]
        sentences.append(
            f"{_esc(t.get('supplier'))} has won more public contracts than any other firm — "
            f"<strong>{_n(t.get('n_awards')):,}</strong> since {min_y or 'records began'}, "
            f"from <strong>{_n(t.get('n_authorities')):,}</strong> public bodies."
        )

    con = fetch_supplier_concentration_result()
    if con.ok and not con.data.empty:
        c = con.data.iloc[0]
        share, n_sup_c = c.get("top_n_share_pct"), _n(c.get("n_suppliers"))
        if share is not None and n_sup_c:
            shape = "a long tail, not a closed shop" if float(share) < 25 else "a concentrated market"
            sentences.append(
                f"Contract-winning is {shape}: across <strong>{n_sup_c:,}</strong> companies, "
                f"the top {_n(c.get('top_n'))} firms hold <strong>{float(share):g}%</strong> "
                f"of all {_n(c.get('total_awards')):,} awards."
            )

    sentences.append(
        f"<strong>{_n(stats.get('n_suppliers')):,}</strong> suppliers and "
        f"<strong>{_n(stats.get('n_authorities')):,}</strong> public bodies appear on the "
        f"register, {_esc(span)}. Rankings count awards — the trustworthy metric — "
        "never naive euro totals."
    )
    finding_lede(sentences)


def _data_completeness_body() -> None:
    """The "how complete is this data?" honesty note, WITHOUT its own expander.

    Static, sourced editorial prose (no live metric — the firewall keeps computation in
    the view layer); the coverage figures are documented point-in-time estimates from the
    2026-06-08 coverage analysis, stated with their caveats so a reader never mistakes
    this corpus for the whole of public spending. The caller nests it inside the single
    "About this data" expander (2026-07-20 clutter pass) — it used to open its own, one
    of three collapsed grey bars stacked between the hero and the section picker.
    """
    st.markdown("**How complete is this data?**")
    st.markdown(
        "**Short answer: this is what public bodies publish — not the whole picture.** "
        "Treat every total here as a *floor* (at least this much, from the records we can see), "
        "never an audited figure.\n\n"
        "- **Awards** (eTenders, TED) name almost every public buyer (~1,950 bodies), but only a "
        "fraction of the euro value can be summed — most contracts fall below the publication "
        "threshold or run through frameworks whose ceilings aren't real spend.\n"
        "- **Money actually paid** comes from the lists bodies publish themselves (mostly over "
        "€20,000, under the FOI Act 2014 s.8 model publication scheme — origin Circular FIN 07/12 — "
        "though some bodies use a different threshold and others publish voluntarily) — and "
        "only about **1 in 40 public buyers (~3%)** does so. Against the State's estimated "
        "**€15–22 billion a year** of procurement, what's traceable here works out at roughly "
        "**7% of the money spent overall** — rising to the **mid-teens (%) in recent years** as more "
        "bodies began publishing, and under 2% before 2021. So on the order of **90%+ of actual "
        "spend is not yet visible** here.\n"
        "- The three records **aren't linked**: a contract's notice, its award, and the eventual "
        "payments sit in separate registers with no shared key, so *awarded* and *paid* can never "
        "be reconciled for the same deal.\n\n"
        "For scale, Ukraine's **Prozorro** publishes 100% of public procurement, full lifecycle, in "
        "one system — the standard Ireland has no equivalent of. National-spend estimate: OECD / "
        "US trade.gov country guide."
    )


def _lifecycle_body() -> None:
    """ "How public money moves" — names the four realisation tiers (PLANNED → AWARDED →
    COMMITTED → SPENT) the page's sections already embody, so a reader sees one contract's
    life rather than four unrelated lists.

    A collapsed, NON-clickable explainer — the section bar below is the page's single
    navigation. This strip used to render the same four ?tab= links a second time: the bold,
    hover-lifting cards read as "the controls", but every click only jumped to the section
    bar directly beneath them (the "nice tabs that don't go anywhere" the reader hit). It is
    now a teaching diagram on demand — static cells, no links, no hover — opened only by a
    reader who wants the model. AFS is shown as a sibling measure OFF the line (different
    grain — budget by service division, never summed with the contract stages).
    Surfacing-only: static copy, no data read, no aggregation."""
    # (plain-language question, tier word, reliability caveat, accent) — no ?tab link: this is
    # an explainer, not navigation, so the section bar is the one place a reader picks a stage.
    stages = [
        ("What's being bought", "Planned", "Open tenders — the pipeline, before any contract is awarded", "#6b7a8a"),
        ("Who won it", "Awarded", "A value at the point of award — a ceiling, not money paid", "#b8862b"),
        ("What was ordered", "Committed", "Purchase orders placed against a contract", "#9c5b2e"),
        ("What was actually paid", "Spent", "Payments out to named suppliers — the real money", "#2f7d5b"),
    ]
    cells: list[str] = []
    for i, (question, tier, note, accent) in enumerate(stages):
        if i:
            cells.append('<span class="pr-lc-arrow">→</span>')
        cells.append(
            f'<div class="pr-lc-stage pr-lc-stage--static" style="--lc-accent:{accent}">'
            f'<span class="pr-lc-tier">{i + 1} · {_esc(tier)}</span>'
            f'<span class="pr-lc-q">{_esc(question)}</span>'
            f'<span class="pr-lc-note">{_esc(note)}</span>'
            "</div>"
        )
    # No expander of its own: the caller nests this inside the single "About this
    # data" expander (2026-07-20 clutter pass).
    st.markdown("**How public money moves**")
    st.html(
        '<div class="pr-lc">'
        '<div class="pr-lc-head">'
        "Four stages of one contract's life — each shown in its own section below, and never "
        "added together (they sit in different registers with no shared key).</div>"
        f'<div class="pr-lc-track">{"".join(cells)}</div>'
        '<div class="pr-lc-sibling"><strong>Measured separately — audited accounts (AFS).</strong> '
        "A council's budget by service division, on a different basis entirely. It lives in each "
        "council's dossier under <em>Who actually gets paid?</em> and is never added to the stages above."
        "</div>"
        "</div>"
    )


_BIDSIG_CSS = """
<style>
/* EXPERIMENTAL (local-only) — scoped styles for the "Should I bid?" signal cards.
   Kept inline so the whole feature is self-contained and deletable in one pass; promote
   into shared_css.py (bs-* family) if/when the feature graduates. */
.bs-card{background:#ffffff;border:1px solid var(--border,#e7e2d8);border-radius:12px;
  padding:16px 18px;margin-bottom:14px}
.bs-head{display:flex;align-items:baseline;gap:10px;flex-wrap:wrap;margin-bottom:4px}
.bs-name{font-weight:650;font-size:1.02rem;color:var(--ink,#1d1d1b)}
.bs-code{font:600 .72rem ui-monospace,monospace;color:#6b6459;background:#f4f1ea;
  padding:1px 7px;border-radius:6px}
.bs-tot{margin-left:auto;font-size:.8rem;color:#7a7367}
.bs-band-cap{font-size:.82rem;color:#57514a;margin:8px 0 4px}
.bs-track{position:relative;height:24px;background:#f4f1ea;border-radius:7px;margin:5px 0 2px}
.bs-fill{position:absolute;top:0;bottom:0;background:#cfe3d6;border-radius:7px}
.bs-med{position:absolute;top:-3px;bottom:-3px;width:3px;background:#2f7d54;border-radius:2px}
.bs-fill-ceil{background:#ecd9b0}
.bs-med-ceil{background:#9c6f24}
.bs-band-lab{display:flex;justify-content:space-between;font-size:.74rem;color:#6b6459}
.bs-band-cap2{font-size:.82rem;color:#57514a;margin:12px 0 4px}
.bs-rows{margin-top:10px;display:flex;flex-direction:column;gap:5px}
.bs-row{font-size:.86rem;color:#332f2a}
.bs-row b{font-weight:650}
.bs-muted{font-size:.78rem;color:#8a8275;margin-top:8px;padding-top:8px;
  border-top:1px dashed #e7e2d8}
.bs-warn{display:inline-block;font:600 .72rem system-ui;color:#8a5a00;background:#fbf0d8;
  padding:1px 8px;border-radius:6px;margin-left:6px}
</style>
"""


def _spread_x(p25, p75):
    try:
        a, b = float(p25), float(p75)
        return (b / a) if a > 0 else None
    except (TypeError, ValueError):
        return None


def _band_bar(p25, med, p75, scale_max: float, *, ceiling: bool = False) -> str:
    """One horizontal p25–median–p75 band bar, display-only scaled against a SHARED ``scale_max``
    so two bands in the same card are visually comparable. ``ceiling`` switches to the amber
    framework palette. Returns '' if the values aren't numeric."""
    try:
        lo, mid, hi = float(p25), float(med), float(p75)
    except (TypeError, ValueError):
        return ""
    if scale_max <= 0:
        return ""
    fill = "bs-fill bs-fill-ceil" if ceiling else "bs-fill"
    medc = "bs-med bs-med-ceil" if ceiling else "bs-med"
    left = max(0.0, min(100.0, lo / scale_max * 100))
    width = max(1.5, min(100.0 - left, (hi - lo) / scale_max * 100))
    medpos = max(0.0, min(100.0, mid / scale_max * 100))
    return (
        f'<div class="bs-track"><div class="{fill}" style="left:{left:.1f}%;width:{width:.1f}%"></div>'
        f'<div class="{medc}" style="left:{medpos:.1f}%"></div></div>'
        f'<div class="bs-band-lab"><span>{_eur(p25)}</span>'
        f"<span>median {_eur(med)}</span><span>{_eur(p75)}</span></div>"
    )


def _bid_signal_card(r) -> str:
    """One CPV-trade "Should I bid?" card. Pure render of pre-aggregated view rows — TWO bands on
    a shared scale so the full market range is visible without ever mixing the grains: (1) the
    single CONTRACT-AWARD band (one job, sum-safe), (2) the FRAMEWORK / multi-supplier ceiling
    band (the big end — money that may be drawn down, not a single job). Plus competition (median
    bids + single-bid %) and SME-win %. No recommendation, no inference — facts only."""
    label = _esc(_coalesce(r.get("trade_label"), "—"))
    code = _esc(_coalesce(r.get("trade_code"), ""))
    n_tot = _n(r.get("n_awards_total"))
    a_p25, a_med, a_p75 = r.get("award_p25_eur"), r.get("award_median_eur"), r.get("award_p75_eur")
    n_aw = _n(r.get("n_contract_awards"))
    n_recent = _n(r.get("n_recent_contract_awards"))
    c_p25, c_med, c_p75 = r.get("ceiling_p25_eur"), r.get("ceiling_median_eur"), r.get("ceiling_p75_eur")
    n_ceil = _n(r.get("n_framework_ceilings"))

    # Shared scale = the larger of the two bands' p75 so the contrast reads honestly.
    def _f(x):
        try:
            return float(x)
        except (TypeError, ValueError):
            return 0.0

    scale_max = max(_f(a_p75), _f(c_p75))

    spread = _spread_x(a_p25, a_p75)
    spread_pill = (
        f'<span class="bs-warn">spread ×{spread:.1f} — a band, not a quote</span>' if spread and spread >= 3 else ""
    )

    band_award = _band_bar(a_p25, a_med, a_p75, scale_max)
    award_block = (
        f'<div class="bs-band-cap">① Typical <b>single contract award</b> '
        f"({n_aw:,} awards · {n_recent:,} since 2022){spread_pill}</div>{band_award}"
        if band_award
        else ""
    )
    band_ceil = _band_bar(c_p25, c_med, c_p75, scale_max, ceiling=True)
    ceil_block = (
        f'<div class="bs-band-cap2">② <b>Framework / multi-supplier ceiling</b> '
        f"({n_ceil:,} agreement{'s' if n_ceil != 1 else ''}) — money that <em>may</em> be drawn "
        f"down, not one job</div>{band_ceil}"
        if band_ceil
        else ""
    )

    n_bid = _n(r.get("n_with_bid_data"))
    med_bids = r.get("median_bids")
    med_bids_txt = f"{float(med_bids):.0f}" if med_bids is not None and not pd.isna(med_bids) else "—"
    sb_pct = r.get("single_bid_pct")
    sb_txt = f"{float(sb_pct):.0f}%" if sb_pct is not None and not pd.isna(sb_pct) else "—"
    n_sme = _n(r.get("n_with_sme_data"))
    sme_pct = r.get("sme_win_pct")
    sme_txt = f"{float(sme_pct):.0f}%" if sme_pct is not None and not pd.isna(sme_pct) else "—"

    rows = (
        f'<div class="bs-row">👥 Competition: <b>{med_bids_txt}</b> bidders typical · '
        f'<b>{sb_txt}</b> drew a single bid <span style="color:#8a8275">(of {n_bid:,} with bid data)</span></div>'
        f'<div class="bs-row">🏢 SME wins: <b>{sme_txt}</b> of awards went to an SME '
        f'<span style="color:#8a8275">(of {n_sme:,} with SME data)</span></div>'
    )
    return (
        '<div class="bs-card">'
        f'<div class="bs-head"><span class="bs-name">{label}</span>'
        f'<span class="bs-code">CPV {code}xxxx</span>'
        f'<span class="bs-tot">{n_tot:,} awards total</span></div>'
        f"{award_block}{ceil_block}"
        f'<div class="bs-rows">{rows}</div></div>'
    )


def _render_bid_signal() -> None:
    """EXPERIMENTAL "Should I bid?" lens (local-only, gated by DAIL_EXPERIMENTAL). Renders the
    per-CPV-trade signal cards from v_procurement_bid_signal. This deliberately does NOT price a
    job — the data can't (intra-trade spread 4.5x–15x; headline value mixes ceilings 14x–79x
    above real awards). It hands a bidder verifiable facts to judge for themselves."""
    st.html(_BIDSIG_CSS)
    st.html('<div class="pr-register-rule"><span>Should I bid? &nbsp;⚗ experimental</span></div>')
    finding_lede(
        [
            "Same logic, <strong>every sector</strong> — not just construction: how competitive "
            "the work is, how often a <strong>single</strong> bidder showed up, whether "
            "<strong>SMEs</strong> win it, and the real contract-award band, each with its "
            "sample size so you weigh it yourself.",
            "It is <strong>not a price</strong>: every trade's award range is "
            "<strong>4.5–15×</strong> wide and headline values mix framework ceilings far above "
            "real awards — so treat the band as orientation, never a quote.",
        ]
    )
    # Honesty rail, made prominent at the user's instruction: this feature is low-value because
    # the data has no project SIZE. Two contracts in the same trade can differ purely by scale
    # (a small rewire vs a hospital wing) and nothing here normalises that — no floor area / m² /
    # GFA / unit count exists anywhere in the source. So the band reflects job size as much as
    # job rate; it cannot tell you whether YOUR job is dear or cheap.
    st.warning(
        "**Low-value by design — read with care.** These bands are **not size-adjusted**: the "
        "source carries no floor area, m², GFA or unit count, so a small job and a large one in "
        "the same category land in the same band. The spread you see is mostly *project size*, "
        "not *price per unit* — this orients you on competition and typical deal size, it cannot "
        "tell you if a specific job is priced right.",
        icon="⚠️",
    )
    st.caption("⚗ Experimental · local only — not shown in the published app.")

    res = fetch_bid_signal_result(min_awards=20)
    if not res.ok:
        empty_state("Signal unavailable", "The bid-signal view did not load.")
        return
    df = res.data
    if df is None or df.empty:
        empty_state("No categories", "No CPV trades met the minimum sample size.")
        return

    sectors = ["All sectors"] + sorted(df["sector_label"].dropna().unique().tolist())
    c1, c2 = st.columns([1, 1])
    with c1:
        sector = st.selectbox("Sector", sectors, index=0, key="bs_sector")
    with c2:
        q = st.text_input(
            "Find your trade (CPV category name)",
            value="",
            key="bs_filter",
            placeholder="e.g. electrical, road, cleaning, software…",
        )
    view = df
    if sector != "All sectors":
        view = view[view["sector_label"] == sector]
    if q.strip():
        view = view[view["trade_label"].fillna("").str.contains(q.strip(), case=False, na=False)]
    if view.empty:
        empty_state("No match", "No category matches that sector / name filter.")
        return

    # Render sector by sector: a quiet sector header, then its trade cards (already ordered
    # biggest-first within the sector by the view). Cap total cards so the page stays light.
    shown = 0
    for sector_label, grp in view.groupby("sector_label", sort=True):
        if shown >= 60:
            break
        st.html(
            f'<div class="pr-register-rule"><span>{html.escape(str(sector_label))} '
            f"&middot; {len(grp)} categor{'y' if len(grp) == 1 else 'ies'}</span></div>"
        )
        rows = grp.head(60 - shown)
        st.html("".join(_bid_signal_card(r) for _, r in rows.iterrows()))
        shown += len(rows)
