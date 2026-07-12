"""Follow the Money — a guided trail through public money: a public body → the
companies it pays → the individual published records.

Why a standalone page (not another Procurement sub-tab): the *navigation* is the
feature. The payment graph is bipartite (bodies ⇄ suppliers) with the published
line items on each edge, and the value here is being able to walk it without
losing your place — a bounded breadcrumb you can step back through, jump along, or
restart, instead of a Back button that dumps you to the section root.

Surfacing-only and DRY: the three node renderers (body dossier, paid-supplier
dossier, line-item leaf) and the paid landing all live in
``pages_code/procurement.py``, co-located with their ``v_procurement_*`` views and
``pr-*`` helpers. This page owns ONLY the trail rail (session-state breadcrumb) and
the router; it computes nothing. It reuses the same ``?paid_supplier=`` /
``?paid_publisher=`` / ``?paid_tier=`` URL scheme the shared renderers already emit,
so every existing payment link works here unchanged — the rail simply remembers the
path and overrides Back to step through it (the renderers' ``on_back`` hook).

The wall (stated in the UI): public records stop at the *direct* contractor. What a
firm then pays its own subcontractors is not published anywhere, so there is no node
below the prime contractor — the leaf (the body's own line items) is the terminus.
"""

from __future__ import annotations

import sys
import urllib.parse
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.corporate_data import fetch_isif_portfolio
from data_access.freshness_data import freshness_line
from data_access.procurement_data import (
    fetch_entity_search_result,
    fetch_payment_group_header_result,
    fetch_payment_group_members_result,
    fetch_payments_supplier_header_result,
)
from pages_code.procurement import (
    _PAY_FOOT_HTML,
    _card,
    _coalesce,
    _cro_pill_from,
    _esc,
    _n,
    _paid_pill,
    _paid_verb,
    _render_payment_lines,
    _render_payments,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    hero_banner,
    hide_sidebar,
    text_search_mask,
)
from ui.source_pdfs import provenance_expander

# A topical, ready-made starting trail — the question that prompted this page. The publisher
# name is exactly as it appears in the payments view; NPHDB publishes purchase ORDERS, so the
# dossier opens on the COMMITTED tier (where its records live).
_FEATURED = {
    "publisher": "National Paediatric Hospital Development Board",
    "tier": "COMMITTED",
    "blurb": "The new children's hospital — its published purchase orders, the companies behind "
    "them, and (for BAM) the conciliator and adjudicator settlements.",
}

# Curated corporate groups offered as one-click starting nodes (the rollup that consolidates a
# parent's many legal entities — see data/_meta/supplier_groups.csv). Slug → landing tier.
_FEATURED_GROUPS = [
    {
        "slug": "bam",
        "label": "BAM",
        "tier": "SPENT",
        "blurb": "The construction group, consolidated — its operating companies, PPP "
        "special-purpose vehicles and joint ventures rolled into one (Building, Civil, the "
        "Schools and Courts bundles, Glasgiven).",
    },
]

_TIERS = ("SPENT", "COMMITTED")
_RAIL_CAP = 4  # nodes shown before the middle collapses to "…" (keeps the breadcrumb bounded)
_ENTITY_KIND_BADGE = {"ppp_spv": "PPP special-purpose vehicle", "jv": "joint venture"}


# ── trail node identity ───────────────────────────────────────────────────────
def _norm_tier(raw) -> str:
    t = (raw or "SPENT").upper()
    return t if t in _TIERS else "SPENT"


def _current_node(params) -> dict | None:
    """The node the URL points at, or None for the landing. Identity is by kind + entity id(s)
    (not the full param dict) so a re-visit at a different tier still matches its breadcrumb step."""
    fg = params.get("flow_group")
    if fg:
        return {
            "kind": "group",
            "key": ("group", fg),
            "label": _group_label(fg),
            "tier": _norm_tier(params.get("paid_tier")),
            "params": {"flow_group": fg, "paid_tier": _norm_tier(params.get("paid_tier"))},
            "group": fg,
        }
    fsl = params.get("flow_supplier_lines")
    if fsl:
        return {
            "kind": "supplier_lines",
            "key": ("supplier_lines", fsl),
            "label": _pretty_supplier(fsl),
            "tier": _norm_tier(params.get("paid_tier")),
            "params": {"flow_supplier_lines": fsl, "paid_tier": _norm_tier(params.get("paid_tier"))},
            "supplier": fsl,
        }
    ps = params.get("paid_supplier")
    pp = params.get("paid_publisher")
    tier = _norm_tier(params.get("paid_tier"))
    if ps and pp:
        return {
            "kind": "ledger",
            "key": ("ledger", ps, pp),
            "label": "Records",
            "tier": tier,
            "params": {"paid_supplier": ps, "paid_publisher": pp, "paid_tier": tier},
            "supplier": ps,
            "publisher": pp,
        }
    if pp:
        return {
            "kind": "body",
            "key": ("body", pp),
            "label": pp,
            "tier": tier,
            "params": {"paid_publisher": pp, "paid_tier": tier},
            "publisher": pp,
        }
    if ps:
        return {
            "kind": "supplier",
            "key": ("supplier", ps),
            "label": _pretty_supplier(ps),
            "tier": tier,
            "params": {"paid_supplier": ps, "paid_tier": tier},
            "supplier": ps,
        }
    return None


def _pretty_supplier(supplier_norm: str) -> str:
    """The firm's published display name (cached header lookup), falling back to a title-cased
    form of the normalised key so the breadcrumb never shows a lowercased slug."""
    try:
        res = fetch_payments_supplier_header_result(supplier_norm)
        if res.ok and not res.data.empty:
            name = _coalesce(res.data.iloc[0].get("supplier"))
            if name:
                return name
    except Exception:  # noqa: BLE001 — a label is cosmetic; never break the page for it
        pass
    return str(supplier_norm).title()


def _group_label(slug: str) -> str:
    """The group's display label for the breadcrumb (cached header lookup), falling back to the
    featured-list label or the upper-cased slug."""
    try:
        res = fetch_payment_group_header_result(slug)
        if res.ok and not res.data.empty:
            lbl = _coalesce(res.data.iloc[0].get("group_label"))
            if lbl:
                return lbl
    except Exception:  # noqa: BLE001 — cosmetic
        pass
    featured = next((g for g in _FEATURED_GROUPS if g["slug"] == slug), None)
    return featured["label"] if featured else str(slug).upper()


# ── trail state ───────────────────────────────────────────────────────────────
def _sync_trail(node: dict) -> list[dict]:
    """Reconcile the current node with the stored breadcrumb: re-visiting an earlier node
    truncates the trail back to it (a jump/Back); a new node extends it (a drill forward)."""
    trail = st.session_state.get("mf_trail", [])
    idx = next((i for i, e in enumerate(trail) if e["key"] == node["key"]), None)
    trail = trail[: idx + 1] if idx is not None else [*trail, node]
    st.session_state["mf_trail"] = trail
    return trail


def _go(params: dict | None) -> None:
    """Soft-navigate to a node (or the landing when params is None) and rerun."""
    st.query_params.clear()
    if params:
        st.query_params.from_dict(params)
    st.rerun()


def _back_one() -> None:
    """Back action handed to every node renderer: step to the breadcrumb's parent (or landing)."""
    trail = st.session_state.get("mf_trail", [])
    _go(trail[-2]["params"] if len(trail) >= 2 else None)


def _rail_href(params: dict) -> str:
    return "?" + urllib.parse.urlencode(params, quote_via=urllib.parse.quote)


def _render_rail(trail: list[dict]) -> None:
    """The bounded breadcrumb. Each earlier step is a soft link (jump back to it); the current
    step is inert. Long trails collapse the middle to '…' so it never grows into a wall."""
    if not trail:
        return
    last = len(trail) - 1
    shown = trail
    gap = False
    if len(trail) > _RAIL_CAP:
        shown = [trail[0], *trail[-(_RAIL_CAP - 1) :]]  # origin + the most recent few
        gap = True

    chips: list[str] = []
    for i, e in enumerate(shown):
        is_current = e["key"] == trail[last]["key"]
        if gap and i == 1:
            chips.append('<span class="mf-rail-sep">›</span><span class="mf-rail-gap">…</span>')
        label = _esc(e["label"])
        if is_current:
            chips.append(f'<span class="mf-rail-here">{label}</span>')
        else:
            chips.append(f'<a class="mf-rail-step" href="{_esc(_rail_href(e["params"]))}" target="_self">{label}</a>')
    body = '<span class="mf-rail-sep">›</span>'.join(chips)
    st.html(
        '<div class="mf-rail"><div class="mf-rail-lede">💶 Following the money</div>'
        f'<div class="mf-rail-path">{body}'
        '<a class="mf-rail-reset" href="?" target="_self">Start over</a></div></div>'
    )


# ── corporate-group node (the consolidation the whole feature was for) ─────────
def _render_group(slug: str, tier: str, *, on_back) -> None:
    """A corporate-group node: the parent's many published legal entities rolled into one, with
    an honest structure disclosure (how many PPP SPVs / JVs / no-CRO entities) and combined-floor
    totals. Each member entity is a card that drills into its own paid-supplier profile — i.e. back
    into the same trail, one rung finer. Aggregation is done in the query; this only renders."""
    if back_button("← Back", key="mfgrpback"):
        on_back()

    hdr = fetch_payment_group_header_result(slug)
    hrow = hdr.data.iloc[0] if (hdr.ok and not hdr.data.empty) else None
    if hrow is None:
        if not hdr.ok:
            empty_state("Group data isn't available right now", "A source/pipeline issue, not an empty result.")
        else:
            empty_state("Group not found", "That link didn't match a curated corporate group. Use Back to return.")
        return

    label = _esc(_coalesce(hrow.get("group_label")) or slug.upper())
    n_ent = _n(hrow.get("n_entities"))
    span = f"{_n(hrow.get('min_year'))}–{_n(hrow.get('max_year'))}" if _n(hrow.get("min_year")) else ""
    np_ = _n(hrow.get("n_publishers"))
    sub_parts = [
        f"{n_ent} legal entit{'ies' if n_ent != 1 else 'y'}",
        f"{np_:,} public bod{'ies' if np_ != 1 else 'y'}",
    ]
    if span:
        sub_parts.append(span)
    st.html(
        '<div class="pr-prof-head"><div class="pr-prof-kicker">CORPORATE GROUP</div>'
        f'<h1 class="pr-prof-name">{label}</h1>'
        f'<div class="pr-prof-sub">{_esc(" · ".join(sub_parts))}</div></div>'
    )

    # Both lifecycle tiers side by side — distinct stages of public money, NEVER summed; combined
    # across member entities, so an indicative FLOOR (vat bases may differ across the paying bodies).
    n_paid, n_ordered = _n(hrow.get("n_paid_lines")), _n(hrow.get("n_ordered_lines"))
    tiers_present = [t for t, c in (("SPENT", n_paid), ("COMMITTED", n_ordered)) if c]
    tier_pills = []
    if n_ordered:
        tier_pills.append(_paid_pill(hrow.get("ordered_safe_eur"), "COMMITTED"))
    if n_paid:
        tier_pills.append(_paid_pill(hrow.get("paid_safe_eur"), "SPENT"))
    tier_pills.append('<span class="pr-pill pr-pill-lob">combined floor</span>')
    st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(p for p in tier_pills if p)}</div>')

    # Honest structure disclosure — what "one BAM" actually contains.
    n_ppp, n_jv, n_no_cro = _n(hrow.get("n_ppp_spv")), _n(hrow.get("n_jv")), _n(hrow.get("n_no_cro"))
    structure = []
    if n_ppp:
        structure.append(f"{n_ppp} PPP special-purpose vehicle{'s' if n_ppp != 1 else ''}")
    if n_jv:
        structure.append(f"{n_jv} joint venture{'s' if n_jv != 1 else ''}")
    struct_clause = f" — including {', '.join(structure)}" if structure else ""
    cro_clause = (
        f" {n_no_cro} of the {n_ent} carry no Companies Registration Office number "
        "(the PPP vehicles and JVs are not separately CRO-matched)."
        if n_no_cro
        else ""
    )
    st.html(
        '<div class="mf-wall">This is a <strong>curated</strong> grouping of the published payment '
        f"entities under {label}{struct_clause}.{cro_clause} Combined figures are sum-safe euros totalled across "
        "these entities and are an indicative <strong>floor</strong>, never an audited total — paid and "
        "ordered are never added, nor are euros summed across bodies with different VAT bases.</div>"
    )

    if not tiers_present:
        empty_state("No payments found", "This group has no sum-safe payment records.")
        st.html(_PAY_FOOT_HTML)
        return

    active = tier if tier in tiers_present else tiers_present[0]
    if len(tiers_present) > 1:
        labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
        default = next(k for k, v in labels.items() if v == active)
        choice = st.segmented_control(
            "Tier", list(labels), default=default, key="mf_grp_tier", label_visibility="collapsed"
        )
        active = labels.get(choice or default, active)

    res = fetch_payment_group_members_result(slug, tier=active)
    df = res.data if res.ok else None
    if df is None or df.empty:
        empty_state("No member entities in this tier", f"No group entity has {_paid_verb(active)} records.")
        st.html(_PAY_FOOT_HTML)
        return
    st.caption(
        f"The {len(df):,} member entit{'ies' if len(df) != 1 else 'y'} by money {_paid_verb(active)} "
        "(sum-safe within each). Click one to follow it into the public bodies that paid it."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        kind_badge = _ENTITY_KIND_BADGE.get(_coalesce(getattr(r, "entity_kind", None)))
        meta = (
            f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · "
            f"{_n(r.n_publishers):,} public bod{'ies' if _n(r.n_publishers) != 1 else 'y'}"
        )
        pills = [_paid_pill(r.total_safe_eur, active)]
        if kind_badge:
            pills.append(f'<span class="pr-pill pr-pill-lob">{_esc(kind_badge)}</span>')
        cro = _cro_pill_from(getattr(r, "cro_company_num", None), getattr(r, "cro_company_status", None))
        if cro:
            pills.append(cro)
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, [p for p in pills if p], rank=i)
        # Company-class members open the line items behind their figure directly — every record
        # across all the bodies in this tier (one click, no intervening body-list hop). Individuals
        # stay static (privacy quarantine).
        if _coalesce(getattr(r, "supplier_class", None)) == "company":
            cards.append(
                clickable_card_link(
                    href=_rail_href({"flow_supplier_lines": r.supplier_normalised, "paid_tier": active}),
                    inner_html=inner,
                    aria_label=f"See the published {_paid_verb(active)} line items behind {r.supplier}'s figure",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_PAY_FOOT_HTML)


# ── search (jump straight onto a node) ─────────────────────────────────────────
def _render_search(href_base: str = "") -> None:
    """Search-first entry to the trail: type a company or public body and drop straight onto its
    node instead of scrolling the top-N lists. DISPLAY-ONLY name filter over the pre-built search
    corpus (v_procurement_entity_search), narrowed to the PAID registers this page walks; renders
    nothing until the reader types. Each hit links via the same ?paid_* scheme the rail tracks, so a
    match begins (or extends) the trail exactly like the featured tiles do — it computes nothing.

    ``href_base`` (Money nav declutter Phase 3): when another page embeds this search
    (the Public Payments hub's Trace section), pass "/follow-the-money" so hits do a
    real cross-page navigation onto the trail's routable home instead of writing
    ?paid_* params onto the host page's own router. Default "" keeps this page's
    same-page soft-nav behaviour unchanged."""
    res = fetch_entity_search_result()
    if not res.ok or res.data.empty:
        return
    q = st.text_input(
        "Search the money trail",
        placeholder="Search a company or public body…",
        key="mf_search_q",
        label_visibility="collapsed",
    )
    qs = (q or "").strip()
    if not qs:
        return
    df = res.data
    df = df[df["entity_kind"].isin(("paid_supplier", "paid_body"))]
    hits = df[text_search_mask(df, qs, ["display_name"])].head(12)
    if hits.empty:
        empty_state("No matches", "Try a shorter term — names are matched as published by the body.")
        return
    kind_label = {"paid_supplier": "COMPANY", "paid_body": "PUBLIC BODY"}
    cards = []
    for r in hits.itertuples():
        kind = str(r.entity_kind)
        tier = (_coalesce(getattr(r, "paid_tier", None)) or "SPENT").upper()
        nc, n = _n(r.n_counterparties), _n(r.n_records)
        meta = f"{n:,} published line{'s' if n != 1 else ''}"
        meta += (
            f" · {nc:,} public bod{'ies' if nc != 1 else 'y'}"
            if kind == "paid_supplier"
            else f" · {nc:,} supplier{'s' if nc != 1 else ''}"
        )
        pills = [f'<span class="pr-pill pr-pill-lob">{kind_label.get(kind, kind)}</span>']
        paid = _paid_pill(getattr(r, "paid_safe_eur", None), tier)
        if paid:
            pills.append(paid)
        if kind == "paid_supplier":
            href = href_base + _rail_href({"paid_supplier": r.url_key, "paid_tier": tier})
            aria = f"Follow the money paid to {r.display_name}"
        else:
            href = href_base + _rail_href({"paid_publisher": r.url_key, "paid_tier": tier})
            aria = f"Follow the money paid by {r.display_name}"
        inner = _card(f"<span>{_esc(r.display_name)}</span>", meta, pills)
        cards.append(clickable_card_link(href=href, inner_html=inner, aria_label=aria))
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


# ── landing ───────────────────────────────────────────────────────────────────
def _isif_amount(stated, currency, is_up_to) -> str:
    """Currency-aware compact amount for an ISIF commitment: '€140m' / 'up to $20m'.
    Never summed across rows (mixed currency / 'up to' ceilings)."""
    try:
        n = float(stated)
    except (TypeError, ValueError):
        return ""
    sym = {"EUR": "€", "USD": "$", "GBP": "£"}.get(_coalesce(currency) or "EUR", "")
    if n >= 1_000_000:
        amt = f"{sym}{n / 1_000_000:.0f}m"
    elif n >= 1_000:
        amt = f"{sym}{n / 1_000:.0f}k"
    else:
        amt = f"{sym}{n:,.0f}"
    return f"up to {amt}" if bool(is_up_to) else amt


def _render_isif_lane() -> None:
    """STATE AS INVESTOR — the other direction of the money trail. Beyond what public bodies
    PAY, the State (via the Ireland Strategic Investment Fund) also INVESTS in companies. Newest
    commitments, as a named list. NEVER summed (mixed currency / 'up to' ceilings); display-only.
    Silently no-ops if the view is unavailable."""
    df = fetch_isif_portfolio(limit=10)
    if df is None or df.empty:
        return
    rows: list[str] = []
    for r in df.itertuples():
        name = _esc(getattr(r, "investee_name", None)) or "—"
        amt = _esc(
            _isif_amount(
                getattr(r, "amount_stated", None),
                getattr(r, "amount_currency", None),
                getattr(r, "amount_is_up_to", None),
            )
        )
        yr = _esc(str(getattr(r, "commitment_year_label", "") or ""))
        desc = _esc(getattr(r, "description", None) or "")
        if len(desc) > 150:
            desc = desc[:147] + "…"
        amt_html = f'<span class="mf-isif-amt">{amt}</span>' if amt else ""
        rows.append(
            '<div class="mf-isif-row">'
            f'<div class="mf-isif-head"><span class="mf-isif-name">{name}</span>{amt_html}'
            f'<span class="mf-isif-yr">{yr}</span></div>'
            f'<div class="mf-isif-desc">{desc}</div>'
            "</div>"
        )
    st.html(
        '<section class="mf-isif" aria-label="State as investor — ISIF commitments">'
        '<div class="mf-isif-kick">STATE AS INVESTOR</div>'
        '<div class="mf-isif-sub">The other direction of the trail: beyond what public bodies '
        "<em>pay</em>, the State also <strong>invests</strong> in companies through the Ireland "
        "Strategic Investment Fund. Recent commitments — a <strong>different instrument</strong> from "
        "payments, in mixed currencies, <strong>never added together</strong>.</div>" + "".join(rows) + "</section>"
    )


def _render_landing() -> None:
    hero_banner(
        kicker="PUBLIC MONEY",
        title="Follow the money",
        dek="Pick a public body or a company, then walk the trail — who it pays, and the "
        "individual published records behind each figure.",
    )
    # Role-clarity (Money nav declutter Phase 2): this page is the payment TRAIL;
    # the whole-firm cross-register view is the company dossier.
    st.caption(
        "The navigable payment graph — trace exactly who paid a firm, line by line. "
        "A firm's full cross-register footprint lives on its company dossier."
    )
    # Search-first: jump straight onto a node rather than scrolling the top-N lists below.
    _render_search()
    # The ready-made trail that prompted the page.
    st.html(
        '<a class="mf-featured" href="'
        + _esc(_rail_href({"paid_publisher": _FEATURED["publisher"], "paid_tier": _FEATURED["tier"]}))
        + '" target="_self"><div class="mf-featured-kick">START HERE</div>'
        f'<div class="mf-featured-name">{_esc(_FEATURED["publisher"])}</div>'
        f'<div class="mf-featured-blurb">{_esc(_FEATURED["blurb"])}</div></a>'
    )
    # Curated corporate groups — a parent's many legal entities as one starting node.
    for g in _FEATURED_GROUPS:
        st.html(
            '<a class="mf-featured" href="'
            + _esc(_rail_href({"flow_group": g["slug"], "paid_tier": g["tier"]}))
            + '" target="_self"><div class="mf-featured-kick">CORPORATE GROUP</div>'
            f'<div class="mf-featured-name">{_esc(g["label"])}</div>'
            f'<div class="mf-featured-blurb">{_esc(g["blurb"])}</div></a>'
        )
    st.html(
        '<div class="mf-wall">Public records stop at the <strong>direct contractor</strong>. '
        "What a company then pays its own subcontractors is not published anywhere — so the trail "
        "ends at each body's own line items, never below them.</div>"
    )
    # The other direction of the trail — the State as investor (ISIF), not just payer.
    _render_isif_lane()
    # The full start-from picker — the existing paid landing (top bodies / top companies), whose
    # cards already link into the same ?paid_* scheme this page's rail tracks.
    _render_payments()

    provenance_expander(
        sections=[
            "**What this shows.** A guided walk through each public body's published "
            "**purchase orders and payments over €20,000** — the company, the body's own "
            "description, PO number and amount — reachable body-by-body and company-by-company.",
            "**Follow a thread, keep your place.** The breadcrumb at the top remembers your path; "
            "click any earlier step to jump back, or **Start over** to begin again.",
            "**Where the trail ends.** The deepest view is the body's own line items. Public "
            "disclosure names the **direct contractor** only — payments below a firm to its own "
            "subcontractors are not published, so there is no node beneath the prime contractor.",
            "**Different grains — never summed.** A line is the body's own reported figure (a "
            "purchase-order *commitment* or an actual *payment*), not an award ceiling, and is never "
            "added across bodies with different VAT bases. A record is a disclosure, not evidence of "
            "wrongdoing.",
        ],
        source_caption=(
            "Data: each public body's over-€20,000 purchase-order / payment lists (most under the "
            "FOI Act 2014 s.8 model publication scheme; origin Circular 07/2012), consolidated and "
            "matched to the Companies Registration Office. Bodies publish at their own cadence."
        ),
        freshness=freshness_line("procurement"),
    )


# ── page ──────────────────────────────────────────────────────────────────────
def follow_the_money_page() -> None:
    hide_sidebar()
    params = st.query_params
    node = _current_node(params)
    if node is None:
        st.session_state.pop("mf_trail", None)  # a clean landing resets the trail
        _render_landing()
        return

    trail = _sync_trail(node)
    _render_rail(trail)

    tier = node["tier"]
    if node["kind"] == "group":
        _render_group(node["group"], tier, on_back=_back_one)
    elif node["kind"] == "ledger":
        _render_payment_lines(node["supplier"], node["publisher"], tier, on_back=_back_one, back_label="← Back")
    elif node["kind"] == "supplier_lines":
        # The 'what comprised this' leaf for a group member: every line behind the firm's figure,
        # across all bodies in the tier (publisher_name=None). One click from the group member card.
        _render_payment_lines(node["supplier"], None, tier, on_back=_back_one, back_label="← Back")
    elif node["kind"] == "body":
        _render_payments_publisher_profile(node["publisher"], tier, on_back=_back_one, back_label="← Back")
    else:  # supplier
        _render_payments_supplier_profile(node["supplier"], tier, on_back=_back_one, back_label="← Back")
