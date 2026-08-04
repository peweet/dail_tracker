from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.procurement_data import (
    awards_register_norms,
    resolve_buyer_identity,
    fetch_entity_chain_for_company_result,
    fetch_payment_lines_for_pair_result,
    fetch_payment_lines_for_supplier_result,
    fetch_payments_for_publisher_result,
    fetch_payments_for_supplier_result,
    fetch_payments_by_year_result,
    fetch_payments_publisher_profile_result,
    fetch_payments_publishers_for_supplier_result,
    fetch_payments_supplier_header_result,
    fetch_supplier_payments_by_year_result,
)
from ui.entity_links import (
    buyer_dossier_cta_html,
    company_dossier_cta_html,
    council_accountability_url,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
)

from ui.format import coalesce as _coalesce
from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _eur,
    _paid_pair_href,
    _yr_axis,
    _return_to_browse,
    _card,
)

from .payments import _paid_verb, _paid_pill
from .councils import _render_council_accounts_context




def _render_payments_publisher_profile(
    publisher_name: str,
    tier: str = "SPENT",
    *,
    on_back=None,
    back_label: str = "← Back to procurement",
    show_back: bool = True,
    money_flow: bool = False,
    councillors_href: str | None = None,
) -> None:
    """Per-buyer dossier (the per-council profile): which tiers the body publishes, both totals
    shown side by side (never summed), and its top suppliers in the active tier. Councils mostly
    publish purchase ORDERS, so this falls back to whichever tier the body actually has.

    ``on_back`` overrides the Back action (default: return to the procurement 'paid' section) so a
    reusing page — e.g. the Follow-the-money trail — can step back through its own breadcrumb
    instead. ``None`` preserves the exact original behaviour for the procurement / council pages.
    ``show_back=False`` suppresses the back button entirely — for embedding this dossier as a section
    of a host page (the Your Council hub) that already provides its own back affordance.
    ``money_flow=True`` renders a local authority's lanes in money-flow order — VOTED (adopted
    budget) before the audited-accounts lanes — for the Your Council hub; ``councillors_href``
    is the VOTED lane's cross-link to the roster of the councillors who adopted it."""
    # Deferred (call-time) imports: this module is itself imported by .councils, .ted and
    # .profiles, so top-level imports back into any of them would be circular within the
    # payments/ted/profiles/pay_profiles/councils mutual-dependency cluster.
    from .councils import _council_summary_row, _lane_header
    from .profiles import _FOOT_HTML
    from .ted import _cro_pill_from

    if show_back and back_button(back_label, key="prpaypub"):
        (on_back or (lambda: _return_to_browse("paid")))()

    prof = fetch_payments_publisher_profile_result(publisher_name)
    prow = prof.data.iloc[0] if (prof.ok and not prof.data.empty) else None
    n_paid = _n(prow.get("n_paid_lines")) if prow is not None else 0
    n_ordered = _n(prow.get("n_ordered_lines")) if prow is not None else 0
    tiers_present = [t for t, c in (("SPENT", n_paid), ("COMMITTED", n_ordered)) if c]

    # A council can publish audited accounts but NO purchase-order list (Dublin City, Dún
    # Laoghaire-Rathdown, Louth, Tipperary). Those carry no row in the payments fact, so look them up
    # in the council directory (the union view) and render the audited-accounts dossier rather than
    # bail with "No payments found" — the old behaviour made the largest LA in the State unreachable.
    # NB the profile query is an ungrouped aggregate, so for an unknown publisher it returns a single
    # ALL-NULL row (prow is not None) with no tiers — gate on tiers_present, not on prow being None.
    csum = _council_summary_row(publisher_name) if not tiers_present else None
    is_afs_only = csum is not None and (_truthy(csum.get("has_running")) or _truthy(csum.get("has_building")))

    is_la = (prow is not None and _coalesce(prow.get("publisher_type")) == "local_authority") or is_afs_only
    kicker = "LOCAL AUTHORITY" if is_la else "PUBLIC BODY"
    sector = _coalesce(prow.get("sector")) if prow is not None else ""
    n_sup = _n(prow.get("n_suppliers")) if prow is not None else 0
    span = ""
    if prow is not None and _n(prow.get("min_year")):
        span = f"{_n(prow.get('min_year'))}–{_n(prow.get('max_year'))}"
    if is_afs_only:
        # Describe the audited-accounts coverage span instead of "0 suppliers over €20,000".
        acc_years = [
            int(y)
            for y in (
                csum.get("running_min_year"),
                csum.get("running_max_year"),
                csum.get("building_min_year"),
                csum.get("building_max_year"),
            )
            if _truthy(y)
        ]
        acc_span = (
            f"{min(acc_years)}–{max(acc_years)}"
            if acc_years and min(acc_years) != max(acc_years)
            else (str(acc_years[0]) if acc_years else "")
        )
        sub_parts = ["Audited accounts" + (f" · {acc_span}" if acc_span else "")]
    else:
        sub_parts = [f"{n_sup:,} supplier{'s' if n_sup != 1 else ''} over €20,000"]
        if span:
            sub_parts.append(span)
    kick = kicker + (f" · {sector.upper()}" if sector and not is_la else "")
    # Forward edge for councils: cross-link the spending dossier to the council's
    # "Who Runs Your County" accountability page (the two council views otherwise
    # never connect). publisher_name is the council join key the local-government
    # page resolves ?la= against; gated on the local_authority flag so only real
    # councils get the link.
    accountability_html = (
        f'<div class="pr-prof-sub" style="margin-top:0.35rem">'
        f'<a class="dt-source-link" href="{_esc(council_accountability_url(publisher_name))}" target="_self">'
        f"Who runs {_esc(publisher_name)} →</a></div>"
        if is_la
        else ""
    )
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">{_esc(kick)}</div>'
        f'<h1 class="pr-prof-name">{_esc(publisher_name)}</h1>'
        f'<div class="pr-prof-sub">{_esc(" · ".join(sub_parts))}</div>'
        f"{accountability_html}</div>"
    )
    # Cross-link to the unified /body dossier — it adds the CONTRACT-AWARDS lane this
    # payments/AFS profile doesn't carry. GATED on the crosswalk so only the ~90 known
    # bodies link (never a "not found" dead end).
    if resolve_buyer_identity(publisher_name):
        st.html(buyer_dossier_cta_html(publisher_name))
    # Both lifecycle tiers side by side — distinct stages of public money, NEVER summed.
    if prow is not None:
        tier_pills = []
        if n_ordered:
            tier_pills.append(_paid_pill(prow.get("ordered_safe_eur"), "COMMITTED"))
        if n_paid:
            tier_pills.append(_paid_pill(prow.get("paid_safe_eur"), "SPENT"))
        tier_pills = [p for p in tier_pills if p]
        if tier_pills:
            st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(tier_pills)}</div>')

    if not tiers_present:
        if is_afs_only:
            # Audited-accounts-only council: render the Running + Building lanes, then say plainly
            # that no purchase-order list is published (the PAYING lane is absent, not empty).
            _render_council_accounts_context(
                publisher_name,
                "COMMITTED",
                has_paying=False,
                lead_with_budget=money_flow,
                councillors_href=councillors_href,
            )
            st.html(
                '<div class="pr-caveat"><strong>Purchase orders:</strong> '
                f"{_esc(publisher_name)} does not publish a machine-readable list of its purchase "
                "orders / payments over €20,000, so the <em>Who it pays</em> named-supplier view "
                "isn’t available — its spending is shown through the audited accounts above.</div>"
            )
            st.html(_FOOT_HTML)
            return
        empty_state("No payments found", "This body has no sum-safe records, or the link didn't match.")
        return

    # Active tier: honour the requested one; else the tier the body actually has. If it has
    # both, a toggle switches the supplier list (the headline pills always show both).
    active = tier if tier in tiers_present else tiers_present[0]
    if len(tiers_present) > 1:
        labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
        default = next(k for k, v in labels.items() if v == active)
        choice = st.segmented_control(
            "Tier", list(labels), default=default, key="pr_paypub_tier", label_visibility="collapsed"
        )
        active = labels.get(choice or default, active)

    # Local-authority dossiers lead with the two AUDITED-ACCOUNTS lanes — RUNNING THE SERVICES
    # (revenue net cost) then BUILDING (capital investment) — the council's whole-budget context.
    # BUDGET grain: siblings, never summed with each other or with the purchase-order euros below.
    # Pass the PO data's max year so the running lane can flag the AFS arrears lag.
    if is_la:
        _render_council_accounts_context(
            publisher_name,
            active,
            po_max_year=_n(prow.get("max_year")),
            lead_with_budget=money_flow,
            councillors_href=councillors_href,
        )
        # LANE 3 — PAYING: the named suppliers over €20,000. The narrowest, most granular slice of
        # council money (most spend never passes through a tendered PO), but the only one named to a
        # firm. A DIFFERENT grain again — never added to the audited-accounts lanes above.
        st.html(
            _lane_header(
                "PAYING · purchase orders over €20,000",
                "Who it pays",
                "The suppliers the council reports paying or ordering more than €20,000 (FOI Act 2014 "
                "s.8 model publication scheme; origin Circular FIN 07/12). This is the <strong>named-supplier</strong> slice — most "
                "council money never passes through a tendered purchase order, so it is far narrower "
                "than the audited accounts above, and <strong>never added to them</strong>.",
            )
        )

    # Spend-over-time spine — one tier only (never stack ordered+paid, which would read as a sum).
    # Meaningful now the council payment data is a decade deep (2016–2026).
    by_year = fetch_payments_by_year_result(publisher_name, tier=active)
    if by_year.ok and len(by_year.data) > 1:
        st.caption(f"Money {_paid_verb(active)} per year (sum-safe)")
        st.bar_chart(
            _yr_axis(by_year.data),
            x="year",
            y="total_safe_eur",
            x_label="Year",
            y_label="€ (sum-safe)",
            height=200,
            color="#9c5b2e",
        )

    res = fetch_payments_for_publisher_result(publisher_name, tier=active)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No suppliers in this tier", f"This body has no sum-safe {_paid_verb(active)} records.")
        st.html(_FOOT_HTML)
        return
    st.caption(
        f"Top {len(df):,} suppliers by money {_paid_verb(active)} (sum-safe). Names as published by the body; "
        "amounts are the body's own reported figures, not award ceilings. Click a company to see every public "
        "body that paid it."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        meta = f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · {_n(r.min_year)}–{_n(r.max_year)}"
        pills = [
            p
            for p in (_paid_pill(r.total_safe_eur, active), _cro_pill_from(getattr(r, "cro_company_num", None), None))
            if p
        ]
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=i)
        # Only company-class suppliers drill down: the supplier dossier composes a firm's
        # cross-body footprint, which for an individual / sole trader would be profile-building
        # (the same privacy quarantine the dossier itself enforces). Individuals stay static.
        if _coalesce(getattr(r, "supplier_class", None)) == "company":
            # Drill to the LEAF (this body's published line items naming this firm), not to the
            # firm's own aggregate profile — the mutual linking is what made the drill-down loop.
            cards.append(
                clickable_card_link(
                    href=_paid_pair_href(r.supplier_normalised, publisher_name, active),
                    inner_html=inner,
                    aria_label=f"See the published {_paid_verb(active)} line items from {publisher_name} to {r.supplier}",
                )
            )
        else:
            cards.append(inner)
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_FOOT_HTML)


_PAY_FOOT_HTML = (
    '<div class="pr-foot"><strong>Source:</strong> each public body\'s own published '
    "purchase-order / payments disclosures — most under the FOI Act 2014 s.8 model publication scheme "
    "(origin: Circular FIN 07/12), some published voluntarily; consolidated and "
    "matched to the Companies Registration Office. Not every public body has this obligation, and "
    "thresholds differ by body. Suppliers and bodies are named as published. "
    "Paid (actual spend) and ordered (purchase orders) are different stages and are never summed "
    "together; totals are never summed across bodies with different VAT bases; never added to award values.</div>"
)


def _render_payments_supplier_profile(
    supplier_norm: str, tier: str = "SPENT", *, on_back=None, back_label: str = "← Back to procurement"
) -> None:
    """Paid-supplier drill-down — the public bodies that paid (SPENT) or ordered (COMMITTED)
    from one firm: the exact mirror of the per-body dossier (which lists a body's suppliers).
    A later lifecycle stage than awards (never added to award totals) and the two payment tiers
    are shown side by side, never blended. Company-class only (cross-body footprints of an
    individual are profile-building — the same quarantine as the awards drill-down).

    ``on_back`` overrides the Back action (see ``_render_payments_publisher_profile``)."""
    # Deferred: see the circular-import note in _render_payments_publisher_profile above.
    from .ted import _cro_pill_from

    if back_button(back_label, key="prpaysup"):
        (on_back or (lambda: _return_to_browse("paid")))()

    hdr = fetch_payments_supplier_header_result(supplier_norm)
    hrow = hdr.data.iloc[0] if (hdr.ok and not hdr.data.empty) else None
    if hrow is None:
        if not hdr.ok:
            empty_state("Payment data isn't available right now", "A source/pipeline issue, not an empty result.")
        else:
            empty_state("Supplier not found", "That link didn't match a paid supplier. Use Back to return.")
        return
    if _coalesce(hrow.get("supplier_class")) != "company":
        empty_state(
            "Not available",
            "Cross-body payment footprints are shown for companies only — composing one individual's is "
            "profile-building. Use Back to return.",
        )
        return

    name = _esc(_coalesce(hrow.get("supplier"))) or "—"
    n_paid, n_ordered = _n(hrow.get("n_paid_lines")), _n(hrow.get("n_ordered_lines"))
    tiers_present = [t for t, c in (("SPENT", n_paid), ("COMMITTED", n_ordered)) if c]
    np_ = _n(hrow.get("n_publishers"))
    span = f"{_n(hrow.get('min_year'))}–{_n(hrow.get('max_year'))}" if _n(hrow.get("min_year")) else ""
    sub_parts = [f"{np_:,} public bod{'ies' if np_ != 1 else 'y'} (over €20,000)"]
    if span:
        sub_parts.append(span)
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">MONEY ACTUALLY PAID</div>'
        f'<h1 class="pr-prof-name">{name}</h1>'
        f'<div class="pr-prof-sub">{_esc(" · ".join(sub_parts))}</div></div>'
    )
    # Both lifecycle tiers side by side — distinct stages of public money, NEVER summed.
    tier_pills = []
    if n_ordered:
        tier_pills.append(_paid_pill(hrow.get("ordered_safe_eur"), "COMMITTED"))
    if n_paid:
        tier_pills.append(_paid_pill(hrow.get("paid_safe_eur"), "SPENT"))
    if _truthy(hrow.get("vat_mixed")):
        tier_pills.append('<span class="pr-pill pr-pill-lob">mixed VAT bases (floor)</span>')
    cro = _cro_pill_from(hrow.get("cro_company_num"), hrow.get("cro_company_status"))
    pills = [p for p in (*tier_pills, cro) if p]
    if pills:
        st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    # Forward edge into the firm's canonical /company dossier (awards, lobbying, CRO) —
    # the reciprocal that lets a payments walk (Follow the Money / council spend) reach the
    # whole-firm footprint. GATED: /company resolves only for suppliers on the awards
    # register, so a payments-only firm gets NO link rather than a "Company not found"
    # dead end (the nav-graph never-a-false-hand-off rule; mirrors the leaf below).
    if supplier_norm in awards_register_norms():
        st.html(company_dossier_cta_html(str(supplier_norm)))

    if not tiers_present:
        empty_state("No payments found", "This firm has no sum-safe payment records.")
        st.html(_PAY_FOOT_HTML)
        return

    active = tier if tier in tiers_present else tiers_present[0]
    if len(tiers_present) > 1:
        labels = {"Paid (actual spend)": "SPENT", "Ordered (purchase orders)": "COMMITTED"}
        default = next(k for k, v in labels.items() if v == active)
        choice = st.segmented_control(
            "Tier", list(labels), default=default, key="pr_paysup_tier", label_visibility="collapsed"
        )
        active = labels.get(choice or default, active)

    res = fetch_payments_publishers_for_supplier_result(supplier_norm, tier=active)
    df = res.data if res.ok else pd.DataFrame()
    if df.empty:
        empty_state("No bodies in this tier", f"No public body has {_paid_verb(active)} records for this firm.")
        st.html(_PAY_FOOT_HTML)
        return
    st.caption(
        f"Public bodies that {_paid_verb(active)} this firm (sum-safe within each body). Names and amounts "
        "as the body published them, not award ceilings. Click a body for its own supplier list."
    )
    cards = []
    for i, r in enumerate(df.itertuples(), start=1):
        meta = (
            f"{_n(r.n_payments):,} {_paid_verb(active)} line{'s' if _n(r.n_payments) != 1 else ''} · "
            f"{_n(r.min_year)}–{_n(r.max_year)}"
        )
        row_pills = [_paid_pill(r.total_safe_eur, active)]
        if _coalesce(getattr(r, "publisher_type", None)) == "local_authority":
            row_pills.append('<span class="pr-pill pr-pill-lob">local authority</span>')
        inner = _card(f"<span>{_esc(r.publisher_name)}</span>", meta, [p for p in row_pills if p], rank=i)
        # Drill to the LEAF (this body's published line items naming this firm), not to the body's
        # own aggregate profile — that mutual linking is what made the drill-down loop endlessly
        # without ever showing a record.
        cards.append(
            clickable_card_link(
                href=_paid_pair_href(supplier_norm, r.publisher_name, active),
                inner_html=inner,
                aria_label=f"See the published {_paid_verb(active)} line items from {r.publisher_name} to this firm",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_PAY_FOOT_HTML)


def _payment_line_row(r, tier: str, *, show_publisher: bool = False) -> str:
    """One published payment line as a list row (the leaf of the payments drill-down): the
    period, the body's own description, PO number, and the amount — with a link to the body's
    source file where it published one. Display-only; the amount is the body's reported figure.

    ``show_publisher`` (the all-bodies leaf — one firm across many bodies): the row's top label
    becomes the paying body and the period folds into the meta line, so each constituent record
    of the firm's total is attributed to the body that published it."""
    period = _esc(_coalesce(getattr(r, "period", None))) or _esc(_n(getattr(r, "year", None)) or "")
    desc = _esc(_coalesce(getattr(r, "description", None)))
    # Per-line payment status, where the body published one (canonicalised in the view from a
    # strict allowlist: 'Paid' / 'Part paid' / 'Not paid'; NULL for the majority that publish
    # none). A factual disclosure tag, never a verdict — shown beside the line description.
    paid_status = _coalesce(getattr(r, "paid_status", None))
    _PAID_CLASS = {"Paid": "is-paid", "Not paid": "is-notpaid", "Part paid": "is-partpaid"}
    status_html = (
        f'<span class="pr-paid-tag {_PAID_CLASS.get(paid_status, "")}">{_esc(paid_status)}</span>'
        if paid_status in _PAID_CLASS
        else ""
    )
    # Recurring-charge flag: this exact amount was published by the body in ≥2 distinct years (the
    # signature of a PPP availability / unitary charge, not a one-off purchase). A factual marker so
    # an annually-repeating charge is not read as distinct spend that should be totalled.
    recurring_years = _n(getattr(r, "recurring_years", None))
    is_recurring = recurring_years >= 2 and getattr(r, "amount_eur", None) is not None
    recurring_html = (
        f'<span class="pr-paid-tag is-recurring" title="The same amount appears in {recurring_years} '
        'different years — a recurring annual charge, not distinct one-off spend.">'
        f"recurring · same amount in {recurring_years} years</span>"
        if is_recurring
        else ""
    )
    title_html = (
        f'<div class="pr-award-title">{desc}{status_html}{recurring_html}</div>'
        if (desc or status_html or recurring_html)
        else ""
    )
    meta_parts = []
    po = _coalesce(getattr(r, "po_number", None))
    if po:
        meta_parts.append(f"PO {_esc(po)}")
    if not _truthy(getattr(r, "value_safe_to_sum", None)):
        meta_parts.append("not sum-safe")
    src = _coalesce(getattr(r, "source_file_url", None))
    if src.startswith("http"):
        meta_parts.append(f'<a href="{_esc(src)}" target="_blank" rel="noopener">source ↗</a>')
    if show_publisher and period:
        meta_parts.insert(0, period)  # period folds into the meta when the body takes the top label
    meta = " · ".join(p for p in meta_parts if p)
    val = _eur(getattr(r, "amount_eur", None))
    val_html = f'<div class="pr-award-val">{val}<small>{_paid_verb(tier)}</small></div>' if val != "—" else ""
    auth = _esc(_coalesce(getattr(r, "publisher_name", None))) if show_publisher else (period or "—")
    return (
        f'<div class="pr-award"><div class="pr-award-body">'
        f'<div class="pr-award-auth">{auth or "—"}</div>{title_html}'
        f'<div class="pr-award-meta">{meta or "—"}</div></div>{val_html}</div>'
    )


def _render_payment_lines(
    supplier_norm: str, publisher_name: str | None, tier: str = "SPENT", *, on_back=None, back_label: str = "← Back"
) -> None:
    """LEAF view — the published payment line items behind one supplier's figure in a tier.
    The terminus that ends the old supplier↔body loop: instead of bouncing between aggregate
    cards, the reader lands here on the individual records (period, description, PO number,
    amount), each linked to the body's own source file. Company-class entry points only (same
    privacy quarantine as the rest of the payments drill-down).

    With ``publisher_name`` set this is one supplier × public body × tier (drilling a body's
    supplier card or a firm's body card). With ``publisher_name=None`` it is the firm's lines
    across ALL bodies in the tier — the 'what comprised this' leaf for a corporate-group member
    card, whose total spans bodies and so has no single pair; each line then shows its body.

    ``on_back`` overrides the Back action; the default returns to the supplier's payers list
    (the natural parent), preserving the tier."""
    all_bodies = publisher_name is None

    def _default_back() -> None:
        st.query_params.clear()
        st.query_params["paid_supplier"] = supplier_norm
        st.query_params["paid_tier"] = tier
        st.rerun()

    if back_button(back_label, key="prpayline"):
        (on_back or _default_back)()

    hdr = fetch_payments_supplier_header_result(supplier_norm)
    hrow = hdr.data.iloc[0] if (hdr.ok and not hdr.data.empty) else None
    sup_name = _esc(_coalesce(hrow.get("supplier"))) if hrow is not None else _esc(supplier_norm)

    res = (
        fetch_payment_lines_for_supplier_result(supplier_norm, tier)
        if all_bodies
        else fetch_payment_lines_for_pair_result(supplier_norm, publisher_name, tier)
    )
    if not res.ok:
        empty_state("Payment data isn't available right now", "A source/pipeline issue, not an empty result.")
        return
    df = res.data

    # Sub-line: the single paying body, or — for the all-bodies leaf — how many bodies the
    # firm's records span (so the figure's makeup is attributed, not implied as one contract).
    if all_bodies:
        n_bodies = int(df["publisher_name"].nunique()) if not df.empty else 0
        sub = f"{_paid_verb(tier)} by {n_bodies:,} public bod{'ies' if n_bodies != 1 else 'y'}"
    else:
        sub = f"as {_paid_verb(tier)} by {_esc(publisher_name)}"
    st.html(
        f'<div class="pr-prof-head"><div class="pr-prof-kicker">PUBLISHED PAYMENT RECORDS · '
        f"{_esc(_paid_verb(tier).upper())}</div>"
        f'<h1 class="pr-prof-name">{sup_name or "—"}</h1>'
        f'<div class="pr-prof-sub">{sub}</div></div>'
    )

    # Forward edge: offer the firm's canonical /company dossier (awards, lobbying, CRO),
    # closing the council / follow-the-money → supplier → ledger → company path (the line
    # items here are one body × one firm; the dossier is the firm's whole footprint).
    # GATED on the awards register: /company shows "Company not found" for a payments-only
    # firm, so an unregistered supplier gets NO link rather than a dead end. (Was ungated —
    # a false hand-off for the ~86% of paid suppliers that never won a public contract.)
    if supplier_norm in awards_register_norms():
        st.html(company_dossier_cta_html(str(supplier_norm)))

    if df.empty:
        empty_state(
            "No line items in this tier",
            f"No {_paid_verb(tier)} lines naming this firm were published, or the link didn't match.",
        )
        return
    where = "across every public body that published them" if all_bodies else "naming this firm"
    caption = (
        f"{len(df):,} published {_paid_verb(tier)} line{'s' if len(df) != 1 else ''} {where}, biggest first. "
        "Each is the body's own reported figure (over €20,000), not an award ceiling — never summed across "
        "bodies with different VAT bases. Open a line's source ↗ for the body's published disclosure."
    )
    # Recurring-charge caution: count the lines whose exact amount repeats across ≥2 years (a PPP
    # availability / unitary charge). These are flagged inline; warn up front that totalling them
    # overstates spend, since the same charge recurs annually rather than being distinct purchases.
    if "recurring_years" in df.columns:
        n_recurring = int(((df["recurring_years"] >= 2) & df["amount_eur"].notna()).sum())
        if n_recurring:
            caption += (
                f" ⚠️ {n_recurring} line{'s' if n_recurring != 1 else ''} marked **recurring** are an "
                "identical amount repeating across years (a recurring annual / PPP charge) — shown "
                "individually but not meaningful to total as one-off spend."
            )
    st.caption(caption)
    st.html("".join(_payment_line_row(r, tier, show_publisher=all_bodies) for r in df.itertuples()))
    st.html(_PAY_FOOT_HTML)


def _render_supplier_register_footprint(company_num) -> None:
    """Cross-register footprint for a CRO-matched firm: which of the three public-money
    registers (eTenders / TED / public-body payments) the same legal entity appears in, with
    each register's own headline figure side by side. The unified backbone over
    ``v_procurement_entity_chain`` — hard CRO company-number match only (no fuzzy name joins).

    ⚠️ The figures are DIFFERENT GRAINS (award ceilings vs realised payments): shown labelled,
    side by side, and NEVER summed. Absence from a register is coverage, not missing money (only
    a fraction of State spend is published in the payments lists). Skipped when the firm has no
    CRO match or appears in only one register (the rest of the profile already covers that)."""
    if not _truthy(company_num):
        return
    res = fetch_entity_chain_for_company_result(str(company_num))
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    n_reg = _n(r.get("n_registers"))
    if n_reg < 2:
        return  # eTenders only — nothing here the awards section above doesn't already show

    items: list[str] = []
    if _truthy(r.get("in_etenders")):
        v = _eur(r.get("etenders_awarded_value_safe_eur"))
        auth = _n(r.get("etenders_n_authorities"))
        val_pre = f"{v} awarded across " if v != "—" else ""
        items.append(
            f"<li><strong>eTenders (national)</strong> — {val_pre}{auth:,} contracting "
            f"authorit{'y' if auth == 1 else 'ies'} "
            '<span class="pr-notice-tag">award ceiling</span></li>'
        )
    if _truthy(r.get("in_ted")):
        v = _eur(r.get("ted_value_safe_eur"))
        nb = _n(r.get("ted_awards"))
        val_pre = f"{v} awarded across " if v != "—" else ""
        items.append(
            f"<li><strong>TED (EU Official Journal)</strong> — {val_pre}{nb:,} award "
            f"notice{'' if nb == 1 else 's'} "
            '<span class="pr-notice-tag">award ceiling</span></li>'
        )
    if _truthy(r.get("in_payments")):
        paid, comm = _eur(r.get("paid_safe_eur")), _eur(r.get("committed_safe_eur"))
        npub = _n(r.get("payments_n_publishers"))
        money = " · ".join(
            x for x in (f"{paid} paid" if paid != "—" else "", f"{comm} ordered" if comm != "—" else "") if x
        )
        money = money or "present"
        items.append(
            f"<li><strong>Public-body payments</strong> — {money} by {npub:,} "
            f"bod{'y' if npub == 1 else 'ies'} "
            '<span class="pr-notice-tag">realised spend</span></li>'
        )
    st.html(
        '<div class="pr-ted-xref">'
        '<div class="pr-ted-xref-h">Register footprint — the same firm across public money</div>'
        '<div class="pr-ted-xref-b">Matched by Companies Registration Office number, this firm appears '
        f"in <strong>{n_reg} of 3</strong> public-money registers:"
        f'<ul class="pr-notice-list">{"".join(items)}</ul>'
        "These are <strong>different stages</strong> — an award ceiling is what a contract <em>could</em> "
        "be worth; realised spend is what a body <em>reported paying</em>. They are shown separately and "
        "<strong>never added together</strong>. Absence from a register is coverage, not missing money "
        "(only a fraction of State spend is published in the payments lists).</div></div>"
    )


def _render_paid_supplier_panel(supplier_norm: str) -> None:
    """Cross-reference on an eTenders supplier profile: what public bodies actually PAID this
    firm (a later lifecycle stage than the awards above — never added to them)."""
    res = fetch_payments_for_supplier_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    parts = []
    for r in res.data.itertuples():
        val = _eur(getattr(r, "total_safe_eur", None))
        if val == "—":
            continue
        verb = _paid_verb(getattr(r, "realisation_tier", "SPENT"))
        floor = " (indicative floor — mixed VAT bases)" if _truthy(getattr(r, "vat_mixed", None)) else ""
        parts.append(
            f"<strong>{val} {verb}</strong> by {_n(r.n_publishers):,} public "
            f"bod{'ies' if _n(r.n_publishers) != 1 else 'y'}{floor}"
        )
    if not parts:
        return
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Money actually paid (public-body disclosures)</div>'
        f'<div class="pr-ted-xref-b">This firm was {", and ".join(parts)} (over €20k, self-published). '
        "A later stage than the awards above — these are <em>not</em> added to the award totals.</div></div>"
    )

    # Payments-received-per-year trend — the supplier-side mirror of the council spend-over-time
    # spine. Paid and ordered are charted on SEPARATE axes (never stacked — that reads as a sum)
    # and are a different grain from the awards-secured trend higher up (never added to it either).
    # The per-tier .sum() gate is a display decision on the already-fetched frame (no groupby).
    yr = fetch_supplier_payments_by_year_result(supplier_norm)
    if yr.ok and not yr.data.empty and len(yr.data) > 1:
        ydf = yr.data
        if float(ydf["paid_safe_eur"].sum()) > 0:
            st.caption("Money actually paid to this firm per year (sum-safe €) — a later stage than an award")
            st.bar_chart(
                _yr_axis(ydf),
                x="year",
                y="paid_safe_eur",
                x_label="Year",
                y_label="€ paid",
                height=180,
                color="#2f7d5b",
            )
        if float(ydf["ordered_safe_eur"].sum()) > 0:
            st.caption("Money ordered from this firm per year (sum-safe €) — purchase-order commitments, not yet paid")
            st.bar_chart(
                _yr_axis(ydf),
                x="year",
                y="ordered_safe_eur",
                x_label="Year",
                y_label="€ ordered",
                height=180,
                color="#3a6b7e",
            )
        st.caption(
            "Paid and ordered are different stages of public money — shown on separate axes, never added "
            "together, and never added to the contracts-won figures above (an award is a different measure again)."
        )
