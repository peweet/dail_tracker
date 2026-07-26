"""Public-body (buyer) dossier — one public body's public-money footprint on one URL.

The buyer-side mirror of the company dossier, and the reciprocal that closes the
supplier↔body loop (doc/NAVIGATION_GRAPH.md — the buyer side had no canonical node). A
body's money story is otherwise shattered across three drills (procurement ?authority=,
public_payments ?publisher=, council_spending ?council=); this page unifies them, keyed by
?buyer=<buyer_id or any register name> and resolved through the curated buyer_xref crosswalk.

Two money lanes, DISTINCT grains, NEVER summed: AWARDED (eTenders contract ceilings) and
ORDERED/PAID (published purchase orders / payments). Each carries its own verb-led total; the
page shows them side by side and never adds them (caveats.MONEY_GRAINS).

Surfacing only: composition, the crosswalk resolution and the grain rules live in
dail_tracker_core.dossiers.build_buyer_dossier; this page renders and computes nothing. The
/company hand-off on each supplier is gated on the awards register (awards_register_norms) so a
payments-only firm never links into a "Company not found" dead end. CSS reuses the pr-* family.

URL identity: /body?buyer=<buyer> (build links with utility/ui/entity_links.body_profile_url).
"""

from __future__ import annotations

import sys
from functools import partial
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.procurement_data import awards_register_norms, fetch_buyer_dossier, fetch_buyer_index
from pages_code.procurement import _card, _paid_pill, _paid_verb, _value_pill
from ui.components import back_button, clickable_card_link, dt_page, empty_state, hero_banner, text_search_mask
from ui.entity_links import body_profile_url, company_profile_url
from ui.format import esc as _esc
from ui.format import eur, to_int as _n

_eur = partial(eur, dash_nonpositive=True)

_BUYER_TYPE_LABEL = {
    "local_authority": "Local authority",
    "state_body": "State body",
    "education_body": "Education body",
    "department": "Government department",
    "health_body": "Health body",
    "semi_state": "Semi-state",
}

_FOOT = (
    '<div class="pr-foot"><strong>Sources:</strong> eTenders / national procurement open data '
    "(awards); public-body &amp; council purchase-order / payment disclosures (payments). "
    "Awarded ceilings, ordered and paid are <strong>different stages of public money and are "
    "never added together</strong>. Totals are sum-safe floors.</div>"
)


def _supplier_link(name: str, norm: object, supplier_class: object, awards_norms: frozenset[str]) -> str:
    """A supplier name as a gated /company door. Company-class + on the awards register →
    link; otherwise plain text (individuals are quarantined; payments-only firms have no
    dossier, so no false hand-off)."""
    safe = _esc(name or "—")
    if str(supplier_class) != "company":
        return safe
    key = str(norm) if norm is not None else ""
    if key and key in awards_norms:
        return f'<a class="dt-member-link" href="{_esc(company_profile_url(key))}" target="_self">{safe}</a>'
    return safe


def _render_awards_lane(awards: dict, awards_norms: frozenset[str]) -> None:
    summary = awards.get("summary")
    recent = awards.get("recent") or []
    if not summary and not recent:
        return
    st.html('<h2 class="section-heading">Money awarded — contract ceilings</h2>')
    if summary:
        n_awards, n_suppliers = _n(summary.get("n_awards")), _n(summary.get("n_suppliers"))
        pills = [_value_pill(summary.get("awarded_value_safe_eur"))]
        meta = f"{n_awards:,} award{'s' if n_awards != 1 else ''} · {n_suppliers:,} supplier{'s' if n_suppliers != 1 else ''}"
        st.html(f'<div class="pr-grid">{_card("<span>Awarded (eTenders)</span>", meta, pills)}</div>')
    st.caption("An award ceiling is the maximum a contract may be worth — not money spent. Newest awards:")
    cards = []
    for i, a in enumerate(recent[:12], start=1):
        name = _supplier_link(a.get("supplier"), a.get("supplier_norm"), a.get("supplier_class"), awards_norms)
        title = _esc(a.get("tender_title")) or _esc(a.get("cpv_description")) or "—"
        when = _esc(str(a.get("award_date"))[:10]) if a.get("award_date") else ""
        meta = " · ".join(x for x in (when, _esc(a.get("cpv_description") or "")) if x)
        pills = [_value_pill(a.get("value_eur"))] if a.get("value_safe_to_sum") else []
        cards.append(_card(f"<span>{name}</span><div class='pr-sub'>{title}</div>", meta, pills, rank=i))
    if cards:
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _render_payments_lane(payments: dict, awards_norms: frozenset[str]) -> None:
    summary = payments.get("summary")
    top = payments.get("top_suppliers") or []
    tier = payments.get("active_tier", "COMMITTED")
    if not summary:
        st.html('<h2 class="section-heading">Money paid</h2>')
        st.caption("No purchase-order or payment disclosures are published for this body.")
        return
    ordered, paid = summary.get("ordered_safe_eur"), summary.get("paid_safe_eur")
    st.html(f'<h2 class="section-heading">Money {_esc(_paid_verb(tier))} — published records</h2>')
    n_sup = _n(summary.get("n_suppliers"))
    # BOTH lifecycle totals side by side, never summed.
    pills = []
    if _n(summary.get("n_ordered_lines")):
        pills.append(_paid_pill(ordered, "COMMITTED"))
    if _n(summary.get("n_paid_lines")):
        pills.append(_paid_pill(paid, "SPENT"))
    meta = (
        f"{n_sup:,} supplier{'s' if n_sup != 1 else ''} · {_n(summary.get('min_year'))}–{_n(summary.get('max_year'))}"
    )
    st.html(f'<div class="pr-grid">{_card("<span>Purchase orders &amp; payments</span>", meta, pills)}</div>')
    st.caption(f"Top suppliers by money {_paid_verb(tier)} (sum-safe within this body):")
    cards = []
    for i, s in enumerate(top[:15], start=1):
        name = _supplier_link(s.get("supplier"), s.get("supplier_normalised"), s.get("supplier_class"), awards_norms)
        np_ = _n(s.get("n_payments"))
        meta = f"{np_:,} line{'s' if np_ != 1 else ''} · {_n(s.get('min_year'))}–{_n(s.get('max_year'))}"
        cards.append(_card(f"<span>{name}</span>", meta, [_paid_pill(s.get("total_safe_eur"), tier)], rank=i))
    if cards:
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


def _dossier(buyer: str) -> None:
    if back_button("← All public bodies", key="body_back"):
        st.query_params.clear()
        st.rerun()
    d = fetch_buyer_dossier(buyer)
    if d is None:
        empty_state(
            "Public body not found",
            "That link didn't resolve to a body in the curated crosswalk. Use Back to browse all bodies.",
        )
        return
    ident = d["identity"]
    type_badge = _BUYER_TYPE_LABEL.get(ident.get("buyer_type"), "Public body")
    hero_banner(
        kicker="PUBLIC BODY",
        title=ident["display_name"],
        dek="Everything one public body spends — contract awards and published payments, on one "
        "page. Two registers, two lifecycle stages, never summed.",
        badges=[type_badge],
    )
    awards_norms = awards_register_norms()
    _render_awards_lane(d["awards"], awards_norms)
    _render_payments_lane(d["payments"], awards_norms)
    st.html(f'<div class="pr-foot" style="margin-top:0.5rem">{_esc(d["caveat"])}</div>')
    st.html(_FOOT)


def _landing() -> None:
    hero_banner(
        kicker="PUBLIC BODIES",
        title="Public bodies — the buyer side of public money",
        dek="One public body's contract awards and published payments in one place — the "
        "buyer-side mirror of the company dossier.",
    )
    index = fetch_buyer_index()
    if not index:
        empty_state(
            "No bodies available", "The buyer crosswalk couldn't be loaded — a source issue, not an empty result."
        )
        return
    q = st.text_input(
        "Search public bodies",
        placeholder="Search a council, department or state body…",
        key="body_search_q",
        label_visibility="collapsed",
    )
    qs = (q or "").strip()
    rows = index
    if qs:
        import pandas as pd

        df = pd.DataFrame(index)
        rows = df[text_search_mask(df, qs, ["display_name"])].to_dict("records")
        if not rows:
            empty_state("No matches", "Try a shorter term — bodies are named as the awards register lists them.")
            return
    st.caption(
        f"{len(rows):,} public bod{'ies' if len(rows) != 1 else 'y'} in the crosswalk. Open one to follow its money."
    )
    cards = []
    for r in rows[:120]:
        type_badge = _BUYER_TYPE_LABEL.get(r.get("buyer_type"), "Public body")
        inner = _card(f"<span>{_esc(r['display_name'])}</span>", type_badge, [])
        cards.append(
            clickable_card_link(
                href=body_profile_url(r["buyer_id"]),
                inner_html=inner,
                aria_label=f"Open the public-money dossier for {r['display_name']}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')


@dt_page
def body_page() -> None:
    buyer = st.query_params.get("buyer")
    if buyer:
        _dossier(buyer)
    else:
        _landing()
