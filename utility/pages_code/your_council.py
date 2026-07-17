"""Your Council — ONE consolidated dossier per local authority (Phase 1 of the council-pages
consolidation; see the design sketch in chat / project_council_spending_rebuild memory).

The council is the spine. Three concerns that used to be three separate pages — "Who Runs Your
County" (the appointed Chief Executive + accountability indicators), "Your Councillors" (the elected
side, sandbox/preview data) and "Council Spending" (audited accounts + purchase orders) — are
recomposed here into ONE index → ONE dossier with a section switcher.

SURFACING-ONLY / NO NEW LOGIC. This page imports and orchestrates the existing, already-tested
render functions from local_government.py and procurement.py (the same pattern council_spending.py
already uses). It computes no metric of its own. Phase 1 fully embeds the two GOLD concerns
(Who runs it · Spending); the SANDBOX councillor flow is cross-linked, not deep-embedded, until that
data is promoted (then it becomes a third inline section — Phase 2).

Key alignment that makes this possible: la_chief_executives.local_authority == la_afs_divisions.council
== payments publisher_name (plain names, e.g. "Dublin City", "Dun Laoghaire-Rathdown"). One ?council=
param keys all three.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from functools import partial
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.local_government_data import (
    fetch_chief_executive_result,
    fetch_chief_executives_result,
)
from data_access.procurement_data import fetch_council_summary_result
from data_access.your_councillors_data import fetch_coverage, fetch_roster_council
from pages_code.local_government import (
    _render_ce_hero,
    _render_choropleth,
    _render_performance,
    _render_power_explainer,
)
from pages_code.procurement import (
    _council_summary_row,
    _render_payment_lines,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from pages_code.your_councillors import _pcolour, _render_cathaoirleach, _render_standing_orders, _tab_agendas
from ui.components import (
    back_button,
    clickable_card_link,
    dt_page,
    empty_state,
    hero_banner,
    info_card,
    party_stripe_html,
    subsection_heading,
)
from ui.format import eur

# Council spend headlines dash non-positive amounts ("€0" = not published, not zero) —
# preserves procurement._eur behaviour now that this page owns the alias directly.
_eur = partial(eur, dash_nonpositive=True)

_SECTIONS = ["Who runs it", "Your councillors", "Spending"]

# Province grouping is fixed Irish geography (the 4 historic provinces, North->South), the same basis
# the spending index encodes in SQL. The summary view carries province for the 27 councils with a
# spending row; these 4 fallbacks cover the councils that publish neither POs nor a readable AFS.
_PROVINCE_ORDER = {"Ulster": 1, "Connacht": 2, "Leinster": 3, "Munster": 4}
_PROVINCE_FALLBACK = {"Carlow": "Leinster", "Cavan": "Ulster", "Kerry": "Munster", "Roscommon": "Connacht"}


# ── routing helpers ───────────────────────────────────────────────────────────
def _go(council: str | None = None, *, section: str | None = None) -> None:
    """Navigate to the council hub (or the index when ``council`` is None), optionally landing on a
    section. Clears drill keys so a leaf's Back returns to clean hub state."""
    st.query_params.clear()
    if council:
        st.query_params["council"] = council
    if section:
        st.session_state["yc_section"] = section
    st.rerun()


def _tier_from(params) -> str:
    t = (params.get("paid_tier") or "COMMITTED").upper()
    return t if t in ("SPENT", "COMMITTED") else "COMMITTED"


# ── index (the directory) ─────────────────────────────────────────────────────
def _spend_headline(s) -> str:
    """One civic line for a council's index card: the firmest money it publishes, the audited-accounts
    flag, or an honest 'nothing readable yet'. ``s`` is its v_procurement_council_summary row (or None).
    NEVER blends the two never-summed lifecycle tiers — shows whichever the council actually publishes."""
    if s is None:
        return "No spending published yet"
    if int(s.get("n_paid") or 0) > 0:
        return f"{_eur(s.get('paid_safe_eur'))} paid"
    if int(s.get("n_ordered") or 0) > 0:
        return f"{_eur(s.get('ordered_safe_eur'))} ordered"
    if bool(s.get("has_running")) or bool(s.get("has_building")):
        return "Audited accounts"
    return "No spending published yet"


def _spend_scale(s) -> float:
    """Within-province sort key — biggest publisher first, accounts-only next, empty last. SORT ONLY,
    never a displayed figure and never a sum of the two never-summed tiers."""
    if s is None:
        return -2.0
    paid, ordered = float(s.get("paid_safe_eur") or 0), float(s.get("ordered_safe_eur") or 0)
    if paid or ordered:
        return max(paid, ordered)
    if bool(s.get("has_running")) or bool(s.get("has_building")):
        return -1.0
    return -2.0


def _render_how_councils_work() -> None:
    """Plain-language power model at the index level (display-only static markup). Who really
    decides what in a council is the single most misunderstood thing about local government, and
    until now it lived three clicks deep inside a council dossier. Surfaced here so every visitor
    reads it before picking a council. Reuses the reserved-vs-executive framing / CSS of the
    per-council explainer; no data, no logic."""
    subsection_heading("How your council works — who really holds power")
    st.html(
        '<p class="con-section-note">Two people run every council. The <strong>councillors you '
        "elect</strong> hold only a short list of <strong>reserved functions</strong> — adopt the "
        "county development plan, adopt the annual budget, set the rates. Almost everything else — "
        "planning permissions, contracts, staff, housing allocation — is an <strong>executive "
        "function</strong> carried out by the <strong>Chief Executive, who is appointed, not "
        "elected</strong>.</p>"
    )
    exec_card = (
        '<div class="con-council-card">'
        '<div class="con-council-name">The Chief Executive (appointed)</div>'
        '<div class="con-grain-row">'
        '<span class="con-grain">Planning permissions</span>'
        '<span class="con-grain">Contracts &amp; spending</span>'
        '<span class="con-grain">Council staff</span>'
        '<span class="con-grain">Housing allocation</span>'
        '<span class="con-grain">…everything not reserved</span>'
        "</div></div>"
    )
    reserved_card = (
        '<div class="con-council-card">'
        '<div class="con-council-name">Your councillors (elected)</div>'
        '<div class="con-grain-row">'
        '<span class="con-grain con-grain-rev">Adopt the budget</span>'
        '<span class="con-grain con-grain-rev">Adopt the development plan</span>'
        '<span class="con-grain con-grain-rev">Set the rates</span>'
        '<span class="con-grain con-grain-rev">Appoint the Chief Executive</span>'
        "</div></div>"
    )
    st.html(f'<div class="con-council-grid">{exec_card}{reserved_card}</div>')


def _render_index() -> None:
    hero_banner(
        kicker="YOUR AREA",
        title="Your Council",
        dek="Your county or city council in one place — who runs it (the appointed Chief Executive), "
        "the councillors you elect, and what it spends. Pick a council.",
    )
    # The power model up front (not three clicks deep): who really decides what in local government.
    _render_how_councils_work()
    # Clickable national map first (the visual entry point) — reuses the local_government choropleth,
    # linking each council to this page's ?council= dossier. Degrades silently to the cards if the
    # map geometry/layers aren't available.
    _render_choropleth(link_key="council")

    res = fetch_chief_executives_result()
    if not res.ok or res.data.empty:
        empty_state(
            "Councils aren't available right now",
            "The local-authority roster couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    summ = fetch_council_summary_result()
    srows = {str(r["council"]): r for _, r in summ.data.iterrows()} if summ.ok and not summ.data.empty else {}

    # Bucket the 31 councils into province bands (North->South). province comes from the summary row;
    # the 4 councils with no spending row fall back to the fixed geography map.
    bands: dict[tuple[int, str], list] = defaultdict(list)  # logic_firewall: display_only
    for r in res.data.itertuples():
        la = str(r.local_authority)
        s = srows.get(la)
        prov = str(s["province"]) if s is not None and s.get("province") else _PROVINCE_FALLBACK.get(la, "Leinster")
        bands[(_PROVINCE_ORDER.get(prov, 3), prov)].append((la, r, s))

    for (_order, prov), rows in sorted(bands.items()):
        subsection_heading(prov)
        cards = []
        for la, r, s in sorted(rows, key=lambda t: (-_spend_scale(t[2]), t[0])):
            ce = _h(str(getattr(r, "chief_executive", "") or "—"))
            title = _h(str(getattr(r, "head_title", "") or "Chief Executive"))
            cname = _h(str(getattr(r, "council_name", la) or la))
            inner = (
                f'<div class="con-card-inner"><div class="con-card-name">{cname}</div>'
                f'<div class="con-card-meta">{title}: <strong>{ce}</strong></div>'
                f'<div class="con-card-sub">{_h(_spend_headline(s))}</div></div>'
            )
            cards.append(
                clickable_card_link(
                    href=f"?council={quote(la)}",
                    inner_html=inner,
                    aria_label=f"Open {la} council — who runs it, councillors and spending",
                )
            )
        st.html(f'<div class="con-card-grid">{"".join(cards)}</div>')


# ── the three sections ────────────────────────────────────────────────────────
def _section_who_runs_it(council: str) -> None:
    res = fetch_chief_executive_result(council)
    if not res.ok or res.data.empty:
        empty_state("Not available", f"No Chief Executive record for “{council}”.")
        return
    _render_ce_hero(council, res.data.iloc[0])
    _render_power_explainer(council)
    _render_performance(council)
    st.caption(
        "Performance figures are each council's published whole-area numbers, shown beside the "
        "national benchmark — not apportioned, never summed across measures. Sources: NOAC "
        "Performance Indicator Report · An Bord Pleanála · Dept of Housing Derelict Sites return."
    )


# How honest to be about each council's voting record — the gold coverage view's tier drives the line.
_CLR_TIER_LINE = {
    "roll_call": "records named roll-call votes — open a councillor to see how they voted.",
    "proposer_seconder": "decides most matters by agreement (a proposer and a seconder), so individual "
    "councillor votes are not recorded.",
    "scanned_pending": "publishes its minutes as scanned images we haven't processed yet.",
    "cmis_pending": "publishes its minutes through a meetings portal we haven't processed yet.",
    "unseeded": "has no processed minutes yet.",
}


# Party-coloured roster tiles — the left stripe shades each councillor by party (reusing the dedicated
# page's `_pcolour`) so the roster reads as a breakdown by party at a glance, matching the stripe above it.
_CLR_CSS = """
<style>
.yc-clr-grid {
  display: grid; grid-template-columns: repeat(auto-fill, minmax(225px, 1fr)); gap: 0.5rem; margin: 0.25rem 0 0.7rem;
}
.yc-clr-card {
  position: relative; display: block; background: #fff; border: 1px solid rgba(0,0,0,0.08);
  border-left: 4px solid var(--clr-accent, #9e9e9e); border-radius: 6px; padding: 0.5rem 1.5rem 0.5rem 0.75rem;
  text-decoration: none; box-shadow: 0 1px 2px rgba(0,0,0,0.05); transition: box-shadow .15s, transform .15s;
}
.yc-clr-card:hover { box-shadow: 0 3px 10px rgba(0,0,0,0.1); transform: translateY(-1px); }
.yc-clr-name { display: block; font-weight: 600; color: #16243a; font-size: 0.9rem; line-height: 1.2; }
.yc-clr-party { display: block; font-size: 0.75rem; color: var(--text-secondary, #666); margin-top: 0.08rem; }
.yc-clr-arrow {
  position: absolute; right: 0.6rem; top: 50%; transform: translateY(-50%); color: #c2c2c2; font-size: 0.9rem;
}
</style>
"""


def _councillor_card(council: str, name: str, party: str) -> str:
    """One party-coloured councillor tile linking to that person's full record (votes + pay) on the
    dedicated councillor page — the heavy per-person detail stays there; the roster lives here."""
    href = f"/your-councillors?clr_county={quote(council)}&clr_name={quote(name)}"
    return (
        f'<a class="yc-clr-card" href="{_h(href)}" target="_self" style="--clr-accent:{_pcolour(party)}" '
        f'aria-label="Open {_h(name)} on {_h(council)}">'
        f'<span class="yc-clr-name">{_h(name)}</span>'
        f'<span class="yc-clr-party">{_h(party or "—")}</span>'
        f'<span class="yc-clr-arrow" aria-hidden="true">→</span>'
        "</a>"
    )


def _section_councillors(council: str) -> None:
    # Inline (no longer a cross-link): the roster is PROMOTED gold (v_la_councillors), read via
    # data_access. The whole-council roster is grouped by electoral area for display; per-councillor
    # votes/pay/agendas stay on the dedicated /your-councillors page (linked from each tile).
    subsection_heading("The councillors you elect")
    info_card(
        "Councillors hold the <b>reserved functions</b> — the county development plan, the annual "
        "budget and the rates — while the appointed Chief Executive holds the executive functions "
        "(staff, contracts, planning permissions).",
        border_left_color="#3a6b7e",
    )
    roster = fetch_roster_council(council)
    if not roster.ok or roster.data is None or roster.data.empty:
        empty_state(
            "Roster not published yet",
            f"We don't have a councillor roster for {council} yet — not that it has none.",
        )
        return
    df = roster.data
    leas = _real_leas(df)

    # Council-wide party breakdown + an honest voting-coverage line from the gold coverage view.
    st.html(party_stripe_html(list(Counter(df["party"]).items()), show_legend=True))  # logic_firewall: display_only
    cov = fetch_coverage(council)
    tier = str(cov.data.iloc[0]["tier"]) if cov.ok and cov.data is not None and not cov.data.empty else "unseeded"
    st.caption(
        f"{len(df)} councillors across {len(leas)} local electoral area{'s' if len(leas) != 1 else ''}. "
        f"{council} {_CLR_TIER_LINE.get(tier, _CLR_TIER_LINE['unseeded'])} "
        "Roster sourced from public listings (~96% complete nationally)."
    )

    # The roster itself — grouped by the area people actually vote in, each tile shaded by party.
    # Members with no recorded LEA (a handful of rosters carry one) are shown last under an honest
    # heading, never dropped.
    st.html(_CLR_CSS)

    def _emit(label: str, rows) -> None:
        subsection_heading(label)
        cards = [_councillor_card(council, str(r["name"]), str(r.get("party") or "")) for _, r in rows.iterrows()]
        st.html(f'<div class="yc-clr-grid">{"".join(cards)}</div>')

    lea_norm = df["lea"].map(_lea_label)
    for lea in leas:
        _emit(lea, df[lea_norm == lea])
    unassigned = df[lea_norm == ""]
    if not unassigned.empty:
        _emit("Electoral area not recorded", unassigned)
    st.caption("Open a councillor for their voting record and pay.")

    # How the council runs its meetings — the Cathaoirleach's role + the council's own Standing Orders
    # (or the generic statutory explainer where they aren't parsed) + the recent agendas it has tabled.
    # Composed from the dedicated councillor page's render helpers so both pages stay in sync.
    _render_cathaoirleach()
    _render_standing_orders(council)
    subsection_heading("Recent agendas")
    _tab_agendas(council)


def _section_spending(council: str) -> None:
    if _council_summary_row(council) is None:
        empty_state(
            "No published spending yet",
            f"{council} doesn't publish a machine-readable purchase-order list or audited accounts we "
            "can read yet, so there's nothing to show in this section — not that it has no spending.",
        )
        return
    # The shared per-council dossier (RUNNING / BUILDING / PAYING lanes). show_back=False because the
    # hub already renders one "← All councils" affordance above the section switcher (no double back).
    # Supplier drill-downs inside it are handled by this page's leaf dispatch.
    _render_payments_publisher_profile(council, "COMMITTED", show_back=False)


# ── at-a-glance triptych (the gist of all three concerns, before any switcher) ──
_GLANCE_PREVIEW_BADGE = (
    '<span style="font-size:0.6rem;letter-spacing:0.04em;text-transform:uppercase;'
    "color:#8a6d2f;background:#f4ecd8;border-radius:3px;padding:0.05rem 0.32rem;"
    'margin-left:0.4rem;vertical-align:middle">Preview</span>'
)


def _spend_glance_sub(summ) -> str:
    """Plain-English provenance line for the spending glance card — which register
    the firm figure comes from (never blending the never-summed lifecycle tiers)."""
    if summ is None:
        return "No machine-readable spending we can read yet"
    if int(summ.get("n_paid") or 0) > 0:
        return "Published payments over the disclosure threshold"
    if int(summ.get("n_ordered") or 0) > 0:
        return "Published purchase orders"
    if bool(summ.get("has_running")) or bool(summ.get("has_building")):
        return "From the council's annual financial statements"
    return "No machine-readable spending we can read yet"


def _glance_card(
    council: str, section: str, kicker: str, figure: str, sub: str, accent: str, *, preview: bool = False
) -> str:
    """One whole-card-clickable summary tile — a solid bordered card showing the
    firmest fact a concern publishes, with a left accent stripe. Clicking opens that
    section's deep dive (via the consumable ?yc= param). Display only: every figure
    arrives pre-computed from a registered view."""
    badge = _GLANCE_PREVIEW_BADGE if preview else ""
    href = f"?council={quote(council)}&yc={quote(section)}"
    return (
        f'<a class="yc-glance-card" href="{_h(href)}" target="_self" '
        f'aria-label="Open the {_h(section)} section for {_h(council)}" '
        f'style="--yc-accent:{accent}">'
        f'<span class="yc-glance-eyebrow">{_h(kicker)}{badge}</span>'
        f'<span class="yc-glance-figure">{_h(figure)}</span>'
        f'<span class="yc-glance-sub">{_h(sub)}</span>'
        '<span class="yc-glance-arrow" aria-hidden="true">→</span>'
        "</a>"
    )


# Scoped styling for the at-a-glance triptych. Self-contained (no shared_css edit /
# server-restart dance for a single page's component); the browser dedupes the
# repeated <style> by content.
_GLANCE_CSS = """
<style>
.yc-glance-grid {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 0.7rem; margin: 0.3rem 0 0.2rem;
}
@media (max-width: 760px) { .yc-glance-grid { grid-template-columns: 1fr; } }
.yc-glance-card {
  position: relative; display: flex; flex-direction: column; gap: 0.22rem;
  background: #fff; border: 1px solid rgba(0,0,0,0.08);
  border-left: 4px solid var(--yc-accent, #16243a); border-radius: 8px;
  padding: 0.75rem 1.9rem 0.8rem 0.95rem; text-decoration: none;
  box-shadow: 0 1px 3px rgba(0,0,0,0.06); transition: box-shadow .15s, transform .15s;
}
.yc-glance-card:hover { box-shadow: 0 4px 14px rgba(0,0,0,0.12); transform: translateY(-1px); }
.yc-glance-eyebrow {
  font-size: 0.64rem; letter-spacing: 0.07em; text-transform: uppercase;
  color: var(--text-meta, #6b7280); font-weight: 700;
}
.yc-glance-figure {
  font-size: 1.18rem; font-weight: 700; color: #16243a; line-height: 1.18;
}
.yc-glance-sub { font-size: 0.8rem; color: var(--text-secondary, #555); line-height: 1.3; }
.yc-glance-arrow {
  position: absolute; right: 0.85rem; top: 0.7rem; color: var(--yc-accent, #16243a);
  font-size: 1.05rem; transition: transform .15s;
}
.yc-glance-card:hover .yc-glance-arrow { transform: translateX(3px); }
</style>
"""


def _lea_label(val) -> str:
    """Normalise a roster row's electoral-area value: blank for missing (a few rosters carry a
    member with no recorded LEA, which arrives as NaN/None) so it's never counted or shown as a
    phantom area. Glance and section share this so their area counts always agree."""
    s = str(val or "").strip()
    return "" if s.lower() in ("nan", "none") else s


def _real_leas(df) -> list[str]:
    """The distinct, real electoral areas in a council roster (sorted; missing excluded)."""
    return sorted({lbl for x in df["lea"].tolist() if (lbl := _lea_label(x))})


def _councillor_glance(council: str) -> tuple[str, str]:
    """(figure, sub) for the councillors glance card — the real elected headcount now
    that the roster is promoted to gold (falls back to a qualitative line if absent)."""
    r = fetch_roster_council(council)
    if r.ok and r.data is not None and not r.data.empty:
        n, k = len(r.data), len(_real_leas(r.data))
        plural = "area" if k == 1 else "areas"
        return f"{n} councillors", f"Across {k} local electoral {plural} — they hold the budget, plan & rates"
    return "Elected by you", "They hold the budget, the development plan and the rates"


def _render_glance(council: str, ce_nm: str, head_title: str, summ) -> None:
    """The gist of all three concerns in one 3-up row — who runs it, what it spends,
    the elected side — so the reader takes in the whole council before choosing a
    section. A CSS grid (not st.columns) keeps the three tiles on one clean row."""
    subsection_heading("At a glance")
    cards = [
        _glance_card(
            council,
            "Who runs it",
            "Who runs it",
            ce_nm or "—",
            f"{head_title or 'Chief Executive'} — appointed, not elected",
            "#16243a",
        ),
        _glance_card(council, "Spending", "Spending", _spend_headline(summ), _spend_glance_sub(summ), "#3d719c"),
        _glance_card(council, "Your councillors", "Your councillors", *_councillor_glance(council), "#3a6b7e"),
    ]
    st.html(f'{_GLANCE_CSS}<div class="yc-glance-grid">{"".join(cards)}</div>')


# ── the hub ───────────────────────────────────────────────────────────────────
def _render_hub(council: str) -> None:
    if back_button("← All councils", key="yc_hub_back"):
        _go()
    # A glance-card deep link (?yc=Spending) opens that section once, then is consumed
    # so the switcher below stays in control on subsequent interactions.
    yc = st.query_params.get("yc")
    if yc:
        if yc in _SECTIONS:
            st.session_state["yc_section"] = yc
        del st.query_params["yc"]

    summ = _council_summary_row(council)
    province = str(summ.get("province")) if summ and summ.get("province") else ""
    ce_res = fetch_chief_executive_result(council)
    ce_nm = ""
    head_title = "Chief Executive"
    council_name = council
    if ce_res.ok and not ce_res.data.empty:
        row = ce_res.data.iloc[0]
        ce_nm = str(row.get("chief_executive") or "")
        head_title = str(row.get("head_title") or "Chief Executive")
        council_name = str(row.get("council_name") or council)
    dek_bits = []
    if ce_nm:
        dek_bits.append(f"Run day-to-day by {ce_nm} (appointed {head_title})")
    if province:
        dek_bits.append(province)
    hero_banner(kicker="YOUR COUNCIL", title=council_name, dek=" · ".join(dek_bits))

    # The gist of all three concerns, before any switcher.
    _render_glance(council, ce_nm, head_title, summ)

    # Deep dive: open one concern in full. Seed once so the segmented control and the
    # glance cards both drive the same session-persisted selection (passing both a
    # default= and a session_state value to a keyed widget would warn).
    st.html('<hr style="border:none;border-top:1px solid rgba(0,0,0,0.1);margin:1.5rem 0 1rem">')
    subsection_heading("Explore in detail")
    if st.session_state.get("yc_section") not in _SECTIONS:
        st.session_state["yc_section"] = _SECTIONS[0]
    section = st.segmented_control("Section", _SECTIONS, key="yc_section", label_visibility="collapsed")
    section = section or st.session_state.get("yc_section", _SECTIONS[0])
    if section == "Spending":
        _section_spending(council)
    elif section == "Your councillors":
        _section_councillors(council)
    else:
        _section_who_runs_it(council)


@dt_page
def your_council_page() -> None:
    p = st.query_params

    # ── spending drill-down LEAVES (leaf-first, mirrors council_spending.py routing order) ──
    # A supplier tile inside a council dossier links with BOTH ?paid_supplier= and ?paid_publisher=
    # (the published line items from this council to this supplier). Check before the supplier-only
    # branch so it isn't shadowed.
    if p.get("paid_supplier") and p.get("paid_publisher"):
        name = p.get("paid_publisher")
        _render_payment_lines(
            p.get("paid_supplier"),
            name,
            _tier_from(p),
            on_back=lambda: _go(name, section="Spending"),
            back_label=f"← Back to {name}",
        )
        return
    if p.get("paid_supplier"):
        _render_payments_supplier_profile(
            p.get("paid_supplier"), _tier_from(p), on_back=lambda: _go(), back_label="← All councils"
        )
        return

    council = p.get("council")
    # Legacy / cross-page deep links land the reader on the hub's Spending section.
    if not council and p.get("paid_publisher"):
        _go(p.get("paid_publisher"), section="Spending")
        return

    if council:
        _render_hub(council)
    else:
        _render_index()
