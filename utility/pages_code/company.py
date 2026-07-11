"""Company dossier — one firm's full public-money footprint on one URL.

The entity-first flagship (doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md §2): a supplier's
national contract awards (eTenders), EU award notices (TED, per-notice deep links),
money actually paid (public-body disclosures) and register overlaps (lobbying),
side by side on a first-class page — three registers, three lifecycle stages,
NEVER summed across.

Surfacing only: every aggregation / CRO join / value gate lives in the registered
``v_procurement_*`` views; rendering helpers are shared with pages_code/procurement.py
so the honesty copy (awarded ≠ paid, ceilings, co-occurrence-not-causation) can never
drift between the in-register profile and this dossier. The landing search is a
display-only name filter over the already-fetched ranking.

URL identity: /company?supplier=<supplier_norm> (build links with
utility/ui/entity_links.company_profile_url). CSS reuses the pr-* family.
"""

from __future__ import annotations

import sys
import urllib.parse
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.corporate_data import fetch_corporate_notices_for_company_result
from data_access.lobbying_data import fetch_all_org_names
from data_access.procurement_data import (
    fetch_entity_xref_result,
    fetch_lobbying_overlap_result,
    fetch_payments_for_supplier_result,
    fetch_supplier_summary_result,
)
from pages_code.procurement import (
    _awards_word,
    _card,
    _cro_pill,
    _esc,
    _eur,
    _lobby_pill,
    _n,
    _render_epa_credentials_panel,
    _render_paid_supplier_panel,
    _render_supplier_call_offs_panel,
    _render_supplier_competition_panel,
    _render_supplier_relationships_panel,
    _render_ted_supplier_panel,
    _supplier_awards_section,
    _truthy,
    _value_pill,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    finding_lede,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    text_search_mask,
)
from ui.entity_links import corporate_notices_url, entity_cta_html, lobbying_org_url

_LAND_PAGE = 24  # landing cards per page (multiple of 3 for the pr-grid)

_DOSSIER_FOOT = (
    '<div class="pr-foot"><strong>Sources:</strong> eTenders / national procurement open data '
    '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
    'target="_blank" rel="noopener">data.gov.ie ↗</a>), the EU Official Journal (TED — each EU '
    "notice above links to the official record), public bodies' own published payment lists, the "
    "Companies Registration Office, the Register of Lobbying, the Register of Charities and the "
    "EPA licence &amp; enforcement register (LEAP). Awards, payments and EU notices "
    "are separate registers at different lifecycle stages — never added together. Appearing in any "
    "register is a public record of procurement or lobbying activity, not evidence of wrongdoing.</div>"
)


def _render_corporate_distress_panel(company_num) -> None:
    """Cross-register block on the company dossier: the firm's CRO registration status and
    its corporate notices in Iris Oifigiúil (insolvency / liquidation / receivership /
    register changes), matched on the hard CRO ``company_num``. A SEPARATE public register —
    statutory notices only, never juxtaposed with or added to the firm's money figures above.

    No-inference rails: appearing on a notice is a public record, not a finding of wrongdoing;
    a firm may be the SUBJECT of a notice or a named insolvency practitioner, so the firm's own
    company_status leads (the hard fact about THIS legal entity) and a firm with no CRO-matched
    notice is silently absent — a zero is never implied as a clean record."""
    if not _truthy(company_num):
        return
    try:
        cnum = int(float(company_num))
    except (TypeError, ValueError):
        return
    res = fetch_corporate_notices_for_company_result(cnum)
    if not res.ok or res.data.empty:
        return  # firm holds no CRO-matched corporate notice — silent absence
    df = res.data
    r0 = df.iloc[0]
    n = len(df)

    status_raw = str(r0.get("company_status") or "").strip()
    status_human = "Normal — in good standing" if status_raw == "Normal" else status_raw
    reg_v, diss_v = r0.get("company_reg_date"), r0.get("comp_dissolved_date")
    reg = str(reg_v)[:10] if pd.notna(reg_v) else ""  # str(pd.NaT)[:10] == "NaT" — guard explicitly
    diss = str(diss_v)[:10] if pd.notna(diss_v) else ""

    # Identity lead — the hard fact about THIS legal entity (whether it is itself in distress).
    ident = f"CRO company {cnum}"
    if reg:
        ident += f", registered {_esc(reg)}"
    if status_raw:
        if diss:
            ident += f"; the entity was <strong>{_esc(status_human)}</strong> on {_esc(diss)}"
        else:
            ident += f"; the entity is recorded as <strong>{_esc(status_human)}</strong>"
    ident += "."

    # Display-only breakdown by subtype of THIS firm's already-fetched notices — a
    # render-time count on the active entity's rows, for one sentence. Not a model.
    counts = df["notice_subtype"].fillna("unspecified").value_counts()  # logic_firewall: display_only
    parts = [f"{int(cnt)}× {_esc(str(sub).replace('_', ' '))}" for sub, cnt in counts.items()]
    dates = df["issue_date"].dropna().astype(str).str.slice(0, 10)
    span = ""
    if not dates.empty:
        lo, hi = dates.min(), dates.max()
        span = f" between {_esc(lo)} and {_esc(hi)}" if lo != hi else f" ({_esc(lo)})"

    body = (
        f"{ident} This entity appears on <strong>{n:,} corporate notice{'' if n == 1 else 's'}</strong> "
        f"in Iris Oifigiúil{span}: {'; '.join(parts)}. "
        "These are statutory public notices (insolvency, liquidation, receivership or register "
        "changes) — a firm may appear as the <em>subject</em> of a notice or as a named "
        "practitioner, so this is a public record, not a finding of wrongdoing."
    )
    # Deep-link into the Corporate Notices page (per-notice detail lives there, not here —
    # one home for notice rendering). Search by the firm's own notice entity_name (the exact
    # text that page searches on) so the link reliably lands on these notices.
    name_mode = df["entity_name"].dropna().astype(str)
    search_name = name_mode.mode().iloc[0] if not name_mode.empty else ""
    notices_link = (
        f'<a href="{_esc(corporate_notices_url(search_name))}" target="_self">'
        "See all corporate notices for this firm ↗</a>"
        if search_name
        else ""
    )
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Corporate register notices (CRO / Iris Oifigiúil)</div>'
        f'<div class="pr-ted-xref-b">{body} {notices_link} '
        '<a href="https://core.cro.ie/search" target="_blank" rel="noopener">'
        "Check the CRO register ↗</a></div></div>"
    )


def _render_charity_register_panel(supplier_norm: str) -> None:
    """Cross-register block: this supplier's name also appears on the Register of Charities.

    Many public-service providers (youth, disability, homelessness, food-poverty) are
    registered charities that ALSO hold state contracts, so the register is genuinely relevant
    on a supplier dossier. Matched on the shared canonical name key (v_supplier_entity_xref) —
    a NAME co-occurrence across two public registers, framed as such, never an assertion that
    the contracting entity IS that charity. A supplier with no name match is silently absent
    (a zero is never implied as "not a charity")."""
    res = fetch_entity_xref_result(supplier_norm)
    if not res.ok or res.data.empty:
        return
    if not _truthy(res.data.iloc[0].get("is_charity")):
        return  # no matching entry on the Register of Charities — silent absence
    body = (
        "This supplier's name also appears on the <strong>Register of Charities</strong>. "
        "Many public-service providers are registered charities that also hold state "
        "contracts; this is a public-record name match across two separate registers, not a "
        "finding about the contracting entity."
    )
    reg_link = (
        '<a href="https://www.charitiesregulator.ie/en/information-for-the-public/'
        'search-the-register-of-charities" target="_blank" rel="noopener">'
        "Search the Register of Charities ↗</a>"
    )
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Register of Charities</div>'
        f'<div class="pr-ted-xref-b">{body} {reg_link}</div></div>'
    )


def _resolved_lobby_url(supplier_norm: str) -> str:
    """The lobbying-record URL for a supplier IF its overlap name exactly resolves on the
    Lobbying page, else "". The procurement↔lobbying overlap is a fuzzy name co-occurrence
    and the Lobbying page validates org names exactly (~64% match), so this membership-checks
    against the register's org-name set — the link can never dead-end."""
    ov = fetch_lobbying_overlap_result()
    if not ov.ok or ov.data.empty:
        return ""
    m = ov.data[ov.data["supplier_norm"] == supplier_norm]
    if m.empty:
        return ""
    # A supplier can carry several overlap name variants (e.g. "Grant Thornton" vs
    # "GRANT THORNTON LIMITED"); return the first that resolves on the register, not iloc[0].
    org_names = set(fetch_all_org_names())
    for lobby_name in m["lobby_name"].dropna().astype(str):
        if lobby_name in org_names:
            return lobbying_org_url(lobby_name)
    return ""


def _lobby_pill_for(row, supplier_norm: str) -> str:
    """Lobbying chip for the dossier hero: a cross-page link to the firm's lobbying record
    when its register name resolves, else the shared plain badge (on the register but the
    fuzzy-matched name doesn't resolve) — never a dead link. Empty when not on the register.
    Only for the hero pills row, which is NOT inside a clickable card (no nested anchors)."""
    if not _truthy(getattr(row, "on_lobbying_register", None)):
        return ""
    url = _resolved_lobby_url(supplier_norm)
    if not url:
        return _lobby_pill(row)
    return f'<a class="pr-pill pr-pill-lob" href="{_esc(url)}" target="_self">also on lobbying register ↗</a>'


def _dossier(supplier_norm: str) -> None:
    if back_button("← All companies", key="co_back"):
        st.query_params.clear()
        st.rerun()

    sup = fetch_supplier_summary_result(limit=None)
    if not sup.ok:
        empty_state(
            "Company data isn't available right now",
            "The procurement views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    match = sup.data[sup.data["supplier_norm"] == supplier_norm] if not sup.data.empty else sup.data
    if match.empty:
        empty_state(
            "Company not found",
            "That link didn't match a company on the procurement register. Use Back to search all companies.",
        )
        return
    row = match.iloc[0]

    n_awards, n_auth = _n(row.get("n_awards")), _n(row.get("n_authorities"))
    # Role-clarity subtitle (Money nav declutter Phase 2): what THIS supplier surface
    # is for — the whole-firm dossier. The awards league table is Procurement's
    # Suppliers lens; the navigable payment ledger is Follow the Money.
    st.html(
        f'<div class="pr-prof-head"><h1 class="pr-prof-name">{_esc(row.get("supplier"))}</h1>'
        '<div class="pr-prof-sub">Everything one firm touches — awards, payments, lobbying, '
        "corporate — on one page. Three registers, never summed.</div></div>"
    )
    pills = [_value_pill(row.get("awarded_value_safe_eur"))]
    pills += [p for p in (_cro_pill(row), _lobby_pill_for(row, supplier_norm)) if p]
    st.html(f'<div class="pr-pills" style="margin:0.1rem 0 0.6rem">{"".join(pills)}</div>')

    # Finding lede — the dossier's own facts, every figure straight off the view rows.
    sentences = [
        f"<strong>{n_awards:,}</strong> recorded contract award{'s' if n_awards != 1 else ''} "
        f"from <strong>{n_auth:,}</strong> public bod{'ies' if n_auth != 1 else 'y'} "
        "on the national register."
    ]
    paid = fetch_payments_for_supplier_result(supplier_norm)
    if paid.ok and not paid.data.empty:
        for r in paid.data.itertuples():
            if str(getattr(r, "realisation_tier", "")) != "SPENT":
                continue
            val = _eur(getattr(r, "total_safe_eur", None))
            if val == "—":
                continue
            floor = " (an indicative floor — mixed VAT bases)" if _truthy(getattr(r, "vat_mixed", None)) else ""
            sentences.append(
                f"Public bodies that publish their payment lists report <strong>{val}</strong> "
                f"actually paid to this firm by {_n(r.n_publishers):,} "
                f"bod{'ies' if _n(r.n_publishers) != 1 else 'y'}{floor} — "
                "a later lifecycle stage, never added to the award figures."
            )
            break
    if _truthy(getattr(row, "on_lobbying_register", None)):
        sentences.append(
            "The firm also appears on the Register of Lobbying — a co-occurrence of two "
            "public records, not evidence of influence."
        )
    finding_lede(sentences)
    # Outbound edge into the payment graph (Money nav declutter Phase 2, §7): the lede
    # summarises what bodies reported paying; Follow the Money holds the navigable
    # ledger behind that figure. GATED on the same non-empty payments fetch — the FtM
    # node joins by string equality on this exact supplier_norm, so a non-empty result
    # guarantees the hand-off resolves; a firm with no payment lines gets no link
    # (never a false hand-off). The tier follows the rows present, preferring actual
    # payments over purchase-order commitments (a display-only membership check).
    if paid.ok and not paid.data.empty:
        ledger_tiers = {str(t) for t in paid.data["realisation_tier"].dropna()}
        ledger_tier = "SPENT" if "SPENT" in ledger_tiers or not ledger_tiers else "COMMITTED"
        st.html(
            '<div style="margin:-0.25rem 0 0.9rem">'
            + entity_cta_html(
                f"/follow-the-money?flow_supplier_lines={urllib.parse.quote(supplier_norm)}"
                f"&paid_tier={ledger_tier}",
                "See the payment ledger behind this figure →",
            )
            + "</div>"
        )

    _supplier_awards_section(row, supplier_norm)
    # cross_page=True: buyer names link to /rankings-procurement?authority=… (a full
    # cross-page nav from this dossier), closing the supplier↔buyer loop.
    _render_supplier_call_offs_panel(supplier_norm, cross_page=True)
    _render_paid_supplier_panel(supplier_norm)
    _render_ted_supplier_panel(supplier_norm)
    _render_supplier_competition_panel(supplier_norm)
    _render_supplier_relationships_panel(supplier_norm, cross_page=True)
    _render_epa_credentials_panel(row.get("company_num"))
    _render_corporate_distress_panel(row.get("company_num"))
    _render_charity_register_panel(supplier_norm)
    st.html(_DOSSIER_FOOT)


def _cnum(v) -> int | None:
    """Coerce a CRO company_num cell (int/float/str/None/NaN) to int, or None."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _landing() -> None:
    hero_banner(
        kicker="PUBLIC MONEY",
        title="Companies",
        dek="Every firm on the national procurement register, searchable — each opens a "
        "dossier of its contract awards, EU notices, payments received and register overlaps.",
    )
    res = fetch_supplier_summary_result(limit=None)
    if not res.ok:
        empty_state(
            "Company data isn't available right now",
            "The procurement views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    df = res.data
    ranks = {str(r.supplier_norm): i for i, r in enumerate(df.itertuples(), start=1)}
    has_epa = df["has_epa_licence"] if "has_epa_licence" in df.columns else pd.Series(False, index=df.index)

    q = st.text_input(
        "Search companies",
        placeholder="Search by company name…",
        key="co_q",
        label_visibility="collapsed",
    )
    # Discovery filter: surface the firms that hold an EPA environmental licence (otherwise
    # the EPA panel is only findable by already knowing a firm's name). Factual register
    # membership only — no enforcement/inference framing on the list. The membership is the
    # precomputed has_epa_licence flag on v_procurement_supplier_summary (the EPA-index join
    # was graduated out of the page); here we only count + filter that column.
    epa_supplier_count = int(has_epa.sum())
    epa_only = (
        st.checkbox(f"Only firms with an EPA licence ({epa_supplier_count:,})", key="co_epa_only")
        if epa_supplier_count
        else False
    )
    view = df
    qs = (q or "").strip()
    if qs:
        view = view[text_search_mask(view, qs, ["supplier"])]
    if epa_only:
        view = view[has_epa.reindex(view.index, fill_value=False)]
    total = len(view)
    epa_clause = " holding an EPA licence" if epa_only else ""
    st.caption(
        f"{total:,} companies{epa_clause}"
        + (f' matching "{qs}"' if qs else (" ranked by number of contract awards" if not epa_only else ""))
        + ". Click a company for its full public-money dossier."
    )
    if total == 0:
        empty_state("No companies match", "Try a shorter search term.")
        return

    page_idx = paginate(total, key_prefix="co_land", page_size=_LAND_PAGE)
    page = view.iloc[page_idx * _LAND_PAGE : (page_idx + 1) * _LAND_PAGE]
    cards = []
    for r in page.itertuples():
        meta = (
            f"{_awards_word(_n(r.n_awards))} · "
            f"{_n(r.n_authorities):,} authorit{'ies' if _n(r.n_authorities) != 1 else 'y'}"
        )
        pills = [_value_pill(r.awarded_value_safe_eur)]
        pills += [p for p in (_cro_pill(r), _lobby_pill(r)) if p]
        if bool(getattr(r, "has_epa_licence", False)):
            pills.append('<span class="pr-pill pr-pill-epa">EPA-licensed</span>')
        inner = _card(f"<span>{_esc(r.supplier)}</span>", meta, pills, rank=ranks.get(str(r.supplier_norm)))
        cards.append(
            clickable_card_link(
                href=f"?supplier={urllib.parse.quote(str(r.supplier_norm))}",
                inner_html=inner,
                aria_label=f"Open the public-money dossier of {r.supplier}",
            )
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html('<div style="height:1rem"></div>')
    pagination_controls(
        total,
        key_prefix="co_land",
        page_sizes=(_LAND_PAGE,),
        default_page_size=_LAND_PAGE,
        label="companies",
    )
    st.html(_DOSSIER_FOOT)


def company_page() -> None:
    hide_sidebar()
    supplier = st.query_params.get("supplier")
    if supplier:
        _dossier(supplier)
    else:
        _landing()
