"""Company dossier — one firm's full public-money footprint on one URL.

The entity-first flagship (doc/APP_REDESIGN_SWEEP_2026_06_10.md §2): a supplier's
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
from data_access.procurement_data import (
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

_LAND_PAGE = 24  # landing cards per page (multiple of 3 for the pr-grid)

_DOSSIER_FOOT = (
    '<div class="pr-foot"><strong>Sources:</strong> eTenders / national procurement open data '
    '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
    'target="_blank" rel="noopener">data.gov.ie ↗</a>), the EU Official Journal (TED — each EU '
    "notice above links to the official record), public bodies' own published payment lists, the "
    "Companies Registration Office, the Register of Lobbying and the EPA licence &amp; enforcement "
    "register (LEAP). Awards, payments and EU notices "
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

    # Display-only breakdown by subtype (small per-firm frame — counts only, no business logic).
    counts = df["notice_subtype"].fillna("unspecified").value_counts()
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
    st.html(
        '<div class="pr-ted-xref"><div class="pr-ted-xref-h">Corporate register notices (CRO / Iris Oifigiúil)</div>'
        f'<div class="pr-ted-xref-b">{body} '
        '<a href="https://core.cro.ie/search" target="_blank" rel="noopener">'
        "Check the CRO register ↗</a></div></div>"
    )


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
    st.html(
        f'<div class="pr-prof-head"><h1 class="pr-prof-name">{_esc(row.get("supplier"))}</h1>'
        f'<div class="pr-prof-sub">Public-money dossier — three registers, never summed</div></div>'
    )
    pills = [_value_pill(row.get("awarded_value_safe_eur"))]
    pills += [p for p in (_cro_pill(row), _lobby_pill(row)) if p]
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

    _supplier_awards_section(row, supplier_norm)
    _render_supplier_call_offs_panel(supplier_norm)
    _render_paid_supplier_panel(supplier_norm)
    _render_ted_supplier_panel(supplier_norm)
    _render_supplier_competition_panel(supplier_norm)
    _render_supplier_relationships_panel(supplier_norm)
    _render_epa_credentials_panel(row.get("company_num"))
    _render_corporate_distress_panel(row.get("company_num"))
    st.html(_DOSSIER_FOOT)


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

    q = st.text_input(
        "Search companies",
        placeholder="Search by company name…",
        key="co_q",
        label_visibility="collapsed",
    )
    view = df
    qs = (q or "").strip()
    if qs:
        view = df[text_search_mask(df, qs, ["supplier"])]
    total = len(view)
    st.caption(
        f"{total:,} companies"
        + (f' matching "{qs}"' if qs else " ranked by number of contract awards")
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
