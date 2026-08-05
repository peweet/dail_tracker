from __future__ import annotations


import streamlit as st

from data_access.procurement_data import (
    fetch_afs_national_by_division_result,
    fetch_afs_national_by_year_result,
    fetch_eu_tam_state_aid_count_result,
    fetch_eu_tam_state_aid_result,
)
from ui.components import (
    empty_state,
)

from ui.format import esc as _esc
from ui.format import truthy as _truthy

from ._shared import (
    _TOP,
    _eur,
    _eur_scale,
    _yr_axis,
    _afs_bar_row,
)

from .councils import _lane_header


def _render_afs_national() -> None:
    """NATIONAL amalgamated AFS — what all 31 county & city councils spend by service, audited
    (2016–2023). The only COMPLETE, AUDITED national picture of local-government finance: a BUDGET
    grain, a sibling of the per-council AFS, NEVER summed with the over-€20k purchase orders below.
    Surfacing-only: figures arrive pre-aggregated/pre-ordered from the views. Silent no-op if the
    amalgamated fact isn't present."""
    by_year_res = fetch_afs_national_by_year_result()
    if not by_year_res.ok or by_year_res.data.empty:
        return
    ay = by_year_res.data
    years = {int(y) for y in ay["year"].dropna()}
    latest = max(years)
    span = f"{min(years)}–{latest}" if len(years) > 1 else str(latest)
    lrow = ay[ay["year"] == latest].iloc[0]
    gross, net = lrow.get("gross_expenditure_eur"), lrow.get("net_expenditure_eur")

    st.html(
        _lane_header(
            "ALL 31 COUNCILS · revenue account, audited",
            "What local government spends, nationally",
            "Every county and city council publishes audited <strong>Annual Financial Statements</strong>. "
            f"Amalgamated across all 31 ({_esc(span)}), the audited <strong>{latest}</strong> accounts show "
            f"<strong>{_eur_scale(gross)}</strong> of gross service spending — of which "
            f"<strong>{_eur_scale(net)}</strong> is the <strong>net cost</strong> the local taxpayer funds "
            "(rates, Local Property Tax, State grant) after each service’s own income and grants. This is the "
            "<strong>complete audited picture</strong>; the council-by-council purchase orders below are a far "
            "narrower over-€20,000 slice, and the two are <strong>never added together</strong>.",
        )
    )

    bd_res = fetch_afs_national_by_division_result(latest)
    if bd_res.ok and not bd_res.data.empty:
        st.caption(f"Net cost to the local taxpayer by service, all councils, {latest} — bar width = net cost")
        bd = bd_res.data
        nets = [float(r.net_expenditure_eur) for r in bd.itertuples() if _truthy(r.net_expenditure_eur)]
        max_net = max([n for n in nets if n > 0], default=0.0)
        rows = []
        has_misc = False
        for r in bd.itertuples():
            net = getattr(r, "net_expenditure_eur", None)
            is_pos = _truthy(net) and float(net) > 0
            if r.division == "Miscellaneous Services":
                has_misc = True
            fig = f"<strong>{_eur(net)}</strong>" if is_pos else '<span class="pr-afsbar-zero">income covers it</span>'
            note = (
                "carries the rates / Local Property Tax income — not a single service"
                if r.division == "Miscellaneous Services"
                else ""
            )
            rows.append(
                _afs_bar_row(r.division, net if is_pos else 0, max_net, fig_html=fig, note=note, accent="#3a6b7e")
            )
        st.html(f'<div class="pr-afsbars">{"".join(rows)}</div>')
        if has_misc:
            st.caption(
                "“Miscellaneous Services” carries councils’ rates / Local Property Tax income, so it can show "
                "as more than covered (a negative net cost) — it isn’t a single service."
            )

    # Net cost over time — the national spine (a distinct teal from the PO chart's brown).
    if len(ay) > 1:
        st.caption("Net cost funded by the local taxpayer per year (revenue account, audited €)")
        st.bar_chart(
            _yr_axis(ay),
            x="year",
            y="net_expenditure_eur",
            x_label="Year",
            y_label="Net € funded locally",
            height=180,
            color="#3a6b7e",
        )
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> Department of Housing’s audited amalgamation of all 31 '
        "local authorities’ Annual Financial Statements (gov.ie). A whole-budget BUDGET measure — never added "
        "to the over-€20,000 purchase-order euros below, a different and far narrower register.</div>"
    )
    st.html('<div style="height:1.2rem"></div>')


def _render_eu_tam() -> None:
    """EU State-Aid Transparency register — the grant/subsidy leg of public support (IDA,
    Enterprise Ireland, DAFM…). A DIFFERENT instrument from contract awards: subsidies, not
    purchases, and never summed with eTenders/TED values. Ranked by aid_element_value (the real
    subsidy value). Surfacing-only: the view pre-orders; this lists named awards, never totals."""
    res = fetch_eu_tam_state_aid_result(limit=_TOP)
    if not res.ok or res.data.empty:
        empty_state(
            "EU State-Aid data isn't available right now",
            "The State-Aid register couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    raw = res.data
    # Set aside rows the view flags as scheme-level totals mis-recorded against one beneficiary
    # (a source artifact — see the view header). Display-only split; the flag is computed upstream.
    suspect = raw["aid_element_suspect_scheme_total"].fillna(False).astype(bool)
    n_setaside = int(suspect.sum())
    df = raw[~suspect]
    if df.empty:
        empty_state("EU State-Aid data isn't available right now", "No per-beneficiary awards to show.")
        return
    years = df["date_granted"].dropna().astype(str).str[:4]
    span = f"{years.min()}–{years.max()}" if not years.empty else ""
    top = df.iloc[0]
    # Disclose the full corpus size rather than silently show only the top slice.
    cnt = fetch_eu_tam_state_aid_count_result()
    total_awards = int(cnt.data.iloc[0]["n_awards"]) if cnt.ok and not cnt.data.empty else len(df)
    # "disclosed", not "disclosed awards": the sentence below already supplies the noun, and the
    # two together rendered as "…of 7,968 disclosed awards EU State-Aid awards…" (found in the
    # 2026-08-01 render audit — the register was unreachable by URL until then, so nobody saw it).
    of_total = f" of <strong>{total_awards:,}</strong> disclosed" if total_awards > len(df) else ""
    st.html(
        '<p class="pr-cap">'
        f"The largest <strong>{len(df)}</strong>{of_total} EU State-Aid awards to Irish beneficiaries"
        f"{f' ({_esc(span)})' if span else ''} — grants and subsidies from bodies like IDA Ireland, "
        "Enterprise Ireland and the Dept of Agriculture, published on the EU Transparency register. "
        f"The largest here is <strong>{_eur_scale(top.get('aid_element_value'))}</strong> to "
        f"<strong>{_esc(top.get('beneficiary_name'))}</strong>. These are <strong>subsidies, a different "
        "instrument from contract awards</strong> — a separate register, never added to eTenders / TED "
        "values. Ranked by the actual subsidy value (aid element), not the often-blank nominal amount.</p>"
    )
    cards: list[str] = []
    for i, r in enumerate(df.itertuples(), start=1):
        beneficiary = _esc(getattr(r, "beneficiary_name", None)) or "—"
        authority = _esc(getattr(r, "granting_authority", None))
        measure = _esc(getattr(r, "aid_measure_title", None))
        date = _esc(str(getattr(r, "date_granted", "") or ""))[:10]
        meta = " · ".join(p for p in (authority, measure, date) if p)
        pills = [
            f'<span class="pr-pill pr-pill-val">{_eur_scale(getattr(r, "aid_element_value", None))} aid element</span>'
        ]
        btype = _esc(getattr(r, "beneficiary_type", None))
        if btype:
            pills.append(f'<span class="pr-pill">{btype}</span>')
        url = str(getattr(r, "award_detail_url", "") or "")
        src = (
            f'<div class="pr-card-src"><a href="{_esc(url)}" target="_blank" rel="noopener">EU register ↗</a></div>'
            if url.startswith("http")
            else ""
        )
        cards.append(
            f'<div class="pr-card"><div class="pr-card-head"><span class="pr-rank">#{i}</span>'
            f'<div class="pr-name">{beneficiary}</div></div>'
            f'<div class="pr-meta">{meta}</div>'
            f'<div class="pr-pills">{"".join(pills)}</div>{src}</div>'
        )
    st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    if n_setaside:
        st.html(
            f'<div class="pr-caveat"><strong>{n_setaside} row{"s" if n_setaside != 1 else ""} set aside.</strong> '
            "The EC register occasionally records a <strong>scheme’s whole budget against a single named "
            "beneficiary</strong> (e.g. a horticulture investment scheme’s total shown against one firm). "
            f"{'Those rows are' if n_setaside != 1 else 'That row is'} excluded from this ranking as a "
            "source artifact, not a real single-firm award.</div>"
        )
    st.html(
        '<div class="pr-foot"><strong>Source:</strong> European Commission State-Aid Transparency '
        "register (awards over €100,000). Aid element is the subsidy value the grantor reported; "
        "tax-incentive measures and aid to individuals are excluded. A disclosed award is a public "
        "record of State support, not evidence of wrongdoing — and is never added to contract-award "
        "values, a different register.</div>"
    )
