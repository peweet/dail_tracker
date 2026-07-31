from __future__ import annotations


import streamlit as st

from data_access.procurement_data import (
    fetch_afs_by_division_result,
    fetch_afs_capital_by_division_result,
    fetch_afs_capital_by_year_result,
    fetch_afs_total_by_year_result,
    fetch_afs_vs_po_coverage_result,
    fetch_council_summary_result,
    fetch_la_budget_divisions_result,
    fetch_la_budget_vs_actual_result,
)
from ui.components import (
    clickable_card_link,
    empty_state,
)

from ui.format import esc as _esc
from ui.format import to_int as _n
from ui.format import truthy as _truthy

from ._shared import (
    _eur,
    _yr_axis,
    _card,
    _afs_bar_row,
)




def _council_tier_pills(row) -> list[str]:
    """The lifecycle pill(s) a council carries: solid 'paid' (actual payments, the firmest
    fact) and/or dashed 'ordered' (purchase-order commitments, provisional). Different stages
    of public money — shown side by side, NEVER summed. The verb is the accessible carrier
    (colour-/border-independent); the dashed/solid contrast is the visual one. In this corpus
    a council has exactly one, but both are handled so the view stays honest if that changes."""
    pills = []
    if _n(getattr(row, "n_paid", 0)) > 0:
        pills.append(f'<span class="pr-pill pr-pill-paid">{_eur(row.paid_safe_eur)} paid</span>')
    if _n(getattr(row, "n_ordered", 0)) > 0:
        pills.append(f'<span class="pr-pill pr-pill-ordered">{_eur(row.ordered_safe_eur)} ordered</span>')
    # A council that publishes audited accounts but no purchase-order list (Dublin City, DLR, Louth,
    # Tipperary) carries no euro pill here — flag the audited-accounts lane so the card isn't bare and
    # the reader knows there IS data behind it (the figures live in the dossier's two budget lanes).
    if not pills and (_truthy(getattr(row, "has_running", False)) or _truthy(getattr(row, "has_building", False))):
        pills.append('<span class="pr-pill pr-pill-ordered">Audited accounts</span>')
    return pills


def _council_summary_row(council: str) -> dict | None:
    """Look up one council's row in the directory view (v_procurement_council_summary — the UNION of
    the three lanes). Used to render a dossier for a council that publishes audited accounts but no
    purchase-order list, so has no row in the payments fact. Returns a plain dict, or None."""
    res = fetch_council_summary_result()
    if not res.ok or res.data.empty:
        return None
    hit = res.data[res.data["council"] == council]
    return hit.iloc[0].to_dict() if not hit.empty else None


def _render_councils() -> None:
    """The "Your council" index — Ireland's publishing local authorities as a civic directory,
    grouped North->South by province, each card linking to its existing per-council dossier
    (?paid_publisher=). Surfacing-only: v_procurement_council_summary is pre-aggregated and
    pre-ordered; this selects and renders, computing nothing. No rank chips — a directory
    ("find your council"), not a league table. 'ordered' and 'paid' are different lifecycle
    stages and are never added together."""
    # Deferred: .payments and .pay_profiles both import from this module, so these
    # reverse edges are call-time (see the payments/ted/profiles/pay_profiles/councils
    # mutual-dependency note in pay_profiles._render_payments_publisher_profile).
    from .payments import _paid_publisher_href
    from .pay_profiles import _PAY_FOOT_HTML

    res = fetch_council_summary_result()
    if not res.ok:
        empty_state(
            "Council spending isn't available right now",
            "The public-body payment views couldn't be loaded — a source/pipeline issue, not an empty result.",
        )
        return
    df = res.data
    if df.empty:
        empty_state("No councils", "No local authority has published payment records yet.")
        return

    n_councils = len(df)
    span = f"{_n(df['min_year'].min())}–{_n(df['max_year'].max())}"
    st.html(
        '<div class="pr-caveat"><strong>What your county and city councils spend.</strong> '
        f"The {n_councils} local authorities that publish their spending — purchase orders and "
        f"payments over €20,000, or their audited annual accounts ({span}) — grouped by province. "
        "Each council shows money <em>ordered</em> (purchase-order commitments) or <em>paid</em> "
        "(actual payments) — different stages of public money, shown per council and "
        "<strong>never added together</strong>. A few publish audited accounts but no purchase-order "
        "list. Click a council for its suppliers and audited-accounts context.</div>"
    )

    # Province bands, North->South via province_order; councils pre-ordered by scale within
    # each band. The band header is a semantic <h3> (heading-navigable). Geography is the
    # fixed band order, not colour.
    for prov_order in sorted(df["province_order"].unique()):
        band = df[df["province_order"] == prov_order]
        prov = _esc(band.iloc[0]["province"])
        n = len(band)
        # <h2>: direct section heading under the page <h1> hero (no h2→h3 skip), so the
        # province bands are screen-reader heading-navigable. CSS sets the visual size.
        st.html(
            f'<h2 class="pr-region-head"><span class="pr-region-name">{prov}</span>'
            f'<span class="pr-region-count">{n} council{"s" if n != 1 else ""} publishing</span></h2>'
        )
        cards = []
        for r in band.itertuples():
            n_sup = _n(r.n_suppliers)
            has_po = _n(r.n_paid) > 0 or _n(r.n_ordered) > 0
            if has_po:
                # Guard the year span: a council whose source carries no usable year (e.g. Mayo)
                # would otherwise render "0–0". Drop the span rather than show a sentinel.
                yr_span = f" · {_n(r.min_year)}–{_n(r.max_year)}" if _n(r.min_year) and _n(r.max_year) else ""
                meta = f"{n_sup:,} supplier{'s' if n_sup != 1 else ''}{yr_span}"
            else:
                # Audited-accounts-only council (no purchase-order list): describe the AFS span so
                # the card carries real information instead of "0 suppliers".
                acc = [
                    int(y)
                    for y in (r.running_min_year, r.running_max_year, r.building_min_year, r.building_max_year)
                    if _truthy(y)
                ]
                acc_span = (
                    f" · {min(acc)}–{max(acc)}" if acc and min(acc) != max(acc) else (f" · {acc[0]}" if acc else "")
                )
                meta = f"Audited accounts{acc_span}"
            # Land the dossier on the tier the council actually publishes, so it opens populated.
            tier = "SPENT" if _n(r.n_paid) > 0 else "COMMITTED"
            inner = _card(f"<span>{_esc(r.council)}</span>", meta, _council_tier_pills(r))
            cards.append(
                clickable_card_link(
                    href=_paid_publisher_href(r.council, tier),
                    inner_html=inner,
                    aria_label=f"View {r.council} council's suppliers and audited accounts",
                )
            )
        st.html(f'<div class="pr-grid">{"".join(cards)}</div>')
    st.html(_PAY_FOOT_HTML)


def _lane_header(tag: str, head: str, dek_html: str) -> str:
    """A bold lane band that opens one of the dossier's three honest grains (Running / Building /
    Paying). ``tag`` is the small caps stratum, ``head`` the section <h2>, ``dek_html`` the prose."""
    return (
        '<div class="pr-lane">'
        f'<div class="pr-lane-tag">{_esc(tag)}</div>'
        f'<h2 class="pr-lane-head">{_esc(head)}</h2>'
        f'<p class="pr-lane-dek">{dek_html}</p></div>'
    )


def _net_cost_label(net) -> str:
    """Net cost of a service to the local taxpayer. A non-positive net means the service's own
    income/grants cover it (housing rents, water recoupment) — say so rather than print '—'."""
    try:
        n = float(net)
    except (TypeError, ValueError):
        return "—"
    return f"{_eur(n)} net cost" if n > 0 else "covered by its own income"


def _self_funded_note(pct, division: str) -> str:
    """Muted sub-label: the share of a service the council recovers itself vs. funds from rates/LPT."""
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return ""
    if division == "Miscellaneous Services":
        return "carries the rates / Local Property Tax allocation — not a like-for-like service"
    if p >= 100:
        return "fully covered by its own income & grants"
    return f"{p:.0f}% funded by its own charges & grants — the rest by you (rates, LPT, State grant)"


def _render_budget_vs_outturn(council: str) -> None:
    """Plan vs outturn for the latest joined year — the ADOPTED budget beside the audited
    outturn per division. Moved verbatim out of the RUNNING lane so the Your Council money
    flow can render it inside its VOTED lane instead; behaviour on this page is unchanged.
    Side-by-side ONLY — the delta is view-computed context, never an overspend verdict."""
    bva = fetch_la_budget_vs_actual_result(council)
    if not bva.ok or bva.data.empty:
        return
    bva_latest = int(bva.data["year"].max())
    bva_rows = bva.data[bva.data["year"] == bva_latest]
    st.caption(f"Adopted budget vs audited outturn by service, {bva_latest} — compared, never added")
    line_html = []
    for r in bva_rows.itertuples():
        pct = getattr(r, "outturn_vs_budget_pct", None)
        chip = (
            f'<span style="background:#eceae4;color:#41403a;border-radius:3px;padding:0 .45rem;'
            f'font-size:.72rem;font-variant-numeric:tabular-nums">{float(pct):+g}%</span>'
            if _truthy(pct)
            else ""
        )
        line_html.append(
            '<div style="display:flex;justify-content:space-between;gap:.6rem;margin:.14rem 0">'
            f"<span>{_esc(r.division)}</span>"
            f'<span style="white-space:nowrap;font-variant-numeric:tabular-nums">'
            f"{_eur(r.budget_expenditure_eur)} planned → <strong>{_eur(r.afs_gross_expenditure_eur)}</strong>"
            f" spent {chip}</span></div>"
        )
    st.html(
        '<div class="pr-afs-trace">'
        + "".join(line_html)
        + '<div class="pr-afs-trace-cap">“Planned” is the budget the elected councillors adopted '
        "before the year began (a reserved function); “spent” is the audited outturn. Different "
        "bases — a plan and an actual — shown side by side and never summed; small gaps either "
        "way are normal, not verdicts.</div></div>"
    )


def _render_council_budget_lane(council: str, *, councillors_href: str | None = None) -> int | None:
    """LANE 0 — VOTED (the adopted budget). The plan the elected councillors voted before the
    year began — adopting the budget is one of their few reserved functions, which is why this
    lane leads the Your Council money flow: it is where the money story starts, and the one
    stage the people you elect actually control.

    ⚠️ BUDGETED grain (a plan, not spend) — never summed with the audited accounts or the
    purchase-order euros in the lanes below. All figures arrive pre-computed from
    v_procurement_la_budget_divisions; bar width is display scaling within this lane only.
    Returns the latest adopted-budget year, or None when the fact has no rows for this council
    (all 31 councils are in the DHLGH publication, so None normally means a load failure)."""
    res = fetch_la_budget_divisions_result(council)
    if not res.ok or res.data.empty:
        return None
    latest = int(res.data["year"].max())
    rows = res.data[res.data["year"] == latest]
    years = sorted({int(y) for y in res.data["year"].dropna()})
    span = f"{years[0]}–{years[-1]}" if len(years) > 1 else str(latest)

    st.html(
        _lane_header(
            "VOTED · adopted budget, a reserved function",
            "The budget your councillors voted",
            f"Before each year begins, the <strong>elected councillors</strong> of {_esc(council)} "
            "vote to adopt the council's budget — one of the few <strong>reserved functions</strong> "
            f"they hold. This is the plan they adopted for <strong>{latest}</strong> (published "
            f"{_esc(span)}): what the council intends to spend running each service. "
            "A <strong>plan, not spend</strong> — the audited accounts below show what actually "
            "happened, and the two are <strong>never added together</strong>.",
        )
    )
    budgets = [float(r.expenditure_adopted_eur) for r in rows.itertuples() if _truthy(r.expenditure_adopted_eur)]
    max_budget = max([b for b in budgets if b > 0], default=0.0)
    st.caption(f"Adopted budget by service, {latest} — bar width = budgeted spend")
    bar_rows = []
    for r in rows.itertuples():
        exp = getattr(r, "expenditure_adopted_eur", None)
        inc = getattr(r, "income_adopted_eur", None)
        bar_rows.append(
            _afs_bar_row(
                r.division,
                exp if _truthy(exp) and float(exp) > 0 else 0,
                max_budget,
                fig_html=f"<strong>{_eur(exp)}</strong>",
                note=f"{_eur(inc)} budgeted income" if _truthy(inc) and float(inc) > 0 else "",
                accent="#6d5a8c",
            )
        )
    st.html(f'<div class="pr-afsbars">{"".join(bar_rows)}</div>')
    src = str(rows.iloc[0].get("source_url") or "")
    foot_bits = []
    if src:
        foot_bits.append(
            f'<a class="dt-source-link" href="{_esc(src)}" target="_blank" rel="noopener">'
            f"DHLGH Local Authority Budgets {latest} →</a>"
        )
    if councillors_href:
        foot_bits.append(
            f'<a class="dt-source-link" href="{_esc(councillors_href)}" target="_self">'
            "Adopted by the councillors you elect →</a>"
        )
    if foot_bits:
        st.html(f'<div class="pr-prof-sub" style="margin:0.2rem 0 0.5rem">{" · ".join(foot_bits)}</div>')
    # Plan vs audited outturn belongs to this lane in the money flow — the direct answer to
    # "did what they voted happen?" (view-computed delta, never a verdict).
    _render_budget_vs_outturn(council)
    return latest


def _render_council_running_lane(
    council: str, active_tier: str, *, po_max_year: int | None, include_budget_vs_outturn: bool = True
) -> int | None:
    """LANE 1 — RUNNING THE SERVICES (audited revenue account). Leads with NET COST by service
    (what the local taxpayer actually funds, the strongest civic figure), then the spend-over-time
    spine and the indicative named-supplier traceability bridge to the PAYING lane below.

    ⚠️ BUDGET grain — a SIBLING fact, NEVER summed with the purchase-order euros. All figures are
    pre-aggregated/pre-ordered in the views; the page selects and renders, computing no metric.
    Returns the latest accounts year (so the BUILDING lane can align its coverage note), or None
    when this council has no audited AFS in the fact yet. ``include_budget_vs_outturn=False`` lets
    a host that already rendered the VOTED (adopted-budget) lane — which carries the same
    plan-vs-outturn block — suppress the duplicate here."""
    # Deferred: see the circular-import note in _render_councils above.
    from .payments import _paid_verb

    by_year = fetch_afs_total_by_year_result(council)
    if not by_year.ok or by_year.data.empty:
        return None
    ay = by_year.data
    years_present = {int(y) for y in ay["year"].dropna()}
    earliest, latest = min(years_present), max(years_present)
    span = f"{earliest}–{latest}" if len(years_present) > 1 else str(latest)

    st.html(
        _lane_header(
            "RUNNING THE SERVICES · revenue account, audited",
            "Where your money goes",
            "Every council publishes an audited <strong>Annual Financial Statement</strong> — its "
            "end-of-year accounts for running each service. Below is the <strong>net cost</strong> of "
            f"each service to {_esc(council)} ({_esc(span)}): what’s left for the local taxpayer "
            "(rates, Local Property Tax, State grant) to fund <em>after</em> the service’s own income "
            "and grants. This is the council’s <strong>whole operating spend</strong> — a separate, "
            "broader measure from the over-€20,000 purchase orders, and <strong>never added to them</strong>.",
        )
    )

    # Coverage flag — the audited AFS is filed in arrears (and the odd year can be missing from a
    # council's own publication run), so be explicit rather than let a gap read as "stopped spending".
    flag_bits: list[str] = []
    if po_max_year and latest < po_max_year:
        flag_bits.append(
            f"audited accounts run to <strong>{latest}</strong>, but the purchase orders below reach "
            f"<strong>{po_max_year}</strong> — councils publish their audited AFS in arrears, so the "
            "most recent year or two isn’t available yet"
        )
    missing = [y for y in range(earliest, latest + 1) if y not in years_present]
    if missing:
        flag_bits.append(f"no published statement for {', '.join(str(y) for y in missing)} in this series")
    if flag_bits:
        st.html(f'<div class="pr-caveat"><strong>Coverage:</strong> {"; ".join(flag_bits)}.</div>')

    # HERO — net cost by service (largest first; the view pre-orders by net DESC). Bar width is a
    # display scaling against the lane's own largest net cost (no aggregation here).
    bd = fetch_afs_by_division_result(council, latest)
    if bd.ok and not bd.data.empty:
        st.caption(f"Net cost to the local taxpayer by service, {latest} — bar width = net cost")
        nets = [float(r.net_expenditure_eur) for r in bd.data.itertuples() if _truthy(r.net_expenditure_eur)]
        max_net = max([n for n in nets if n > 0], default=0.0)
        has_misc = False
        rows = []
        for r in bd.data.itertuples():
            net = getattr(r, "net_expenditure_eur", None)
            pct = getattr(r, "pct_self_funded", None)
            if r.division == "Miscellaneous Services":
                has_misc = True
            fig = (
                f"<strong>{_eur(net)}</strong>"
                if _truthy(net) and float(net) > 0
                else '<span class="pr-afsbar-zero">income covers it</span>'
            )
            rows.append(
                _afs_bar_row(
                    r.division,
                    net if _truthy(net) and float(net) > 0 else 0,
                    max_net,
                    fig_html=fig,
                    note=_self_funded_note(pct, r.division),
                    accent="#3a6b7e",
                )
            )
        st.html(f'<div class="pr-afsbars">{"".join(rows)}</div>')
        if has_misc:
            st.caption(
                "“Miscellaneous Services” carries the council’s rates / Local Property Tax income, so it "
                "can show as fully covered — it isn’t a single service."
            )

    # Spend-over-time spine — distinct teal from the PO chart's brown (a different grain).
    if len(ay) > 1:
        st.caption("Total operating spending per year (revenue account, audited gross €)")
        st.bar_chart(
            _yr_axis(ay),
            x="year",
            y="gross_expenditure_eur",
            x_label="Year",
            y_label="Audited € spent",
            height=180,
            color="#3a6b7e",
        )

    # PLAN vs OUTTURN — the ADOPTED budget (a fourth grain: a plan, from DHLGH's consolidated
    # publication) beside the audited outturn for the same divisions. Side-by-side ONLY — the
    # delta is view-computed context; a few % either way is normal, never an overspend verdict.
    if include_budget_vs_outturn:
        _render_budget_vs_outturn(council)

    # Traceability bridge to the PAYING lane — the latest year present in both accounts + active PO tier.
    cov = fetch_afs_vs_po_coverage_result(council)
    if cov.ok and not cov.data.empty:
        pct_col = "pct_spent_of_gross" if active_tier == "SPENT" else "pct_committed_of_gross"
        po_col = "po_spent_safe_eur" if active_tier == "SPENT" else "po_committed_safe_eur"
        usable = cov.data[cov.data[pct_col].notna()]
        if not usable.empty:
            crow = usable.sort_values("year").iloc[-1]
            yr, gross, po, pct = (_n(crow.get("year")), crow.get("afs_gross_eur"), crow.get(po_col), crow.get(pct_col))
            verb = _paid_verb(active_tier)  # 'paid' / 'ordered'
            st.html(
                '<div class="pr-afs-trace">'
                f'<div class="pr-afs-trace-fig"><strong>{_eur(gross)}</strong> spent (accounts, {yr})'
                f" · <strong>{_eur(po)}</strong> traceable to named suppliers"
                f" · <strong>{float(pct):g}%</strong></div>"
                f'<div class="pr-afs-trace-cap">Indicative coverage only. The accounts figure is the '
                "council’s full audited operating spend; the supplier figure counts only purchases over "
                f"the €20,000 publication threshold ({verb} via purchase orders). Different thresholds and "
                "stages — a coverage signal, not a reconciliation.</div></div>"
            )
    return latest


def _render_council_building_lane(council: str, *, accounts_latest: int | None) -> int | None:
    """LANE 2 — BUILDING (audited capital account). What the council is investing in / acquiring —
    the homes, roads and facilities being built. A THIRD, DISTINCT grain: the revenue account shows
    housing netting to ~€0 (rents/HAP recoupment pass through), so the real housing investment only
    shows up here. NEVER summed with the revenue net cost or the purchase-order euros.

    Returns the latest capital-account year present (so the caller can tell whether this lane
    rendered), or None when this council's capital appendix isn't in the fact yet."""
    by_year = fetch_afs_capital_by_year_result(council)
    if not by_year.ok or by_year.data.empty:
        return None
    cy = by_year.data
    cap_years = {int(y) for y in cy["year"].dropna()}
    cap_latest = max(cap_years)
    span = f"{min(cap_years)}–{cap_latest}" if len(cap_years) > 1 else str(cap_latest)

    st.html(
        _lane_header(
            "BUILDING · capital account, audited",
            "What your council is building",
            f"Beyond running services day to day, {_esc(council)} invests in <strong>building and "
            "acquiring</strong> — housing, roads, libraries, water infrastructure. This is the audited "
            f"<strong>capital programme</strong> ({_esc(span)}), funded largely by central-government "
            "grants and loans. It is a <strong>different kind of money</strong> from the running costs "
            "above — investment, not operating spend — and the two are <strong>never added together</strong>.",
        )
    )
    if accounts_latest and cap_latest < accounts_latest:
        st.html(
            f'<div class="pr-caveat"><strong>Coverage:</strong> the capital appendix runs to '
            f"<strong>{cap_latest}</strong> here.</div>"
        )

    # Capital invested per year — a DISTINCT green (a third grain after brown PO + teal revenue).
    if len(cy) > 1:
        st.caption("Capital invested per year (audited €)")
        st.bar_chart(
            _yr_axis(cy),
            x="year",
            y="capital_expenditure_eur",
            x_label="Year",
            y_label="€ invested",
            height=180,
            color="#2f7d5b",
        )

    # Capital by service in the latest year — bars, largest investment first (view pre-orders).
    bd = fetch_afs_capital_by_division_result(council, cap_latest)
    if bd.ok and not bd.data.empty:
        st.caption(f"What it built in {cap_latest} — capital investment by service")
        capex = [float(r.capital_expenditure_eur) for r in bd.data.itertuples() if _truthy(r.capital_expenditure_eur)]
        max_cap = max(capex, default=0.0)
        rows = [
            _afs_bar_row(
                r.division,
                getattr(r, "capital_expenditure_eur", None),
                max_cap,
                fig_html=f"<strong>{_eur(getattr(r, 'capital_expenditure_eur', None))}</strong>",
                note="",
                accent="#2f7d5b",
            )
            for r in bd.data.itertuples()
        ]
        st.html(f'<div class="pr-afsbars">{"".join(rows)}</div>')
    return cap_latest


def _render_council_accounts_context(
    council: str,
    active_tier: str,
    *,
    po_max_year: int | None = None,
    has_paying: bool = True,
    lead_with_budget: bool = False,
    councillors_href: str | None = None,
) -> None:
    """The two AUDITED-ACCOUNTS lanes of a local-authority dossier, in civic reading order:
    RUNNING THE SERVICES (revenue net cost) then BUILDING (capital investment). Both are BUDGET
    grain — sibling facts, each pre-aggregated in its view, NEVER summed with each other or with the
    purchase-order euros in the PAYING lane. ``po_max_year`` lets the running lane flag the AFS
    arrears lag against the PO data.

    Lane honesty: a council can publish a purchase-order list but NOT a machine-readable audited
    statement (e.g. Mayo / Wexford / Kildare publish their AFS only through an interactive viewer or
    a scanned image). When neither accounts lane is available we say so explicitly — otherwise the
    missing lanes read as 'this council doesn't run services', which is false. ``has_paying`` keeps
    the note honest: it only points the reader 'down to the purchase orders' when that lane exists.

    ``lead_with_budget=True`` (the Your Council money flow) opens with the VOTED lane — the
    adopted budget the elected councillors voted, including the plan-vs-outturn block — and
    suppresses that block's duplicate inside the RUNNING lane. Default False preserves this
    page's original order exactly."""
    if lead_with_budget:
        _render_council_budget_lane(council, councillors_href=councillors_href)
    ran = _render_council_running_lane(
        council, active_tier, po_max_year=po_max_year, include_budget_vs_outturn=not lead_with_budget
    )
    built = _render_council_building_lane(council, accounts_latest=ran)
    if ran is None and built is None:
        tail = " The purchase orders below are the only machine-readable spending we hold for it." if has_paying else ""
        st.html(
            '<div class="pr-caveat"><strong>Audited accounts:</strong> we don’t yet hold '
            f"{_esc(council)}’s audited <strong>Annual Financial Statement</strong> in a "
            "machine-readable form (some councils publish it only through an interactive viewer or "
            "as a scanned image), so the <em>Running the services</em> and <em>Building</em> views "
            f"aren’t shown here — not because it has none.{tail}</div>"
        )
