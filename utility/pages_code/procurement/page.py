from __future__ import annotations


import pandas as pd
import streamlit as st

from data_access.freshness_data import freshness_line
from data_access.procurement_data import (
    fetch_available_years,
    fetch_charity_overlap_result,
    fetch_coverage_stats_result,
    fetch_lobbying_overlap_result,
)
from ui.components import (
    dt_page,
    empty_state,
    glossary_strip,
    hero_banner,
)

from ui.format import to_int as _n

from ._shared import (
    _year_pills,
    _register_picker,
    _section_picker,
)

from .profiles import _render_supplier_profile, _render_authority_profile, _render_cpv_profile
from .pay_profiles import (
    _render_payment_lines,
    _render_payments_publisher_profile,
    _render_payments_supplier_profile,
)
from .ted import _render_ted_winner_profile, _render_ted, _render_ted_tenders
from .patterns import (
    _render_single_bid_cpv,
    _entity_search_hero,
    _page_lede,
    _data_completeness_body,
    _lifecycle_body,
    _render_patterns,
    _render_bid_signal,
)
from .national import _render_eu_tam
from .browse import (
    _render_charity_overlap,
    _render_overlap,
    _render_authorities,
    _render_cpv,
    _render_suppliers,
)
from .payments import _render_payments_bridge
from .tenders import _render_national_expiring, _render_expiring_contracts, _render_national_open_tenders




# ──────────────────────────────────────────────────────────────────────────────
@dt_page
def procurement_page() -> None:
    # Drill-downs — full-width detail views with back nav.
    params = st.query_params
    if params.get("supplier"):
        _render_supplier_profile(params.get("supplier"))
        return
    if params.get("authority"):
        _render_authority_profile(params.get("authority"))
        return
    if params.get("cpv"):
        _render_cpv_profile(params.get("cpv"))
        return
    if params.get("paid_supplier") and params.get("paid_publisher"):
        # LEAF: both keys present → the published line items for that supplier × body pair (the
        # terminus that breaks the supplier↔body card loop). Checked before the single-key branches.
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payment_lines(
            params.get("paid_supplier"),
            params.get("paid_publisher"),
            req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT",
        )
        return
    if params.get("paid_publisher"):
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payments_publisher_profile(
            params.get("paid_publisher"), req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT"
        )
        return
    if params.get("paid_supplier"):
        req_tier = (params.get("paid_tier") or "SPENT").upper()
        _render_payments_supplier_profile(
            params.get("paid_supplier"), req_tier if req_tier in ("SPENT", "COMMITTED") else "SPENT"
        )
        return
    if params.get("ted_winner"):
        _render_ted_winner_profile(params.get("ted_winner"))
        return
    if params.get("single_bid_cpv"):
        _render_single_bid_cpv(params.get("single_bid_cpv"))
        return

    # coverage_stats is the source-state gate AND the scale anchor: a missing view /
    # parquet / DuckDB error is NOT "no results".
    stats_res = fetch_coverage_stats_result()
    if not stats_res.ok:
        hero_banner(
            kicker="PUBLIC MONEY",
            title="Public Procurement",
            dek="Contract awards published on eTenders / national procurement open data.",
        )
        empty_state(
            "Procurement data isn't available right now",
            "The procurement views couldn't be loaded — the gold parquet may be missing "
            "or a view failed to register. This is a source/pipeline issue, not an empty result.",
        )
        return

    stats = stats_res.data.iloc[0]

    # Hero carries no stat badges: the corpus counts + the top-winner / market-shape
    # findings live in the single _page_lede below, so the data isn't pushed off-screen
    # by a second stat block and the sum-safe total is shown exactly once.
    # The dek covers the WHOLE page, so it names all four stages rather than only the award
    # register. It previously described awards alone while heading the payments, open-tender
    # and patterns sections too (2026-08-01 render audit).
    hero_banner(
        kicker="PUBLIC MONEY",
        title="Public Procurement",
        dek="Who wins public contracts, who actually gets paid, what is open for bidding now — "
        "from eTenders, the EU journal and public bodies' own payment lists.",
    )

    # Search-first entry: one box across companies / public bodies / categories (renders
    # results only when the user types; the lenses below are untouched otherwise).
    _entity_search_hero()

    # ONE explainer door (2026-07-20 clutter pass). Terms, coverage honesty and the
    # money-lifecycle model each used to open their own collapsed expander, so three
    # grey bars stacked between the hero and the section picker and read as page
    # furniture. Same three texts, same order, one bar — a reader who wants the
    # background opens it once; a reader who wants the registers scrolls past one row.
    with st.expander("About this data — terms, coverage, and how public money moves"):
        glossary_strip(
            [
                ("Award value", "the contract value at the point of award — not money actually paid out"),
                ("Framework / DPS", "an agreement a buyer may draw down against — the ceiling is not a payment"),
                ("CPV", "Common Procurement Vocabulary — the EU category code for what was bought"),
                ("CRO", "Companies Registration Office — a matched company registration number"),
            ]
        )
        st.divider()
        _data_completeness_body()
        st.divider()
        # Names the four realisation tiers the sections below embody, so the section bar
        # reads as "stages of one contract's life", not four disconnected lists.
        _lifecycle_body()

    if _n(stats.get("n_suppliers")) == 0:
        empty_state("No supplier records", "The procurement views are loaded but returned no rows.")
        return

    # Four top-level sections, phrased as the questions a reader actually brings
    # (doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md §1 + doc/archive/PROCUREMENT_UI_BRIEF.md: registers →
    # questions). "Who wins contracts?" holds the award-stage registers (eTenders national /
    # TED EU) plus the register-overlap disclosures behind one register picker; "Who actually
    # gets paid?" is the payment stage; "Open right now" promotes the pre-award tender
    # pipeline to a first-class lens (the forward-looking view, no longer buried two pickers
    # deep); "Patterns" is the factual signal feed. The section bar is a ?tab=-synced segmented
    # control (NOT st.tabs, which reset to the first tab on every rerun — losing the reader's
    # place on a drill-down Back or a cross-page round-trip). Surfacing-only: every lens calls a
    # _render_* function; no logic moves into this layer.
    section = _section_picker()

    if section == "wins":
        register = _register_picker()
        if register == "EU register (TED)":
            # TED contract awards WON (2016–2026). The pre-award tender pipeline moved to
            # the top-level "Open right now" section (different grain, never summed).
            _render_ted()
        elif register == "EU State Aid (grants)":
            # State-Aid grants/subsidies (IDA/EI/DAFM…) — a DIFFERENT instrument from contract
            # awards. Separate register, never value-merged with eTenders/TED.
            _render_eu_tam()
        elif register == "Register overlaps":
            # Co-occurrence disclosures (same pattern, two registers). All-time scope.
            ov_lens = st.segmented_control(
                "View",
                ["Lobbying", "Charities"],
                default="Lobbying",
                key="pr_overlap_lens",
                label_visibility="collapsed",
            )
            if ov_lens == "Charities":
                charity_overlap = fetch_charity_overlap_result()
                _render_charity_overlap(charity_overlap.data if charity_overlap.ok else pd.DataFrame())
            else:
                overlap = fetch_lobbying_overlap_result()
                _render_overlap(overlap.data if overlap.ok else pd.DataFrame(), None)
        else:
            # The award caveat and the Deloitte lede belong to THIS register only, and are
            # rendered here rather than page-wide (2026-08-01 render audit). Page-wide they
            # appeared on all four sections and were wrong on three — on "Who actually gets
            # paid?" the caveat sent the reader to the tab they were already on, directly above
            # that section's own caveat saying the opposite; on "Open right now" it captioned
            # 227 open tenders as awarded values. Scoped to the wins section they were still
            # wrong for two of its four registers: State Aid is GRANTS, not contract awards, and
            # the lede's figures (44,164 awards, 10,016 suppliers) are the national register's
            # alone. TED, State Aid and Register overlaps each carry their own description.
            st.html(
                '<div class="pr-caveat"><strong>Awarded value, not money paid.</strong> '
                "These are values at the point of award — see <em>Money actually paid</em> for real "
                "payments. A contract award is a public record of a procurement decision, not evidence "
                "of influence or wrongdoing.</div>"
            )
            _page_lede(stats)
            # Lens + year on ONE refinement band (was three stacked rows: register / a "filter by
            # year" caption + pills / lens). The lens is the primary choice — what to rank — so it
            # leads; the year is a quiet refinement beside it. Year stays pills, never a dropdown
            # (app-wide convention: year navigation is always pills). Mirrors the columns pattern
            # the payments section already uses for its tier + view controls.
            lens_col, year_col = st.columns([1.15, 2], vertical_alignment="center")
            with lens_col:
                awards_lens = st.segmented_control(
                    "View awards by",
                    ["By supplier", "By authority", "By category"],
                    default="By supplier",
                    key="pr_awards_lens",
                    label_visibility="collapsed",
                )
            with year_col:
                year = _year_pills(fetch_available_years())
            if awards_lens == "By authority":
                _render_authorities(year)
            elif awards_lens == "By category":
                _render_cpv(year)
            else:
                _render_suppliers(year)

    elif section == "paid":
        # Bridge, not browse (Money nav declutter Phase 2.5): the full payments browse
        # lives on the Public Payments hub; this section keeps the awards→paid pivot.
        _render_payments_bridge()

    elif section == "open":
        # Two forward-looking lenses, same grain discipline: open competition notices
        # (pre-award) and advertised contract terms due to end (post-award fact — when
        # the contracted period runs out, as stated on the notice; never summed).
        fwd_lens = st.segmented_control(
            "View",
            ["Open tenders", "Contract terms ending"],
            default="Open tenders",
            key="pr_forward_lens",
            label_visibility="collapsed",
        )
        # Two registers, rendered as separate sections (never value-merged): the national
        # eTenders feed (the sub-EU-threshold mass) first, then the EU-journal (TED) feed.
        if fwd_lens == "Contract terms ending":
            _render_national_expiring()
            st.html('<div class="pr-register-rule"><span>EU Official Journal (TED)</span></div>')
            _render_expiring_contracts()
        else:
            _render_national_open_tenders()
            st.html('<div class="pr-register-rule"><span>EU Official Journal (TED)</span></div>')
            _render_ted_tenders()

    elif section == "bidsignal":  # EXPERIMENTAL, local-only (DAIL_EXPERIMENTAL=1)
        _render_bid_signal()

    else:  # "patterns"
        _render_patterns()

    st.html(
        '<div class="pr-foot"><strong>Source:</strong> eTenders / national procurement open data '
        '(<a href="https://data.gov.ie/dataset/contract-notices-published-on-etenders" '
        'target="_blank" rel="noopener">data.gov.ie ↗</a>), cross-referenced to the Companies '
        "Registration Office and the Register of Lobbying. Values are awarded contract values, not "
        "actual payments; only sum-safe award values are shown. Suppliers shown are company-class "
        "registrations — sole traders and individuals are excluded.</div>"
    )
    _fresh = freshness_line("procurement")
    if _fresh:
        # The OGP open-data export itself publishes with a lag of several months,
        # so the newest notice held legitimately predates the latest pipeline run.
        st.caption(f"{_fresh} The national export publishes with a lag of several months.")
