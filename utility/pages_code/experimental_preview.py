"""
============================================================================
  EXPERIMENTAL — DELETE ON INTEGRATION
============================================================================

Sandbox preview page. Shows the effect of two unmerged pipeline fixes side
by side with current production state, so the user can verify what the data
will look like before integrating either change:

  1. Unscoped legislation feed (Government + Private Member bills)
       -> pipeline_sandbox/legislation_unscoped_*.py
       -> sql_views/experimental_legislation_unscoped_*.sql

  2. Full PSA re-parse (TAA + PRA + Dublin allowance)
       -> pipeline_sandbox/payments_full_psa_etl.py
       -> sql_views/experimental_payments_full_psa.sql

This page is deliberately spartan — its purpose is data inspection, not
final UX. Counts, dataframes, and a side-by-side delta against production.

REMOVAL CHECKLIST when graduating either fix:
  - Delete this file.
  - Delete utility/data_access/experimental_data.py.
  - Delete sql_views/experimental_*.sql.
  - Remove the experimental_preview_page entry from utility/app.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.experimental_data import (
    fetch_experimental_government_bills_sample,
    fetch_experimental_government_bills_with_debates,
    fetch_experimental_legislation_index,
    fetch_experimental_legislation_phase_crosstab,
    fetch_experimental_legislation_source_breakdown,
    fetch_experimental_payments_by_kind,
    fetch_experimental_payments_by_year,
    fetch_experimental_payments_summary,
    fetch_experimental_payments_top_members,
    fetch_experimental_seanad_origin_sample,
    fetch_production_legislation_count,
    fetch_production_payments_summary,
)
from shared_css import inject_css
from ui.components import hero_banner, sidebar_page_header


def _eur(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"€{float(v):,.0f}"


def _int(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    return f"{int(v):,}"


def _warn_banner() -> None:
    st.html(
        """
        <div style="
            background: #fff7ed;
            border-left: 6px solid #ea580c;
            padding: 18px 22px;
            margin: 8px 0 24px 0;
            border-radius: 4px;
            font-family: var(--font-sans, inherit);
        ">
          <div style="font-weight:700; font-size:1.05rem; color:#9a3412; margin-bottom:6px;">
            Experimental preview — sandbox data, not yet integrated
          </div>
          <div style="color:#431407; line-height:1.5;">
            This page reads from sandbox-only outputs
            (<code>pipeline_sandbox/out/silver/</code> and
            <code>data/gold/parquet/payments_full_psa.parquet</code>) via
            <code>experimental_*.sql</code> views. Production pages
            (Legislation, Payments) are unaffected. Delete the experimental files
            once each fix has been integrated — see the
            <code>REMOVAL CHECKLIST</code> at the top of each
            <code>experimental_*.sql</code> file.
          </div>
        </div>
        """
    )


# ── Section: Legislation ───────────────────────────────────────────────────

def _render_legislation_section() -> None:
    st.html('<h2 style="margin-top:32px;">1. Unscoped Legislation Feed</h2>')
    st.html(
        '<p style="color:#475569; margin-top:-8px;">'
        'Currently the production pipeline calls <code>/v1/legislation</code> '
        'with a per-TD <code>member_id</code> filter, which excludes Government '
        '(Minister-sponsored) bills. The sandbox fetch removes that filter.'
        '</p>'
    )

    exp_index = fetch_experimental_legislation_index()
    prod_count = fetch_production_legislation_count()
    src = fetch_experimental_legislation_source_breakdown()

    if exp_index.empty:
        st.warning(
            "No experimental legislation data found. Run "
            "`python -m pipeline_sandbox.legislation_unscoped_fetch` then "
            "`python -m pipeline_sandbox.legislation_unscoped_silver_views` first."
        )
        return

    exp_count = len(exp_index)
    govt_count = int(src.loc[src["source"] == "Government", "bills"].sum()) if not src.empty else 0
    pm_count = int(src.loc[src["source"] == "Private Member", "bills"].sum()) if not src.empty else 0
    delta = exp_count - prod_count

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Production bills",  _int(prod_count))
    c2.metric("Experimental bills", _int(exp_count), f"{delta:+,}")
    c3.metric("Government bills (new)", _int(govt_count))
    c4.metric("Private Member", _int(pm_count))

    st.html('<h3 style="margin-top:24px;">Bills by source</h3>')
    st.dataframe(src, hide_index=True, width="stretch")

    st.html('<h3 style="margin-top:24px;">Phase × source crosstab</h3>')
    st.html(
        '<p style="color:#475569; margin-top:-8px;">'
        'The new <code>bill_phase</code> rule is origin-aware: Seanad-origin '
        'bills land in <code>seanad</code> phase even at low stage numbers.'
        '</p>'
    )
    st.dataframe(
        fetch_experimental_legislation_phase_crosstab(),
        hide_index=True,
        width="stretch",
    )

    st.html('<h3 style="margin-top:24px;">Sample: Government bills (most recent)</h3>')
    st.dataframe(
        fetch_experimental_government_bills_sample(limit=25),
        hide_index=True,
        width="stretch",
    )

    st.html('<h3 style="margin-top:24px;">Sample: Seanad-origin bills</h3>')
    st.dataframe(
        fetch_experimental_seanad_origin_sample(limit=25),
        hide_index=True,
        width="stretch",
    )

    st.html('<h3 style="margin-top:24px;">Government bills with downstream debates</h3>')
    st.html(
        '<p style="color:#475569; margin-top:-8px;">'
        'These bills are 100% invisible in the current production index but '
        'have already accumulated debate sessions in the gold votes table.'
        '</p>'
    )
    st.dataframe(
        fetch_experimental_government_bills_with_debates(limit=15),
        hide_index=True,
        width="stretch",
    )

    with st.expander("How to integrate this fix"):
        st.markdown(
            "1. Update [`legislation.py`](legislation.py) — extend `BILL_META` "
            "with `originHouse`, replace the `dropna(subset=['sponsor.by.showAs'])` "
            "with a coalesce of `by` / `as`.\n"
            "2. Update [`sql_views/legislation_index.sql`](sql_views/legislation_index.sql) "
            "and [`sql_views/legislation_detail.sql`](sql_views/legislation_detail.sql) "
            "with the same coalesce, plus origin-aware `bill_phase` and "
            "`source` / `origin_house` projection.\n"
            "3. Switch the fetch from per-TD `member_id` to unscoped in "
            "[`services/urls.py`](services/urls.py).\n"
            "4. **Delete all `experimental_legislation_*.sql` files and "
            "this preview page when done.**\n\n"
            "Full plan: [`pipeline_sandbox/legislation_unscoped_integration_plan.md`]"
            "(pipeline_sandbox/legislation_unscoped_integration_plan.md)"
        )


# ── Section: Payments ──────────────────────────────────────────────────────

def _render_payments_section() -> None:
    st.html('<h2 style="margin-top:48px;">2. Full PSA Re-parse (TAA + PRA)</h2>')
    st.html(
        '<p style="color:#475569; margin-top:-8px;">'
        'Production parses TAA-only because it assumes a single 5-column PDF '
        'schema. The sandbox parser is schema-aware and recovers the May–Jun 2020 '
        '6-column layout plus the post-Jul-2020 PRA rows.'
        '</p>'
    )

    exp_summary = fetch_experimental_payments_summary()
    prod_summary = fetch_production_payments_summary()

    if exp_summary.empty:
        st.warning(
            "No experimental payments data found. Run "
            "`python pipeline_sandbox/payments_full_psa_etl.py` first."
        )
        return

    exp = exp_summary.iloc[0]
    prod = prod_summary.iloc[0] if not prod_summary.empty else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Production rows",
        _int(prod["rows"]) if prod is not None else "—",
    )
    c2.metric(
        "Experimental rows",
        _int(exp["rows"]),
        delta=f"{int(exp['rows']) - int(prod['rows']):+,}" if prod is not None else None,
    )
    c3.metric(
        "Production total",
        _eur(prod["total_paid"]) if prod is not None else "—",
    )
    delta_eur = (
        f"€{(float(exp['total_paid']) - float(prod['total_paid'])):+,.0f}"
        if prod is not None and pd.notna(prod["total_paid"]) and pd.notna(exp["total_paid"])
        else None
    )
    c4.metric(
        "Experimental total",
        _eur(exp["total_paid"]),
        delta=delta_eur,
    )

    st.html('<h3 style="margin-top:24px;">By payment kind</h3>')
    st.html(
        '<p style="color:#475569; margin-top:-8px;">'
        'Production only sees <code>TAA</code> rows; everything else is '
        'currently quarantined or dropped.'
        '</p>'
    )
    st.dataframe(
        fetch_experimental_payments_by_kind(),
        hide_index=True,
        width="stretch",
    )

    st.html('<h3 style="margin-top:24px;">By year</h3>')
    yearly = fetch_experimental_payments_by_year()
    st.dataframe(
        yearly,
        hide_index=True,
        width="stretch",
        column_config={
            "total_paid": st.column_config.NumberColumn("Total paid", format="€%.0f"),
            "taa_total": st.column_config.NumberColumn("TAA",        format="€%.0f"),
            "dublin_total": st.column_config.NumberColumn("Dublin",  format="€%.0f"),
            "pra_total": st.column_config.NumberColumn("PRA",        format="€%.0f"),
        },
    )

    st.html('<h3 style="margin-top:24px;">Top members by total received</h3>')
    st.dataframe(
        fetch_experimental_payments_top_members(limit=25),
        hide_index=True,
        width="stretch",
        column_config={
            "total_paid": st.column_config.NumberColumn("Total paid", format="€%.0f"),
            "taa_total":  st.column_config.NumberColumn("TAA",        format="€%.0f"),
            "pra_total":  st.column_config.NumberColumn("PRA",        format="€%.0f"),
        },
    )

    with st.expander("How to integrate this fix"):
        st.markdown(
            "1. Replace the parser stage in [`payments.py`](payments.py) (top-level) "
            "with the schema-aware logic in "
            "[`pipeline_sandbox/payments_full_psa_etl.py`]"
            "(pipeline_sandbox/payments_full_psa_etl.py).\n"
            "2. Update [`sql_views/payments_base.sql`]"
            "(sql_views/payments_base.sql) to read from "
            "`payments_full_psa.parquet` (renaming `amount` → `amount_num`, "
            "adding `payment_year`, `payment_kind`).\n"
            "3. (Optional) Run "
            "`python pipeline_sandbox/payments_2019_backfill_probe.py --download` "
            "to fetch the 12 missing 2019 PDFs into bronze, then re-run the parser.\n"
            "4. **Delete `experimental_payments_full_psa.sql` and this preview "
            "page when done.**\n\n"
            "Full plan: [`pipeline_sandbox/payments_full_psa_integration_plan.md`]"
            "(pipeline_sandbox/payments_full_psa_integration_plan.md)"
        )


# ── Page entry ─────────────────────────────────────────────────────────────

def experimental_preview_page() -> None:
    inject_css()
    sidebar_page_header(
        title="Experimental Preview",
        kicker="Sandbox · Dáil Tracker",
    )

    hero_banner(
        kicker="Sandbox · Preview · Dáil Tracker",
        title="Experimental Preview",
        dek=(
            "Side-by-side view of two unmerged pipeline fixes against current "
            "production state. Use this to inspect the data before integrating "
            "either change methodically. All experimental files are clearly "
            "marked and easy to delete."
        ),
    )

    _warn_banner()
    _render_legislation_section()
    _render_payments_section()
