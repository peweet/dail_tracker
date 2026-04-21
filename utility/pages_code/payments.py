from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import streamlit as st
from shared_css import inject_css

_ROOT = Path(__file__).parent.parent.parent
_CSV  = _ROOT / "data" / "silver" / "aggregated_payment_tables.csv"

_TAA_LABELS = {
    "Dublin": "Dublin / under 25 km",
    "1": "Band 1 — 25–60 km",
    "2": "Band 2 — 60–80 km",
    "3": "Band 3 — 80–100 km",
    "4": "Band 4 — 100–130 km",
    "5": "Band 5 — 130–160 km",
    "6": "Band 6 — 160–190 km",
    "7": "Band 7 — 190–210 km",
    "8": "Band 8 — over 210 km",
}

_DISCLAIMER = """
**About the Parliamentary Standard Allowance (PSA)**

The PSA is an allowance paid to all Oireachtas members to cover the costs of carrying out their
parliamentary duties. It has two components:

- **Travel & Accommodation Allowance (TAA)** — covers travel to Leinster House and overnight
  expenses. The rate is determined by the member's TAA band, which reflects the shortest
  practicable road distance between their normal place of residence and Leinster House.
  Dublin-based members (under 25 km) receive no TAA; the allowance scales up to Band 8 for
  members resident more than 210 km away.

- **Public Representation Allowance (PRA)** — covers constituency and public representation
  expenses.

**Attendance and deductions**

PSA payments are linked to attendance. Under the rules set out in the
[Oireachtas guide to salaries and allowances](https://www.oireachtas.ie/en/members/salaries-and-allowances/parliamentary-standard-allowances/),
members are required to attend a minimum of **120 days per year** to receive the full TAA.
For each day below that threshold, **1% of the annual TAA is deducted**. Certain absences are
excused — including committee work, official duties abroad, and certified ill-health — so
a reduction in TAA does not necessarily indicate non-attendance.
""".strip()


@st.cache_data
def _load() -> pd.DataFrame:
    df = pd.read_csv(_CSV, low_memory=False)
    df["Date_Paid"] = pd.to_datetime(df["Date_Paid"], errors="coerce")
    df["Amount_num"] = (
        df["Amount"]
        .str.replace("€", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float, errors="ignore")
    )
    # Normalise name: "Last, First" → "First Last"
    def _flip(name: str) -> str:
        if "," in str(name):
            parts = [p.strip() for p in name.split(",", 1)]
            return f"{parts[1]} {parts[0]}"
        return name

    df["full_name"] = df["Full_Name"].apply(_flip)
    df["TAA_Band"]  = df["TAA_Band"].astype(str).str.strip()
    df["taa_label"] = df["TAA_Band"].map(_TAA_LABELS).fillna(df["TAA_Band"])
    return df


def payments_page() -> None:
    inject_css()
    st.title("Politician Payments")
    st.caption("Parliamentary Standard Allowance (PSA) payments to TDs — sourced from Oireachtas payment records")

    with st.expander("About the PSA and TAA bands", expanded=False):
        st.markdown(_DISCLAIMER)

    df = _load()

    min_date = df["Date_Paid"].min().date()
    max_date = df["Date_Paid"].max().date()
    all_names = sorted(df["full_name"].dropna().unique())

    # ── Sidebar ──────────────────────────────────────────────────────────────
    st.sidebar.header("Controls")

    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date = max(min_date, min(date_range[0], max_date))
        end_date   = max(min_date, min(date_range[1], max_date))
    else:
        start_date, end_date = min_date, max_date

    selected_tds = st.sidebar.multiselect(
        "Filter to specific TDs (leave blank for all)",
        all_names,
    )

    position_opts = sorted(df["Position"].dropna().unique())
    selected_positions = st.sidebar.multiselect(
        "Position",
        position_opts,
        default=position_opts,
    )

    view_mode = st.sidebar.radio(
        "View mode",
        ["Rankings", "Timeline", "Individual TD"],
        index=0,
    )

    # ── Filter ────────────────────────────────────────────────────────────────
    mask = (
        (df["Date_Paid"].dt.date >= start_date) &
        (df["Date_Paid"].dt.date <= end_date) &
        (df["Position"].isin(selected_positions))
    )
    filtered = df[mask]
    if selected_tds:
        filtered = filtered[filtered["full_name"].isin(selected_tds)]

    # ── Per-TD totals ─────────────────────────────────────────────────────────
    totals = (
        filtered.groupby(["full_name", "taa_label", "Position"])
        .agg(
            total_paid  =("Amount_num", "sum"),
            payment_count=("Amount_num", "count"),
        )
        .reset_index()
        .sort_values("total_paid", ascending=False)
        .reset_index(drop=True)
    )

    # ── Top-level metrics ─────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("TDs in view",      totals["full_name"].nunique())
    c2.metric("Total paid out",   f"€{filtered['Amount_num'].sum():,.0f}")
    c3.metric("Avg per TD",       f"€{totals['total_paid'].mean():,.0f}" if len(totals) else "—")
    c4.metric("Highest earner",   totals.iloc[0]["full_name"] if len(totals) else "—")

    st.divider()

    # ── Rankings ──────────────────────────────────────────────────────────────
    if view_mode == "Rankings":
        st.subheader("Total PSA received by TD")

        col_l, col_r = st.columns([2, 1])

        with col_l:
            st.dataframe(
                totals[["full_name", "Position", "taa_label", "total_paid", "payment_count"]],
                hide_index=True,
                use_container_width=True,
                column_config={
                    "full_name":     st.column_config.TextColumn("TD"),
                    "Position":      st.column_config.TextColumn("Position"),
                    "taa_label":     st.column_config.TextColumn("TAA Band"),
                    "total_paid":    st.column_config.NumberColumn("Total paid (€)", format="€%.2f"),
                    "payment_count": st.column_config.NumberColumn("Payments", format="%d"),
                },
            )

        with col_r:
            st.subheader("Payments by TAA Band")
            band_totals = (
                filtered.groupby("taa_label")["Amount_num"]
                .sum()
                .sort_values(ascending=False)
            )
            st.bar_chart(band_totals)

        st.download_button(
            "Download payments CSV",
            totals.to_csv(index=False),
            file_name=f"td_payments_{start_date}_to_{end_date}.csv",
            mime="text/csv",
        )

    # ── Timeline ──────────────────────────────────────────────────────────────
    elif view_mode == "Timeline":
        st.subheader("Payments over time")

        monthly = (
            filtered.groupby(filtered["Date_Paid"].dt.to_period("M").astype(str))["Amount_num"]
            .sum()
            .rename("Total paid (€)")
        )
        st.subheader("Monthly total PSA paid out")
        st.bar_chart(monthly)

        if selected_tds:
            st.subheader("Monthly payments per TD")
            per_td = (
                filtered.copy()
                .assign(month=filtered["Date_Paid"].dt.to_period("M").astype(str))
                .groupby(["month", "full_name"])["Amount_num"]
                .sum()
                .reset_index()
                .pivot(index="month", columns="full_name", values="Amount_num")
                .fillna(0)
            )
            st.line_chart(per_td)
        else:
            st.info("Select specific TDs in the sidebar to compare their payment timelines.")

    # ── Individual TD ─────────────────────────────────────────────────────────
    elif view_mode == "Individual TD":
        st.subheader("Individual TD payment history")
        td_name = st.selectbox("Select TD", all_names)
        td_df   = filtered[filtered["full_name"] == td_name].copy().sort_values("Date_Paid")

        if td_df.empty:
            st.warning("No payment data for this TD in the selected date range.")
        else:
            row = totals[totals["full_name"] == td_name]
            total   = row["total_paid"].values[0]  if len(row) else 0
            n_pays  = row["payment_count"].values[0] if len(row) else 0
            band    = row["taa_label"].values[0] if len(row) else "—"

            m1, m2, m3 = st.columns(3)
            m1.metric("Total received",  f"€{total:,.2f}")
            m2.metric("No. of payments", int(n_pays))
            m3.metric("TAA Band",        band)

            st.subheader("Payment history")
            st.dataframe(
                td_df[["Date_Paid", "Narrative", "Amount", "taa_label"]].rename(columns={
                    "Date_Paid": "Date",
                    "Narrative": "Description",
                    "Amount":    "Amount",
                    "taa_label": "TAA Band",
                }),
                hide_index=True,
                use_container_width=True,
            )

            st.subheader("Monthly payments")
            td_df["month"] = td_df["Date_Paid"].dt.to_period("M").astype(str)
            st.bar_chart(td_df.groupby("month")["Amount_num"].sum().rename("Amount (€)"))

            st.download_button(
                f"Download {td_name} CSV",
                td_df.to_csv(index=False),
                file_name=f"{td_name.replace(' ', '_')}_payments.csv",
                mime="text/csv",
            )
