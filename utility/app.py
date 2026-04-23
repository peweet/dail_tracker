from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st
import yaml

APP_FILE = Path(__file__).resolve()
PROJECT_ROOT = APP_FILE.parents[1] if APP_FILE.parent.name == "utility" else APP_FILE.parent
DEFAULT_CONTRACT_PATH = PROJECT_ROOT / "utility" / "page_contracts" / "member_overview.yaml"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "gold" / "dail.duckdb"


@st.cache_data(show_spinner=False)
def load_contract(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_resource(show_spinner=False)
def get_connection(db_path: str):
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(show_spinner=True, ttl=300)
def load_data(db_path: str, relation: str) -> pd.DataFrame:
    conn = get_connection(db_path)
    query = f"select * from {relation}"
    return conn.execute(query).df()


def load_shared_css(contract: dict[str, Any], repo_root: Path) -> None:
    ui_assets = contract.get("ui_assets", {})
    stylesheet_paths = ui_assets.get("stylesheet_paths", [])

    for css_path in stylesheet_paths:
        resolved = repo_root / css_path
        if resolved.exists():
            st.html(resolved)
        else:
            st.warning(f"Shared stylesheet not found: {resolved}")


def render_hero(contract: dict[str, Any]) -> None:
    page = contract.get("page", {})
    grain = contract.get("data_source", {}).get("grain", "")
    refresh_expectation = contract.get("data_source", {}).get("refresh_expectation", "")

    st.markdown(
        f"""
        <section class="dt-hero">
            <div class="dt-kicker">Dáil Tracker</div>
            <h1>{page.get('title', 'Dáil Tracker')}</h1>
            <p class="dt-dek">{page.get('description', '')}</p>
            <div style="margin-top: 0.65rem; display: flex; flex-wrap: wrap; gap: 0.45rem;">
                <span class="dt-badge">Grain: {grain or 'Not specified'}</span>
                <span class="dt-badge">Refresh: {refresh_expectation or 'Pipeline owned'}</span>
                <span class="dt-badge">UI logic: thin</span>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )



def apply_filters(df: pd.DataFrame, contract: dict[str, Any]) -> pd.DataFrame:
    filtered = df.copy()

    with st.sidebar:
        st.header("Filters")
        for filt in contract.get("filters", []):
            column = filt["column"]
            if column not in filtered.columns:
                continue

            filter_type = filt["type"]
            label = filt.get("label", column)

            if filter_type == "multiselect":
                options = sorted(v for v in filtered[column].dropna().unique().tolist())
                selected = st.multiselect(label, options=options, default=[])
                if selected:
                    filtered = filtered[filtered[column].isin(selected)]

            elif filter_type == "slider":
                min_value = float(filt.get("min", 0))
                max_value = float(filt.get("max", 1))
                step = float(filt.get("step", 0.05))
                default = float(filt.get("default", min_value))
                threshold = st.slider(
                    label,
                    min_value=min_value,
                    max_value=max_value,
                    value=default,
                    step=step,
                )
                filtered = filtered[filtered[column].fillna(0) >= threshold]

    return filtered



def render_metrics(df: pd.DataFrame, contract: dict[str, Any]) -> None:
    metrics = contract.get("headline_metrics", [])
    if not metrics:
        return

    cols = st.columns(len(metrics))
    for col, metric in zip(cols, metrics):
        label = metric.get("label", metric["name"])
        kind = metric["kind"]
        value: Any = None

        if kind == "row_count":
            value = len(df)
        elif kind == "sum":
            series = df[metric["column"]].fillna(0)
            value = float(series.sum())
        elif kind == "mean":
            series = df[metric["column"]].dropna()
            value = float(series.mean()) if not series.empty else None

        fmt = metric.get("format")
        if value is None:
            display_value = "—"
        elif fmt == "percent":
            display_value = f"{value:.1%}"
        elif isinstance(value, float):
            display_value = f"{value:,.0f}"
        else:
            display_value = str(value)

        col.metric(label, display_value)



def format_dataframe(df: pd.DataFrame, contract: dict[str, Any]) -> pd.DataFrame:
    visible_columns = [
        c["name"]
        for c in contract.get("columns", [])
        if c.get("visible", True) and c["name"] in df.columns
    ]
    if visible_columns:
        df = df[visible_columns]

    sort_cfg = contract.get("sorting", {})
    sort_by = sort_cfg.get("default_sort_by")
    sort_direction = sort_cfg.get("default_sort_direction", "desc")
    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=(sort_direction == "asc"))

    return df



def render_table_section(df: pd.DataFrame) -> None:
    st.subheader("Member table")
    st.caption("Filter, scan, and export the current view. KPI logic stays in DuckDB, not in this page.")
    st.dataframe(df, use_container_width=True, hide_index=True)



def render_export(display_df: pd.DataFrame, contract: dict[str, Any], page_name: str) -> None:
    exports_cfg = contract.get("exports", {})
    if not exports_cfg.get("enabled", False):
        return

    csv_bytes = display_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered CSV",
        data=csv_bytes,
        file_name=f"{page_name}.csv",
        mime="text/csv",
    )



def render_provenance(df: pd.DataFrame, contract: dict[str, Any]) -> None:
    prov_cfg = contract.get("provenance", {})
    if not prov_cfg.get("show_panel", False):
        return

    st.subheader("Data provenance")
    st.caption(prov_cfg.get("panel_text", ""))

    fields = prov_cfg.get("fields", [])
    present_fields = [field for field in fields if field in df.columns]
    if not present_fields or df.empty:
        st.info("No provenance fields available in this mart yet.")
        return

    first_row = df.iloc[0]
    html = ["<div class='dt-provenance-box'>"]
    for field in present_fields:
        html.append(
            f"<p style='margin: 0.15rem 0;'><strong>{field}</strong>: {first_row[field]}</p>"
        )
    html.append("</div>")
    st.markdown("\n".join(html), unsafe_allow_html=True)



def render_refactor_notes(contract: dict[str, Any]) -> None:
    notes = contract.get("notes_for_ai", {})
    blocked = notes.get("do_not_do", []) if isinstance(notes, dict) else []

    with st.expander("UI layer guardrails"):
        st.markdown(
            """
            - The pipeline owns joins, grains, and KPI definitions.
            - DuckDB serves a ready-to-use mart for the page.
            - Streamlit only filters, formats, and exports.
            - If a page needs merge/groupby/pivot logic, flag it as `TODO_GOLD_REFACTOR`.
            """
        )
        if blocked:
            st.markdown("**Do not push these into Streamlit:**")
            for item in blocked:
                st.markdown(f"- {item}")



def main() -> None:
    st.set_page_config(page_title="Dáil Tracker", page_icon="📊", layout="wide")

    contract_path = DEFAULT_CONTRACT_PATH
    contract = load_contract(contract_path)
    load_shared_css(contract, PROJECT_ROOT)

    page = contract.get("page", {})
    data_source = contract.get("data_source", {})
    page_name = page.get("name", "member_overview")
    relation = data_source.get("relation", "mart_member_overview")
    default_db = str(PROJECT_ROOT / data_source.get("database", str(DEFAULT_DB_PATH))) if not str(data_source.get("database", "")).startswith("/") else str(data_source.get("database"))

    render_hero(contract)

    with st.sidebar:
        st.header("Data source")
        db_path = st.text_input("DuckDB path", value=default_db)
        st.caption(f"Relation: {relation}")
        st.caption(f"Contract: {contract_path.relative_to(PROJECT_ROOT)}")

    try:
        raw_df = load_data(db_path, relation)
    except Exception as exc:
        st.error(
            "Could not load the mart. Make sure your pipeline created the DuckDB file "
            "and the mart/view exists."
        )
        st.exception(exc)
        return

    filtered_df = apply_filters(raw_df, contract)
    render_metrics(filtered_df, contract)

    display_df = format_dataframe(filtered_df, contract)
    render_table_section(display_df)
    render_export(display_df, contract, page_name)
    render_provenance(raw_df, contract)
    render_refactor_notes(contract)


if __name__ == "__main__":
    main()
