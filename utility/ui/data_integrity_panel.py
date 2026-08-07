"""Streamlit renderer for the framework-neutral quarantine-ledger summary."""

from __future__ import annotations

import streamlit as st
from data_access.quarantine_data import held_summary


def _heading_html() -> str:
    """Return the stable heading markup for class-contract scanning."""
    return '<h2 class="section-heading">Data integrity — values held for review</h2>'


def render_data_integrity_panel() -> None:
    """Render values held for review without moving ledger reads into the UI."""
    summary = held_summary()
    st.html(_heading_html())

    if not summary["n_rows_held"]:
        st.caption(
            "No values are currently held back. Every figure on the site passed the automated "
            "plausibility checks — for example, no amount larger than is physically possible has "
            "been allowed through to a published total."
        )
        return

    st.caption(
        f"{summary['n_rows_held']} value(s) across {summary['n_resources']} source(s) were "
        "automatically held back because they fell outside plausible bounds, and are excluded "
        "from the site pending manual review. They are listed here for transparency."
    )
    for name, entry in sorted(summary["resources"].items()):
        rows = entry.get("rows", [])
        if not rows:
            continue
        offending = entry.get("offending_columns", [])
        with st.expander(f"{name} — {entry.get('n_held', len(rows))} held back"):
            for row in rows[:20]:
                value = "; ".join(f"{column} = {row.get(column)}" for column in offending if column in row)
                source_pdf, source_page = row.get("source_pdf"), row.get("source_page")
                where = (
                    f"  ·  {source_pdf} p.{source_page}"
                    if source_pdf and source_page is not None
                    else (f"  ·  {source_pdf}" if source_pdf else "")
                )
                st.write(f"- {value or '(value)'}{where}")
