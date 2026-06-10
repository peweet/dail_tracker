"""
Corporate — standalone browser page.

Sources from the registered DuckDB view v_corporate_notices
(sql_views/corporate/corporate_corporate_notices.sql), which reads
data/gold/parquet/corporate_notices.parquet — produced by
corporate_notices_enrichment.py.

Civic frame: corporate notices in Iris Oifigiúil. The page leads with the
recognition story — who's been calling in Irish loans (brand → parent fund
translation: Promontoria → Cerberus, Beltany → Goldman Sachs etc.) — followed
by a per-company search and a sectioned feed of the recent record.

Personal insolvency (individual bankruptcies) is excluded by policy at the
enrichment layer — see [[feedback_personal_insolvency_privacy]].

Sections (top → bottom):
  1. Editorial hero + glossary strip (SPV / ICAV / SCARP / MVL / CVL) +
     constitutional / privacy caveat
  2. Featured: receiver-appointer ranking (by parent fund) + year sparkline
     of the receiver wave. Honest 31%-coverage subhead.
  3. EXPERIMENTAL: regulated firms in repeat distress — CBI-authorised firms
     that appear in 2+ genuine-distress notices. Sourced from
     v_corporate_cbi_repeat_distress (sandbox), see
     sql_views/corporate/corporate_cbi_distress.sql header for provenance.
  4. Per-company search (entity_name primary, raw_text fallback flagged)
  5. Sectioned feed: tabbed by sub-type, month-grouped cards, paginated.
     Cards carry a CBI authorisation badge when notice_ref matches the
     sandbox cross-ref.
  6. Detail view on ?ref= (also carries the CBI badge when matched)
"""

from __future__ import annotations

import datetime
import html
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.corporate_data import (
    fetch_brand_aliases,
    fetch_cbi_notice_matches,
    fetch_cbi_repeat_distress,
    fetch_corporate_notices,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    fmt_civic_date as _fmt_date,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    paginate,
    pagination_controls,
    sidebar_page_header,
    sidebar_subtitle,
)
from ui.export_controls import export_button
from ui.source_pdfs import iris_archive_url

PAGE_SIZE = 12
FEATURED_TOP_N = 8

_SUBTYPE_LABEL = {
    "receivership": "Receivership",
    "examinership": "Examinership",
    "scarp_process_adviser": "SCARP (small-company rescue)",
    "members_voluntary_liquidation": "Members' Voluntary Liquidation",
    "creditors_voluntary_liquidation": "Creditors' Voluntary Liquidation",
    "voluntary_liquidation_unspecified": "Voluntary Liquidation",
    "liquidation_unspecified": "Liquidation",
    "court_winding_up": "Court Winding-up",
    "companies_act_notice": "Companies Act notice",
    "icav_voluntary_strike_off": "ICAV strike-off",
}

# Tabs in the feed. Order: receiverships first (the lead story), then rescue,
# then volume bulk, then ICAV.
_TYPE_GROUPS = [
    ("All", None),
    ("Receiverships", {"receivership"}),
    ("Examinership", {"examinership", "scarp_process_adviser"}),
    (
        "Liquidations",
        {
            "members_voluntary_liquidation",
            "creditors_voluntary_liquidation",
            "voluntary_liquidation_unspecified",
            "liquidation_unspecified",
            "court_winding_up",
        },
    ),
    ("Companies Act notices", {"companies_act_notice"}),
    ("ICAV strike-offs", {"icav_voluntary_strike_off"}),
]

# Junk-pattern rejection for the entity_name display fallback. When entity_name
# matches one of these, the card shows display_title instead (or a graceful
# "Company name not extracted in this notice" if both are junky).
_JUNK_RE = "NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET|^Notice is hereby"


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS (corp-* family). Same precedent as si-* / pa-*.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_corp_css() -> None:
    st.markdown(
        """
        <style>
        .corp-context {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.5;
            margin: 0.35rem 0 1rem; max-width: 62rem;
        }
        .corp-context strong { color: #14232b; font-weight: 600; }
        .corp-context .corp-privacy {
            display: block; margin-top: 0.4rem;
            color: #7a5a00; font-size: 0.78rem;
        }
        /* Compact privacy line below the hero/callout — single sentence,
           visible but unobtrusive. Replaces the larger context paragraph
           which used to carry methodology language now lifted into the
           Sources & methodology expander. */
        .corp-privacy-line {
            font-size: 0.78rem; color: #7a5a00;
            margin: 0 0 1rem; line-height: 1.5;
        }
        .corp-privacy-line strong { color: #14232b; font-weight: 600; }

        /* FEATURED panel — receiver-appointer recognition story */
        .corp-featured {
            background: #fbf8f1;
            border: 1px solid #e5dfd0;
            border-radius: 10px;
            padding: 1.1rem 1.3rem 1.25rem;
            margin: 0 0 1.6rem;
            display: grid;
            grid-template-columns: minmax(0, 1.6fr) minmax(0, 1fr);
            gap: 1.4rem;
        }
        @media (max-width: 760px) {
            .corp-featured { grid-template-columns: 1fr; gap: 1.1rem; padding: 0.95rem 1rem 1.1rem; }
        }
        .corp-featured-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; font-weight: 600;
        }
        .corp-featured-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.55rem; color: #14232b;
        }
        .corp-featured-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem; line-height: 1.45;
        }
        .corp-rank-row {
            display: grid; grid-template-columns: 9rem 1fr 2.5rem;
            align-items: center; gap: 0.65rem;
            padding: 0.22rem 0; font-size: 0.86rem;
        }
        .corp-rank-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .corp-rank-name .corp-rank-type {
            display: block; font-size: 0.68rem; color: #7a5a00;
            text-transform: uppercase; letter-spacing: 0.05em;
            margin-top: -0.05rem;
        }
        /* Receiver-appointer type chip — coloured pill replacing the small
           grey subtitle so users see the vulture / bank / state mix at a glance. */
        .corp-rank-row { grid-template-columns: 9rem 1fr 7rem 2.5rem; }
        .corp-rank-typechip {
            display: inline-block;
            font-size: 0.66rem; line-height: 1.4;
            padding: 0.1rem 0.45rem;
            border-radius: 999px;
            text-transform: uppercase; letter-spacing: 0.04em;
            font-weight: 600;
            white-space: nowrap; text-align: center;
        }
        .corp-rank-typechip.vulture { background: #f9ebe7; border: 1px solid #d8b09e; color: #7c2e1e; }
        .corp-rank-typechip.bank    { background: #eef4f7; border: 1px solid #b9d0dc; color: #1f4757; }
        .corp-rank-typechip.state   { background: #eef3ec; border: 1px solid #cfe0c8; color: #2c4a23; }
        .corp-rank-typechip.servicer{ background: #f6f0e6; border: 1px solid #e6d9c2; color: #6b3f00; }
        .corp-rank-typechip.other   { background: #f5f1ea; border: 1px solid #e5e2db; color: #5b6b73; }

        /* Type-mix headline stat above the ranked list. */
        .corp-typebreakdown {
            display: flex; flex-wrap: wrap;
            gap: 0.4rem 0.85rem;
            font-size: 0.78rem; color: #14232b;
            margin: 0.55rem 0 0.85rem;
            line-height: 1.55;
        }
        .corp-typebreakdown span {
            display: inline-flex; align-items: baseline; gap: 0.3rem;
            font-variant-numeric: tabular-nums;
        }
        .corp-typebreakdown b { font-weight: 600; }
        .corp-typebreakdown .dot {
            width: 0.6rem; height: 0.6rem; border-radius: 50%;
            display: inline-block;
            transform: translateY(0.05rem);
        }
        .corp-typebreakdown .dot.vulture { background: #7c2e1e; }
        .corp-typebreakdown .dot.bank    { background: #1f4757; }
        .corp-typebreakdown .dot.state   { background: #2c4a23; }
        .corp-typebreakdown .dot.servicer{ background: #6b3f00; }
        .corp-typebreakdown .dot.other   { background: #5b6b73; }
        @media (max-width: 760px) {
            .corp-rank-row { grid-template-columns: 1fr 4rem 2rem; }
            .corp-rank-typechip { display: none; }
        }
        .corp-rank-bar {
            background: #efe6cf; border-radius: 2px; height: 0.55rem;
            overflow: hidden; position: relative;
        }
        .corp-rank-bar > span {
            display: block; height: 100%; background: #6b3f00; border-radius: 2px;
        }
        .corp-rank-count {
            text-align: right; color: #5b6b73; font-variant-numeric: tabular-nums;
            font-size: 0.85rem;
        }

        /* Year sparkline (clickable bars; no JS) */
        .corp-spark-wrap { display: flex; flex-direction: column; gap: 0.35rem; }
        .corp-spark-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #7a5a00; font-weight: 600;
        }
        .corp-spark-row {
            display: flex; align-items: flex-end; gap: 4px;
            height: 78px; padding: 0.25rem 0.1rem 0;
            border-bottom: 1px solid #d8cda9;
        }
        .corp-spark-bar {
            flex: 1 1 0; min-width: 10px; max-width: 26px;
            background: #c8b787; border-radius: 2px 2px 0 0;
            text-decoration: none;
            transition: background 100ms ease-out;
        }
        .corp-spark-bar:hover { background: #6b3f00; }
        .corp-spark-bar.is-spike { background: #6b3f00; }
        .corp-spark-bar.is-current { outline: 2px solid #14232b; outline-offset: 2px; }
        .corp-spark-years {
            display: flex; justify-content: space-between;
            font-size: 0.65rem; color: #7a5a00; letter-spacing: 0.04em;
            font-variant-numeric: tabular-nums;
        }
        .corp-spark-note {
            font-size: 0.75rem; color: #5b6b73; margin-top: 0.35rem;
        }

        /* Operator-side receiver-firms strip — sits as a thin sub-strip
           immediately below the appointer panel. Shows which professional
           firms are appointed AS receivers (Big 6 accountancy + boutique
           insolvency firms). Visually subordinate to the appointer panel:
           paler beige, smaller chips, single line. */
        .corp-operator-strip {
            background: #fef9f0;
            border: 1px solid #ead9b3;
            border-radius: 0 0 8px 8px;
            border-top: none;
            padding: 0.65rem 1.25rem 0.75rem;
            margin: -1.65rem 0 1.6rem;
            font-size: 0.78rem;
        }
        .corp-operator-strip-h {
            font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #7a5a00; font-weight: 600;
            display: inline-block; margin-right: 0.85rem;
        }
        .corp-operator-strip-context {
            color: #5b6b73; line-height: 1.55;
            margin-bottom: 0.4rem; font-size: 0.78rem;
        }
        .corp-operator-strip-chips {
            display: flex; flex-wrap: wrap; gap: 0.35rem 0.45rem;
            align-items: center;
        }
        .corp-operator-chip {
            display: inline-flex; align-items: baseline; gap: 0.3rem;
            background: #ffffff; border: 1px solid #e6d9c2;
            border-radius: 999px;
            padding: 0.12rem 0.55rem 0.12rem 0.7rem;
            font-size: 0.78rem;
            color: #14232b; line-height: 1.45;
            text-decoration: none; white-space: nowrap;
            font-variant-numeric: tabular-nums;
            transition: background 120ms ease-out, border-color 120ms ease-out;
        }
        .corp-operator-chip:hover {
            background: #fff7e6; border-color: #f0d99b;
        }
        .corp-operator-chip:focus-visible {
            outline: 2px solid #14232b; outline-offset: 1px;
        }
        .corp-operator-chip-n {
            color: #7a5a00; font-weight: 600;
        }
        .corp-operator-strip-tail {
            color: #5b6b73; font-size: 0.72rem;
            margin-left: 0.35rem;
        }

        /* Latest-full-year callout — small bordered card under the hero
           showing headline numbers with year-over-year deltas. Pure data;
           no interpretive labels. */
        .corp-thisyear {
            background: #ffffff;
            border: 1px solid #e5e2db;
            border-radius: 8px;
            padding: 0.85rem 1.1rem 0.95rem;
            margin: 0.5rem 0 1.2rem;
        }
        .corp-thisyear-h {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #5b6b73; font-weight: 600; margin-bottom: 0.6rem;
        }
        .corp-thisyear-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 1.2rem;
        }
        @media (max-width: 760px) {
            .corp-thisyear-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1rem 1.2rem; }
        }
        .corp-thisyear-num {
            font-family: ui-serif, Georgia, serif;
            font-size: 1.55rem; font-weight: 700;
            color: #14232b; line-height: 1.1;
            font-variant-numeric: tabular-nums;
        }
        .corp-thisyear-lbl {
            font-size: 0.78rem; color: #5b6b73; line-height: 1.4;
            margin-top: 0.2rem;
        }
        .corp-thisyear-delta {
            display: inline-block; margin-left: 0.3rem;
            font-variant-numeric: tabular-nums;
            font-weight: 500;
        }
        .corp-thisyear-delta.up   { color: #7c2e1e; }
        .corp-thisyear-delta.down { color: #2c4a23; }
        .corp-thisyear-delta.flat { color: #5b6b73; }

        /* Corporate-rescue panel — Examinership + SCARP. Counterbalances the
           failure-story panels with the rescue-story. Cool green palette to
           visually distinguish from the warm beige (distress) and cool blue
           (CBI regulatory) panels. */
        .corp-rescue-panel {
            background: #f1f6ee;
            border: 1px solid #cbdcc1;
            border-radius: 10px;
            padding: 1.05rem 1.25rem 1.2rem;
            margin: 0 0 1.6rem;
        }
        .corp-rescue-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #2c4a23; font-weight: 600;
        }
        .corp-rescue-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.45rem; color: #14232b;
        }
        .corp-rescue-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem; line-height: 1.45;
        }
        .corp-rescue-grid {
            display: grid;
            grid-template-columns: minmax(0, 1fr) minmax(0, 1.4fr);
            gap: 1.4rem;
        }
        @media (max-width: 760px) {
            .corp-rescue-grid { grid-template-columns: 1fr; gap: 1.1rem; }
        }
        .corp-rescue-yearbars {
            display: flex; flex-direction: column; gap: 0.3rem;
            font-size: 0.82rem;
        }
        .corp-rescue-yearrow {
            display: grid; grid-template-columns: 3rem 1fr 2.2rem;
            gap: 0.55rem; align-items: center;
            color: #14232b; font-variant-numeric: tabular-nums;
        }
        .corp-rescue-yearbar {
            background: #dbe6d3; border-radius: 2px; height: 0.55rem;
            overflow: hidden; position: relative;
        }
        .corp-rescue-yearbar > span {
            display: block; height: 100%; background: #5c7a4a; border-radius: 2px;
        }
        .corp-rescue-yearcount {
            text-align: right; color: #5b6b73; font-size: 0.8rem;
        }
        .corp-rescue-list { display: flex; flex-direction: column; gap: 0.35rem; }
        .corp-rescue-list-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #2c4a23; font-weight: 600; margin-bottom: 0.2rem;
        }
        .corp-rescue-item {
            display: grid;
            grid-template-columns: 4.4rem 1fr auto;
            gap: 0.55rem; align-items: baseline;
            padding: 0.32rem 0; font-size: 0.82rem;
            border-top: 1px solid #dee9d6;
        }
        .corp-rescue-item:first-of-type { border-top: none; }
        .corp-rescue-item-date {
            font-variant-numeric: tabular-nums; color: #5b6b73; font-size: 0.8rem;
        }
        .corp-rescue-item-firm {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .corp-rescue-typepill {
            display: inline-block;
            font-size: 0.66rem; line-height: 1.4;
            padding: 0.08rem 0.45rem;
            border-radius: 999px;
            text-transform: uppercase; letter-spacing: 0.04em;
            font-weight: 600;
            background: #dde9d4; border: 1px solid #b8cdab; color: #2c4a23;
            white-space: nowrap;
        }
        .corp-rescue-typepill.scarp {
            background: #f0e9d6; border-color: #d6c89a; color: #6b4f00;
        }

        /* Sources & methodology expander — collapsed by default; opens to
           a brand→parent table + plain-English glossary so experts can see
           how Beltany → Goldman Sachs etc. without leaving the page. */
        .corp-methodology {
            border: 1px solid #e5dfd0;
            border-radius: 8px;
            background: #f8f6f4;
            padding: 0;
            margin: -1.2rem 0 1.6rem;
        }
        .corp-methodology > summary {
            list-style: none;
            cursor: pointer;
            font-size: 0.78rem;
            color: #6b3f00;
            padding: 0.55rem 0.95rem;
            user-select: none;
            display: flex; align-items: center; gap: 0.5rem;
            font-weight: 500;
        }
        .corp-methodology > summary::-webkit-details-marker { display: none; }
        .corp-methodology > summary::before {
            content: "▸";
            font-size: 0.7rem;
            color: #7a5a00;
            transition: transform 120ms ease-out;
            display: inline-block;
        }
        .corp-methodology[open] > summary::before {
            transform: rotate(90deg);
        }
        .corp-methodology > summary:hover { color: #14232b; background: #f5f1ea; border-radius: 8px 8px 0 0; }
        .corp-methodology[open] > summary { border-bottom: 1px solid #e5e2db; border-radius: 8px 8px 0 0; }
        .corp-methodology-body {
            padding: 0.85rem 1.05rem 1rem;
        }
        .corp-methodology-intro {
            font-size: 0.82rem; color: #5b6b73; line-height: 1.55;
            margin: 0 0 0.85rem; max-width: 64rem;
        }
        .corp-methodology-intro code {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 3px;
            padding: 0.05rem 0.3rem; font-size: 0.78rem; color: #14232b;
        }
        .corp-methodology-table {
            width: 100%; border-collapse: collapse; font-size: 0.82rem;
            margin-bottom: 1rem;
        }
        .corp-methodology-table th {
            text-align: left; font-size: 0.7rem; text-transform: uppercase;
            letter-spacing: 0.06em; color: #5b6b73; font-weight: 600;
            padding: 0.4rem 0.55rem; border-bottom: 1px solid #cfcabf;
        }
        .corp-methodology-table td {
            padding: 0.5rem 0.55rem; border-bottom: 1px solid #ebe6d8;
            vertical-align: top; color: #14232b; line-height: 1.5;
        }
        .corp-methodology-table tr:last-child td { border-bottom: none; }
        .corp-meth-parent { font-weight: 600; white-space: nowrap; }
        .corp-meth-brands { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.78rem; color: #5b6b73; }
        .corp-meth-notes  { font-size: 0.78rem; color: #5b6b73; }
        .corp-methodology-glossary {
            margin: 0.6rem 0 0;
            font-size: 0.82rem; color: #14232b; line-height: 1.55;
            display: grid;
            grid-template-columns: 9rem 1fr;
            gap: 0.4rem 1rem;
        }
        .corp-methodology-glossary dt {
            font-weight: 600; color: #14232b;
        }
        .corp-methodology-glossary dd { margin: 0; color: #5b6b73; }
        .corp-methodology-glossary .corp-meth-glossary-h {
            grid-column: 1 / -1;
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.06em;
            color: #5b6b73; font-weight: 600;
            padding-top: 0.4rem; border-top: 1px solid #cfcabf;
            margin-top: 0.4rem;
        }
        @media (max-width: 640px) {
            .corp-methodology-glossary { grid-template-columns: 1fr; gap: 0.2rem 0; }
            .corp-methodology-table th:nth-child(3),
            .corp-methodology-table td:nth-child(3) { display: none; }
        }

        /* Active-filter chip bar */
        .corp-active-bar {
            display: flex; flex-wrap: wrap; gap: 0.4rem; align-items: center;
            padding: 0.5rem 0.75rem; background: #f5f1ea;
            border: 1px solid #e5e2db; border-radius: 6px;
            margin: 0.4rem 0 0.85rem;
        }
        .corp-active-label {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #5b6b73; margin-right: 0.3rem;
        }
        .corp-active-chip {
            display: inline-flex; align-items: center; gap: 0.3rem;
            background: #ffffff; border: 1px solid #cfdde6; border-radius: 999px;
            padding: 0.18rem 0.45rem 0.18rem 0.7rem; font-size: 0.78rem;
            color: #14232b; line-height: 1.4; white-space: nowrap;
            text-decoration: none;
            transition: background 120ms ease-out, border-color 120ms ease-out;
        }
        .corp-active-chip:hover { background: #fef2f2; border-color: #fca5a5; color: #7f1d1d; }
        .corp-active-chip:focus-visible { outline: 2px solid #14232b; outline-offset: 1px; }
        .corp-active-chip-x { font-size: 0.95rem; line-height: 1; color: #5b6b73; font-weight: 400; }
        .corp-active-chip:hover .corp-active-chip-x { color: #7f1d1d; }
        .corp-active-chip-all {
            background: transparent; border-color: #5b6b73; color: #5b6b73; padding-right: 0.7rem;
        }
        .corp-active-chip-all:hover { background: #14232b; border-color: #14232b; color: #ffffff; }

        /* Per-year breakdown strip (shown when a fund filter is active) */
        .corp-yearcount {
            background: #fbf8f1; border: 1px solid #e5dfd0; border-radius: 8px;
            padding: 0.6rem 0.85rem 0.7rem; margin: 0.5rem 0 0.4rem;
        }
        .corp-yearcount-h {
            font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; font-weight: 600; margin-bottom: 0.45rem;
        }
        .corp-yearcount-row {
            display: flex; flex-wrap: wrap; gap: 0.35rem 0.4rem; align-items: stretch;
        }
        .corp-yearcount-chip {
            display: inline-flex; flex-direction: column; align-items: center;
            gap: 0.05rem; min-width: 3.2rem;
            background: #ffffff; border: 1px solid #e6d9c2; border-radius: 7px;
            padding: 0.3rem 0.5rem; text-decoration: none; line-height: 1.2;
            transition: background 120ms ease-out, border-color 120ms ease-out;
        }
        .corp-yearcount-chip:hover { background: #fff7e6; border-color: #f0d99b; }
        .corp-yearcount-chip:focus-visible { outline: 2px solid #14232b; outline-offset: 1px; }
        .corp-yearcount-chip b {
            font-size: 0.82rem; color: #14232b; font-weight: 600;
            font-variant-numeric: tabular-nums;
        }
        .corp-yearcount-chip span {
            font-size: 0.92rem; color: #6b3f00; font-weight: 700;
            font-variant-numeric: tabular-nums;
        }
        .corp-yearcount-chip.is-active {
            background: #6b3f00; border-color: #6b3f00;
        }
        .corp-yearcount-chip.is-active b,
        .corp-yearcount-chip.is-active span { color: #ffffff; }

        /* Firm view (?firm=) — receiver/insolvency-firm landing */
        .corp-firm-head {
            background: #fbf8f1; border: 1px solid #e5dfd0; border-radius: 10px;
            padding: 1.05rem 1.3rem 1.2rem; margin: 0.6rem 0 1.4rem;
        }
        .corp-firm-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; font-weight: 600;
        }
        .corp-firm-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.45rem;
            line-height: 1.3; margin: 0.2rem 0 0.4rem; color: #14232b;
        }
        .corp-firm-sub { font-size: 0.85rem; color: #5b6b73; line-height: 1.5; max-width: 60rem; }
        .corp-firm-caveat {
            display: block; margin-top: 0.45rem; font-size: 0.76rem; color: #7a5a00;
        }
        .corp-firm-grid {
            display: grid; grid-template-columns: minmax(0,1fr) minmax(0,1fr);
            gap: 1.4rem; margin: 0 0 1.5rem;
        }
        @media (max-width: 760px) { .corp-firm-grid { grid-template-columns: 1fr; gap: 1.1rem; } }
        .corp-firm-panel {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 9px;
            padding: 0.9rem 1.1rem 1rem;
        }
        .corp-firm-panel-h {
            font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.07em;
            color: #5b6b73; font-weight: 600; margin-bottom: 0.6rem;
        }
        .corp-firm-row {
            display: grid; grid-template-columns: minmax(0,1fr) 1fr 2.6rem;
            gap: 0.6rem; align-items: center; padding: 0.24rem 0; font-size: 0.85rem;
            text-decoration: none; color: inherit;
        }
        a.corp-firm-row:hover .corp-firm-row-name { color: #6b3f00; text-decoration: underline; }
        .corp-firm-row-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
        }
        .corp-firm-bar { background: #efe6cf; border-radius: 2px; height: 0.5rem; overflow: hidden; }
        .corp-firm-bar > span { display: block; height: 100%; background: #6b3f00; border-radius: 2px; }
        .corp-firm-count {
            text-align: right; color: #5b6b73; font-variant-numeric: tabular-nums; font-size: 0.82rem;
        }

        /* Recent feed */
        .corp-month-h {
            font-family: ui-serif, Georgia, serif;
            font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.12em;
            color: #5b6b73; font-weight: 700;
            margin: 1.2rem 0 0.45rem;
            padding-bottom: 0.3rem; border-bottom: 1px solid #e5e2db;
        }
        .corp-month-h:first-of-type { margin-top: 0.4rem; }

        .corp-card-link {
            display: block; text-decoration: none; color: inherit;
            margin-bottom: 0.5rem;
        }
        .corp-card-link:focus-visible {
            outline: 2px solid #14232b; outline-offset: 2px; border-radius: 8px;
        }
        .corp-card-link:hover .corp-card { border-color: #c9cfd3;
            box-shadow: 0 1px 3px rgba(20,35,43,0.08); }
        .corp-card-link:hover .corp-card-who { color: #0a1418; }

        .corp-card {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.85rem 1.05rem;
            display: grid; grid-template-columns: 5.5rem 1fr auto;
            gap: 1rem; align-items: baseline;
            transition: border-color 120ms ease-out, box-shadow 120ms ease-out;
        }
        @media (max-width: 640px) {
            .corp-card { grid-template-columns: 1fr; gap: 0.35rem; padding: 0.75rem 0.9rem; }
            .corp-card-meta { order: -1; }
        }
        .corp-card-date {
            font-variant-numeric: tabular-nums; font-size: 0.82rem; color: #5b6b73;
        }
        .corp-card-who {
            font-family: ui-serif, Georgia, serif; font-size: 1.0rem; line-height: 1.45;
            color: #14232b; margin: 0;
        }
        .corp-card-who .name { font-weight: 600; }
        .corp-card-who .missing { color: #8a7a4a; font-style: italic; }
        .corp-card-meta {
            display: inline-flex; gap: 0.35rem; flex-wrap: wrap;
            justify-content: flex-end; align-items: center;
        }

        .corp-pill {
            display: inline-block; padding: 0.16rem 0.55rem; border-radius: 999px;
            font-size: 0.7rem; line-height: 1.45; white-space: nowrap;
        }
        .corp-pill-subtype {
            background: #f5f1ea; border: 1px solid #e5e2db; color: #14232b;
        }
        .corp-pill-fund {
            background: #f6f0e6; border: 1px solid #e6d9c2; color: #6b3f00;
            font-weight: 500;
        }
        .corp-pill-receiver { background: #f0eaf6; border: 1px solid #cdbfe2; color: #41306a; }
        .corp-pill-examiner { background: #eef3ec; border: 1px solid #cfe0c8; color: #2c4a23; }
        .corp-pill-icav { background: #fff7e6; border: 1px solid #f0d99b; color: #7a5a00; }
        /* CBI authorisation badge — applied on cards/detail when the
           wound-up entity is itself a CBI-authorised firm. */
        .corp-pill-cbi {
            background: #eef4f7; border: 1px solid #b9d0dc; color: #1f4757;
            font-weight: 500;
            font-variant-numeric: tabular-nums;
        }
        .corp-pill-cbi-ref { color: #5b6b73; font-weight: 400; }

        /* CBI repeat-distress panel — sibling of corp-featured but in a
           cooler regulatory tone to distinguish from the receiver-appointer
           story. Marked as experimental in the kicker. */
        .corp-cbi-panel {
            background: #f4f8fb;
            border: 1px solid #cfdde6;
            border-radius: 10px;
            padding: 1.05rem 1.25rem 1.2rem;
            margin: 0 0 1.6rem;
        }
        .corp-cbi-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #1f4757; font-weight: 600;
            display: inline-flex; align-items: center; gap: 0.45rem;
        }
        .corp-cbi-kicker .corp-cbi-tag {
            background: #fffaeb; border: 1px solid #f0d99b; color: #7a5a00;
            border-radius: 999px; padding: 0.05rem 0.45rem; font-size: 0.65rem;
            letter-spacing: 0.05em; font-weight: 600;
        }
        .corp-cbi-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.18rem;
            line-height: 1.35; margin: 0.25rem 0 0.45rem; color: #14232b;
        }
        .corp-cbi-sub {
            font-size: 0.82rem; color: #5b6b73; margin-bottom: 0.85rem; line-height: 1.45;
        }
        .corp-cbi-row {
            display: grid;
            grid-template-columns: minmax(0, 1.8fr) 4rem 4rem minmax(0, 1.2fr);
            gap: 0.65rem; align-items: baseline;
            padding: 0.45rem 0;
            border-top: 1px solid #e0ebf2;
            font-size: 0.86rem;
        }
        .corp-cbi-row:first-of-type { border-top: none; }
        .corp-cbi-row-head {
            font-size: 0.66rem; text-transform: uppercase; letter-spacing: 0.06em;
            color: #5b6b73; padding-bottom: 0.25rem; border-bottom: 1px solid #cfdde6;
            border-top: none;
        }
        .corp-cbi-name {
            color: #14232b; overflow: hidden; text-overflow: ellipsis;
            white-space: nowrap;
        }
        .corp-cbi-name strong { font-weight: 600; }
        .corp-cbi-name a { color: inherit; text-decoration: none; }
        .corp-cbi-name a:hover { text-decoration: underline; }
        .corp-cbi-count {
            text-align: right; color: #14232b; font-variant-numeric: tabular-nums;
        }
        .corp-cbi-count-routine {
            text-align: right; color: #8a7a4a; font-variant-numeric: tabular-nums;
            font-size: 0.82rem;
        }
        .corp-cbi-reg {
            color: #5b6b73; font-size: 0.78rem; line-height: 1.35;
            overflow: hidden; text-overflow: ellipsis;
        }
        .corp-cbi-reg .corp-cbi-refno {
            font-family: ui-monospace, "SF Mono", Menlo, monospace;
            color: #1f4757;
        }
        @media (max-width: 760px) {
            .corp-cbi-row {
                grid-template-columns: 1fr 3.5rem 3.5rem;
            }
            .corp-cbi-row .corp-cbi-reg {
                grid-column: 1 / -1;
                padding-left: 0;
            }
        }

        /* Detail */
        .corp-detail {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 10px;
            padding: 1.4rem 1.55rem; margin-top: 0.6rem;
        }
        .corp-detail-ref {
            font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.78rem;
            color: #5b6b73;
        }
        .corp-detail-title {
            font-family: ui-serif, Georgia, serif; font-size: 1.45rem; line-height: 1.3;
            color: #14232b; margin: 0.35rem 0 1rem;
        }
        .corp-detail-row {
            display: flex; gap: 0.85rem; padding: 0.55rem 0;
            border-top: 1px solid #f0ece5; align-items: flex-start;
        }
        .corp-detail-row:first-of-type { border-top: none; padding-top: 0; }
        .corp-detail-label {
            width: 170px; flex-shrink: 0; font-size: 0.72rem; text-transform: uppercase;
            letter-spacing: 0.06em; color: #5b6b73; padding-top: 0.18rem;
        }
        .corp-detail-val { flex: 1; font-size: 0.95rem; color: #14232b; line-height: 1.55; }
        .corp-detail-raw {
            background: #f9f5ec; border: 1px solid #ead9b3; border-radius: 6px;
            padding: 0.85rem 1rem; margin-top: 0.8rem;
            font-size: 0.88rem; line-height: 1.6; color: #14232b;
            white-space: pre-wrap;
        }
        .corp-detail-raw-kicker {
            font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.08em;
            color: #7a5a00; margin-bottom: 0.4rem;
        }

        /* Per-company search results panel */
        .corp-company-panel {
            background: #ffffff; border: 1px solid #e5e2db; border-radius: 8px;
            padding: 0.9rem 1.1rem; margin: 0.4rem 0 1rem;
        }
        .corp-company-h {
            font-family: ui-serif, Georgia, serif; font-size: 1.05rem;
            color: #14232b; margin: 0 0 0.2rem;
        }
        .corp-company-sub {
            font-size: 0.78rem; color: #5b6b73; margin-bottom: 0.6rem;
        }

        /* Mobile hero tightening (SI/PA precedent) */
        @media (max-width: 640px) {
            .dt-hero .dt-kicker { display: none; }
            .dt-hero .dt-dek {
                display: -webkit-box; -webkit-line-clamp: 2;
                -webkit-box-orient: vertical; overflow: hidden;
                font-size: 0.82rem; margin-top: 0.25rem;
            }
            .dt-hero { padding: 0.7rem 0.95rem 0.65rem !important; }
            .dt-hero h1 { font-size: 1.2rem !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Corporate notices…")
def load_corporate() -> pd.DataFrame:
    df = fetch_corporate_notices()
    if df.empty:
        return df
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["year"] = df["issue_date"].dt.year
    df = df.reset_index(drop=True)
    # Stable display-time ref for rows where notice_ref is null (split fragments)
    fallback = pd.Series([f"row-{i}" for i in range(len(df))], index=df.index)
    has_ref = df["notice_ref"].notna() & (df["notice_ref"].astype(str).str.strip() != "")
    df["display_ref"] = df["notice_ref"].where(has_ref, fallback)

    # Materialise an exploded "any-parent-mentioned" column for fund filtering
    # without the page doing list-join business logic on its own. Stored as a
    # comma-joined string for easy str.contains() filtering.
    def _join_list(lst, sep=", "):
        if lst is None or not hasattr(lst, "__iter__"):
            return ""
        items = list(lst)
        return sep.join(str(x) for x in items) if items else ""

    df["_parent_mentions_str"] = df["parent_fund_mentions"].apply(_join_list)
    return df


# Same legal-form strip as extractors/cbi_registers_extract._norm_firm —
# inlined here so the page can normalise entity_name → entity_norm at row time
# (the corporate_notices parquet has no stable per-row primary key we can use
# to pre-join; notice_ref is sparse and shared across many rows).
_NORM_SUFFIX_RE = re.compile(
    r"\b(public limited company|limited liability partnership|limited|ltd\.?|"
    r"plc|llp|sa|nv|gmbh|inc\.?)\b\.?",
    re.I,
)


def _norm_entity(name) -> str:
    if name is None or (isinstance(name, float) and pd.isna(name)):
        return ""
    s = str(name)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    s = re.sub(r"\bt/?a\b.*$", "", s)
    s = _NORM_SUFFIX_RE.sub(" ", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


@st.cache_data(show_spinner=False)
def load_cbi_badges() -> list[tuple[str, dict]]:
    """Sorted (entity_norm, badge_info) list for on-card CBI badge lookup.

    Sandbox source — see sql_views/corporate/corporate_cbi_distress.sql header for
    provenance. Returned as a list sorted by name-length descending so the
    badge resolver can take the *longest* matching CBI firm name appearing as
    a word-boundary substring of a notice's normalised entity_name. This is
    needed because corporate_notices entity_name often carries notice-text
    prefixes ('presented to the High Court by ...', '... in its capacity as
    trustee') that exact-match-equality misses but substring matching
    correctly handles. The CBI side already requires corporate keywords +
    ≥6 chars in the firm name, so substring matching is safe from noise.
    """
    df = fetch_cbi_notice_matches()
    if df.empty:
        return []
    seen: dict[str, dict] = {}
    for _, r in df.iterrows():
        norm = r.get("entity_norm")
        if not norm or len(str(norm)) < 6:
            continue
        if norm not in seen:
            seen[str(norm)] = {
                "register": str(r.get("primary_register") or ""),
                "ref_no": str(r.get("primary_ref_no") or ""),
            }
    # Longest-first ordering so longest match wins (avoids "wealth" eating
    # "wealth options trustees").
    return sorted(seen.items(), key=lambda kv: -len(kv[0]))


def _resolve_cbi_badge(entity_name, badges: list[tuple[str, dict]]) -> dict | None:
    """Find the longest CBI firm name appearing as a delimited substring of
    the given notice's normalised entity_name. Returns None when no match."""
    if not badges:
        return None
    ent_norm = _norm_entity(entity_name)
    if not ent_norm or len(ent_norm) < 6:
        return None
    padded = f" {ent_norm} "
    for cand_norm, info in badges:
        if f" {cand_norm} " in padded:
            return info
    return None


@st.cache_data(show_spinner=False)
def load_cbi_repeat_distress() -> pd.DataFrame:
    """Per-firm aggregate for the regulated-firms-in-repeat-distress panel."""
    return fetch_cbi_repeat_distress()


@st.cache_data(show_spinner=False)
def load_brand_aliases() -> pd.DataFrame:
    """Curated brand → parent → fund_type table for the methodology expander."""
    return fetch_brand_aliases()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _safe(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v)


def _pretty_subtype(s: str) -> str:
    return _SUBTYPE_LABEL.get(s, (s or "").replace("_", " ").capitalize() or "—")


def _subtype_pill_class(subtype: str) -> str:
    if subtype == "receivership":
        return "corp-pill-receiver"
    if subtype in ("examinership", "scarp_process_adviser"):
        return "corp-pill-examiner"
    if subtype == "icav_voluntary_strike_off":
        return "corp-pill-icav"
    return "corp-pill-subtype"


# Iris archive-URL formula is shared with public_appointments — see ui.source_pdfs.
_iris_archive_url = iris_archive_url


def _card_name(row: pd.Series) -> tuple[str, bool]:
    """Return (display_name, was_missing_or_junk). Prefers entity_name; falls back
    to display_title when entity_name matches junk patterns or is null."""
    en = _safe(row.get("entity_name"))
    if en and not pd.Series([en]).str.upper().str.contains(_JUNK_RE, regex=True).iloc[0]:
        return en, False
    dt = _safe(row.get("display_title"))
    if dt and not pd.Series([dt]).str.upper().str.contains(_JUNK_RE, regex=True).iloc[0]:
        return dt, True  # cleaner, but flagged as fallback
    return "Company name not extracted in this notice", True


# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────
def _apply_filters(
    df: pd.DataFrame, years: list[int], subtypes: set | None, fund: str, search: str, type_label: str
) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["year"].isin(years)]
    if subtypes:
        out = out[out["notice_subtype"].isin(subtypes)]
    if fund and fund != "All":
        out = out[out["_parent_mentions_str"].astype(str).str.contains(fund, case=False, na=False)]
    if search:
        s = search.strip().lower()
        if s:
            mask = (
                out["entity_name"].fillna("").astype(str).str.lower().str.contains(s, na=False)
                | out["display_title"].fillna("").astype(str).str.lower().str.contains(s, na=False)
                | out["raw_text"].fillna("").astype(str).str.lower().str.contains(s, na=False)
            )
            out = out[mask]
    # type_label tracked in chips even though it's the same as subtypes set
    return out


def _clear_facet(key: str) -> None:
    defaults = {
        "year": ([], "corp_year_filter"),
        "fund": ("All", "corp_fund_filter"),
        "search": ("", "corp_search"),
        "type": (0, "corp_type_idx"),
    }
    if key == "all":
        for v, k in defaults.values():
            st.session_state[k] = v
        # Also reset the segmented_control's own widget state (keyed separately).
        st.session_state["corp_type_radio"] = _TYPE_GROUPS[0][0]
        return
    if key in defaults:
        v, k = defaults[key]
        st.session_state[k] = v
        if key == "type":
            st.session_state["corp_type_radio"] = _TYPE_GROUPS[0][0]


# ──────────────────────────────────────────────────────────────────────────────
# Featured panel — receiver-appointer ranking + year sparkline
# ──────────────────────────────────────────────────────────────────────────────
def _render_featured(df: pd.DataFrame) -> None:
    """logic_firewall: display_only — ranks by parent fund + a small yearly
    trend. All aggregation here is presentation, not modelling."""
    # Receivership-shaped subset, same definition as the enrichment uses.
    recv = df[
        (df["notice_subtype"] == "receivership")
        | df["raw_text"]
        .fillna("")
        .astype(str)
        .str.contains(
            "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER",
            case=False,
            regex=True,
            na=False,
        )
    ]
    n_recv = len(recv)
    if n_recv == 0:
        return

    # Explode parent_fund_mentions and count per parent (each notice counted once
    # per distinct parent).
    parent_rows: list[dict] = []
    for _, r in recv.iterrows():
        parents = r.get("parent_fund_mentions")
        parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []
        ftypes = r.get("fund_type_mentions")
        ftypes = list(ftypes) if ftypes is not None and hasattr(ftypes, "__iter__") else []
        for i, p in enumerate(parents):
            if p:
                parent_rows.append(
                    {
                        "parent": p,
                        "ftype": (ftypes[i] if i < len(ftypes) else "") or "",
                    }
                )
    if not parent_rows:
        st.html(
            '<section class="corp-featured" aria-label="Featured receiver-appointer panel">'
            '<div><div class="corp-featured-kicker">Receiver-appointers</div>'
            '<h2 class="corp-featured-h">Who&apos;s calling in Irish loans</h2>'
            f'<div class="corp-featured-sub">No known major loan-book buyer or Irish bank '
            f"mentioned in the current scope (of {n_recv:,} receivership notices).</div>"
            "</div></section>"
        )
        return

    # SPV-shape detector — entity_name ending in / containing DAC, ICAV, or
    # the long form "DESIGNATED ACTIVITY COMPANY". These are Section 110 SPVs
    # or fund vehicles used by vulture funds / banks to hold loan books.
    _SPV_RE = re.compile(r"\b(DAC|DESIGNATED ACTIVITY COMPANY|ICAV)\b", re.I)
    n_spv = int(recv["entity_name"].fillna("").astype(str).str.contains(_SPV_RE, regex=True, na=False).sum())
    spv_pct = round(100 * n_spv / max(n_recv, 1))

    pdf = pd.DataFrame(parent_rows)
    # Dominant type per parent — mode, not "first" (the old picker mislabelled
    # Cerberus as 'credit servicer' even though its dominant role across
    # receivership notices is 'vulture fund'). Ties broken by canonical priority.
    _TYPE_PRIORITY = {
        "vulture fund": 0,
        "credit servicer": 1,
        "Irish bank": 2,
        "Irish bank (winding down)": 2,
        "Irish bank (exited)": 2,
        "state asset manager": 3,
        "state agency": 3,
    }

    def _dominant_ftype(s: pd.Series) -> str:
        counts = s.value_counts()  # logic_firewall: display_only
        if counts.empty:
            return ""
        top_n = counts.iloc[0]
        winners = counts[counts == top_n].index.tolist()
        # Priority tiebreak so canonical role (vulture > servicer > bank > state) wins
        winners.sort(key=lambda x: (_TYPE_PRIORITY.get(x, 99), x))
        return winners[0]

    parent_to_ftype: dict[str, str] = pdf.groupby("parent")["ftype"].agg(_dominant_ftype).to_dict()

    top = (
        pdf.groupby("parent")
        .size()
        .rename("n")
        .reset_index()
        .assign(ftype=lambda d: d["parent"].map(parent_to_ftype))
        .sort_values("n", ascending=False)
        .head(FEATURED_TOP_N)
        .set_index("parent")
    )
    n_tagged = int(
        recv["parent_fund_mentions"]
        .apply(lambda x: bool(x is not None and hasattr(x, "__iter__") and len(list(x)) > 0))
        .sum()
    )
    coverage_pct = round(100 * n_tagged / max(n_recv, 1))

    # Type-mix breakdown across ALL tagged parent mentions (not just top-N).
    # Bucket the fine-grained Irish-bank variants + state variants for the
    # headline stat.
    def _type_bucket(ft: str) -> str:
        if not ft:
            return "other"
        ft_low = ft.lower()
        if "vulture" in ft_low:
            return "vulture"
        if "servicer" in ft_low:
            return "servicer"
        if "irish bank" in ft_low or ft_low.startswith("bank"):
            return "bank"
        if "state" in ft_low or "nama" in ft_low or "revenue" in ft_low:
            return "state"
        return "other"

    pdf["bucket"] = pdf["parent"].map(parent_to_ftype).map(_type_bucket)
    bucket_counts = pdf["bucket"].value_counts()  # logic_firewall: display_only
    bucket_total = int(bucket_counts.sum()) or 1
    bucket_pct = {k: round(100 * v / bucket_total) for k, v in bucket_counts.items()}
    _BUCKET_LABEL = {
        "vulture": "vulture funds",
        "servicer": "credit servicers",
        "bank": "Irish banks",
        "state": "state (NAMA / Revenue)",
        "other": "other",
    }
    breakdown_parts: list[str] = []
    for b in ("vulture", "bank", "servicer", "state", "other"):
        pct = bucket_pct.get(b, 0)
        if pct == 0:
            continue
        breakdown_parts.append(f'<span><span class="dot {b}"></span><b>{pct}%</b> {_BUCKET_LABEL[b]}</span>')
    breakdown_html = (
        (
            '<div class="corp-typebreakdown" aria-label="Type breakdown of appointing parties">'
            + "".join(breakdown_parts)
            + "</div>"
        )
        if breakdown_parts
        else ""
    )

    # Short labels used inside the chip — the full type sits in the title tooltip.
    _BUCKET_CHIP = {
        "vulture": "Vulture fund",
        "bank": "Irish bank",
        "servicer": "Credit servicer",
        "state": "State",
        "other": "Other",
    }
    max_n = int(top["n"].iloc[0])
    rows_html: list[str] = []
    for parent, row in top.iterrows():
        width = max(8, int(round(100 * (int(row["n"]) / max_n))))
        ftype = row.get("ftype") or ""
        bucket = _type_bucket(ftype)
        chip_html = (
            (
                f'<span class="corp-rank-typechip {bucket}" title="{html.escape(ftype)}">'
                f"{html.escape(_BUCKET_CHIP.get(bucket, 'Other'))}"
                "</span>"
            )
            if ftype
            else ""
        )
        rows_html.append(
            f'<a class="corp-rank-row" href="?fund={html.escape(str(parent), quote=True)}" '
            f'target="_self" style="text-decoration:none;color:inherit;" '
            f'aria-label="Filter to {html.escape(parent, quote=True)} ({int(row["n"])} notices)">'
            f'<div class="corp-rank-name" title="{html.escape(parent)}">'
            f"{html.escape(parent)}"
            f"</div>"
            f'<div class="corp-rank-bar"><span style="width:{width}%"></span></div>'
            f"{chip_html}"
            f'<div class="corp-rank-count">{int(row["n"])}</div>'
            "</a>"
        )

    # Year sparkline of the receiver wave. Pure-data note (peak / low /
    # latest-full-year + counts) — no causal framing per the no-inference rule.
    yc = recv["year"].dropna().astype(int).value_counts().sort_index()  # logic_firewall: display_only
    if yc.empty:
        spark_html = ""
    else:
        ymin, ymax = int(yc.index.min()), int(yc.index.max())
        years_full = list(range(ymin, ymax + 1))
        counts = [int(yc.get(y, 0)) for y in years_full]
        spike_threshold = sorted(counts, reverse=True)[2] if len(counts) >= 3 else max(counts)
        peak = max(counts) if counts else 1
        current_years = set(int(y) for y in (st.session_state.get("corp_year_filter") or []))

        bars: list[str] = []
        for y, c in zip(years_full, counts, strict=True):
            h_pct = max(4, int(round(100 * (c / peak)))) if peak else 4
            klass = ["corp-spark-bar"]
            if spike_threshold > 0 and c >= spike_threshold:
                klass.append("is-spike")
            if y in current_years:
                klass.append("is-current")
            tip = f"{y}: {c} receivership{'s' if c != 1 else ''}"
            bars.append(
                f'<a class="{" ".join(klass)}" '
                f'href="?spark={y}" target="_self" '
                f'style="height:{h_pct}%" '
                f'aria-label="{tip}" title="{tip}"></a>'
            )

        # Pure-data annotation: peak year + count, low year + count, latest
        # full year + count. No editorial framing.
        import datetime as _dt

        today_y = _dt.date.today().year
        full_year_counts = [(y, c) for y, c in zip(years_full, counts, strict=True) if y < today_y]
        peak_y, peak_c = max(full_year_counts, key=lambda t: t[1]) if full_year_counts else (None, None)
        low_y, low_c = min(full_year_counts, key=lambda t: t[1]) if full_year_counts else (None, None)
        latest_full_y, latest_full_c = full_year_counts[-1] if full_year_counts else (None, None)
        ann_parts = []
        if peak_y is not None:
            ann_parts.append(f"peak: <strong>{peak_y}</strong> ({peak_c})")
        if low_y is not None and low_y != peak_y:
            ann_parts.append(f"low: <strong>{low_y}</strong> ({low_c})")
        if latest_full_y is not None and latest_full_y not in (peak_y, low_y):
            ann_parts.append(f"{latest_full_y}: ({latest_full_c})")
        ann_text = " · ".join(ann_parts)

        spark_html = (
            '<div class="corp-spark-wrap">'
            '<div class="corp-spark-label">Receivership notices by year</div>'
            f'<div class="corp-spark-row">{"".join(bars)}</div>'
            f'<div class="corp-spark-years"><span>{ymin}</span><span>{ymax}</span></div>'
            f'<div class="corp-spark-note">Click a year to filter · {ann_text}</div>'
            "</div>"
        )

    st.markdown(
        '<section class="corp-featured" aria-label="Featured receiver-appointer panel">'
        "<div>"
        '<div class="corp-featured-kicker">Receiver-appointers</div>'
        '<h2 class="corp-featured-h">Who\'s calling in Irish loans</h2>'
        f'<div class="corp-featured-sub">'
        f"Of <strong>{n_recv:,}</strong> receivership notices, "
        f"<strong>{n_tagged:,}</strong> ({coverage_pct}%) name a known appointer. "
        f"<strong>{n_spv:,}</strong> ({spv_pct}%) have a wound-up entity whose name contains "
        f"<em>DAC</em>, <em>Designated Activity Company</em> or <em>ICAV</em>. "
        f"Click a row to filter the page."
        f"</div>" + breakdown_html + "".join(rows_html) + "</div>"
        f"<div>{spark_html}</div>"
        "</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Latest-full-year callout — pure-data snapshot under the hero. The "latest
# full year" is determined by excluding the current calendar year (which is
# always partial at render time). Numbers update automatically.
# logic_firewall: display_only.
# ──────────────────────────────────────────────────────────────────────────────
def _render_this_year_callout(df: pd.DataFrame, cbi_repeat: pd.DataFrame | None) -> None:
    if df is None or df.empty:
        return
    today_year = datetime.date.today().year
    all_years = sorted(int(y) for y in df["year"].dropna().astype(int).unique())
    # Latest "full" year — exclude the calendar current year as partial-at-render.
    full_years = [y for y in all_years if y < today_year]
    if not full_years:
        return
    cur = full_years[-1]
    prev = full_years[-2] if len(full_years) >= 2 else None

    recv = df[df["notice_subtype"] == "receivership"]
    n_recv_cur = int((recv["year"].astype("Int64") == cur).sum())
    n_recv_prev = int((recv["year"].astype("Int64") == prev).sum()) if prev else 0

    resc = df[df["notice_subtype"].isin(_RESCUE_SUBTYPES)]
    n_resc_cur = int((resc["year"].astype("Int64") == cur).sum())
    n_resc_prev = int((resc["year"].astype("Int64") == prev).sum()) if prev else 0

    # Distinct firms named in this year's notices (across all subtypes).
    n_firms_cur = int(df.loc[df["year"].astype("Int64") == cur, "entity_name"].nunique())

    # CBI repeat distress count — already filtered by SQL view.
    n_repeat = len(cbi_repeat) if cbi_repeat is not None else 0

    def _delta_html(c: int, p: int) -> str:
        if not p:
            return ""
        d = c - p
        pct = round(100 * d / p)
        if d > 0:
            cls, sign = "up", "▲"
        elif d < 0:
            cls, sign = "down", "▼"
        else:
            return ""
        return f'<span class="corp-thisyear-delta {cls}">{sign}{abs(pct)}% vs {prev}</span>'

    st.markdown(
        f'<section class="corp-thisyear" aria-label="Latest full year snapshot">'
        f'<div class="corp-thisyear-h">Latest full year — {cur}</div>'
        f'<div class="corp-thisyear-grid">'
        f'<div><div class="corp-thisyear-num">{n_recv_cur:,}</div>'
        f'<div class="corp-thisyear-lbl">receivership notices{_delta_html(n_recv_cur, n_recv_prev)}</div></div>'
        f'<div><div class="corp-thisyear-num">{n_resc_cur:,}</div>'
        f'<div class="corp-thisyear-lbl">rescues (examinership + SCARP){_delta_html(n_resc_cur, n_resc_prev)}</div></div>'
        f'<div><div class="corp-thisyear-num">{n_firms_cur:,}</div>'
        f'<div class="corp-thisyear-lbl">distinct companies named in {cur}</div></div>'
        f'<div><div class="corp-thisyear-num">{n_repeat:,}</div>'
        f'<div class="corp-thisyear-lbl">CBI-authorised firms with repeat distress (since 2016)</div></div>'
        f"</div>"
        f"</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Operator-side: which professional firms are appointed AS receiver. This is
# the counterpart to the appointer panel — the latter shows the funds calling
# in loans; this strip shows the accountancy / boutique firms doing the work.
# logic_firewall: display_only. Single-pass regex over raw_text of the
# receivership-shaped subset; cached via @st.cache_data.
# ──────────────────────────────────────────────────────────────────────────────
_OPERATOR_PATTERNS_WORD = [
    ("Deloitte", re.compile(r"\bDeloitte\b")),
    ("Grant Thornton", re.compile(r"\bGrant Thornton\b")),
    ("Mazars", re.compile(r"\bMazars\b")),
    ("Kroll", re.compile(r"\bKroll\b")),
    ("Crowe", re.compile(r"\bCrowe\b")),
    ("Friel Stafford", re.compile(r"\bFriel Stafford\b")),
    ("McKeogh Gallagher Ryan", re.compile(r"\bMcKeogh Gallagher Ryan\b")),
    ("McStay Luby", re.compile(r"\bMcStay Luby\b")),
    ("Hughes Blake", re.compile(r"\bHughes Blake\b")),
    ("Baker Tilly", re.compile(r"\bBaker Tilly\b")),
    ("Cooney Carey", re.compile(r"\bCooney Carey\b")),
    ("FTI Consulting", re.compile(r"\bFTI Consulting\b")),
    ("Interpath", re.compile(r"\bInterpath\b")),
    ("Teneo", re.compile(r"\bTeneo\b")),
]
# Case-sensitive uppercase abbreviation matches — lowercase variants are too
# noisy ('ey' matched inside many unrelated words during probing).
_OPERATOR_PATTERNS_CASE = [
    ("EY", re.compile(r"\bEY\b")),
    ("KPMG", re.compile(r"\bKPMG\b")),
    ("BDO", re.compile(r"\bBDO\b")),
    ("RBK", re.compile(r"\bRBK\b")),
    ("OCKT", re.compile(r"\bOCKT\b")),
]
_OPERATOR_PWC = re.compile(r"\b(?:PwC|PWC|PricewaterhouseCoopers|PriceWaterhouseCoopers)\b")


@st.cache_data(show_spinner=False)
def _receiver_firm_concentration(raw_texts: tuple[str, ...]) -> list[tuple[str, int]]:
    """Count distinct receivership notices mentioning each professional firm.
    Each firm is counted at most ONCE per notice — so the number reflects
    notice presence, not raw mention frequency."""
    if not raw_texts:
        return []
    counts: dict[str, int] = {}
    for raw in raw_texts:
        if not raw:
            continue
        present: set[str] = set()
        for name, pat in _OPERATOR_PATTERNS_WORD:
            if pat.search(raw):
                present.add(name)
        for name, pat in _OPERATOR_PATTERNS_CASE:
            if pat.search(raw):
                present.add(name)
        if _OPERATOR_PWC.search(raw):
            present.add("PwC")
        for name in present:
            counts[name] = counts.get(name, 0) + 1
    return sorted(counts.items(), key=lambda kv: -kv[1])


def _render_operator_strip(df: pd.DataFrame) -> None:
    """Thin sub-strip below the appointer panel listing receiver firms by
    notice presence. Each chip links to ?q=<firm> to filter the feed."""
    if df is None or df.empty:
        return
    recv = df[
        (df["notice_subtype"] == "receivership")
        | df["raw_text"]
        .fillna("")
        .astype(str)
        .str.contains(
            "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER",
            case=False,
            regex=True,
            na=False,
        )
    ]
    if recv.empty:
        return
    raw_texts = tuple(recv["raw_text"].fillna("").astype(str).tolist())
    top = _receiver_firm_concentration(raw_texts)[:10]
    if not top:
        return

    n_recv = len(recv)
    # Distinct notices mentioning at least one operator firm — recompute cheaply
    # via a single combined regex pass (only used for the headline %).
    combined_pat = re.compile(
        r"\bDeloitte\b|\bGrant Thornton\b|\bMazars\b|\bKroll\b|\bCrowe\b|"
        r"\bFriel Stafford\b|\bMcKeogh Gallagher Ryan\b|\bMcStay Luby\b|"
        r"\bHughes Blake\b|\bBaker Tilly\b|\bCooney Carey\b|\bFTI Consulting\b|"
        r"\bInterpath\b|\bTeneo\b|\bEY\b|\bKPMG\b|\bBDO\b|\bRBK\b|\bOCKT\b|"
        r"\b(?:PwC|PWC|PricewaterhouseCoopers|PriceWaterhouseCoopers)\b"
    )
    n_any_tagged = sum(1 for t in raw_texts if combined_pat.search(t or ""))
    cov_pct = round(100 * n_any_tagged / max(n_recv, 1))

    # Big 6 share of those tagged (the concentration story)
    big6 = {"Deloitte", "EY", "PwC", "KPMG", "Grant Thornton", "BDO", "Mazars"}
    n_big6 = sum(n for name, n in top if name in big6)
    big6_pct_of_tagged = round(100 * n_big6 / max(n_any_tagged, 1))

    chip_html: list[str] = []
    for name, n in top:
        chip_html.append(
            f'<a class="corp-operator-chip" href="?firm={html.escape(name, quote=True)}" '
            f'target="_self" aria-label="Open the {html.escape(name, quote=True)} firm view">'
            f"{html.escape(name)}"
            f'<span class="corp-operator-chip-n">{n:,}</span>'
            "</a>"
        )

    st.markdown(
        '<section class="corp-operator-strip" aria-label="Receiver-firm concentration strip">'
        '<div class="corp-operator-strip-context">'
        '<span class="corp-operator-strip-h">Who&apos;s doing the work</span>'
        f"Across <strong>{n_recv:,}</strong> receivership notices, "
        f"<strong>{n_any_tagged:,}</strong> ({cov_pct}%) mention a known professional firm — "
        f"the Big 6 accountancy firms account for <strong>{big6_pct_of_tagged}%</strong> of those. "
        f"Click a chip to filter the feed."
        "</div>"
        f'<div class="corp-operator-strip-chips">{"".join(chip_html)}'
        f'<span class="corp-operator-strip-tail">notice presence · regex over raw text</span>'
        "</div>"
        "</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Firm view (?firm=) — a dedicated landing for a receiver / insolvency firm.
# Mirrors the fund click-through but adds the firm-specific cross-references:
#   1. the fund↔firm connection (which appointing funds/banks the firm is most
#      often named alongside) — the insight unique to this dataset,
#   2. the role mix (which notice types the firm shows up in), then
#   3. the case list, year-grouped, with Iris links (reuses _render_feed).
# Honesty: matching is regex notice-presence over raw_text (same as the strip),
# so copy says "named in", never "was receiver in". logic_firewall: display_only.
# ──────────────────────────────────────────────────────────────────────────────
_OPERATOR_PATTERN_BY_NAME: dict[str, re.Pattern[str]] = {
    **{name: pat for name, pat in _OPERATOR_PATTERNS_WORD},
    **{name: pat for name, pat in _OPERATOR_PATTERNS_CASE},
    "PwC": _OPERATOR_PWC,
}


def _firm_notice_mask(df: pd.DataFrame, firm: str) -> pd.Series:
    """Boolean mask of notices whose raw_text names the firm, using the same
    pattern the operator strip counts with (literal word-boundary fallback for
    any firm not in the curated list)."""
    pat = _OPERATOR_PATTERN_BY_NAME.get(firm) or re.compile(r"\b" + re.escape(firm) + r"\b")
    return df["raw_text"].fillna("").astype(str).apply(lambda t: bool(pat.search(t)))


def _explode_fund_counts(sub: pd.DataFrame) -> list[tuple[str, int]]:
    """Count how often each appointing parent fund is co-named across a subset,
    one count per notice per distinct parent."""
    counts: dict[str, int] = {}
    for parents in sub["parent_fund_mentions"]:
        seen: set[str] = set()
        if parents is None or not hasattr(parents, "__iter__"):
            continue
        for p in parents:
            if p and p not in seen:
                counts[p] = counts.get(p, 0) + 1
                seen.add(p)
    return sorted(counts.items(), key=lambda kv: -kv[1])


def _firm_ranked_rows(items: list[tuple[str, int]], link_fund: bool = False) -> str:
    """Render a small ranked bar list (name · bar · count). When link_fund, each
    row pivots to that fund's view via ?fund=."""
    if not items:
        return '<div style="font-size:0.82rem;color:#8a7a4a;">None named in these notices.</div>'
    top = items[:8]
    max_n = max(n for _, n in top) or 1
    rows: list[str] = []
    for name, n in top:
        width = max(8, int(round(100 * n / max_n)))
        inner = (
            f'<div class="corp-firm-row-name" title="{html.escape(name)}">{html.escape(name)}</div>'
            f'<div class="corp-firm-bar"><span style="width:{width}%"></span></div>'
            f'<div class="corp-firm-count">{n:,}</div>'
        )
        if link_fund:
            rows.append(
                f'<a class="corp-firm-row" href="?fund={html.escape(name, quote=True)}" target="_self" '
                f'aria-label="See {html.escape(name, quote=True)} ({n} notices)">{inner}</a>'
            )
        else:
            rows.append(f'<div class="corp-firm-row">{inner}</div>')
    return "".join(rows)


def _render_firm_view(df: pd.DataFrame, firm: str, cbi_badges: list[tuple[str, dict]] | None = None) -> None:
    if back_button("← Back to corporate notices", key="corp_firm"):
        st.session_state.pop("corp_firm_view", None)
        st.query_params.clear()
        st.rerun()

    fdf = df[_firm_notice_mask(df, firm)]
    n = len(fdf)
    if n == 0:
        empty_state(
            f"No notices name {firm}",
            "This firm isn't matched in the current corporate-notice corpus.",
        )
        return

    # Receivership-shaped subset — the fund↔firm connection is sharpest here.
    recv = fdf[
        (fdf["notice_subtype"] == "receivership")
        | fdf["raw_text"]
        .fillna("")
        .astype(str)
        .str.contains(
            "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER",
            case=False,
            regex=True,
            na=False,
        )
    ]
    fund_rows = _explode_fund_counts(recv if not recv.empty else fdf)

    # Role mix — notice-type breakdown.  logic_firewall: display_only
    sub_counts = [(_pretty_subtype(s), int(c)) for s, c in fdf["notice_subtype"].value_counts().items()]

    yrs = fdf["year"].dropna().astype(int)
    yr_span = f"{int(yrs.min())}–{int(yrs.max())}" if not yrs.empty else "—"

    st.html(
        '<section class="corp-firm-head" aria-label="Firm overview">'
        '<div class="corp-firm-kicker">Receiver / insolvency firm</div>'
        f'<h1 class="corp-firm-h">{html.escape(firm)}</h1>'
        '<div class="corp-firm-sub">'
        f"Named in <strong>{n:,}</strong> corporate notice{'s' if n != 1 else ''} "
        f"({yr_span}), of which <strong>{len(recv):,}</strong> are receiverships. "
        '<span class="corp-firm-caveat">A firm named in a notice is not necessarily the '
        "appointed receiver — it may appear as solicitor, auditor or filing agent. "
        "Counts are notice presence (regex over the published text), not confirmed appointments.</span>"
        "</div></section>"
    )

    st.html(
        '<div class="corp-firm-grid">'
        '<div class="corp-firm-panel">'
        '<div class="corp-firm-panel-h">Most often named alongside (appointing fund / bank)</div>'
        + _firm_ranked_rows(fund_rows, link_fund=True)
        + "</div>"
        '<div class="corp-firm-panel">'
        '<div class="corp-firm-panel-h">By notice type</div>'
        + _firm_ranked_rows(sub_counts, link_fund=False)
        + "</div>"
        "</div>"
    )

    # Case list — year-grouped, Iris-linked (reuses the feed renderer).
    _render_feed(fdf, cbi_badges, group_by_year=True)


# ──────────────────────────────────────────────────────────────────────────────
# Methodology expander — sits just below the receiver-appointer panel.
# Surfaces the brand → parent_fund mapping so a reader sees, e.g., that
# Beltany is tagged as Goldman Sachs. Collapsed by default.
# ──────────────────────────────────────────────────────────────────────────────
_TYPE_BUCKET_FOR_HTML = {
    "vulture fund": "vulture",
    "credit servicer": "servicer",
    "Irish bank": "bank",
    "Irish bank (winding down)": "bank",
    "Irish bank (exited)": "bank",
    "state asset manager": "state",
    "state agency": "state",
}

_METHODOLOGY_GLOSSARY = [
    (
        "Vulture fund",
        "US or UK private-equity / distressed-debt investor that buys Irish loan "
        "books at a discount. Cerberus, Goldman Sachs, Oaktree, Lone Star, Apollo "
        "are the largest active in Ireland.",
    ),
    (
        "Credit servicer",
        "CBI-authorised firm that operates day-to-day collection on loans owned "
        "by a vulture fund. Sometimes the same parent (Cerberus / Pepper); "
        "sometimes a third-party servicer (Pepper, Mars Capital, BCMGlobal).",
    ),
    (
        "SPV / DAC",
        "Special-Purpose Vehicle, usually a Designated Activity Company. "
        "An Irish legal shell incorporated to hold a specific loan portfolio. "
        "Most are Section 110 companies, which gives tax-efficient treatment of "
        "interest payments.",
    ),
    (
        "Schedule 2 firm",
        "Non-bank financial firm registered with the Central Bank under "
        "Schedule 2 of the Criminal Justice Act 2010 (as amended) for AML / CFT "
        "supervision only — not prudentially regulated like a bank. Most Section "
        "110 SPVs sit here.",
    ),
    (
        "NAMA",
        "National Asset Management Agency — the state bad-bank that bought "
        "distressed property loans from Irish banks 2010-2014.",
    ),
]


def _render_methodology_expander(aliases: pd.DataFrame) -> None:
    """Collapsed `<details>` block under the receiver-appointer panel showing
    the curated brand → parent mapping + plain-English type glossary. Source
    of truth is data/_meta/loan_book_fund_aliases.csv."""
    if aliases is None or aliases.empty:
        return

    # Group brands by parent + canonical type for the table. `notes` is an
    # optional column in the curated CSV — aggregate it only when present so a
    # leaner alias file (brand/parent_fund/fund_type only) still renders.
    agg_kwargs = {"brands": ("brand", lambda s: ", ".join(sorted(s)))}
    if "notes" in aliases.columns:
        agg_kwargs["notes_concat"] = ("notes", lambda s: " · ".join(sorted({str(n) for n in s if str(n).strip()})))
    grouped = (
        aliases.groupby(["parent_fund", "fund_type"], as_index=False)  # logic_firewall: display_only
        .agg(**agg_kwargs)
        .sort_values(["fund_type", "parent_fund"])
    )
    if "notes_concat" not in grouped.columns:
        grouped["notes_concat"] = ""

    rows_html: list[str] = []
    for _, r in grouped.iterrows():
        bucket = _TYPE_BUCKET_FOR_HTML.get(str(r["fund_type"]), "other")
        rows_html.append(
            "<tr>"
            f'<td class="corp-meth-parent">{html.escape(str(r["parent_fund"]))}</td>'
            f'<td class="corp-meth-brands">{html.escape(str(r["brands"]))}</td>'
            f'<td><span class="corp-rank-typechip {bucket}">{html.escape(str(r["fund_type"]))}</span></td>'
            f'<td class="corp-meth-notes">{html.escape(str(r["notes_concat"]))}</td>'
            "</tr>"
        )

    glossary_html = "".join(f"<dt>{html.escape(t)}</dt><dd>{html.escape(d)}</dd>" for t, d in _METHODOLOGY_GLOSSARY)

    st.markdown(
        '<details class="corp-methodology">'
        "<summary>Sources &amp; methodology — how brands map to parent funds</summary>"
        '<div class="corp-methodology-body">'
        '<p class="corp-methodology-intro">'
        "Brand-to-parent classification is hand-curated in "
        "<code>data/_meta/loan_book_fund_aliases.csv</code> from public sources "
        "(CBI Credit Servicers register, news reporting, regulatory filings). "
        "Each notice's raw text is scanned for the brand strings below; matching "
        "rolls up to the parent and the fund_type. The receiver-appointer panel "
        "counts unique parent appearances."
        "</p>"
        '<table class="corp-methodology-table">'
        "<thead><tr>"
        "<th>Parent fund</th><th>Brand strings matched in notices</th>"
        "<th>Type</th><th>Source / note</th>"
        "</tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>"
        '<dl class="corp-methodology-glossary">'
        '<dt class="corp-meth-glossary-h">Plain-English terms</dt><dd></dd>'
        f"{glossary_html}"
        "</dl>"
        "</div>"
        "</details>",
        unsafe_allow_html=True,
    )

    # Let the reader take the full curated mapping away as a CSV (the same
    # source-of-truth rows the table above is built from, ungrouped). Captioned
    # so it stays self-describing even when the methodology block is collapsed.
    st.caption(
        "Curated financial-institution list — every loan-book brand, its parent "
        "fund and type (vulture fund, credit servicer, Irish bank, state body)."
    )
    export_button(
        aliases,
        label=f"Download financial-institution mapping ({len(aliases):,} brands, CSV)",
        filename="dail_tracker_loan_book_fund_aliases.csv",
        key="corp_alias_csv_download",
    )


# ──────────────────────────────────────────────────────────────────────────────
# CBI repeat-distress panel (experimental)
# ──────────────────────────────────────────────────────────────────────────────
def _shorten_register(name: str) -> str:
    """Compact a long CBI register name for the side cell of the panel row.

    Order matters: do the long specific matches FIRST while the original
    "Register of" / "Authorised" wording is still intact, then strip the
    generic prefixes for any register names not covered above.
    """
    if not name:
        return ""
    n = name
    # Drop trailing "as at [date]" — also matches when the trailing text is
    # truncated to just "as at" with no date (source data quirk).
    n = re.sub(r"\s+as at\b.*$", "", n).strip()
    # Long-phrase canonical labels FIRST (depend on prefixes still being present).
    long_map = [
        ("Insurance Distribution Register", "Insurance Distribution"),
        ("Register of Insurance Distribution", "Insurance Distribution"),
        ("Register of Authorised and Registered Alternative Investment Fund Managers", "AIFMs"),
        (
            "Register of Alternative Investment Fund Managers operating in Ireland on a Branc",
            "AIFMs (Branch/Cross-border)",
        ),
        (
            "Register of Investment Business Firms authorised under Section 10 of the Investm",
            "Investment Business (S.10)",
        ),
        (
            "Register of Investment Business Firms deemed to be authorised as Investment Inte",
            "Investment Business (RAIPI)",
        ),
        (
            "Register of Investment Product Intermediaries Section 31 Register",
            "Investment Product Intermediaries (S.31)",
        ),
        ("Authorised UCITS European Communities Undertakings for Collective Investment in", "UCITS"),
        (
            "Register of UCITS Management Companies operating in Ireland on a Branch or Cross",
            "UCITS Mgmt Cos (Branch/Cross-border)",
        ),
        ("Authorised UCITS Management Companies", "UCITS Mgmt Cos"),
        ("Register of Trust or Company Service Providers that are Subsidiaries of Credit o", "TCSPs (subsidiaries)"),
        ("Register of Credit Unions maintained by the Central Bank of Ireland pursuant to", "Credit Unions (PSD)"),
        ("Register of Credit Unions maintained by the Central Bank of Ireland under Regula", "Credit Unions (EMI)"),
        ("Register of Crowdfunding Service Providers", "Crowdfunding"),
        ("Register of Authorised Crypto Asset Service Providers Under Article 63 of Regula", "Crypto (MiCAR)"),
        ("Register of Virtual Asset Service Providers", "VASPs"),
        ("Authorised Irish Collective Asset management Vehicles Irish Collective Asset man", "ICAV (authorised)"),
        ("Authorised Designated Investment Companies Companies Act 1990 Part XIII", "Designated Investment Companies"),
        (
            "Authorised Investment Limited Partnerships Investment Limited Partnerships Act 1",
            "Investment Limited Partnerships",
        ),
        (
            "Authorised Common Contractual Funds Investment Funds Companies and Miscellaneous",
            "Common Contractual Funds",
        ),
        ("Authorised Unit Trust Schemes Unit Trust Act 1990", "Unit Trust Schemes"),
        ("Register of Registered Irish Collective Asset management Vehicles ICAV", "Registered ICAVs"),
        ("Register of Charges created by Irish Collective Asset management Vehicles ICAV", "Charges by ICAVs"),
    ]
    for old, new in long_map:
        if old in n:
            n = n.replace(old, new)
            break
    # Generic prefixes for anything not covered above.
    n = n.replace("Register of ", "").replace("List of ", "")
    return n.strip().rstrip(",")


def _render_cbi_repeat_distress(df_cbi: pd.DataFrame) -> None:
    """logic_firewall: display_only — surfaces v_corporate_cbi_repeat_distress
    as a 7-row panel. No aggregations are done here; HAVING clause in the SQL
    view enforces the distress-vs-routine filter so the page can't drift."""
    if df_cbi is None or df_cbi.empty:
        return

    n_firms = len(df_cbi)
    n_distress_total = int(df_cbi["n_distress"].sum())

    rows_html: list[str] = [
        '<div class="corp-cbi-row corp-cbi-row-head">'
        "<div>Firm (CBI-authorised)</div>"
        '<div style="text-align:right;">Distress<br>notices</div>'
        '<div style="text-align:right;">Total<br>notices</div>'
        "<div>Register</div>"
        "</div>"
    ]

    for _, r in df_cbi.iterrows():
        name = _safe(r.get("entity_name")) or "—"
        primary_reg = _shorten_register(_safe(r.get("primary_register")))
        primary_ref = _safe(r.get("primary_ref_no"))
        n_distress = int(r.get("n_distress") or 0)
        n_total = int(r.get("n_notices_total") or 0)
        # Link the firm name to a search on the existing corp_search box
        link = f"?clear=year&q={html.escape(name, quote=True)}"
        ref_html = f' · <span class="corp-cbi-refno">{html.escape(primary_ref)}</span>' if primary_ref else ""
        rows_html.append(
            '<div class="corp-cbi-row">'
            f'<div class="corp-cbi-name" title="{html.escape(name)}">'
            f'<a href="{link}" target="_self"><strong>{html.escape(name)}</strong></a>'
            f"</div>"
            f'<div class="corp-cbi-count">{n_distress}</div>'
            f'<div class="corp-cbi-count-routine">{n_total}</div>'
            f'<div class="corp-cbi-reg">{html.escape(primary_reg)}{ref_html}</div>'
            "</div>"
        )

    st.markdown(
        '<section class="corp-cbi-panel" aria-label="Regulated firms in repeat distress (experimental)">'
        '<div class="corp-cbi-kicker">CBI authorisation '
        '<span class="corp-cbi-tag">EXPERIMENTAL</span></div>'
        '<h2 class="corp-cbi-h">Regulated firms with repeat distress notices</h2>'
        '<div class="corp-cbi-sub">'
        f"<strong>{n_firms}</strong> CBI-authorised firms have appeared in "
        f"<strong>{n_distress_total}</strong> receivership, court winding-up, examinership, "
        f"creditors' liquidation or SCARP notices since 2016 — and at least twice each. "
        f"Solvent fund wind-ups (Members' Voluntary Liquidation) are filtered out. "
        f"Match is by exact normalised entity name against CBI register snapshots; "
        f"see the sandbox extract for caveats."
        "</div>" + "".join(rows_html) + "</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Corporate-rescue panel — Examinership + SCARP. Counterbalances the
# distress story with the rescue story. logic_firewall: display_only.
# ──────────────────────────────────────────────────────────────────────────────
_RESCUE_SUBTYPES = {"examinership", "scarp_process_adviser"}


def _render_rescue_panel(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    resc = df[df["notice_subtype"].isin(_RESCUE_SUBTYPES)].copy()
    n_total = len(resc)
    if n_total == 0:
        return

    n_exam = int((resc["notice_subtype"] == "examinership").sum())
    n_scarp = int((resc["notice_subtype"] == "scarp_process_adviser").sum())

    # Year-by-year bars — exclude any null/invalid years
    resc_years = resc["year"].dropna().astype(int)
    if resc_years.empty:
        yc = pd.Series(dtype=int)
    else:
        ymin, ymax = int(resc_years.min()), int(resc_years.max())
        yc = (  # logic_firewall: display_only
            resc_years.value_counts().reindex(range(ymin, ymax + 1), fill_value=0).sort_index()
        )

    max_yc = int(yc.max()) if not yc.empty else 1

    year_rows_html: list[str] = []
    for y, c in yc.items():
        if c == 0 and y < int(resc_years.min()):  # don't render leading zeros
            continue
        width = max(4, int(round(100 * (int(c) / max_yc)))) if max_yc else 4
        year_rows_html.append(
            f'<a class="corp-rescue-yearrow" href="?spark={int(y)}" target="_self" '
            f'style="text-decoration:none;color:inherit;" '
            f'aria-label="Filter feed to {int(y)} ({int(c)} rescue notice{"s" if c != 1 else ""})">'
            f"<div>{int(y)}</div>"
            f'<div class="corp-rescue-yearbar"><span style="width:{width}%"></span></div>'
            f'<div class="corp-rescue-yearcount">{int(c)}</div>'
            "</a>"
        )

    # Recent rescues — newest 8 with extracted entity name
    recent = resc.sort_values("issue_date", ascending=False).head(8)
    list_rows_html: list[str] = []
    for _, r in recent.iterrows():
        name, _missing = _card_name(r)
        date_str = _fmt_date(r.get("issue_date"))
        st_label = "SCARP" if r["notice_subtype"] == "scarp_process_adviser" else "Examiner"
        st_cls = "scarp" if r["notice_subtype"] == "scarp_process_adviser" else ""
        list_rows_html.append(
            '<div class="corp-rescue-item">'
            f'<div class="corp-rescue-item-date">{html.escape(date_str)}</div>'
            f'<div class="corp-rescue-item-firm" title="{html.escape(name)}">{html.escape(name)}</div>'
            f'<span class="corp-rescue-typepill {st_cls}">{html.escape(st_label)}</span>'
            "</div>"
        )

    st.markdown(
        '<section class="corp-rescue-panel" aria-label="Corporate rescue panel">'
        '<div class="corp-rescue-kicker">Corporate rescue</div>'
        '<h2 class="corp-rescue-h">Firms in rescue</h2>'
        '<div class="corp-rescue-sub">'
        f"<strong>{n_total:,}</strong> firms have entered formal rescue since 2016 — "
        f"<strong>{n_exam}</strong> examinerships (court-supervised restructure) and "
        f"<strong>{n_scarp}</strong> SCARP filings (small-company process; first filings 2021). "
        f"See year bars for the annual trend."
        "</div>"
        '<div class="corp-rescue-grid">'
        "<div>"
        '<div class="corp-rescue-list-label">Rescues by year (click to filter feed)</div>'
        f'<div class="corp-rescue-yearbars">{"".join(year_rows_html)}</div>'
        "</div>"
        "<div>"
        '<div class="corp-rescue-list-label">Most recent</div>'
        f'<div class="corp-rescue-list">{"".join(list_rows_html)}</div>'
        "</div>"
        "</div>"
        "</section>",
        unsafe_allow_html=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Search + facets
# ──────────────────────────────────────────────────────────────────────────────
def _active_filter_chips(full_df: pd.DataFrame) -> list[tuple[str, str]]:
    chips: list[tuple[str, str]] = []
    all_yrs = set(int(y) for y in full_df["year"].dropna().unique())
    yrs = st.session_state.get("corp_year_filter") or []
    if yrs and set(int(y) for y in yrs) != all_yrs:
        if len(yrs) <= 2:
            for y in sorted(yrs, reverse=True):
                chips.append((str(int(y)), "year"))
        else:
            chips.append((f"Years ({len(yrs)})", "year"))
    if (f := st.session_state.get("corp_fund_filter")) and f != "All":
        chips.append((f"Fund: {f}", "fund"))
    s = (st.session_state.get("corp_search") or "").strip()
    if s:
        chips.append((f'"{s}"', "search"))
    return chips


def _render_facets(full_df: pd.DataFrame) -> int:
    """Renders search + facets and returns the selected type-tab index."""
    if full_df.empty:
        return 0

    # Row 1 — full-record search.
    st.text_input(
        "Search by company name",
        placeholder="Search company name, body of notice, anywhere in the corpus…",
        key="corp_search",
        label_visibility="collapsed",
    )

    # Active-filter chip bar.
    active = _active_filter_chips(full_df)
    if active:
        chip_html = "".join(
            f'<a class="corp-active-chip" href="?clear={k}" target="_self" '
            f'aria-label="Remove filter: {html.escape(label, quote=True)}">'
            f'{html.escape(label)}<span class="corp-active-chip-x" aria-hidden="true">×</span>'
            "</a>"
            for label, k in active
        )
        chip_html += (
            '<a class="corp-active-chip corp-active-chip-all" href="?clear=all" '
            'target="_self" aria-label="Clear all filters">Clear all</a>'
        )
        st.html(f'<div class="corp-active-bar"><span class="corp-active-label">Filtered by</span>{chip_html}</div>')

    # Row 2 — year pills.
    yrs = sorted((int(y) for y in full_df["year"].dropna().unique()), reverse=True)
    yc = full_df["year"].astype("Int64").value_counts().to_dict()  # logic_firewall: display_only
    current_year = datetime.date.today().year
    st.pills(
        "Year",
        yrs,
        default=[],
        selection_mode="multi",
        key="corp_year_filter",
        format_func=lambda y: f"{y} · {yc.get(y, 0):,} YTD" if y == current_year else f"{y} · {yc.get(y, 0):,}",
    )

    # Row 3 — fund picker (parent funds known in the data).
    parents_in_data: list[str] = []
    for lst in full_df["parent_fund_mentions"]:
        if lst is not None and hasattr(lst, "__iter__"):
            parents_in_data.extend(list(lst))
    fund_counts = pd.Series(parents_in_data).value_counts()  # logic_firewall: display_only
    fund_opts = ["All"] + fund_counts.index.tolist()
    st.selectbox(
        "Appointing fund / bank",
        fund_opts,
        index=0,
        key="corp_fund_filter",
        format_func=lambda x: "All funds and banks" if x == "All" else f"{x} · {int(fund_counts.get(x, 0)):,}",
    )

    # Row 4 — sub-type tabs.
    tab_labels = [g[0] for g in _TYPE_GROUPS]
    type_idx = st.session_state.get("corp_type_idx", 0) or 0
    # Seed the widget's own key (not `default=`) so a ?clear=type chip can reset
    # it by writing corp_type_radio directly, and so we avoid the "default value
    # but also set via Session State" warning when both exist.
    st.session_state.setdefault("corp_type_radio", tab_labels[type_idx])
    selected = st.segmented_control(
        "Type",
        tab_labels,
        key="corp_type_radio",
        label_visibility="collapsed",
    )
    if selected not in tab_labels:  # deselection → keep the current sub-type
        selected = tab_labels[type_idx]
    new_idx = tab_labels.index(selected)
    if new_idx != type_idx:
        st.session_state["corp_type_idx"] = new_idx
    return new_idx


# ──────────────────────────────────────────────────────────────────────────────
# Feed
# ──────────────────────────────────────────────────────────────────────────────
def _render_card(row: pd.Series, cbi_badges: list[tuple[str, dict]] | None = None) -> str:
    ref = html.escape(_safe(row.get("display_ref")) or "", quote=True)
    subtype = _safe(row.get("notice_subtype"))
    subtype_label = _pretty_subtype(subtype)
    subtype_cls = _subtype_pill_class(subtype)
    parents = row.get("parent_fund_mentions")
    parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []

    name, is_missing = _card_name(row)
    date_str = _fmt_date(row.get("issue_date"))

    name_html = (
        f'<span class="missing">{html.escape(name)}</span>'
        if is_missing
        else f'<span class="name">{html.escape(name)}</span>'
    )

    pills = [f'<span class="corp-pill {subtype_cls}">{html.escape(subtype_label)}</span>']
    for p in parents[:2]:
        pills.append(f'<span class="corp-pill corp-pill-fund">{html.escape(str(p))}</span>')
    if len(parents) > 2:
        pills.append(f'<span class="corp-pill corp-pill-fund">+{len(parents) - 2}</span>')

    # Experimental: CBI authorisation badge, when the notice's entity is on
    # a Central Bank register. Substring-resolved so notice-text prefixes
    # ("presented to the High Court by …") don't break the match.
    if cbi_badges:
        info = _resolve_cbi_badge(row.get("entity_name"), cbi_badges)
        if info:
            short_reg = _shorten_register(info.get("register", ""))
            refno = info.get("ref_no") or ""
            tip = f"CBI-authorised: {info.get('register', '')}" + (f" · {refno}" if refno else "")
            label = "CBI · " + short_reg if short_reg else "CBI-authorised"
            pills.append(
                f'<span class="corp-pill corp-pill-cbi" title="{html.escape(tip)}">'
                f"{html.escape(label)}"
                + (f' <span class="corp-pill-cbi-ref">{html.escape(refno)}</span>' if refno else "")
                + "</span>"
            )

    return (
        f'<a class="corp-card-link" href="?ref={ref}" target="_self">'
        '<div class="corp-card">'
        f'<div class="corp-card-date">{html.escape(date_str)}</div>'
        f'<div class="corp-card-who">{name_html}</div>'
        f'<div class="corp-card-meta">{"".join(pills)}</div>'
        "</div></a>"
    )


def _render_feed(
    df: pd.DataFrame,
    cbi_badges: list[tuple[str, dict]] | None = None,
    group_by_year: bool = False,
) -> None:
    if df.empty:
        empty_state(
            "No notices match these filters",
            "Try widening the year, dropping the fund filter, or clearing the search.",
        )
        return

    total = len(df)

    # When a fund is selected, lead with a per-year breakdown of that fund's
    # notices so the "separated by year" picture is visible before paging. Each
    # chip drills into that year (re-uses the ?spark= year handler).
    if group_by_year:
        yc = df["year"].dropna().astype(int).value_counts().sort_index()  # logic_firewall: display_only
        if not yc.empty:
            active_years = set(int(y) for y in (st.session_state.get("corp_year_filter") or []))
            chips = "".join(
                f'<a class="corp-yearcount-chip{" is-active" if y in active_years else ""}" '
                f'href="?spark={y}" target="_self" '
                f'aria-label="Filter to {y} ({c} notice{"s" if c != 1 else ""})">'
                f"<b>{y}</b><span>{c:,}</span></a>"
                for y, c in yc.items()
            )
            st.html(
                '<div class="corp-yearcount" aria-label="Notices by year">'
                '<div class="corp-yearcount-h">By year</div>'
                f'<div class="corp-yearcount-row">{chips}</div>'
                "</div>"
            )

    st.html(
        f'<div style="margin:0.4rem 0 0.2rem;font-size:0.85rem;color:#5b6b73;">'
        f"<strong>{total:,}</strong> notice{'s' if total != 1 else ''} "
        f"match the current filters, sorted newest first."
        f"</div>"
    )

    page_idx = paginate(total, key_prefix="corp_feed", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    last_key: str | None = None
    parts: list[str] = []
    for _, r in visible.iterrows():
        d = r.get("issue_date")
        try:
            ts = pd.Timestamp(d) if pd.notna(d) else None
        except Exception:
            ts = None
        if ts is None:
            group_key = "UNDATED"
        elif group_by_year:
            group_key = str(ts.year)
        else:
            group_key = ts.strftime("%B %Y").upper()
        if group_key != last_key:
            parts.append(f'<div class="corp-month-h">{html.escape(group_key)}</div>')
            last_key = group_key
        parts.append(_render_card(r, cbi_badges))

    st.html("".join(parts))

    pagination_controls(
        total,
        key_prefix="corp_feed",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="notices",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Detail
# ──────────────────────────────────────────────────────────────────────────────
def _render_detail(row: pd.Series, cbi_badges: list[tuple[str, dict]] | None = None) -> None:
    if back_button("← Back to corporate notices", key="corp_detail"):
        st.session_state.pop("corp_selected_ref", None)
        st.query_params.clear()
        st.rerun()

    subtype = _safe(row.get("notice_subtype"))
    subtype_label = _pretty_subtype(subtype)
    subtype_cls = _subtype_pill_class(subtype)
    entity = _safe(row.get("entity_name"))
    display_title = _safe(row.get("display_title"))
    parents = row.get("parent_fund_mentions")
    parents = list(parents) if parents is not None and hasattr(parents, "__iter__") else []
    raw_text = _safe(row.get("raw_text"))
    src_pdf = _safe(row.get("iris_source_pdf"))
    date_str = _fmt_date(row.get("issue_date"))
    ref = _safe(row.get("notice_ref")) or _safe(row.get("display_ref"))

    name, is_missing = _card_name(row)
    headline = name if not is_missing else (display_title or "—")

    # Detail-header CBI badge (experimental).
    cbi_badge_html = ""
    if cbi_badges:
        info = _resolve_cbi_badge(row.get("entity_name"), cbi_badges)
        if info:
            short_reg = _shorten_register(info.get("register", ""))
            refno = info.get("ref_no") or ""
            tip = f"CBI-authorised: {info.get('register', '')}" + (f" · {refno}" if refno else "")
            label = "CBI · " + short_reg if short_reg else "CBI-authorised"
            cbi_badge_html = (
                f'<span class="corp-pill corp-pill-cbi" title="{html.escape(tip)}">'
                f"{html.escape(label)}"
                + (f' <span class="corp-pill-cbi-ref">{html.escape(refno)}</span>' if refno else "")
                + "</span> "
            )

    st.html(
        '<div class="corp-detail">'
        f'<div class="corp-detail-ref">{html.escape(ref)}</div>'
        f'<div class="corp-detail-title">{html.escape(headline)}</div>'
        f'<span class="corp-pill {subtype_cls}">{html.escape(subtype_label)}</span> '
        + "".join(f'<span class="corp-pill corp-pill-fund">{html.escape(str(p))}</span> ' for p in parents)
        + cbi_badge_html
        + "</div>"
    )

    rows_html: list[str] = []

    def _row(label: str, value_html: str) -> None:
        rows_html.append(
            f'<div class="corp-detail-row">'
            f'<div class="corp-detail-label">{html.escape(label)}</div>'
            f'<div class="corp-detail-val">{value_html}</div>'
            f"</div>"
        )

    _row("Date", html.escape(date_str))
    _row("Notice type", html.escape(subtype_label))
    if entity:
        _row("Entity (extracted)", html.escape(entity))
    if display_title and display_title != entity:
        _row("Display title", html.escape(display_title))
    if parents:
        _row(
            "Parent fund / bank",
            ", ".join(html.escape(str(p)) for p in parents),
        )
    iris_url = _iris_archive_url(row.get("issue_date"))
    if src_pdf or iris_url:
        if iris_url:
            val_html = (
                f'<a href="{html.escape(iris_url, quote=True)}" target="_blank" rel="noopener" '
                f'style="color:#1f4757;text-decoration:none;border-bottom:1px solid #b9d0dc;">'
                f"Iris Oifigiúil — {html.escape(date_str)} ↗</a>"
                f'<div style="font-family:ui-monospace,Menlo,monospace;font-size:0.78rem;'
                f'color:#8a8a8a;margin-top:0.2rem;">{html.escape(src_pdf)} '
                f"· opens the issue's PDF list</div>"
            )
        else:
            val_html = (
                f'<span style="font-family:ui-monospace,Menlo,monospace;'
                f'font-size:0.85rem;color:#5b6b73;">{html.escape(src_pdf)}</span>'
            )
        rows_html.append(
            '<div class="corp-detail-row">'
            '<div class="corp-detail-label">Iris Oifigiúil source</div>'
            f'<div class="corp-detail-val">{val_html}</div>'
            "</div>"
        )

    st.html('<div class="corp-detail">' + "".join(rows_html) + "</div>")

    if raw_text:
        pretty = raw_text.replace(" // ", "\n").replace(" | ", "\n")
        st.html(
            '<div class="corp-detail-raw">'
            '<div class="corp-detail-raw-kicker">Original notice text</div>'
            f"{html.escape(pretty)}"
            "</div>"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Entry
# ──────────────────────────────────────────────────────────────────────────────
def corporate_page() -> None:
    inject_css()
    hide_sidebar()
    _inject_corp_css()

    df = load_corporate()
    cbi_badges = load_cbi_badges()
    cbi_repeat = load_cbi_repeat_distress()
    brand_aliases = load_brand_aliases()

    # URL handlers — run before widgets so session_state is right.
    url_ref = st.query_params.get("ref")
    if url_ref:
        st.session_state["corp_selected_ref"] = url_ref

    url_clear = st.query_params.get("clear")
    if url_clear:
        _clear_facet(url_clear)
        del st.query_params["clear"]
        st.rerun()

    url_spark = st.query_params.get("spark")
    if url_spark:
        try:
            yr = int(url_spark)
            st.session_state["corp_year_filter"] = [yr]
        except ValueError:
            pass
        del st.query_params["spark"]
        st.rerun()

    url_fund = st.query_params.get("fund")
    if url_fund:
        st.session_state["corp_fund_filter"] = url_fund
        st.session_state.pop("corp_firm_view", None)  # fund pivot exits firm view
        del st.query_params["fund"]
        st.rerun()

    url_firm = st.query_params.get("firm")
    if url_firm:
        st.session_state["corp_firm_view"] = url_firm
        del st.query_params["firm"]
        st.rerun()

    # Click-through from the CBI repeat-distress panel — set the search box.
    url_q = st.query_params.get("q")
    if url_q:
        st.session_state["corp_search"] = url_q
        del st.query_params["q"]
        st.rerun()

    # Sidebar
    selected = st.session_state.get("corp_selected_ref")
    with st.sidebar:
        sidebar_page_header("Corporate Notices")
        if not selected:
            sidebar_subtitle("Corporate notices · receiver / examinership / liquidation · Iris Oifigiúil")

    # Detail view
    if selected:
        if df.empty:
            empty_state(
                "Corporate data unavailable",
                "The underlying gold parquet did not load. If this persists, regenerate "
                "data/gold/parquet/corporate_notices.parquet via the corporate enrichment.",
            )
            return
        match = df[(df["notice_ref"] == selected) | (df["display_ref"] == selected)]
        if match.empty:
            empty_state(
                f"Notice {selected!r} isn't in the index",
                "Old bookmark or typed URL? Try the search box or browse the recent feed.",
            )
            if back_button("← Back to corporate notices", key="corp_detail_nf"):
                st.session_state.pop("corp_selected_ref", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_detail(match.iloc[0], cbi_badges)
        return

    # Firm view (?firm=) — dedicated receiver/insolvency-firm landing.
    firm_view = st.session_state.get("corp_firm_view")
    if firm_view:
        if df.empty:
            empty_state(
                "Corporate data unavailable",
                "The underlying gold parquet did not load.",
            )
            return
        _render_firm_view(df, firm_view, cbi_badges)
        return

    # Index — pure-data lede. No interpretive framing per the no-inference
    # rule; the reader arrives at conclusions from the panels below.
    n_total = len(df)
    n_distinct = int(df["entity_name"].nunique()) if "entity_name" in df.columns else 0
    hero_banner(
        kicker="Iris Oifigiúil",
        title="Corporate Notices",
        dek=(
            f"{n_total:,} corporate notices since 2016 naming {n_distinct:,} distinct "
            f"Irish companies — receiverships, examinerships, SCARP filings, "
            f"liquidations, ICAV strike-offs, and Companies Act filings. The panels "
            f"below break down the appointing parties, the firms doing the operational "
            f"work, the regulated entities in repeat distress, and the rescues."
        ),
    )
    _render_this_year_callout(df, cbi_repeat)

    # Constitutional / privacy caveat + acronyms.
    # Compact one-line privacy disclosure — brand-mapping methodology lives
    # in the Sources & methodology expander below the appointer panel, so
    # not repeated here. Privacy stance retained as a single visible line.
    st.html(
        '<p class="corp-privacy-line">'
        "<strong>Privacy:</strong> personal insolvency (individual bankruptcy notices) "
        "is excluded by policy. Companies only."
        "</p>"
    )

    glossary_strip(
        [
            ("SPV", "Special-Purpose Vehicle (a fund structure that holds loan books or assets)"),
            ("ICAV", "Irish Collective Asset-management Vehicle"),
            ("MVL", "Members' Voluntary Liquidation (solvent wind-up)"),
            ("CVL", "Creditors' Voluntary Liquidation (insolvent wind-up)"),
            ("SCARP", "Small Company Administrative Rescue Process"),
        ]
    )

    if df.empty:
        empty_state(
            "Corporate data unavailable",
            "The view returned no rows. Check data/gold/parquet/corporate_notices.parquet "
            "and the corporate SQL view registration.",
        )
        return

    # Featured panel — receiver-appointer ranking, independent of filters.
    _render_featured(df)
    _render_operator_strip(df)
    _render_methodology_expander(brand_aliases)

    # Experimental — regulated firms in repeat distress (CBI x corporate cross-ref).
    _render_cbi_repeat_distress(cbi_repeat)

    # Counter-narrative — corporate rescue (Examinership + SCARP).
    _render_rescue_panel(df)

    # Facets (search + year + fund + type-tab)
    type_idx = _render_facets(df)
    subtype_filter = _TYPE_GROUPS[type_idx][1]

    # Apply filters
    filtered = _apply_filters(
        df,
        years=st.session_state.get("corp_year_filter") or [],
        subtypes=subtype_filter,
        fund=st.session_state.get("corp_fund_filter") or "All",
        search=st.session_state.get("corp_search") or "",
        type_label=_TYPE_GROUPS[type_idx][0],
    )

    # CSV export
    if not filtered.empty:
        csv_cols = [
            "issue_date",
            "notice_category",
            "notice_subtype",
            "entity_name",
            "display_title",
            "parent_fund_mentions",
            "brand_mentions",
            "iris_source_pdf",
        ]
        export_df = filtered[csv_cols].copy()

        # Flatten list columns for CSV friendliness.
        def _csv_join(lst):
            if lst is None or not hasattr(lst, "__iter__"):
                return ""
            items = [str(x) for x in lst]
            return "; ".join(items)

        for c in ("parent_fund_mentions", "brand_mentions"):
            export_df[c] = export_df[c].apply(_csv_join)
        export_button(
            export_df,
            label=f"Download {len(filtered):,} notices (CSV)",
            filename="dail_tracker_corporate_notices.csv",
            key="corp_csv_download",
        )

    fund_active = (st.session_state.get("corp_fund_filter") or "All") != "All"
    _render_feed(filtered, cbi_badges, group_by_year=fund_active)


if __name__ == "__main__":
    corporate_page()
