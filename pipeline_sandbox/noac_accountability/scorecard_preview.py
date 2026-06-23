"""EXPERIMENTAL sandbox PREVIEW — council NOAC scorecard cards.

Renders the proposed two new dossier cards (Finances & workforce, Front-line services)
from the validated sandbox scorecard, so the UI can be shaped + civic-ui-reviewed BEFORE
any gold/view work. The production page (utility/pages_code/local_government.py) is
display-only and forbids read_parquet, so this lives here, not there.

The card functions below mirror the page's _metric/_bench/_stat_card vocabulary 1:1 and
are written to drop straight into local_government.py at promotion (when a registered
v_la_noac_scorecard view replaces the sandbox read).

Run:  streamlit run pipeline_sandbox/noac_accountability/scorecard_preview.py
"""
from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path

import pandas as pd
import polars as pl
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "utility"))
from shared_css import inject_css  # noqa: E402  (reuse the real lg-* styling)

D = Path(__file__).resolve().parent

# Card grouping (the "without overwhelming" decision): 5 metrics -> 2 single-theme cards.
# Titles name ONE concept each (internal management vs services to residents) per the
# civic-ui-review "one concept per card" rule.
CARDS = {
    "How the council is run": ["revenue_balance_pct", "sickness_absence_pct"],
    "Services to residents": ["roads_poor_pct", "fire_within_10min_pct", "litter_problem_pct"],
}
NOAC_REPORT = "https://www.noac.ie/noac_publications/report-77-noac-performance-indicator-report-2024/"
# councils with no own fire service (regionally covered) — sub-line for the n/a fire cell
FIRE_NA_NOTE = "fire service provided regionally (Dublin Fire Brigade / Galway County)"


# ── helpers mirroring local_government.py (with two small additions: per-metric doc link,
#    linked source line). These port back verbatim at promotion. ──────────────────────────
def _pct(v, dp: int = 1) -> str:
    return "n/a" if v is None or pd.isna(v) else f"{float(v):.{dp}f}%"


def _bench(value, national) -> str:
    """Neutral position vs the national median: ▲ above / ▼ below, NO good/bad colour.
    civic-ui-review fix — the green/red tint (a) put the good/bad verdict in colour only
    (lost under deuteranopia, since the glyph encodes above/below) and (b) contradicted the
    'no judgement implied' framing. Plain arrow = factual position, firewall-clean."""
    if value is None or pd.isna(value) or national is None or pd.isna(national):
        return ""
    arrow = "▲" if float(value) >= float(national) else "▼"
    return f'national {float(national):.1f}% <span class="lg-arrow-neutral">{arrow}</span>'


def _metric(value: str, label: str, bench: str = "", doc_url: str = "", doc_page: int | None = None) -> str:
    doc = (f' <a class="lg-metric-doc" href="{_h(doc_url)}" target="_blank" rel="noopener" '
           f'title="NOAC report, page {doc_page}">NOAC p.{doc_page} ↗</a>') if doc_url else ""
    return (
        '<div class="lg-metric"><div class="lg-metric-main">'
        f'<span class="lg-metric-value">{value}</span>'
        f'<span class="lg-metric-label">{label}{doc}</span></div>'
        f'<div class="lg-metric-bench">{bench}</div></div>'
    )


def _stat_card(title: str, rows: list[str], source: str, src_url: str = "", extra: str = "") -> str:
    body = "".join(r for r in rows if r)
    if not body:
        return ""
    src = (f'<a href="{_h(src_url)}" target="_blank" rel="noopener">{_h(source)} ↗</a>'
           if src_url else _h(source))
    return (
        f'<div class="lg-card"><div class="lg-card-title">{title}</div>'
        f"{body}{extra}"
        f'<div class="lg-card-src">{src}</div></div>'
    )


@st.cache_data
def _load():
    # TODO_PIPELINE_VIEW_REQUIRED: v_la_noac_scorecard (long: local_authority, metric_key,
    # value, national_median, direction_good, source_page, deep_link) fed by gold noac_*_wide.
    # Sandbox read below is preview-only and MUST NOT be ported into local_government.py —
    # the page + its data-access layer forbid read_parquet/pivot.
    sc = pl.read_parquet(D / "noac_council_scorecard.parquet")
    meta = pl.read_csv(D / "scorecard_meta.csv")
    return sc.to_pandas(), {m["metric_key"]: m for m in meta.to_dicts()}


def _render_card(title: str, keys: list[str], vals: dict, meta: dict) -> str:
    rows = []
    for k in keys:
        m = meta[k]
        v = vals.get(k)
        bench = _bench(v, m["national_median"])
        if k == "fire_within_10min_pct" and (v is None or pd.isna(v)):
            bench = f'<span class="lg-na-note">{FIRE_NA_NOTE}</span>'
        rows.append(_metric(_pct(v), _h(m["label"]), bench, m["deep_link"], int(m["source_page"])))
    return _stat_card(title, rows, "NOAC Performance Indicator Report 2024",
                      src_url=NOAC_REPORT)


def main() -> None:
    st.set_page_config(page_title="NOAC scorecard preview", layout="centered")
    inject_css()
    # one new class the production CSS would add: per-metric doc link + n/a note
    st.html(
        "<style>"
        ".lg-metric-doc{font-size:0.7rem;font-weight:600;color:#8d4f24;text-decoration:none;"
        "white-space:nowrap;margin-left:0.3rem;}"
        ".lg-metric-doc:hover{text-decoration:underline;}"
        ".lg-arrow-neutral{color:var(--text-secondary);font-weight:700;}"
        ".lg-na-note{font-size:0.72rem;color:var(--text-meta);font-style:italic;}"
        "</style>"
    )
    df, meta = _load()
    councils = sorted(df["local_authority"].unique())
    st.markdown("#### Council scorecard — preview (sandbox, NOAC 2024)")
    name = st.selectbox("Council", councils, index=councils.index("Sligo") if "Sligo" in councils else 0)

    vals = {r["metric_key"]: r["value"] for _, r in df[df["local_authority"] == name].iterrows()}
    st.html(
        '<p class="con-section-note">Indicators published by the <strong>National Oversight &amp; '
        "Audit Commission (NOAC)</strong>, each beside the <strong>national benchmark</strong> "
        "(median across the 31 councils). These are <strong>executive</strong> responsibilities "
        "— the Chief Executive's administration. ▲/▼ shows position relative to the benchmark; "
        "no judgement implied.</p>"
    )
    cards = "".join(_render_card(t, ks, vals, meta) for t, ks in CARDS.items())
    st.html(f'<div class="lg-perf-grid">{cards}</div>')


if __name__ == "__main__":
    main()
