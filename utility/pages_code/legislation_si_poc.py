"""
Statutory Instruments — POC integration page.

POC sibling of legislation.py. Self-contained: reads CSVs directly, does its
own filtering, fuzzy matching, and classification in-page. **Not a production
pattern** — the firewall rules (no business logic in Streamlit) are
intentionally suspended here so the user can see what the feature could feel
like before any pipeline/contract work.

Five POC features:
  1. Editorial hero + KPI strip (totals, top domain, top actor, EU share)
  2. Trends — domain × year heatmap and minister-activity bars
  3. Filterable SI index — year pills + facet selectboxes + paginated cards
  4. SI detail panel — full taxonomy, irishstatutebook.ie link, Iris source
  5. Cross-link to legislation — when si_parent_legislation fuzzy-matches a
     known bill, an inline "Made under …" panel surfaces the bill detail
     without leaving the page.
"""
from __future__ import annotations

import html
import re
import string
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    evidence_heading,
    hero_banner,
    paginate,
    pagination_controls,
    render_stat_strip,
    sidebar_page_header,
    stat_item,
)
from ui.entity_links import source_link_html

# ── Data paths ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]
SI_GOLD = ROOT / "out" / "iris_si_taxonomy.csv"
SPONSORS_CSV = ROOT / "data" / "silver" / "sponsors.csv"

# ── POC config ─────────────────────────────────────────────────────────────────
SI_YEAR_FLOOR = 2018       # density threshold — older issues are messier
MIN_TAXO_CONFIDENCE = 0.5  # drop low-confidence classifications
MATCH_THRESHOLD = 0.40     # token-set Jaccard threshold (was SequenceMatcher 0.72)
MATCH_YEAR_WINDOW = 3      # accept bills within ±N years of the SI's "Act YYYY" hint
PAGE_SIZE = 10

# Token-stop list for the title-Jaccard matcher: noise words that would
# otherwise inflate set-overlap scores between unrelated titles.
_TITLE_STOP = {"act", "bill", "of", "the", "and", "an", "a", "for", "to", "in", "no", "no.", "amendment"}


def _title_tokens(s: str) -> set[str]:
    """Lowercased, punctuation-stripped, stop-word-filtered token set used by
    the fast bill-title matcher."""
    if not s:
        return set()
    out: set[str] = set()
    for w in s.lower().split():
        t = w.strip(string.punctuation)
        if t and t not in _TITLE_STOP:
            out.add(t)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Page-local CSS — POC only. Reuses dt-* tokens but lives in this file rather
# than shared_css.py so we don't pollute the canonical class set.
# ──────────────────────────────────────────────────────────────────────────────
def _inject_poc_css() -> None:
    st.html(
        """
        <style>
        .si-stat-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(170px,1fr));
            gap: 0.85rem; margin: 1.1rem 0 1.6rem; }
        .si-stat { background:#ffffff; border:1px solid #e5e2db; border-radius:8px; padding:0.95rem 1.1rem; }
        .si-stat-num { font-family: ui-serif, Georgia, serif; font-size:1.7rem; font-weight:700;
            line-height:1.1; color:#14232b; }
        .si-stat-label { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-top:0.35rem; }
        .si-stat-sub { font-size:0.75rem; color:#5b6b73; margin-top:0.15rem; }

        .si-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem; margin-bottom:0.55rem; }
        .si-card-head { display:flex; align-items:baseline; gap:0.6rem; flex-wrap:wrap; }
        .si-card-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.78rem;
            color:#5b6b73; }
        .si-card-date { margin-left:auto; font-size:0.76rem; color:#5b6b73; }
        .si-card-title { font-family: ui-serif, Georgia, serif; font-size:1.02rem; line-height:1.4;
            color:#14232b; margin: 0.35rem 0 0.55rem; }
        .si-card-foot { display:flex; gap:0.4rem; flex-wrap:wrap; align-items:center; }

        .si-pill { background:#f5f1ea; border:1px solid #e5e2db; border-radius:999px;
            padding:0.16rem 0.6rem; font-size:0.7rem; color:#14232b; line-height:1.4;
            white-space:nowrap; }
        .si-pill-domain { background:#ecf2f6; border-color:#cfdde6; }
        .si-pill-op     { background:#f6f0e6; border-color:#e6d9c2; }
        .si-pill-eu     { background:#fff7e6; border-color:#f0d99b; color:#7a5a00; }
        .si-pill-act    { background:#e8efe6; border-color:#bcd1b3; color:#2c4a23; }
        .si-pill-actor  { background:#ffffff; border-color:#dfd9cf; color:#5b6b73; }

        .si-detail { background:#ffffff; border:1px solid #e5e2db; border-radius:10px;
            padding:1.4rem 1.55rem; margin-top:0.5rem; }
        .si-detail-ref { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.82rem;
            color:#5b6b73; }
        .si-detail-title { font-family: ui-serif, Georgia, serif; font-size:1.4rem; line-height:1.3;
            color:#14232b; margin: 0.4rem 0 1rem; }
        .si-detail-row { display:flex; gap:0.85rem; padding:0.55rem 0;
            border-top:1px solid #f0ece5; align-items:flex-start; }
        .si-detail-row:first-of-type { border-top:none; padding-top:0; }
        .si-detail-label { width:170px; flex-shrink:0; font-size:0.72rem; text-transform:uppercase;
            letter-spacing:0.06em; color:#5b6b73; padding-top:0.18rem; }
        .si-detail-val { flex:1; font-size:0.93rem; color:#14232b; line-height:1.5; }
        .si-detail-val .si-pill { margin-right:0.25rem; }

        .si-billlink { background:#fbfcf9; border:1px solid #c9d6c0; border-radius:8px;
            padding:1.1rem 1.25rem; margin-top:1.1rem; }
        .si-billlink-kicker { font-size:0.7rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#2c4a23; font-weight:600; }
        .si-billlink-title { font-family: ui-serif, Georgia, serif; font-size:1.1rem; margin:0.35rem 0 0.45rem;
            color:#14232b; line-height:1.35; }
        .si-billlink-meta { font-size:0.82rem; color:#5b6b73; margin-bottom:0.55rem; }
        .si-billlink-conf { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size:0.7rem;
            color:#5b6b73; margin-top:0.35rem; }

        .si-section-h { font-family: ui-serif, Georgia, serif; font-size:1.05rem; margin: 1.5rem 0 0.55rem;
            color:#14232b; }

        .si-trend-grid { display:grid; grid-template-columns: 3fr 2fr; gap: 1.2rem; margin: 0.5rem 0 1.4rem; }
        @media (max-width: 900px) { .si-trend-grid { grid-template-columns: 1fr; } }
        .si-trend-card { background:#ffffff; border:1px solid #e5e2db; border-radius:8px;
            padding:1rem 1.15rem; }
        .si-trend-card-h { font-size:0.78rem; text-transform:uppercase; letter-spacing:0.07em;
            color:#5b6b73; margin-bottom:0.35rem; }

        .si-poc-note { font-size:0.78rem; color:#5b6b73; font-style:italic; margin: 0.4rem 0 1rem; }
        </style>
        """
    )


# ──────────────────────────────────────────────────────────────────────────────
# Data loading — POC level cleaning. Keep cleanest rows, drop messy ones.
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading Statutory Instruments…")
def load_si() -> pd.DataFrame:
    df = pd.read_csv(SI_GOLD, low_memory=False)
    df = df[df["notice_category"] == "statutory_instrument"]
    if "is_quarantined" in df.columns:
        df = df[~df["is_quarantined"].fillna(False).astype(bool)]
    df = df[df["si_number"].notna() & df["si_year"].notna() & df["title"].notna()]
    df = df[~df["title"].astype(str).str.contains("�", na=False)]
    df["si_year"] = pd.to_numeric(df["si_year"], errors="coerce").fillna(0).astype(int)
    df["si_number"] = pd.to_numeric(df["si_number"], errors="coerce").fillna(0).astype(int)
    df = df[df["si_year"] >= SI_YEAR_FLOOR]
    if "si_taxonomy_confidence" in df.columns:
        df = df[df["si_taxonomy_confidence"].fillna(0) >= MIN_TAXO_CONFIDENCE]
    df["si_id"] = df["si_year"].astype(str) + "-" + df["si_number"].astype(str).str.zfill(3)
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df = df.drop_duplicates(subset=["si_id"]).reset_index(drop=True)
    return df


@st.cache_data(show_spinner=False)
def load_bills() -> pd.DataFrame:
    df = pd.read_csv(SPONSORS_CSV, low_memory=False)
    keep = ["bill_no", "bill_year", "bill_type", "short_title_en", "long_title_en",
            "status", "bill_url", "most_recent_stage_event_show_as", "sponsor_by_show_as"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].copy()
    df["bill_year_num"] = pd.to_numeric(df["bill_year"], errors="coerce")
    df["bill_no_num"]   = pd.to_numeric(df["bill_no"],   errors="coerce")
    df = df.dropna(subset=["bill_no_num", "bill_year_num", "short_title_en"])
    df = df.drop_duplicates(subset=["bill_no_num", "bill_year_num", "short_title_en"])
    df["bill_id"] = df["bill_year_num"].astype(int).astype(str) + "/" + df["bill_no_num"].astype(int).astype(str)
    return df.reset_index(drop=True)


@st.cache_data(show_spinner="Matching SIs to parent Acts…")
def match_si_to_bill(si_df: pd.DataFrame, bills_df: pd.DataFrame) -> pd.DataFrame:
    """Year-bucketed token-set Jaccard match between si_parent_legislation
    free-text and known bill short titles. POC-grade.

    Why Jaccard and not character ratio: SequenceMatcher.ratio() is O(n*m)
    per pair and dominated wall time when the prior implementation fell back
    to scanning all 642 bills for SIs without year hints. Token-set Jaccard
    on pre-computed sets is roughly 50× faster on this corpus and produces
    equivalent matches for the bill-style titles seen here.

    SIs whose parent text contains no "X Act YYYY" pattern are skipped
    entirely (returned as unmatched) — the pattern is the only reliable
    signal we have to year-constrain candidates."""
    parent_re = re.compile(r"([A-Z][^,()\n]{2,80}?\bAct\s+(\d{4}))")

    bills = bills_df.dropna(subset=["short_title_en"]).copy()
    bills["_tokens"] = bills["short_title_en"].astype(str).map(_title_tokens)

    # Precompute year buckets once. List-of-dicts iterates ~3× faster than
    # repeatedly slicing the DataFrame.
    bills_by_year: dict[int, list[dict]] = {}
    for rec in bills.to_dict("records"):
        y = rec.get("bill_year_num")
        if pd.notna(y):
            bills_by_year.setdefault(int(y), []).append(rec)

    def candidates_in_window(year_hint: int) -> list[dict]:
        out: list[dict] = []
        for dy in range(-MATCH_YEAR_WINDOW, MATCH_YEAR_WINDOW + 1):
            out.extend(bills_by_year.get(year_hint + dy, []))
        return out

    def best(parent_text) -> pd.Series:
        if not isinstance(parent_text, str):
            return pd.Series([None, None, None, 0.0])
        m = parent_re.search(parent_text)
        if not m:
            return pd.Series([None, None, None, 0.0])

        ref_tokens = _title_tokens(m.group(1))
        if not ref_tokens:
            return pd.Series([None, None, None, 0.0])
        year_hint = int(m.group(2))
        candidates = candidates_in_window(year_hint)
        if not candidates:
            return pd.Series([None, None, None, 0.0])

        best_row, best_score = None, 0.0
        for cand in candidates:
            bt = cand["_tokens"]
            if not bt:
                continue
            inter = len(ref_tokens & bt)
            if inter == 0:
                continue
            score = inter / len(ref_tokens | bt)
            if score > best_score:
                best_score, best_row = score, cand

        if best_row is None or best_score < MATCH_THRESHOLD:
            return pd.Series([None, None, None, round(best_score, 2)])
        return pd.Series([
            best_row["bill_id"],
            best_row["short_title_en"],
            best_row.get("bill_url"),
            round(best_score, 2),
        ])

    matched = si_df["si_parent_legislation"].apply(best)
    matched.columns = ["matched_bill_id", "matched_bill_title", "matched_bill_url", "match_score"]
    # Reset BOTH sides so concat aligns positionally — apply() preserves the
    # input's index, so a shuffled si_df would otherwise mis-align after
    # reset_index on only the left side.
    return pd.concat([si_df.reset_index(drop=True), matched.reset_index(drop=True)], axis=1)


# ──────────────────────────────────────────────────────────────────────────────
# URL helpers
# ──────────────────────────────────────────────────────────────────────────────
def _eisb_url(row: pd.Series) -> str:
    """Prefer the extracted eisb_url; fall back to the canonical eli pattern."""
    eisb = row.get("eisb_url")
    if isinstance(eisb, str) and eisb.startswith("http"):
        return eisb
    yr, no = row.get("si_year"), row.get("si_number")
    if pd.notna(yr) and pd.notna(no):
        return f"https://www.irishstatutebook.ie/eli/{int(yr)}/si/{int(no)}/made/en/print"
    return ""


def _iris_pdf_label(row: pd.Series) -> str:
    src = row.get("source_file")
    if not isinstance(src, str):
        return ""
    return src


def _safe(v) -> str:
    """Coerce a possibly-NaN/None CSV cell to a string. NaN is truthy in
    Python so `row.get(x) or ""` does NOT guard against missing values —
    every CSV cell that's about to hit html.escape or string ops must go
    through this."""
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    return str(v)


def _fmt_date(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        ts = pd.Timestamp(val)
        return f"{ts.day} {ts.strftime('%b %Y')}"
    except Exception:
        return str(val)


def _pretty_token(s: str) -> str:
    """snake_case → Title Case, leaves human strings alone."""
    if not isinstance(s, str) or not s:
        return ""
    if "_" in s and s.lower() == s:
        return s.replace("_", " ").title()
    return s


def _split_multi(s, sep="|"):
    if not isinstance(s, str):
        return []
    return [p.strip() for p in s.split(sep) if p.strip()]


# ──────────────────────────────────────────────────────────────────────────────
# Filters (sidebar)
# ──────────────────────────────────────────────────────────────────────────────
def _apply_filters(df: pd.DataFrame, years, domain, op, actor, search) -> pd.DataFrame:
    out = df
    if years:
        out = out[out["si_year"].isin(years)]
    if domain and domain != "All":
        out = out[out["si_policy_domain_primary"] == domain]
    if op and op != "All":
        out = out[out["si_operation_primary"] == op]
    if actor and actor != "All":
        out = out[out["si_responsible_actor"] == actor]
    if search:
        s = search.strip().lower()
        out = out[out["title"].astype(str).str.lower().str.contains(s, na=False)]
    return out


# ──────────────────────────────────────────────────────────────────────────────
# View 1 — KPI strip
# ──────────────────────────────────────────────────────────────────────────────
def _render_kpi_strip(df: pd.DataFrame) -> None:
    total = len(df)
    if total == 0:
        return
    top_domain = df["si_policy_domain_primary"].dropna().value_counts().head(1)
    top_actor  = df["si_responsible_actor"].dropna().value_counts().head(1)
    eu_count   = int((df["si_eu_relationship"].fillna("").astype(str)
                        .str.contains("eu_", na=False)).sum())
    eu_share   = (eu_count / total * 100) if total else 0
    yrs        = sorted(df["si_year"].unique())
    yr_span    = f"{yrs[0]}–{yrs[-1]}" if len(yrs) >= 2 else (str(yrs[0]) if yrs else "—")

    td = top_domain.index[0] if not top_domain.empty else "—"
    tdc = int(top_domain.iloc[0]) if not top_domain.empty else 0
    ta = top_actor.index[0] if not top_actor.empty else "—"
    tac = int(top_actor.iloc[0]) if not top_actor.empty else 0

    st.html(f"""
    <div class="si-stat-grid">
      <div class="si-stat">
        <div class="si-stat-num">{total:,}</div>
        <div class="si-stat-label">Statutory Instruments</div>
        <div class="si-stat-sub">{html.escape(yr_span)}</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{html.escape(_pretty_token(td))}</div>
        <div class="si-stat-label">Top policy domain</div>
        <div class="si-stat-sub">{tdc:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{html.escape(_pretty_token(ta))}</div>
        <div class="si-stat-label">Most active actor</div>
        <div class="si-stat-sub">{tac:,} SIs</div>
      </div>
      <div class="si-stat">
        <div class="si-stat-num">{eu_count:,}</div>
        <div class="si-stat-label">EU-derived</div>
        <div class="si-stat-sub">{eu_share:.0f}% of window</div>
      </div>
    </div>
    """)


# ──────────────────────────────────────────────────────────────────────────────
# View 2 — Trends (top row: domain heatmap + top-actors bar)
# ──────────────────────────────────────────────────────────────────────────────
def _render_minister_strip(df: pd.DataFrame, top_n: int = 6) -> None:
    """Compact minister × year activity strip — same axis discipline as the
    domain heatmap so the eye can scan both at once."""
    if df.empty:
        return
    top_actors = (df.dropna(subset=["si_responsible_actor"])
                    .groupby("si_responsible_actor").size()
                    .reset_index(name="total")
                    .sort_values("total", ascending=False).head(top_n)
                    ["si_responsible_actor"].tolist())
    if not top_actors:
        return
    strip = (df[df["si_responsible_actor"].isin(top_actors)]
                .groupby(["si_responsible_actor", "si_year"]).size()
                .reset_index(name="n"))
    strip["actor_pretty"] = strip["si_responsible_actor"].map(_pretty_token)

    st.html('<div class="si-trend-card-h">Minister activity strip · top actors × year</div>')
    chart = (
        alt.Chart(strip)
        .mark_rect(stroke="#ffffff", strokeWidth=2)
        .encode(
            x=alt.X("si_year:O", title=None,
                    axis=alt.Axis(labelFontSize=11, ticks=False)),
            y=alt.Y("actor_pretty:N", sort=top_actors, title=None,
                    axis=alt.Axis(labelFontSize=11, ticks=False, labelLimit=240)),
            color=alt.Color("n:Q", scale=alt.Scale(scheme="oranges"),
                            legend=alt.Legend(title="SIs", orient="bottom")),
            tooltip=[alt.Tooltip("actor_pretty:N", title="Actor"),
                     alt.Tooltip("si_year:O", title="Year"),
                     alt.Tooltip("n:Q", title="SIs")],
        )
        .properties(height=42 * len(top_actors) + 30)
        .configure_view(stroke=None)
        .configure_axis(grid=False, domain=False)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_op_breakdown(df: pd.DataFrame) -> None:
    """What are these SIs *doing*? Amending, commencing, revoking, …"""
    if df.empty:
        return
    ops = (df.dropna(subset=["si_operation_primary"])
              .groupby("si_operation_primary").size()
              .reset_index(name="n").sort_values("n", ascending=False).head(10))
    if ops.empty:
        return
    ops["op_pretty"] = ops["si_operation_primary"].map(_pretty_token)
    st.html('<div class="si-trend-card-h">Operation breakdown · what SIs do</div>')
    chart = (
        alt.Chart(ops)
        .mark_bar(color="#a85d2a", cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
        .encode(
            y=alt.Y("op_pretty:N", sort="-x", title=None,
                    axis=alt.Axis(labelFontSize=11, ticks=False, labelLimit=240)),
            x=alt.X("n:Q", title=None,
                    axis=alt.Axis(labelFontSize=10, ticks=False)),
            tooltip=[alt.Tooltip("op_pretty:N", title="Operation"),
                     alt.Tooltip("n:Q", title="SIs")],
        )
        .properties(height=42 * len(ops) + 20)
        .configure_view(stroke=None)
        .configure_axis(grid=False, domain=False)
    )
    st.altair_chart(chart, use_container_width=True)


def _render_trends(df: pd.DataFrame) -> None:
    if df.empty:
        return

    st.html('<div class="si-section-h">Trends in the corpus</div>')
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.html('<div class="si-trend-card-h">Policy domain × year</div>')
        heat = (df.dropna(subset=["si_policy_domain_primary"])
                  .groupby(["si_year", "si_policy_domain_primary"])
                  .size().reset_index(name="n"))
        if heat.empty:
            empty_state("No domain data", "No domain classifications in the current filter.")
        else:
            heat["domain_pretty"] = heat["si_policy_domain_primary"].map(_pretty_token)
            chart = (
                alt.Chart(heat)
                .mark_rect(stroke="#ffffff", strokeWidth=2)
                .encode(
                    x=alt.X("si_year:O", title=None,
                            axis=alt.Axis(labelFontSize=11, ticks=False)),
                    y=alt.Y("domain_pretty:N", title=None, sort="-x",
                            axis=alt.Axis(labelFontSize=11, ticks=False, labelLimit=220)),
                    color=alt.Color("n:Q",
                                    scale=alt.Scale(scheme="blues"),
                                    legend=alt.Legend(title="SIs", orient="bottom")),
                    tooltip=[alt.Tooltip("domain_pretty:N", title="Domain"),
                             alt.Tooltip("si_year:O", title="Year"),
                             alt.Tooltip("n:Q", title="SIs")],
                )
                .properties(height=320, padding={"left": 5, "right": 5, "top": 5, "bottom": 5})
                .configure_view(stroke=None)
                .configure_axis(grid=False, domain=False)
            )
            st.altair_chart(chart, use_container_width=True)

    with col_r:
        st.html('<div class="si-trend-card-h">Top responsible actors · totals</div>')
        top = (df.dropna(subset=["si_responsible_actor"])
                 .groupby("si_responsible_actor").size()
                 .reset_index(name="n").sort_values("n", ascending=False).head(8))
        if top.empty:
            empty_state("No actor data", "No responsible-actor entries in the current filter.")
        else:
            top["actor_pretty"] = top["si_responsible_actor"].map(_pretty_token)
            chart = (
                alt.Chart(top)
                .mark_bar(color="#3b6e8f", cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
                .encode(
                    y=alt.Y("actor_pretty:N", sort="-x", title=None,
                            axis=alt.Axis(labelFontSize=11, ticks=False, labelLimit=220)),
                    x=alt.X("n:Q", title=None,
                            axis=alt.Axis(labelFontSize=10, ticks=False)),
                    tooltip=[alt.Tooltip("actor_pretty:N", title="Actor"),
                             alt.Tooltip("n:Q", title="SIs")],
                )
                .properties(height=320)
                .configure_view(stroke=None)
                .configure_axis(grid=False, domain=False)
            )
            st.altair_chart(chart, use_container_width=True)

    # ── Second row — minister activity strip + operation breakdown ────────────
    col_l2, col_r2 = st.columns([3, 2])
    with col_l2:
        _render_minister_strip(df)
    with col_r2:
        _render_op_breakdown(df)


# ──────────────────────────────────────────────────────────────────────────────
# View 3 — SI index (cards + pagination + select button)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_card(row: pd.Series) -> str:
    si_id     = html.escape(_safe(row["si_id"]) or "—")
    title     = html.escape(_safe(row.get("title")) or "—")
    issue_dt  = html.escape(_fmt_date(row.get("issue_date")))
    domain    = _pretty_token(_safe(row.get("si_policy_domain_primary")))
    op        = _pretty_token(_safe(row.get("si_operation_primary")))
    eu        = _safe(row.get("si_eu_relationship"))
    actor     = _pretty_token(_safe(row.get("si_responsible_actor")))
    matched_t = _safe(row.get("matched_bill_title"))

    pills = []
    if domain:
        pills.append(f'<span class="si-pill si-pill-domain">{html.escape(domain)}</span>')
    if op:
        pills.append(f'<span class="si-pill si-pill-op">{html.escape(op)}</span>')
    if eu.startswith("eu_"):
        pills.append(f'<span class="si-pill si-pill-eu">{html.escape(_pretty_token(eu))}</span>')
    if matched_t:
        pills.append(
            f'<span class="si-pill si-pill-act">↪ Made under {html.escape(matched_t)}</span>'
        )
    if actor:
        pills.append(f'<span class="si-pill si-pill-actor">{html.escape(actor)}</span>')

    return (
        '<div class="si-card">'
        '<div class="si-card-head">'
        f'<span class="si-card-ref">SI No. {si_id}</span>'
        f'<span class="si-card-date">{issue_dt}</span>'
        '</div>'
        f'<div class="si-card-title">{title}</div>'
        f'<div class="si-card-foot">{"".join(pills)}</div>'
        '</div>'
    )


def _render_si_index(df: pd.DataFrame) -> None:
    if df.empty:
        empty_state("No SIs in scope", "Adjust filters to widen the year, domain, or actor.")
        return

    total = len(df)
    st.html(
        f'<div class="si-section-h">{total:,} statutory instrument'
        f'{"s" if total != 1 else ""} match the current filters</div>'
    )

    page_idx = paginate(total, key_prefix="si_idx", page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    # Render cards followed by a thin "View detail" button per row.
    # Cards rendered as one HTML block, buttons as separate Streamlit widgets
    # below — keeps the visual scan clean while remaining clickable.
    for _, row in visible.iterrows():
        st.html(_render_si_card(row))
        if st.button("View detail →", key=f"si_open_{row['si_id']}", type="tertiary"):
            st.session_state["si_selected_id"] = row["si_id"]
            st.query_params["si"] = row["si_id"]
            st.rerun()

    pagination_controls(
        total,
        key_prefix="si_idx",
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="SIs",
    )


# ──────────────────────────────────────────────────────────────────────────────
# View 4 — SI detail
# View 5 — Cross-link to legislation (rendered inline within detail)
# ──────────────────────────────────────────────────────────────────────────────
def _render_si_detail(row: pd.Series, bills_df: pd.DataFrame) -> None:
    if back_button("← Back to SI Index", key="si_poc"):
        st.session_state.pop("si_selected_id", None)
        st.query_params.clear()
        st.rerun()

    si_id     = html.escape(_safe(row["si_id"]) or "—")
    title     = html.escape(_safe(row.get("title")) or "—")
    domain    = _safe(row.get("si_policy_domain_primary"))
    op        = _safe(row.get("si_operation_primary"))
    actor     = _safe(row.get("si_responsible_actor"))
    si_form   = _safe(row.get("si_form"))
    eu_rel    = _safe(row.get("si_eu_relationship"))
    parent    = _safe(row.get("si_parent_legislation"))
    eff_text  = _safe(row.get("si_effective_date_text"))
    op_flags  = _split_multi(_safe(row.get("si_operation_flags")))
    domains   = _split_multi(_safe(row.get("si_policy_domains")))
    flags     = _split_multi(_safe(row.get("classification_flags")), sep=";")
    confidence = row.get("si_taxonomy_confidence")

    eisb = _eisb_url(row)
    matched_id    = _safe(row.get("matched_bill_id"))
    matched_title = _safe(row.get("matched_bill_title"))
    matched_url   = _safe(row.get("matched_bill_url"))
    match_score   = row.get("match_score")

    eisb_html = source_link_html(
        eisb,
        f"View on irishstatutebook.ie",
        aria_label="Open this SI on the Electronic Irish Statute Book",
    ) if eisb else ""

    # ── Header card ───────────────────────────────────────────────────────────
    st.html(f"""
    <div class="si-detail">
      <div class="si-detail-ref">Statutory Instrument No. {si_id}</div>
      <div class="si-detail-title">{title}</div>
    </div>
    """)

    # ── Quick stat strip — uses shared helper for visual consistency ──────────
    render_stat_strip(
        stat_item(_fmt_date(row.get("issue_date")), "Issued"),
        stat_item(_pretty_token(op) or "—", "Operation"),
        stat_item(_pretty_token(domain) or "—", "Policy domain"),
        stat_item(_pretty_token(actor) or "—", "Responsible actor"),
    )

    # ── Detail rows ───────────────────────────────────────────────────────────
    rows_html: list[str] = []

    def _row(label: str, val_html: str) -> None:
        rows_html.append(
            f'<div class="si-detail-row">'
            f'<div class="si-detail-label">{html.escape(label)}</div>'
            f'<div class="si-detail-val">{val_html}</div>'
            f'</div>'
        )

    _row("SI form", html.escape(_pretty_token(si_form)) or "—")
    op_pills = (" ".join(
        f'<span class="si-pill si-pill-op">{html.escape(_pretty_token(f))}</span>'
        for f in op_flags
    ) if op_flags else "—")
    _row("Operation flags", op_pills)
    domain_pills = (" ".join(
        f'<span class="si-pill si-pill-domain">{html.escape(_pretty_token(d))}</span>'
        for d in domains
    ) if domains else "—")
    _row("Policy domains", domain_pills)
    if eu_rel:
        _row("EU relationship",
             f'<span class="si-pill si-pill-eu">{html.escape(_pretty_token(eu_rel))}</span>')
    if eff_text:
        _row("Effective date (raw)", html.escape(eff_text))
    if flags:
        _row("Classification flags",
             " ".join(f'<span class="si-pill">{html.escape(_pretty_token(f))}</span>' for f in flags))
    if parent.strip():
        # Each '|'-separated Act becomes a clickable link into the Legislation
        # (POC) Act detail, where the user can see every other SI made under
        # the same Act. Uses dt-source-link styling for consistency.
        from urllib.parse import quote as _qp
        act_links = []
        for piece in parent.split("|"):
            piece = piece.strip(" .,;")
            if not piece:
                continue
            href = f"/legislation-poc?act={_qp(piece)}"
            act_links.append(
                f'<a class="dt-source-link" href="{html.escape(href, quote=True)}" '
                f'target="_self">{html.escape(piece)}</a>'
            )
        _row("Parent legislation", " &nbsp;·&nbsp; ".join(act_links) if act_links else html.escape(parent))
    if isinstance(confidence, (int, float)) and pd.notna(confidence):
        _row("Taxonomy confidence", f"{float(confidence):.2f}")

    # Source links row — uses dt-source-link styling.
    src_iris = _iris_pdf_label(row)
    src_links = []
    if eisb_html:
        src_links.append(eisb_html)
    if src_iris:
        src_links.append(
            f'<span class="si-detail-val" style="color:#5b6b73;font-size:0.85rem;">'
            f'Iris source: {html.escape(src_iris)}</span>'
        )
    if src_links:
        _row("Official sources", " &nbsp; · &nbsp; ".join(src_links))

    st.html('<div class="si-detail">' + "".join(rows_html) + "</div>")

    # ── Cross-link panel (View 5) ─────────────────────────────────────────────
    if matched_id:
        bill_row = bills_df[bills_df["bill_id"] == matched_id]
        sponsor     = _safe(bill_row.iloc[0].get("sponsor_by_show_as")) if not bill_row.empty else ""
        bill_status = _safe(bill_row.iloc[0].get("status"))            if not bill_row.empty else ""
        bill_stage  = _safe(bill_row.iloc[0].get("most_recent_stage_event_show_as")) if not bill_row.empty else ""

        oir_link = source_link_html(
            matched_url,
            "View Bill on Oireachtas.ie",
            aria_label="Open the matched Bill on oireachtas.ie",
        ) if matched_url else ""

        score_str = f"{float(match_score):.2f}" if pd.notna(match_score) else "—"

        st.html(f"""
        <div class="si-billlink">
          <div class="si-billlink-kicker">↪ Made under (closest matched Bill in Oireachtas index)</div>
          <div class="si-billlink-title">{html.escape(matched_title) or "—"}</div>
          <div class="si-billlink-meta">
            Bill {html.escape(matched_id)}
            {('&nbsp;·&nbsp; ' + html.escape(bill_status)) if bill_status else ''}
            {('&nbsp;·&nbsp; ' + html.escape(bill_stage)) if bill_stage else ''}
            {('&nbsp;·&nbsp; sponsor: ' + html.escape(sponsor)) if sponsor else ''}
          </div>
          {oir_link}
          <div class="si-billlink-conf">match confidence: {score_str}</div>
        </div>
        """)
    elif parent.strip():
        st.html(f"""
        <div class="si-billlink" style="background:#fbf6ed; border-color:#e9dab3;">
          <div class="si-billlink-kicker" style="color:#7a5a00;">Parent legislation (unmatched)</div>
          <div class="si-billlink-title">{html.escape(parent)}</div>
          <div class="si-billlink-meta">
            No confident match against the bills index. Many SIs cite older
            statutes that pre-date the Open Data API window — POC threshold
            is {MATCH_THRESHOLD:.2f}.
          </div>
        </div>
        """)


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
def statutory_instruments_page() -> None:
    inject_css()        # base app styling (dt-source-link etc.)
    _inject_poc_css()   # POC-only classes

    si_df    = load_si()
    bills_df = load_bills()
    si_df    = match_si_to_bill(si_df, bills_df)

    # URL-driven entry: ?si=<si_id> opens the detail view.
    url_si = st.query_params.get("si")
    if url_si:
        st.session_state["si_selected_id"] = url_si
    selected = st.session_state.get("si_selected_id")

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Statutory Instruments")
        if selected:
            st.html('<div class="page-subtitle">SI detail</div>')
        else:
            st.html('<div class="page-subtitle">POC · Iris Oifigiúil → Acts of the Oireachtas</div>')
            st.divider()

            yrs = sorted(si_df["si_year"].unique(), reverse=True)
            year_sel = st.multiselect("Year", yrs, default=yrs[:3] if len(yrs) >= 3 else yrs,
                                      key="si_year_filter")

            domains = ["All"] + sorted([d for d in si_df["si_policy_domain_primary"].dropna().unique()])
            domain_sel = st.selectbox("Policy domain", domains,
                                       format_func=_pretty_token, key="si_domain_filter")

            ops = ["All"] + sorted([o for o in si_df["si_operation_primary"].dropna().unique()])
            op_sel = st.selectbox("Operation type", ops,
                                   format_func=_pretty_token, key="si_op_filter")

            actors = ["All"] + sorted([a for a in si_df["si_responsible_actor"].dropna().unique()])
            actor_sel = st.selectbox("Responsible actor", actors,
                                      format_func=_pretty_token, key="si_actor_filter")

            search = st.text_input("Search title", placeholder="e.g. fisheries, vehicles, COVID…",
                                    key="si_title_search").strip()

    # ── Detail view ───────────────────────────────────────────────────────────
    if selected:
        match = si_df[si_df["si_id"] == selected]
        if match.empty:
            st.warning(f"SI '{selected}' not found in the current filtered set.")
            if back_button("← Back to SI Index", key="si_poc_nf"):
                st.session_state.pop("si_selected_id", None)
                st.query_params.clear()
                st.rerun()
            return
        _render_si_detail(match.iloc[0], bills_df)
        return

    # ── Index view ────────────────────────────────────────────────────────────
    hero_banner(
        kicker="Iris Oifigiúil · POC integration",
        title="Statutory Instruments before the Oireachtas",
        dek=(
            "Every Act of the Oireachtas spawns regulations — the SIs that fill "
            "in the detail. This POC indexes the Iris Oifigiúil corpus, classifies "
            "each instrument by policy domain and operation type, and links it back "
            "to the parent Act when the join is confident."
        ),
    )

    st.html(
        '<div class="si-poc-note">POC notice — figures, classifications, and Act '
        'matches are derived in-page from the gold extract; not yet pipeline-blessed. '
        f"Window: SI year ≥ {SI_YEAR_FLOOR}, taxonomy confidence ≥ {MIN_TAXO_CONFIDENCE:.1f}, "
        f"parent-Act match threshold {MATCH_THRESHOLD:.2f}.</div>"
    )

    filtered = _apply_filters(
        si_df,
        years=st.session_state.get("si_year_filter") or [],
        domain=st.session_state.get("si_domain_filter"),
        op=st.session_state.get("si_op_filter"),
        actor=st.session_state.get("si_actor_filter"),
        search=st.session_state.get("si_title_search"),
    )

    _render_kpi_strip(filtered)
    _render_trends(filtered)
    _render_si_index(filtered)


if __name__ == "__main__":
    statutory_instruments_page()
