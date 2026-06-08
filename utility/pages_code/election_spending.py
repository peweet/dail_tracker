"""Election Spending — per-candidate GE2024 campaign expenses (SIPO, granular tier).

Companion to the party-tier "Election expenses" lens on the Payments page. This page
shows each INDIVIDUAL candidate's election-expenses statement, down to the Part-5 line
items (e.g. Noel Grealish -> "Galway Advertiser" €2,799.48).

Layout / controls / HTML rendering only — no business logic. All figures come from the
v_sipo_candidate_* views via data_access.sipo_candidate_data (which forbids parquet
reads / GROUP BY / pandas aggregation). No-inference posture throughout:
  * OCR-derived rows carry a "verify · SIPO PDF" mark.
  * OCR is INCREMENTAL — a coverage note states how many of 607 statements are loaded.
  * 'detail' is the form's free-text "Details of item" — a MIX of suppliers + item
    descriptions, never asserted to be a payee.
  * decimal-loss OCR mis-reads are excluded from gold, never shown as a magnitude.
"""

from __future__ import annotations

from html import escape as _h
from urllib.parse import quote

import pandas as pd
import streamlit as st

from data_access.sipo_candidate_data import (
    fetch_by_category,
    fetch_candidate,
    fetch_line_items,
    fetch_ranked,
    fetch_top_details,
    fetch_totals,
)
from shared_css import inject_css
from ui.components import (
    back_button,
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    page_error_boundary,
    party_colour,
    totals_strip,
)
from ui.entity_links import member_profile_url
from ui.source_pdfs import provenance_expander

# Total per-candidate expense statements in the SIPO GE2024 corpus (the OCR work-list).
# The page shows how many are loaded so the coverage gap is explicit, never implied-complete.
_CORPUS_TOTAL = 607

_CAVEAT = (
    "Source: Standards in Public Office Commission — individual candidates' 2024 "
    "general-election Expenses Statements. Figures are OCR-read from the official "
    "scanned returns; rows marked “verify” should be checked against the source PDF. "
    "Spending here is what a candidate spent campaigning — nothing implies influence "
    "or wrongdoing."
)

_CAT_ORDER = ["5A", "5B", "5C", "5D", "5E", "5F", "5G", "5H"]


# ── candidate league-table card (clickable -> drill-down) ───────────────────────


def _cand_card(row: pd.Series, rank: int) -> str:
    name = str(row["candidate_name"] or "—")
    const = str(row["constituency_name"] or "—")
    party = row["party"]
    stripe = party_colour(str(party)) if pd.notna(party) and party else "rgba(0,0,0,0.14)"
    total = float(row["total_spend_eur"] or 0)
    ptxt = _h(str(party)) if pd.notna(party) and party else "Unknown party"
    td = '<span class="don-vmark" style="background:#eef;color:#224">TD</span>' if bool(
        row["is_elected_td"]
    ) else ""
    verify = '<span class="don-vmark">verify · SIPO PDF</span>' if bool(row["needs_verify"]) else ""
    return (
        f'<a class="don-card" href="?cand={quote(name)}" target="_self" '
        f'style="--don-stripe:{stripe}">'
        f'<span class="don-dir">#{rank} · spent campaigning</span>'
        f'<div class="don-ptitle"><span class="don-swatch"></span><h3>{_h(name)}</h3></div>'
        f'<div class="don-amount">€{total:,.0f}</div>'
        f'<div class="don-sub">{_h(const)} · {ptxt}</div>'
        f'<div class="don-cardfoot"><span class="go">View breakdown →</span>{td}{verify}</div>'
        "</a>"
    )


def _category_bars(by_cat: pd.DataFrame) -> str:
    """Simple proportional bars for the 8 statutory categories."""
    if by_cat.empty:
        return ""
    mx = float(by_cat["total_spend"].max() or 1)
    rows = []
    for _, c in by_cat.iterrows():
        label = _h(str(c["category_label"] or c["category"]))
        total = float(c["total_spend"] or 0)
        pct = max(2.0, total / mx * 100)
        rows.append(
            f'<div class="esp-catrow">'
            f'<span class="esp-catlabel">{_h(str(c["category"]))} · {label}</span>'
            f'<span class="esp-cattrack"><span class="esp-catbar" style="width:{pct:.0f}%"></span></span>'
            f'<span class="esp-catval">€{total:,.0f}</span>'
            "</div>"
        )
    return f'<div class="esp-cats">{"".join(rows)}</div>'


_ESP_CSS = """
<style>
.esp-cats{display:flex;flex-direction:column;gap:0.35rem;margin:0.4rem 0 0.2rem}
.esp-catrow{display:grid;grid-template-columns:13rem 1fr 6rem;align-items:center;gap:0.6rem;font-size:0.85rem}
.esp-catlabel{color:var(--text-meta)}
.esp-cattrack{background:#eee;border-radius:4px;height:0.62rem;overflow:hidden}
.esp-catbar{display:block;height:100%;background:#3a6ea5}
.esp-catval{text-align:right;font-variant-numeric:tabular-nums}
.esp-items{display:flex;flex-direction:column;gap:0.2rem;margin-top:0.4rem}
.esp-irow{display:grid;grid-template-columns:8.5rem 1fr 6rem;gap:0.6rem;align-items:baseline;
  padding:0.32rem 0.55rem;background:#ffffff;border:1px solid rgba(0,0,0,0.07);border-radius:5px;font-size:0.88rem}
.esp-icat{color:var(--text-meta);font-size:0.78rem}
.esp-ivendor{font-weight:500}
.esp-icost{text-align:right;font-variant-numeric:tabular-nums}
</style>
"""


# ── candidate drill-down ────────────────────────────────────────────────────────


def _render_candidate(name: str) -> None:
    if back_button("← All candidates", key="esp_back"):
        st.query_params.pop("cand", None)
        st.rerun()

    cand = fetch_candidate(name)
    if cand is None:
        empty_state("Candidate not found", f"No election-expenses statement is loaded for {name}.")
        return

    party = cand["party"]
    ptxt = _h(str(party)) if pd.notna(party) and party else "Unknown party"
    const = _h(str(cand["constituency_name"] or "—"))
    total = float(cand["total_spend_eur"] or 0)
    stripe = party_colour(str(party)) if pd.notna(party) and party else "rgba(0,0,0,0.14)"

    st.markdown(f"### {_h(name)}")
    badges = [f"{const}", ptxt, f"€{total:,.0f} total"]
    if bool(cand["is_elected_td"]):
        badges.append("Elected TD")
    totals_strip([(f"€{total:,.0f}", "total spend"),
                  (f"€{float(cand['spend_not_public_eur'] or 0):,.0f}", "not public funds"),
                  (f"€{float(cand['spend_public_eur'] or 0):,.0f}", "public funds")])

    # Cross-link to the canonical member profile (the roster join enabled this).
    code = cand["unique_member_code"]
    if pd.notna(code) and code:
        href = member_profile_url(str(code))
        st.markdown(f'<a class="dt-member-link" href="{_h(href)}" target="_self">View TD profile ↗</a>',
                    unsafe_allow_html=True)

    # Category split from the candidate's own grid row.
    cat_rows = []
    for cat in _CAT_ORDER:
        v = cand.get(f"cat_{cat}_eur")
        if pd.notna(v) and v:
            cat_rows.append({"category": cat, "category_label": cat, "total_spend": float(v)})
    if cat_rows:
        st.markdown("#### Where the money went · by category")
        st.html(_category_bars(pd.DataFrame(cat_rows)))

    # Part-5 line items (the Grealish -> Galway Advertiser detail).
    items = fetch_line_items(name)
    st.markdown("#### Line items")
    if items.empty:
        empty_state("No line items parsed",
                    "This statement's itemised Part-5 lines are not available yet.")
    else:
        rows = []
        for _, it in items.iterrows():
            cat = _h(str(it["category_label"] or it["category"] or ""))
            detail = _h(str(it["detail"])) if pd.notna(it["detail"]) else "—"
            cost = float(it["cost_eur"] or 0)
            rows.append(
                f'<div class="esp-irow"><span class="esp-icat">{cat}</span>'
                f'<span class="esp-ivendor">{detail}</span>'
                f'<span class="esp-icost">€{cost:,.2f}</span></div>'
            )
        st.html(f'<div class="esp-items" style="border-left:3px solid {stripe};padding-left:0.5rem">'
                f'{"".join(rows)}</div>')
        st.caption("“Detail” is the candidate's free-text entry — a mix of supplier names "
                   "(e.g. Galway Advertiser) and item descriptions (e.g. Posters).")

    if bool(cand["needs_verify"]):
        st.caption("⚠ This statement's totals did not reconcile cleanly on OCR — verify against "
                   "the source PDF.")
    pdf = cand["source_pdf_url"]
    if pd.notna(pdf) and pdf:
        st.markdown(f"[Official SIPO statement (PDF) ↗]({_h(str(pdf))})")


# ── primary view ────────────────────────────────────────────────────────────────


def _render_primary() -> None:
    hero_banner(
        kicker="POLITICAL FINANCE · GENERAL ELECTION 2024",
        title="Election Spending",
        dek=(
            "What individual candidates spent campaigning at the 2024 general election, "
            "from the Expenses Statements each filed with the Standards in Public Office "
            "Commission — down to the suppliers and items on each return."
        ),
    )
    glossary_strip([
        ("SIPO", "Standards in Public Office Commission"),
        ("TD", "Teachta Dála (member of the Dáil)"),
    ])

    totals = fetch_totals()
    loaded = totals["candidates"]
    totals_strip([
        (f"{loaded}", f"of {_CORPUS_TOTAL} statements"),
        (f"€{totals['total']:,.0f}", "total spend"),
        (f"€{totals['median']:,.0f}", "median candidate"),
        (str(totals["elected"]), "elected TDs"),
        (str(totals["constituencies"]), "constituencies"),
    ])
    if loaded < _CORPUS_TOTAL:
        st.caption(f"Coverage: {loaded} of {_CORPUS_TOTAL} candidate statements processed so far "
                   "(extraction is ongoing).")

    ranked = fetch_ranked()
    if ranked.empty:
        empty_state("No candidate expenses loaded",
                    "No per-candidate election-expense statements are available yet.")
        return

    # Display-only filters (row selection, not aggregation).
    parties = ["All parties"] + sorted(
        {str(p) for p in ranked["party"].dropna().tolist() if str(p).strip()}
    )
    fcol, scol = st.columns([1, 2])
    sel_party = fcol.selectbox("Party", parties, key="esp_party")
    search = scol.text_input("Search candidate", key="esp_search",
                             placeholder="e.g. Grealish").strip().lower()

    view = ranked
    if sel_party != "All parties":
        view = view[view["party"] == sel_party]
    if search:
        view = view[view["candidate_name"].str.lower().str.contains(search, na=False)]

    st.markdown(f"#### Top campaign spenders · {len(view)} candidate{'' if len(view) == 1 else 's'}")
    if view.empty:
        empty_state("No matches", "No candidates match the current filters.")
    else:
        cards = "".join(_cand_card(r, i + 1) for i, (_, r) in enumerate(view.head(120).iterrows()))
        st.html(f'<div class="don-grid">{cards}</div>')
        if len(view) > 120:
            st.caption(f"Showing the top 120 of {len(view)}. Use the filters to narrow.")
    st.caption(_CAVEAT)

    # Category breakdown across all loaded candidates.
    by_cat = fetch_by_category()
    if not by_cat.empty:
        st.markdown("#### Where campaign money goes · by category")
        st.html(_category_bars(by_cat))

    # Top spend-detail lines (suppliers + descriptions).
    top = fetch_top_details(20)
    if not top.empty:
        st.markdown("#### Most common spend lines")
        rows = []
        for _, d in top.iterrows():
            detail = _h(str(d["detail"]))
            total = float(d["total_spend"] or 0)
            nc = int(d["candidate_count"] or 0)
            rows.append(
                f'<div class="esp-irow"><span class="esp-icat">{nc} candidate'
                f'{"" if nc == 1 else "s"}</span><span class="esp-ivendor">{detail}</span>'
                f'<span class="esp-icost">€{total:,.0f}</span></div>'
            )
        st.html(f'<div class="esp-items">{"".join(rows)}</div>')
        st.caption("A mix of supplier names and item descriptions as written on the returns — "
                   "not a verified vendor list.")

    provenance_expander(
        sections=[
            "**What this is.** Each candidate's 2024 general-election Expenses Statement, "
            "filed with the Standards in Public Office Commission (SIPO). Figures are spend, "
            "broken into eight statutory categories and itemised line by line.",
            "**OCR-derived.** Values are read from the official scanned PDFs. Rows that did not "
            "reconcile carry a “verify” mark; decimal-loss mis-reads are excluded, never shown "
            "as a number. Extraction is incremental, so coverage grows over time.",
            "**No inference.** Spending is just spending — nothing here implies influence or "
            "wrongdoing. Party is the registry party for elected TDs and the declared party "
            "otherwise; where it could not be determined it is shown as unknown, never guessed.",
        ],
        source_caption="Source: Standards in Public Office Commission — GE2024 candidate "
        "Election Expenses Statements.",
    )


@page_error_boundary
def election_spending_page() -> None:
    inject_css()
    hide_sidebar()
    st.html(_ESP_CSS)
    selected = st.query_params.get("cand")
    if selected:
        _render_candidate(selected)
        return
    _render_primary()
