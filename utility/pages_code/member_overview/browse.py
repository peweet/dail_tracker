from __future__ import annotations

import datetime
from html import escape as _h

import pandas as pd
import streamlit as st

from ui.avatars import avatar_data_url, initials as _initials
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    find_a_td_filter,
    glossary_strip,
    member_card_html,
    paginate,
    pagination_controls,
    party_colour,
    text_search_mask,
)
from ui.entity_links import member_profile_url

from ._shared import _member_codes_for_dail, _member_list_all

_OTHER_PILL = "Other / Independent"
_OTHER_MIN = 3  # UI display threshold — parties with fewer TDs collapse into
# the "Other / Independent" pill. This is a chip-layout
# decision (keep the pill row scannable), not a civic metric:
# changing it shouldn't require a pipeline rebuild.


def _named_parties(df: pd.DataFrame) -> list[str]:
    """Parties with >= _OTHER_MIN members, sorted by size desc then name."""
    if df.empty or "party_name" not in df.columns:
        return []
    counts = df["party_name"].value_counts()  # logic_firewall: display_only
    parties = df["party_name"].dropna().astype(str).unique().tolist()
    parties = [p for p in parties if p and p.lower() not in ("nan", "")]
    named = [p for p in parties if int(counts.get(p, 0)) >= _OTHER_MIN]
    return sorted(named, key=lambda p: (-int(counts.get(p, 0)), p))


def _party_pill_options(df: pd.DataFrame) -> list[str]:
    named = _named_parties(df)
    if not named:
        return []
    counts = df["party_name"].value_counts()  # logic_firewall: display_only
    in_named = sum(int(counts.get(p, 0)) for p in named)
    has_other = (len(df) - in_named) > 0
    return named + ([_OTHER_PILL] if has_other else [])


_CURRENT_YEAR = datetime.date.today().year  # for the historic "year served" filter ceiling


def _dail_filter_options(df: pd.DataFrame) -> list[str]:
    """['All', '34', '33', …] from the dails_served comma lists, highest first."""
    nums: set[int] = set()
    for s in df.get("dails_served", pd.Series(dtype=str)).dropna().astype(str):
        for tok in s.split(","):
            tok = tok.strip()
            if tok.isdigit():
                nums.add(int(tok))
    return ["All"] + [str(n) for n in sorted(nums, reverse=True)]


def _render_browse(conn) -> None:
    df = _member_list_all(conn)

    # House scope — Dáil (default) or Seanad. Keeps the list, labels and glossary
    # coherent: a mixed 236-member list with a "TDs" heading would mislead.
    house = (
        st.segmented_control(
            "Chamber",
            options=["Dáil", "Seanad"],
            default="Dáil",
            key="mo_browse_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    is_seanad = house == "Seanad"
    term = "Senator" if is_seanad else "TD"
    terms = "Senators" if is_seanad else "TDs"
    place_word = "panel" if is_seanad else "constituency"

    st.html(
        '<div class="dt-hero">'
        '<p class="dt-kicker">MEMBER OVERVIEW</p>'
        f'<h1 class="mo-browse-h1">Browse all {_h(terms)}</h1>'
        f'<p class="dt-dek">Pick a {_h(term)} to open their accountability profile: '
        "attendance, votes by policy area, payments, lobbying, and legislation.</p>"
        "</div>"
    )
    glossary_strip(
        [
            (
                term,
                "Seanadóir, a member of the Seanad (Senate)" if is_seanad else "Teachta Dála, a member of the Dáil",
            ),
        ]
    )

    # ── Historic members ──────────────────────────────────────────────────────
    # Default OFF → only sitting members (the page's original behaviour). When on,
    # former TDs/Senators (back as far as the registers parse cleanly) join the
    # list and the Dáil + year filters appear.
    has_current_flag = "is_current" in df.columns
    has_historic = has_current_flag and (~df["is_current"].astype(bool)).any()
    include_historic = False
    if has_historic:
        include_historic = st.toggle(
            f"Include former {terms}",
            value=False,
            key="mo_browse_historic",
            help=f"Show {terms} from past terms. Interest declarations go back to the "
            "earliest cleanly-parsed register; scanned years are omitted.",
        )
    if has_current_flag and not include_historic:
        df = df[df["is_current"].astype(bool)].reset_index(drop=True)
    elif include_historic and {"dails_served", "served_from_year"} <= set(df.columns):
        fcol1, fcol2 = st.columns(2)
        with fcol1:
            dail_opts = _dail_filter_options(df)
            dail_pick = (
                st.pills(
                    f"{'Seanad' if is_seanad else 'Dáil'} term",
                    options=dail_opts,
                    default="All",
                    key="mo_browse_dail",
                    label_visibility="collapsed",
                    help="Filter to members who served in a given term.",
                )
                or "All"
            )
        with fcol2:
            yr_lo = int(pd.to_numeric(df["served_from_year"], errors="coerce").min() or 2011)
            yr_hi = _CURRENT_YEAR
            year_choices = ["All years"] + [str(y) for y in range(yr_hi, yr_lo - 1, -1)]
            year_sel = st.selectbox(
                "Year served",
                options=year_choices,
                index=0,
                key="mo_browse_year",
                label_visibility="collapsed",
            )
        if dail_pick != "All":
            # Term membership resolved in SQL (list_contains over dails_served);
            # here it's just an isin() filter on the approved key column.
            df = df[df["unique_member_code"].astype(str).isin(_member_codes_for_dail(conn, dail_pick))]
        if year_sel != "All years":
            y = int(year_sel)
            frm = pd.to_numeric(df["served_from_year"], errors="coerce")
            to = pd.to_numeric(df["served_to_year"], errors="coerce").fillna(_CURRENT_YEAR)
            df = df[(frm <= y) & (to >= y)]
        df = df.reset_index(drop=True)

    # v_member_registry is unique on unique_member_code (verified on the
    # silver parquet: 176 rows / 176 distinct codes). The page-side
    # drop_duplicates that used to live here was defensive against a
    # historical pipeline gap that no longer exists.
    # Search box only — no helper dropdown. The card grid below is the result
    # list and every card is a link, so the dropdown duplicated navigation
    # while its combobox read as a second, broken search box (typing or
    # deleting text in it never changed the grid).
    member_names = df["member_name"].dropna().astype(str).tolist()
    search, _ = find_a_td_filter(
        member_names,
        key_prefix="mo_browse",
        label=f"Find a {term}",
        placeholder=f"Search by name, party or {place_word}…",
        show_picker=False,
    )

    # Multi-select pills: pick any combination of parties (e.g. Fianna Fáil +
    # Fine Gael). No selection = all parties, so the explicit "All parties"
    # pill is gone — clearing the pills restores the full list.
    party_options = _party_pill_options(df)
    selected_parties = st.pills(
        "Party",
        options=party_options,
        selection_mode="multi",
        key="mo_browse_party",
        label_visibility="collapsed",
        help="Pick one or more parties; leave empty to show every party.",
    )

    filtered = df.copy()
    if selected_parties:
        mask = pd.Series(False, index=filtered.index)
        named_picks = [p for p in selected_parties if p != _OTHER_PILL]
        if named_picks:
            mask |= filtered["party_name"].isin(named_picks)
        if _OTHER_PILL in selected_parties:
            named_set = set(_named_parties(df))
            mask |= filtered["party_name"].isna() | ~filtered["party_name"].isin(named_set)
        filtered = filtered[mask]
    if search and search.strip():
        # Hyphen/space/case-tolerant + regex-safe: "Dublin South West" matches
        # the stored "Dublin South-West" and a "(" never crashes the search.
        filtered = filtered[text_search_mask(filtered, search, ["member_name", "party_name", "constituency"])]

    filtered = filtered.sort_values("member_name", kind="stable").reset_index(drop=True)

    showing = len(filtered)

    # Results pill — shows the current filtered count above the grid.
    evidence_heading(f"{showing:,} {term if showing == 1 else terms}")

    if filtered.empty:
        empty_state(
            f"No {terms} match your filters",
            "Try clearing the search box or choosing a different party.",
        )
        return

    # Resolve the current page slice via the reusable paginate() helper.
    # The pagination_controls() call below renders the chip row + caption
    # underneath the grid using the same key_prefix / page_size.
    # 24, the site-wide list convention (2026-07-20 clutter pass). At 12 this
    # grid needed 15 pages for 176 TDs while two-thirds of the viewport sat
    # empty below the cards.
    MO_PAGE_SIZE = 24
    pager_key = "mo_browse"
    page_idx = paginate(showing, key_prefix=pager_key, page_size=MO_PAGE_SIZE)
    visible = filtered.iloc[page_idx * MO_PAGE_SIZE : (page_idx + 1) * MO_PAGE_SIZE]

    cards = ['<div class="mo-grid">']
    for _, row in visible.iterrows():
        name = str(row.get("member_name", ""))
        party = str(row.get("party_name", "") or "")
        constit = str(row.get("constituency", "") or "")
        code = str(row["unique_member_code"])
        meta = clean_meta(party, constit)
        # Mark former members so they aren't mistaken for sitting ones. The served
        # span (e.g. "Former · 2011–2016") comes from the registry view.
        if "is_current" in row and not bool(row.get("is_current", True)):
            frm = row.get("served_from_year")
            to = row.get("served_to_year")
            span = ""
            if pd.notna(frm):
                span = f" · {int(frm)}–{int(to)}" if pd.notna(to) else f" · from {int(frm)}"
            former = f"Former {term}{span}"
            meta = f"{former} — {meta}" if meta else former
        # Audit P2-3: same party-swatch as the profile hero.
        swatch_html = (
            f'<span class="mo-party-swatch" style="background:{party_colour(party)};" aria-hidden="true"></span>'
            if party
            else ""
        )
        cards.append(
            clickable_card_link(
                href=member_profile_url(code),
                inner_html=member_card_html(
                    name=name,
                    meta=meta,
                    avatar_url=avatar_data_url(name),
                    avatar_initials=_initials(name),
                    meta_prefix_html=swatch_html,
                ),
                aria_label=f"View {name}",
            )
        )
    cards.append("</div>")
    st.html("\n".join(cards))

    # Pager sits BELOW the grid for less visual noise above.
    st.html('<div class="mo-browse-pager-spacer"></div>')
    pagination_controls(
        total=showing,
        key_prefix=pager_key,
        page_sizes=(MO_PAGE_SIZE,),
        default_page_size=MO_PAGE_SIZE,
        label=terms,
    )
