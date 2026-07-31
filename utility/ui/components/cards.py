"""Member / party / committee card builders — the ranked-list card tier.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.
"""

from __future__ import annotations

from html import escape as _h

import streamlit as st

from .navigation import pill
from .text import search_matches


def member_card_html(
    name: str,
    meta: str = "",
    rank: int | None = None,
    pills_html: str = "",
    badge_html: str = "",
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
    meta_prefix_html: str = "",
) -> str:
    """Canonical member name card HTML string.

    Avatar slot priority: photo → rank chip → initials chip → empty. The
    slot is always 2.25rem wide so layout is stable across all states.

    pills_html       — raw HTML for pill <span> elements (use int-stat-pill class)
    badge_html       — optional right-side metric; use dt-name-card-badge-metric
                       sub-class for the standard blue days/amount style
    avatar_initials  — 1–2 letter fallback when neither photo nor rank fits
                       the page (e.g. profile-context cards).
    """
    # Audit fix (2026-05-26, interests P1-2): the previous priority was
    # photo → rank → initials, which meant rank was INVISIBLE on every
    # card that had a member photo (~80% of Dáil members). Critical for a
    # leaderboard. Now: when a photo is present, the rank renders as a
    # small overlay chip on the avatar; rank-only and initials-only paths
    # are unchanged. The overlay slot is positioned by
    # ``.dt-name-card-rank-overlay`` in shared_css.py.
    if avatar_url:
        rank_overlay = ""
        if rank is not None:
            rank_overlay_cls = (
                "dt-name-card-rank-overlay dt-name-card-rank-overlay-top" if rank <= 3 else "dt-name-card-rank-overlay"
            )
            rank_overlay = f'<span class="{rank_overlay_cls}">#{rank}</span>'
        left_inner = f'<img class="dt-name-card-avatar" src="{_h(avatar_url)}" alt="" loading="lazy">{rank_overlay}'
    elif rank is not None:
        rank_cls = "dt-name-card-rank dt-name-card-rank-top" if rank <= 3 else "dt-name-card-rank"
        left_inner = f'<span class="{rank_cls}">#{rank}</span>'
    elif avatar_initials:
        left_inner = f'<span class="dt-name-card-initials" aria-hidden="true">{_h(avatar_initials)}</span>'
    else:
        left_inner = ""
    meta_html = f'<div class="dt-name-card-meta">{meta_prefix_html}{_h(meta)}</div>' if meta or meta_prefix_html else ""
    pills_sec = f'<div class="dt-name-card-pills">{pills_html}</div>' if pills_html else ""
    badge_sec = f'<div class="dt-name-card-badge">{badge_html}</div>' if badge_html else ""
    return (
        f'<div class="dt-name-card">'
        f'<div class="dt-name-card-left">{left_inner}</div>'
        f'<div class="dt-name-card-body">'
        f'<div class="dt-name-card-name">{_h(name)}</div>'
        f"{meta_html}{pills_sec}"
        f"</div>"
        f"{badge_sec}"
        f"</div>"
    )


def ranked_member_card(
    name: str,
    meta: str,
    *,
    rank: int | None = None,
    pills_html: str = "",
    badge_html: str = "",
    profile_href: str = "",
    avatar_url: str | None = None,
    avatar_initials: str | None = None,
) -> str:
    """Canonical ranked-list member card — derives avatar from the member name
    and routes pills/badge through ``member_card_html``.

    Use ``pills_html`` (pre-built string) so callers can mix the canonical
    :func:`pill` helper with domain-specific CSS classes (e.g. ``pay-taa-pill``).
    When ``profile_href`` is provided, a small "Profile ↗" pill is appended to
    the pill row (the link goes to the canonical /member-overview profile).

    Replaces the byte-similar ``_pay_card_html`` / ``_int_member_card_html`` /
    ``_lob_card_html`` / ``_ranked_card_html`` closing boilerplate that used
    to live in 4 page files.
    """
    if avatar_url is None or avatar_initials is None:
        from ui.avatars import avatar_data_url
        from ui.avatars import initials as _initials_fn

        if avatar_url is None:
            avatar_url = avatar_data_url(name)
        if avatar_initials is None:
            avatar_initials = _initials_fn(name)
    if profile_href:
        pills_html = pills_html + (
            f'<a class="dt-member-link int-stat-pill-link" href="{_h(profile_href)}" '
            f'target="_self" aria-label="View profile of {_h(name)}">Profile ↗</a>'
        )
    return member_card_html(
        name=name,
        meta=meta,
        rank=rank,
        pills_html=pills_html,
        badge_html=badge_html,
        avatar_url=avatar_url,
        avatar_initials=avatar_initials,
    )


def rank_card_row(
    name: str,
    meta: str,
    pills: list[str],
    btn_key: str,
    rank: int | None = None,
    quote: str = "",
    btn_help: str = "",
    col_ratio: tuple[int, int] = (14, 1),
    profile_href: str = "",
) -> bool:
    """Name card + navigation arrow. Returns True when the arrow is clicked.

    rank          — pass an int to show the #N badge (gold for top 3); omit for unranked lists.
    quote         — optional italic snippet shown below the pills (e.g. top declaration text).
    profile_href  — optional cross-page profile URL. When provided, appends a small
                    "Profile ↗" anchor pill. Build with utility/ui/entity_links.member_profile_url.
    Caller is responsible for navigation + st.rerun() on True.
    """
    card_col, btn_col = st.columns(col_ratio)
    pills_html = "".join(pill(p) for p in pills)
    if profile_href:
        pills_html += (
            f'<a class="dt-member-link int-stat-pill-link" href="{_h(profile_href)}" '
            f'target="_self" aria-label="View profile of {_h(name)}">Profile ↗</a>'
        )
    if quote:
        pills_html += f'<p class="int-highlight-quote">{quote}</p>'
    card_col.markdown(
        member_card_html(name=name, meta=meta, rank=rank, pills_html=pills_html),
        unsafe_allow_html=True,
    )
    btn_col.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
    return btn_col.button("→", key=btn_key, help=btn_help or f"View {name}")


PARTY_COLOURS: dict[str, str] = {
    "Fianna Fáil": "#66bb6a",
    "Fine Gael": "#1e88e5",
    "Sinn Féin": "#2e7d32",
    "Labour": "#e53935",
    "Social Democrats": "#8e24aa",
    "Green Party": "#43a047",
    "People Before Profit": "#d81b60",
    "Solidarity": "#c2185b",
    "Aontú": "#3949ab",
    "Independent": "#9e9e9e",
    "Independent Ireland": "#ff7043",
    "Right To Change": "#7b1fa2",
    "Unknown": "#bdbdbd",
}


def party_colour(party: str) -> str:
    if not party:
        return PARTY_COLOURS["Unknown"]
    return PARTY_COLOURS.get(party.strip(), PARTY_COLOURS["Unknown"])


def party_stripe_html(parties: list[tuple[str, int]], *, show_legend: bool = True) -> str:
    """Inline horizontal stacked stripe of party seat shares.

    parties — ordered list of (party_name, seat_count). Caller controls order
              (e.g. descending by seats). Zero-count entries are skipped.
    show_legend — render the dot-and-count legend below the stripe.
    """
    cleaned = [(p, int(c)) for p, c in parties if c and int(c) > 0]
    if not cleaned:
        return ""
    total = sum(c for _, c in cleaned) or 1
    segs = "".join(
        f'<div class="cmt-stripe-seg" style="width:{(c / total) * 100:.2f}%;'
        f'background:{party_colour(p)}" title="{_h(p)}: {c}"></div>'
        for p, c in cleaned
    )
    legend = ""
    if show_legend:
        chips = "".join(
            f'<span><span class="cmt-stripe-legend-dot" style="background:{party_colour(p)}"></span>'
            f"<strong>{_h(p)}</strong> {c}</span>"
            for p, c in cleaned
        )
        legend = f'<div class="cmt-stripe-legend">{chips}</div>'
    return f'<div class="cmt-stripe">{segs}</div>{legend}'


# Non-party distribution palettes. Sequential = single-hue light→dark for ORDERED
# scales (e.g. time-on-list, so the long-wait tail reads as "heavy"). Categorical =
# distinct neutral hues for nominal dimensions (tenure/employment/…). Deliberately
# NOT party colours and NOT red/green (no good/bad encoding).
_SEQ_RAMP = [
    "#e9eff5",
    "#cfe0ec",
    "#aecbdf",
    "#86afcd",
    "#5d8fb6",
    "#3d719c",
    "#275680",
    "#173e5e",
]
_CAT_PALETTE = [
    "#4c78a8",
    "#72b7b2",
    "#dba43c",
    "#b07aa1",
    "#9c755f",
    "#83b26f",
    "#a3acb9",
    "#5b9bd5",
]


def proportion_stripe_html(
    segments: list[tuple[str, float]],
    *,
    palette: str = "categorical",
    show_legend: bool = True,
    unit: str = "",
) -> str:
    """Generic stacked proportion stripe — the non-party sibling of party_stripe_html.

    segments — ordered list of (label, value). Caller controls order.
    palette  — 'sequential' (ordered single-hue ramp) | 'categorical' (distinct hues).
    Reuses the .cmt-stripe* CSS. Zero/None values are skipped; legend shows % shares.
    """
    cleaned = [(str(lbl), float(v)) for lbl, v in segments if v and float(v) > 0]
    if not cleaned:
        return ""
    total = sum(v for _, v in cleaned) or 1.0
    ramp = _SEQ_RAMP if palette == "sequential" else _CAT_PALETTE
    n = len(cleaned)

    def colour(i: int) -> str:
        if palette == "sequential":
            return ramp[round(i * (len(ramp) - 1) / max(n - 1, 1))]
        return ramp[i % len(ramp)]

    segs = "".join(
        f'<div class="cmt-stripe-seg" style="width:{(v / total) * 100:.2f}%;'
        f'background:{colour(i)}" title="{_h(lbl)}: {v:,.0f}{_h(unit)} ({v / total * 100:.0f}%)"></div>'
        for i, (lbl, v) in enumerate(cleaned)
    )
    legend = ""
    if show_legend:
        chips = "".join(
            f'<span><span class="cmt-stripe-legend-dot" style="background:{colour(i)}"></span>'
            f"<strong>{_h(lbl)}</strong> {v / total * 100:.0f}%</span>"
            for i, (lbl, v) in enumerate(cleaned)
        )
        legend = f'<div class="cmt-stripe-legend">{chips}</div>'
    return f'<div class="cmt-stripe">{segs}</div>{legend}'


def committee_row_html(
    name: str,
    *,
    rank: int | None = None,
    chair: str | None = None,
    chair_party: str | None = None,
    members: int = 0,
    type_: str = "",
    status: str = "",
    party_seats: list[tuple[str, int]] | None = None,
    oireachtas_url: str | None = None,
) -> str:
    """Single committee register row — card with chair, type, status, party stripe, link.

    Use the adjacent `→` button column (st.columns) for navigation; the CSS
    rule on stHorizontalBlock:has(.cmt-row) collapses the row so the button
    sits next to the fit-content card.
    """
    rank_html = f'<div class="cmt-row-rank">#{int(rank)}</div>' if rank is not None else ""
    # Only NON-active statuses render (2026-07-20 clutter pass). "ACTIVE" was
    # printed on all 25 register cards — the default state, so the badge marked
    # nothing. "Ended"/"Unknown" is the exception a reader needs to see, and an
    # unbadged card now reads as active.
    status_cls = "cmt-row-status-active" if status == "Active" else "cmt-row-status-ended"
    status_html = (
        f'<span class="cmt-row-status {status_cls}">{_h(status)}</span>' if status and status != "Active" else ""
    )
    meta_parts: list[str] = []
    if chair:
        chair_meta = f"Chair: <strong>{_h(chair)}</strong>"
        if chair_party:
            chair_meta += f" ({_h(chair_party)})"
        meta_parts.append(chair_meta)
    if type_:
        meta_parts.append(f"Type: <strong>{_h(type_)}</strong>")
    if members:
        meta_parts.append(f"<strong>{int(members)}</strong> member{'s' if members != 1 else ''}")
    meta_html = f'<div class="cmt-row-meta">{" · ".join(meta_parts)}</div>' if meta_parts else ""
    # No per-card legend (2026-07-21 clutter pass): the dot-and-count legend
    # listed every party on all 25 register cards — a text wall duplicating the
    # stacked bar right above it. The bar keeps per-segment hover titles, and the
    # register renders ONE shared colour key above the grid (committees.py).
    stripe_html = party_stripe_html(party_seats, show_legend=False) if party_seats else ""
    # P2-5 audit fix: previously each register card carried its own
    # "Oireachtas.ie ↗" link — five identical accent-coloured external
    # links per page created a vertical column of click-bait that
    # competed with the actual card click target. The committee detail
    # identity strip already surfaces this link in context (one link,
    # the right time). The `oireachtas_url` argument is kept on the
    # signature so callers don't need editing; it's just not rendered
    # on the register row any more.
    return (
        f'<div class="cmt-row">'
        f"{rank_html}"
        f'<div class="cmt-row-body">'
        f'<div class="cmt-row-head"><span class="cmt-row-name">{_h(name)}</span>{status_html}</div>'
        f"{meta_html}"
        f"{stripe_html}"
        f"</div>"
        f"</div>"
    )


def committee_identity_strip(
    name: str,
    *,
    type_: str = "",
    status: str = "",
    chair: str | None = None,
    chair_party: str | None = None,
    chair_html: str | None = None,
    member_count: int = 0,
    oireachtas_url: str | None = None,
    source_document_url: str | None = None,
) -> None:
    """Stage-2 identity strip for a single committee.

    Pass ``chair_html`` to render the chair name as an already-safe HTML
    fragment (e.g. a ``member_link_html`` anchor) instead of the default
    escaped ``chair`` text — the caller is then responsible for escaping.
    """
    # P2-3 audit fix: Active / Ended was rendered as inline text inside the
    # meta line, despite the register cards rendering the same value as a
    # coloured chip. Lift status out of the meta line and emit it with the
    # same chip CSS so the detail page is visually consistent with the
    # register.
    status_html = ""
    if status:
        status_cls = "cmt-row-status-active" if status == "Active" else "cmt-row-status-ended"
        status_html = f'<span class="cmt-row-status {status_cls}">{_h(status)}</span>'
    # Plain parts are escaped here; the chair part may carry already-safe HTML
    # (a member-profile anchor) so it is assembled separately and spliced in
    # unescaped after the others are escaped.
    meta_parts: list[str] = []
    if type_:
        meta_parts.append(type_)
    if member_count:
        meta_parts.append(f"{member_count} members")
    safe_parts = [_h(p) for p in meta_parts]
    if chair_html:
        party_suffix = f" ({_h(chair_party)})" if chair_party else ""
        safe_parts.append(f"Chair: {chair_html}{party_suffix}")
    elif chair:
        chair_text = chair if not chair_party else f"{chair} ({chair_party})"
        safe_parts.append(_h(f"Chair: {chair_text}"))
    meta_html = " · ".join(safe_parts)
    links: list[str] = []
    if oireachtas_url or source_document_url:
        from ui.entity_links import source_link_html  # local — avoids any future circular risk
    if oireachtas_url:
        links.append(
            source_link_html(
                oireachtas_url,
                "Oireachtas.ie",
                aria_label=f"Open {name} on oireachtas.ie",
            )
        )
    if source_document_url:
        links.append(
            source_link_html(
                source_document_url,
                "Source document",
                aria_label=f"Open the source document for {name}",
            )
        )
    links_html = f'<div class="cmt-identity-links">{"".join(links)}</div>' if links else ""
    st.markdown(
        f'<div class="cmt-identity">'
        f'<div class="cmt-identity-head">'
        f'<p class="cmt-identity-name">{_h(name)}</p>'
        f"{status_html}"
        f"</div>"
        f'<p class="cmt-identity-meta">{meta_html}</p>'
        f"{links_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def find_a_td_search(
    members: list[str],
    *,
    key_prefix: str,
    placeholder: str = "Type a TD name…",
) -> str | None:
    """Inline search-and-select for a TD. Returns the selected name or None.

    Designed for the command bar in the committee register's primary view.
    Same shape as `main_member_jump` but no kicker label — fits the bar layout.
    """
    cols = st.columns([3, 2])
    with cols[0]:
        search = st.text_input(
            "Find a TD",
            placeholder=placeholder,
            key=f"{key_prefix}_td_search",
            label_visibility="collapsed",
        )
    filtered = [m for m in members if search_matches(search, m)]
    with cols[1]:
        chosen = st.selectbox(
            "Find a TD",
            ["— pick a TD —"] + filtered,
            key=f"{key_prefix}_td_select",
            label_visibility="collapsed",
        )
    return chosen if chosen and chosen != "— pick a TD —" else None


def find_a_td_filter(
    members: list[str],
    *,
    key_prefix: str,
    label: str = "Find a TD",
    placeholder: str = "Search by name, party or constituency…",
    select_placeholder: str = "— select —",
    show_label: bool = True,
    show_picker: bool = True,
    width_ratio: tuple[int, int, int] = (3, 2, 4),
) -> tuple[str, str | None]:
    """Compact Find-a-TD filter: search input + helper dropdown side-by-side.

    Use this anywhere in the app where the user needs to find a TD by name,
    party, or constituency. The component is deliberately narrower than the
    full content column (last ratio slot is an empty spacer) so the filter
    doesn't dominate the page.

    ``show_picker=False`` drops the helper dropdown and renders the search
    input alone. Use it when the results below are themselves clickable (a
    card grid): the dropdown duplicates that affordance, and users mistake
    its combobox for the search box — text typed or deleted there filters
    only the option list, never the page, which reads as a broken filter.

    Returns ``(query, picked)``:
        query   raw search text — caller filters the list/grid below by this
                across whatever fields are relevant (name, party, constituency).
        picked  name selected from the helper dropdown, or None (always None
                with ``show_picker=False``). Caller should treat
                ``picked is not None`` as a navigation event (set the
                relevant session-state key + ``st.rerun()``).
    """
    if show_label:
        st.html(f'<p class="dt-main-search-kicker">{_h(label)}</p>')
    cols = st.columns(width_ratio)
    with cols[0]:
        query = (
            st.text_input(
                label,
                placeholder=placeholder,
                key=f"{key_prefix}_filter_query",
                label_visibility="collapsed",
                icon=":material/search:",
            )
            or ""
        )
    if not show_picker:
        return query, None
    filtered = [m for m in members if search_matches(query, m)]
    with cols[1]:
        chosen = st.selectbox(
            label,
            [select_placeholder] + filtered,
            key=f"{key_prefix}_filter_pick",
            label_visibility="collapsed",
        )
    picked = chosen if chosen and chosen != select_placeholder else None
    return query, picked
