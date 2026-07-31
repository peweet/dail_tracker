"""Findings-lede / evidence-strip / callout tier — the app's prose components.

Split out of the former monolithic ``ui/components.py`` (2026-07 package
split, C4 of doc/REFACTORING_CANDIDATES.md). Pure Move-Function — no body
changes.
"""

from __future__ import annotations

from html import escape as _h

import streamlit as st


def finding_lede(sentences: list[str], *, source_html: str = "") -> None:
    """The page's opening findings — the app-wide replacement for stat strips.

    Renders 1–3 plain-English sentences under the hero, each stating a fact the
    page's data supports ("Deloitte Ireland has won more public contracts than
    any other firm — <strong>329</strong> since 2013, from <strong>54</strong>
    public bodies."). Numbers go inside ``<strong>`` for the tabular-figure
    emphasis treatment; everything else reads as prose. This is the
    findings-not-filters pattern from doc/archive/APP_REDESIGN_SWEEP_2026_06_10.md:
    the page opens by answering its own headline question, and the controls
    come after the first facts.

    DISPLAY-ONLY: every figure must arrive pre-computed from a registered view
    via ``dail_tracker_core/queries``; this helper renders, it never derives.
    Sentences are already-built HTML — escape free-text tokens with ``_h()``
    at the call site before interpolating.

    ``source_html``: optional pre-built anchor(s) from ``source_link_html()``;
    rendered as a quiet trailing source attribution on the last line.
    """
    if not sentences:
        return
    body = "".join(f"<p>{s}</p>" for s in sentences if s)
    src = f'<span class="dt-lede-source">{source_html}</span>' if source_html else ""
    st.html(f'<div class="dt-finding-lede">{body}{src}</div>')


def _bn_eur(val) -> str:
    """Compact national-scale euro: €133.8bn / €149.0bn."""
    try:
        n = float(val)
    except (TypeError, ValueError):
        return "—"
    if abs(n) >= 1_000_000_000:
        return f"€{n / 1_000_000_000:.1f}bn"
    if abs(n) >= 1_000_000:
        return f"€{n / 1_000_000:.0f}m"
    return f"€{n:,.0f}"


def render_national_finance_context(*, year: int | None = None, note: str = "") -> None:
    """Reusable national-scale anchor from the (previously orphaned) ``v_gov_finance_annual``
    view: the State's total general-government revenue / expenditure / balance for one year,
    as a denominator a reader can eyeball big public-money figures against.

    DELIBERATELY NOT a computed "% of total spend": general-government expenditure is a
    whole-economy national-accounts measure, NOT a clean superset of any single register here
    (published over-€20k payments, contract awards, etc. mix bases and tiers). Stating it as a
    share would be the "never mix registers" trap. So this renders the denominator as context
    only, with that caveat. Silently no-ops if the view is unavailable. ``note`` appends a
    page-specific framing sentence.
    """
    # Lazy import keeps ui/ free of a hard data_access dependency at module load.
    from data_access.publicfinance_data import fetch_gov_finance_annual_result

    res = fetch_gov_finance_annual_result()
    if not res.ok or res.data.empty:
        return
    df = res.data  # newest-first
    row = df[df["year"] == year] if year is not None else df.head(1)
    if row.empty:
        row = df.head(1)
    r = row.iloc[0]
    yr = int(r["year"])
    rev, exp, bal = r.get("revenue_eur"), r.get("expenditure_eur"), r.get("surplus_deficit_eur")
    balance_word = "surplus" if (bal is not None and float(bal) >= 0) else "deficit"
    extra = f" {_h(note)}" if note else ""
    st.html(
        '<div class="dt-natfin">'
        f'<span class="dt-natfin-k">National scale · {yr}</span>'
        f'<span class="dt-natfin-v">Total government spending <strong>{_bn_eur(exp)}</strong> · '
        f"revenue <strong>{_bn_eur(rev)}</strong> · "
        f"<strong>{_bn_eur(abs(float(bal)) if bal is not None else None)}</strong> {balance_word}</span>"
        '<span class="dt-natfin-c">A whole-economy national-accounts measure (CSO) — context for the '
        f"figures here, not a total they sum into.{extra}</span>"
        "</div>"
    )


def card_sources_html(links: list[str]) -> str:
    """Quiet conduit row for a card footer — splice into card HTML.

    Pass pre-built anchors from ``source_link_html()`` (which no-ops to ``""``
    on missing/non-http URLs); empties are dropped here, and the whole row
    collapses to ``""`` when nothing survives, so callers can interpolate the
    result unconditionally. One consistent placement app-wide: the conduit
    principle says every card that represents an official record links to that
    record at its official source.
    """
    kept = [x for x in links if x]
    if not kept:
        return ""
    return f'<div class="dt-card-sources">{"".join(kept)}</div>'


def glossary_strip(terms: list[tuple[str, str]]) -> None:
    """Render a one-line glossary of acronyms under the hero.

    Each entry is (acronym, expansion). The strip is small, secondary,
    designed for first-time citizen readers who don't know "TD" or "DPO".
    Journalists ignore it; citizens don't have to Google.

    Usage:
        glossary_strip([
            ("TD", "Teachta Dála (member of the Dáil)"),
            ("DPO", "Designated Public Official"),
        ])
    """
    if not terms:
        return
    items = "".join(f'<span class="dt-glossary-term"><b>{_h(a)}</b> {_h(d)}</span>' for a, d in terms)
    st.html(f'<div class="dt-glossary-strip">{items}</div>')


def totals_strip(items: list[tuple[str, str]]) -> None:
    """Compact horizontal strip of value / label pairs, with thin dividers
    between cells. Replaces ``st.metric`` triplets / quadruplets on Stage 2
    views that previously read as a fintech-dashboard hero block. CSS
    classes (``.dt-totals-*``) live in ``shared_css.py``.

    Each tuple is ``(value, label)``; value is rendered escaped, label is
    rendered escaped + UPPERCASED via CSS.

    Use this rather than ``st.columns(N)`` + ``st.metric`` on:
    - payments Rankings view (since-2020 summary)
    - lobbying org Stage 2 (returns / politicians / periods / span)
    - lobbying topic Stage 2 (returns / orgs / areas / period)
    - lobbying DPO Stage 2b individual (firms / clients / politicians / returns)

    The year-view of payments has historically used the older ``pay-totals-*``
    markup directly; that call site should migrate to this helper in the same
    pass and the ``pay-totals-*`` classes can be retired.
    """
    if not items:
        return
    cells: list[str] = []
    for value, label in items:
        cells.append(
            f'<div class="dt-totals-item">'
            f'<span class="dt-totals-num">{_h(str(value))}</span>'
            f'<span class="dt-totals-lbl">{_h(str(label))}</span>'
            f"</div>"
        )
    inner = '<div class="dt-totals-divider"></div>'.join(cells)
    st.html(f'<div class="dt-totals-strip">{inner}</div>')


def stat_strip(stats: list[tuple[str, str, str]] | list[tuple[str, str, str, str]]) -> None:
    """Render evidence stats. Each stat is (value, label, colour) or
    (value, label, colour, sub_label) where sub_label adds comparative
    context like "rank 87 of 174" below the label. Reuses .stat-strip CSS."""
    items = ""
    for stat in stats:
        if len(stat) == 4:
            value, label, colour, sub = stat
        else:
            value, label, colour = stat  # type: ignore[misc]
            sub = ""
        sub_html = f'<div class="stat-sub">{_h(sub)}</div>' if sub else ""
        items += (
            f'<div><div class="stat-num" style="color:{colour}">{_h(value)}</div>'
            f'<div class="stat-lbl">{_h(label)}</div>'
            f"{sub_html}</div>"
        )
    st.html(f'<div class="stat-strip">{items}</div>')


def outcome_badge(outcome: str) -> str:
    s = _h(outcome)
    if outcome == "Carried":
        return f'<span class="dt-outcome-carried">{s}</span>'
    if outcome == "Lost":
        return f'<span class="dt-outcome-lost">{s}</span>'
    return f'<span class="dt-outcome-unknown">{s or "—"}</span>'


def evidence_heading(text: str) -> None:
    """Cross-page section heading.

    Tier-2 audit fix (2026-05-26): emits a real ``<h2>`` rather than
    ``<p class="section-heading">``. Screen readers can now navigate by
    heading level between the page ``<h1>`` (in `hero_banner`) and
    section content. Visual styling is unchanged — same class is kept
    so the existing CSS rule still applies; only the tag changes.
    Resolves: votes Appendix #4, interests Part 3 H4, legislation P2-3,
    attendance P2-6.

    2026-06-05: switched from st.markdown(unsafe_allow_html=True) to st.html
    to comply with the page contracts' `no_unsafe_allow_html` rule (the input
    is already escaped, so this is a like-for-like swap).
    """
    st.html(f'<h2 class="section-heading">{_h(text)}</h2>')


def subsection_heading(text: str) -> None:
    """Sub-section heading nested one level below `evidence_heading`.

    Emits a real ``<h3>`` so screen readers see proper h2 → h3 nesting when a
    section (h2) contains several labelled sub-sections (e.g. the Member
    Overview "Legislation" section's "Legislation sponsored" / "Ministerial
    roles" / "Statutory Instruments signed" blocks). Reuses the
    `.section-heading` class for visual parity; only the tag level differs.
    """
    st.html(f'<h3 class="section-heading section-subheading">{_h(text)}</h3>')


def todo_callout(message: str) -> None:
    """Citizen-facing "Coming soon" callout.

    Round-3 audit fix (P1-A): previously rendered the project-internal
    `TODO_PIPELINE_VIEW_REQUIRED` token and the full developer message
    verbatim — leaked SQL view names and yaml refs to end users. Now
    strips the internal scaffolding and shows a clean "Coming soon"
    headline; the rest of the message is rendered but with the
    `TODO_PIPELINE_VIEW_REQUIRED:` prefix stripped if callers included it
    for grep-ability.

    For richer pipeline diagnostics in dev, set DT_SHOW_TODO_DETAIL=1 in
    the environment — the original developer-facing detail is then shown
    in a small monospace block under the headline.
    """
    import os
    import re

    # Strip the internal tag from the message if present so the citizen
    # sees only the human-readable trailer. Source strings can still
    # include the tag for grep-ability ("TODO_PIPELINE_VIEW_REQUIRED:
    # v_member_interests_index — Coming soon, ranked leaderboard").
    cleaned = re.sub(
        r"^\s*TODO_PIPELINE_VIEW_REQUIRED\s*:\s*",
        "",
        message,
        flags=re.IGNORECASE,
    ).strip()

    # Pull out the citizen sentence: take everything AFTER the first em-dash
    # or the first sentence ending. If neither, just show "Coming soon".
    parts = re.split(r"\s+[—–-]\s+|\.\s+", cleaned, maxsplit=1)
    citizen_msg = parts[1].strip() if len(parts) > 1 else ""
    if not citizen_msg:
        citizen_msg = "More data coming soon."
    # Audit fix (2026-05-26, interests P1-1 / committees P1-1): callers
    # often write the citizen sentence in lowercase because the developer
    # prefix before the em-dash naturally flows into it. Capitalise the
    # first character so the rendered sentence reads as a complete
    # standalone statement ("A ranked leaderboard..." not "a ranked
    # leaderboard...").
    citizen_msg = citizen_msg[0].upper() + citizen_msg[1:] if citizen_msg else citizen_msg

    show_detail = os.getenv("DT_SHOW_TODO_DETAIL") == "1"
    detail_html = (
        f'<div style="margin-top:0.4rem;font-family:monospace;font-size:0.72rem;'
        f'color:var(--text-meta);">{_h(cleaned)}</div>'
        if show_detail and cleaned
        else ""
    )
    st.html(
        f'<div class="dt-callout"><strong>Coming soon.</strong><br>'
        f'<span style="color:var(--text-meta)">{_h(citizen_msg)}</span>'
        f"{detail_html}</div>"
    )


def empty_state(heading: str, body: str) -> None:
    st.html(
        f'<div class="dt-callout"><strong>{_h(heading)}</strong><br>'
        f'<span style="color:var(--text-meta)">{_h(body)}</span></div>'
    )


def member_moved_callout(
    name: str,
    section: str,
    *,
    section_label: str = "this section",
    legacy_param: str | None = None,
    state_keys: tuple[str, ...] = (),
) -> None:
    """Render a "Member profiles have moved" callout and stop the page.

    Round-3 audit fix for two issues that were duplicated across 5 pages:
    (1) every dimension page's redirect callout was producing a broken
    target href because it used the deprecated ``name_join_key()``; this
    helper looks up the actual ``unique_member_code`` via
    :func:`data_access.identity_resolver.resolve_member_code`. (2) every
    redirect callout fell through to render the full page body underneath;
    this helper calls ``st.stop()`` so the user only sees the moved
    notice + a working link.

    Args:
        name: the TD name from the legacy URL / sidebar selection.
        section: the section-anchor id (``"interests"``, ``"payments"``,
            ``"attendance"``, ``"committees"``, etc.) — appended as
            ``#<section>`` on the target URL.
        section_label: human label for the callout copy
            (``"the Interests section"`` / ``"per-TD attendance"``).
        legacy_param: query-param key to scrub from the URL (``"member"``,
            ``"att_td"``, ``"lob_pol"``) so a refresh doesn't re-stick the
            callout.
        state_keys: session-state keys to clear (e.g. ``("selected_td_pay",)``)
            so sidebar selectboxes don't immediately re-trigger the callout.

    The page stops after rendering. Callers should put this BEFORE any
    other rendering they don't want shown when the redirect fires.
    """
    from data_access.identity_resolver import resolve_member_code
    from ui.entity_links import member_profile_url

    code = resolve_member_code(name)
    if code:
        target = member_profile_url(code, section=section)
        # Audit 2026-05-27 P2-5: button-styled CTA (was a plain underlined
        # text-link) so the redirect action carries the visual weight of an
        # affordance, not an afterthought. .dt-moved-cta lives in shared_css.
        link_html = (
            f'<a class="dt-moved-cta" href="{_h(target)}" target="_self">'
            f'Open {_h(name)}\'s profile <span aria-hidden="true">&rarr;</span></a>'
        )
    else:
        link_html = (
            f'<span class="dt-moved-fallback">'
            f"Couldn't find {_h(name)} in the member registry. Try the "
            f'<a class="dt-member-link" href="/member-overview">'
            f"All TDs browse</a>.</span>"
        )

    # Sentence-case the section label while preserving the canonical acronym
    # casing for TD / TAA / PRA / EU / US / SI. `str.capitalize()` lowercases
    # everything after the first letter — turning "Per-TD attendance" into the
    # ugly "Per-td attendance". Instead, uppercase the first letter only.
    label_display = section_label[:1].upper() + section_label[1:] if section_label else section_label

    st.html(
        f'<div class="dt-callout dt-moved-callout">'
        f"<strong>Member profiles have moved.</strong><br>"
        f'<span class="dt-moved-body">{_h(label_display)} '
        f"now lives on the canonical member-overview page.</span><br>"
        f"{link_html}"
        f"</div>"
    )

    if legacy_param:
        st.query_params.pop(legacy_param, None)
    for k in state_keys:
        st.session_state.pop(k, None)

    st.stop()


def back_button(label: str, key: str, *, help: str | None = None) -> bool:
    """Pill-shaped, dark-navy back button that stands out against the beige page bg.

    Pass any unique key — it is auto-prefixed with `dt_back_` so the single CSS
    rule in shared_css.py styles every back button consistently.
    """
    return st.button(label, key=f"dt_back_{key}", help=help)
