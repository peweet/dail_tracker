from __future__ import annotations

from html import escape
from typing import Iterable, Mapping, Any

import streamlit as st


def civic_page_header(kicker: str, title: str, dek: str, *, badges: Iterable[str] | None = None) -> None:
    badge_html = ""
    if badges:
        badge_html = "<div class='dt-badge-row'>" + "".join(
            f"<span class='dt-badge'>{escape(str(b))}</span>" for b in badges
        ) + "</div>"
    st.html(
        f"""
        <section class="dt-hero">
          <div class="dt-kicker">{escape(kicker)}</div>
          <h1>{escape(title)}</h1>
          <p class="dt-dek">{escape(dek)}</p>
          {badge_html}
        </section>
        """
    )


def section_heading(title: str, caption: str | None = None) -> None:
    st.html(
        f"""
        <div class="dt-section-header">
          <h2>{escape(title)}</h2>
          {f"<p>{escape(caption)}</p>" if caption else ""}
        </div>
        """
    )


def evidence_summary_panel(items: list[Mapping[str, Any]]) -> None:
    cards = []
    for item in items:
        label = escape(str(item.get("label", "")))
        value = escape(str(item.get("value", "—")))
        help_text = escape(str(item.get("help", "")))
        cards.append(
            f"""
            <article class="dt-evidence-card">
              <div class="dt-evidence-label">{label}</div>
              <div class="dt-evidence-value">{value}</div>
              {f"<div class='dt-evidence-help'>{help_text}</div>" if help_text else ""}
            </article>
            """
        )
    st.html("<section class='dt-evidence-grid'>" + "".join(cards) + "</section>")


def provenance_box(title: str, body: str) -> None:
    st.html(
        f"""
        <aside class="dt-provenance-box">
          <strong>{escape(title)}</strong>
          <p>{escape(body)}</p>
        </aside>
        """
    )


def empty_state(title: str, body: str, *, action: str | None = None) -> None:
    st.html(
        f"""
        <section class="dt-empty-state">
          <h3>{escape(title)}</h3>
          <p>{escape(body)}</p>
          {f"<p class='dt-empty-action'>{escape(action)}</p>" if action else ""}
        </section>
        """
    )


def command_bar_start() -> None:
    st.html("<section class='dt-command-bar'>")


def command_bar_end() -> None:
    st.html("</section>")
