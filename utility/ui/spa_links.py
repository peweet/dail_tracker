"""Single-load navigation for in-page ``?param`` links.

Tiles, cards, chips and spark-bars across the app are plain
``<a href="?key=value">`` anchors (whole-card click targets — see
feedback_css_card_pattern). A raw anchor click is a full browser
navigation: the frontend reboots, the websocket reconnects and
``st.session_state`` is lost, which reads as "the entire app reloads"
on every tile click.

``install_spa_links()`` (called once at app level in utility/app.py,
before ``pg.run()``) fixes that without touching any page markup:

* a zero-height custom component (frontend: ``spa_links/index.html``)
  installs ONE delegated click listener on the app document that
  intercepts clicks on any ``a[href^="?"]``;
* the intercepted query string is returned here as the component
  value, written into ``st.query_params`` *before* the page body runs,
  so page-top handlers (``?clear=``, ``?si=``, ``?spark=`` …) see
  exactly what a real navigation would have delivered;
* Streamlit then syncs the browser URL itself (PageInfoChanged →
  ``history.pushState``) — a soft rerun over the live websocket, no
  reload, widget and session state preserved.

Anchors with a real path (cross-page links like ``/member-overview?…``)
and modified clicks (ctrl/middle-click for a new tab) are left to the
browser.
"""

from pathlib import Path
from urllib.parse import parse_qsl

import streamlit as st
import streamlit.components.v1 as components

_spa_links = components.declare_component(
    "dt_spa_links",
    path=str(Path(__file__).resolve().parent / "spa_links"),
)


def install_spa_links() -> None:
    """Mount the interceptor and apply any pending tile-link navigation.

    Must run at app level (outside ``pg.run()``) so the component stays
    mounted across page switches, and before the page body so the page
    sees the updated ``st.query_params`` in the same run.
    """
    event = _spa_links(key="_dt_spa_links", default=None)
    if not isinstance(event, dict):
        return
    seq = event.get("seq")
    href = str(event.get("href") or "")
    # Component values are sticky across reruns — only act on a fresh click.
    if not seq or seq == st.session_state.get("_dt_spa_seq"):
        return
    st.session_state["_dt_spa_seq"] = seq
    if not href.startswith("?"):
        return
    # An anchor navigation replaces the WHOLE query string; from_dict
    # mirrors that (plain assignment would merge with leftover params).
    st.query_params.from_dict(dict(parse_qsl(href[1:], keep_blank_values=True)))
