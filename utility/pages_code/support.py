"""Support — what the site costs to run, and how to help.

Two asks, in that order of usefulness: report an error (a GitHub issue, public
and auditable) and, if you want to, buy a coffee.

DATA BOUNDARY: this page holds no business logic. The three scale figures come
from ``data_access.support_data.load_support_stats``, which derives them from
the repo's own metadata; the markup comes from the ``support_*_html`` builders
in ``ui.components``. Nothing is computed here, and no figure is typed into
copy — a hard-coded number is true the day it is written and quietly wrong
after the next refresh.

CONFIG: the public Buy Me a Coffee page is the checked-in default;
``DT_COFFEE_URL`` can replace it or explicitly disable it with an empty value.
``DT_CONTACT_EMAIL`` remains env-gated and unset by default, so a fresh checkout
publishes no private-contact address. See the builders in ui/components.py.
"""

from __future__ import annotations

import streamlit as st
from data_access.support_data import load_support_stats
from ui.components import dt_page, support_page_html


@dt_page
def support_page() -> None:
    st.markdown(support_page_html(load_support_stats()), unsafe_allow_html=True)
