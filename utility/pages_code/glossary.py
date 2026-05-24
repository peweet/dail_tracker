"""Glossary — full reference for Irish political acronyms and data terms.

Citizen-facing single page. The inline `glossary_strip()` callers on each
page pull from the same source so the wording matches.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path

_UTIL = Path(__file__).resolve().parent.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

import streamlit as st

from shared_css import inject_css
from ui.components import hero_banner, page_error_boundary, sidebar_page_header


# Single source of truth. `pages_code/*.py` import GLOSSARY_TERMS and pass
# subsets into glossary_strip() so wording is consistent everywhere.
GLOSSARY_TERMS: dict[str, str] = {
    "TD": "Teachta Dála — a member of the Dáil, Ireland's lower house of parliament. Elected from one of 43 multi-seat constituencies.",
    "Dáil": "Dáil Éireann — the lower house of the Oireachtas (Irish parliament). 174 TDs serve a maximum five-year term.",
    "Seanad": "Seanad Éireann — the upper house. 60 senators, partly appointed and partly elected.",
    "Oireachtas": "The Irish parliament as a whole, comprising the President, Dáil, and Seanad.",
    "Plenary": "A full sitting of the Dáil chamber. Does not include committee meetings, ministerial duties, or constituency work.",
    "Division": "A formal recorded vote in the Dáil. Each TD's vote (Yes / No / Abstained) is published.",
    "Bill": "A proposed law before the Oireachtas. Progresses through five stages in the Dáil and Seanad before becoming an Act.",
    "Act": "A bill that has been passed by both houses and signed by the President. Becomes law.",
    "SI": "Statutory Instrument — secondary legislation made under powers granted by an Act. Does not require a fresh parliamentary vote.",
    "Minister": "A TD appointed to the Cabinet (Government). Constitutionally required to attend Cabinet meetings and represent Ireland abroad; plenary attendance is therefore lower.",
    "Taoiseach": "The head of Government, equivalent to Prime Minister.",
    "Tánaiste": "The Deputy Prime Minister.",
    "TAA": "Travel & Accommodation Allowance — reimbursed mileage and overnight stays for TDs travelling to Leinster House. Verified against attendance records.",
    "PRA": "Public Representation Allowance — an unvouched flat allowance for constituency work. TDs choose annually whether to take it vouched or unvouched.",
    "PSA": "Parliamentary Standard Allowance — the umbrella name for the combined TAA + PRA payments published monthly.",
    "DPO": "Designated Public Official — politicians, ministers, and senior civil servants whom lobbyists are required to register contact with.",
    "Return": "A quarterly filing on lobbying.ie by an organisation declaring its lobbying activity in that period.",
    "Revolving door": "Former Designated Public Officials (ex-TDs, ex-ministers, ex-senior-civil-servants) now working in the lobbying industry. Subject to a one-year cooling-off period before they can lobby.",
    "Designated Public Official": "See DPO.",
    "Register of Members' Interests": "The annual declaration each TD and senator must file disclosing directorships, shareholdings, landlord status, gifts received, and other potential conflicts.",
    "Constituency": "The geographic area a TD is elected to represent. Ireland has 43 constituencies, each electing 3, 4, or 5 TDs.",
    "Whip": "The party-level instruction on how members should vote. Voting against the whip can have consequences ranging from a warning to expulsion from the parliamentary party.",
}


def _render_term_block(acronym: str, definition: str) -> str:
    return (
        '<div class="dt-glossary-row">'
        f'<dt class="dt-glossary-row-term">{_h(acronym)}</dt>'
        f'<dd class="dt-glossary-row-def">{_h(definition)}</dd>'
        "</div>"
    )


@page_error_boundary
def glossary_page() -> None:
    inject_css()

    with st.sidebar:
        sidebar_page_header("Glossary")
        st.caption(
            "A reference for Irish political acronyms and data terms used "
            "throughout Dáil Tracker. Each page also shows a short glossary "
            "strip under its hero with only the terms relevant to that page."
        )

    hero_banner(
        kicker="REFERENCE",
        title="Glossary",
        dek=(
            "Plain-language definitions for every acronym used in Dáil Tracker. "
            "Citizen-facing: written without parliamentary jargon."
        ),
    )

    body = "".join(_render_term_block(k, v) for k, v in GLOSSARY_TERMS.items())
    st.html(f'<dl class="dt-glossary-list">{body}</dl>')

    st.caption(
        "Source: Houses of the Oireachtas, lobbying.ie, and the Standards "
        "in Public Office Commission. Suggest a missing term by opening an "
        "issue on GitHub."
    )
