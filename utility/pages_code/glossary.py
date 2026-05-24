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
    "Committee": "A smaller group of TDs and senators that scrutinises bills line-by-line, holds inquiries, and questions ministers and officials. Most of the substantive work on legislation happens here, not on the chamber floor.",
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


# Long-form explainers shown under the term list. Body is trusted HTML
# (handwritten, no user input) so <p>, <ol>, <strong> render as intended.
EXPLAINERS: list[tuple[str, str]] = [
    (
        "Statutory Instruments (SIs)",
        """
        <p>An SI is <strong>secondary legislation</strong>. A parent Act delegates
        powers to a minister (or another body, such as a regulator), and an SI
        is the document the minister signs to exercise those powers — to set
        fees, commence parts of an Act, transpose an EU directive, prescribe
        a form, or make detailed regulations.</p>
        <p>SIs do not require a fresh vote in the Dáil or Seanad. They are
        <strong>laid before the Houses</strong>, and either House can annul most
        SIs within 21 sitting days. A smaller category requires a positive
        resolution (an explicit approval vote) before it can take effect.</p>
        <p>Far more SIs are made each year than Acts are passed, so the bulk of
        live legal change in Ireland sits in this layer. Each SI is published in
        <em>Iris Oifigiúil</em> and on irishstatutebook.ie.</p>
        """,
    ),
    (
        "Oireachtas Committees",
        """
        <p>Committees are smaller working groups of TDs and senators. They do
        the detailed work that is impractical in a chamber of 174 members:
        line-by-line scrutiny of bills, pre-legislative scrutiny of draft
        proposals (the <em>heads of a bill</em>), inquiries into policy areas,
        and questioning of ministers, officials, and outside witnesses.</p>
        <p>The main types are <strong>Select Committees</strong> (Dáil-only),
        <strong>Joint Committees</strong> (members from both Houses),
        <strong>Sectoral Committees</strong> that shadow government departments
        (Health, Finance, Justice, etc.), and the <strong>Public Accounts
        Committee</strong>, which examines how public money has been spent.</p>
        <p>Committee membership is allocated in proportion to party strength
        in the Dáil. Reports and transcripts are public; most are the best
        single source on the technical content of a bill.</p>
        """,
    ),
    (
        "How a Bill becomes law — the five stages",
        """
        <p>A bill is a proposed law. To become an Act, it must pass
        <strong>five stages in each House</strong> of the Oireachtas (Dáil and
        Seanad) and then be signed by the President.</p>
        <ol>
            <li><strong>First Stage — Initiation.</strong> The bill is formally
            introduced and printed. No debate.</li>
            <li><strong>Second Stage — General principles.</strong> Members
            debate the purpose and policy of the bill and vote on whether it
            should proceed. Amendments are not yet considered.</li>
            <li><strong>Third Stage — Committee.</strong> Detailed, line-by-line
            examination, usually in the relevant Sectoral Committee. This is
            where most amendments are tabled and voted on.</li>
            <li><strong>Fourth Stage — Report.</strong> The bill returns to the
            full chamber. Further amendments can be moved, typically reflecting
            what emerged at Committee Stage.</li>
            <li><strong>Fifth Stage — Final.</strong> A final overall vote on
            the bill as amended. No further changes.</li>
        </ol>
        <p>The bill then passes to the <strong>other House</strong> and goes
        through the same five stages. If the second House amends it, the bill
        returns to the originating House to agree those amendments. Once both
        Houses have agreed an identical text, the bill is sent to the
        <strong>President</strong> for signature and becomes an Act.</p>
        <p>Two constitutional wrinkles: <strong>Money Bills</strong> (taxation
        and most spending) must start in the Dáil, and the Seanad can only
        recommend changes — it cannot amend them. And under
        <strong>Article 26</strong>, the President may refer a bill to the
        Supreme Court to test whether it is consistent with the Constitution
        before signing it into law.</p>
        """,
    ),
]


def _render_explainer_block(title: str, body_html: str) -> str:
    return (
        '<section class="dt-explainer">'
        f'<h2 class="dt-explainer-title">{_h(title)}</h2>'
        f'<div class="dt-explainer-body">{body_html}</div>'
        "</section>"
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

    st.html('<div class="section-heading">In depth</div>')
    explainers_html = "".join(_render_explainer_block(title, body_html) for title, body_html in EXPLAINERS)
    st.html(explainers_html)

    st.caption(
        "Source: Houses of the Oireachtas, lobbying.ie, and the Standards "
        "in Public Office Commission. Suggest a missing term by opening an "
        "issue on GitHub."
    )
