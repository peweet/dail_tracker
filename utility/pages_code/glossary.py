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
from ui.components import hero_banner, hide_sidebar, page_error_boundary, search_matches


# Single source of truth. `pages_code/*.py` import GLOSSARY_TERMS and pass
# subsets into glossary_strip() so wording is consistent everywhere.
GLOSSARY_TERMS: dict[str, str] = {
    "Act": "A bill that has been passed by both houses and signed by the President. Becomes law.",
    "AHB": "Approved Housing Body — a not-for-profit housing association (such as Clúid or Respond) approved to build and manage social housing alongside local authorities.",
    "Appointing party": "On a receivership notice, the bank or fund that appointed the receiver. Naming the appointing party makes visible who has called in a particular loan.",
    "Bill": "A proposed law before the Oireachtas. Progresses through five stages in the Dáil and Seanad before becoming an Act.",
    "Committee": "A smaller group of TDs and senators that scrutinises bills line-by-line, holds inquiries, and questions ministers and officials. Most of the substantive work on legislation happens here, not on the chamber floor.",
    "Constituency": "The geographic area a TD is elected to represent. Ireland has 43 constituencies, each electing 3, 4, or 5 TDs.",
    "CSO": "Central Statistics Office — Ireland's national statistics agency. Source of the housing-supply figures (new-home completions, residential vacancy, house prices) shown on the Constituency page.",
    "Dáil": "Dáil Éireann — the lower house of the Oireachtas (Irish parliament). 174 TDs serve a maximum five-year term.",
    "Designated Public Official": "See DPO.",
    "Division": "A formal recorded vote in the Dáil. Each TD's vote (Yes / No / Abstained) is published.",
    "DPO": "Designated Public Official — politicians, ministers, and senior civil servants whom lobbyists are required to register contact with.",
    "Examinership": "A court-supervised rescue for an insolvent but viable company. An examiner is appointed for up to 100 days to negotiate a debt-restructuring scheme with creditors while the company keeps trading.",
    "ICAV": "Irish Collective Asset-management Vehicle — a corporate fund structure widely used by international asset managers based in Ireland. ICAV strike-off (closure) notices appear in Iris Oifigiúil.",
    "Iris Oifigiúil": "The official state gazette of Ireland, published twice weekly. Carries statutory instruments, corporate notices (receiverships, liquidations, examinerships), public appointments, and ICAV strike-offs.",
    "Liquidation": "The formal winding up of a company. Solvent (Members' Voluntary, \"MVL\") liquidation closes a company that can pay its debts; creditors' or court-ordered liquidation handles insolvent ones.",
    "Loan book": "The portfolio of loans held by a bank or fund. After 2010 large Irish loan books were sold by domestic banks (AIB, PTSB, Ulster Bank) to international funds, who hold them through SPVs.",
    "Local Authority": 'A county or city council — the elected local-government body responsible for housing, planning, roads, and local services in its area. Ireland has 31. Often shortened to "council" or "LA".',
    "Minister": "A TD appointed to the Cabinet (Government). Constitutionally required to attend Cabinet meetings and represent Ireland abroad; plenary attendance is therefore lower.",
    "NOAC": "National Oversight and Audit Commission — the independent body that scrutinises how local authorities (councils) perform and spend. Its annual Performance Indicator Report is the source of the council housing-performance figures.",
    "Oireachtas": "The Irish parliament as a whole, comprising the President, Dáil, and Seanad.",
    "Plenary": "A full sitting of the Dáil chamber. Does not include committee meetings, ministerial duties, or constituency work.",
    "PRA": "Public Representation Allowance — an unvouched flat allowance for constituency work. TDs choose annually whether to take it vouched or unvouched.",
    "PSA": "Parliamentary Standard Allowance — the umbrella name for the combined TAA + PRA payments published monthly.",
    "Receivership": "A lender (or a fund that bought the loan) appoints a receiver to take control of a company's assets after a default and sell what's needed to recover the debt. The borrower keeps legal ownership but loses control.",
    "Register of Members' Interests": "The annual declaration each TD and senator must file disclosing directorships, shareholdings, landlord status, gifts received, and other potential conflicts.",
    "Retrofit": "Upgrading an existing home's energy efficiency — insulation, heating, windows — rather than building new. The council figures show what share of each council's social-housing stock was retrofitted in the year.",
    "Return": "A quarterly filing on lobbying.ie by an organisation declaring its lobbying activity in that period.",
    "Revolving door": "Former Designated Public Officials (ex-TDs, ex-ministers, ex-senior-civil-servants) now working in the lobbying industry. Subject to a one-year cooling-off period before they can lobby.",
    "RPPI": "Residential Property Price Index — the CSO's official measure of house and apartment prices, used for the median house price shown for each constituency.",
    "SCARP": "Small Companies Administrative Rescue Process — a faster, out-of-court alternative to examinership for small and micro companies, introduced in 2021. Led by a process advisor rather than a court-appointed examiner.",
    "Seanad": "Seanad Éireann — the upper house. 60 senators, partly appointed and partly elected.",
    "SI": "Statutory Instrument — secondary legislation made under powers granted by an Act. Does not require a fresh parliamentary vote.",
    "Social housing": "Homes owned or arranged by a local authority or Approved Housing Body and let at below-market rents to households that qualify on the social-housing waiting list.",
    "SPV": "Special Purpose Vehicle — a separate company set up to hold specific assets, typically a loan book bought from a bank. Many Irish receivership notices name SPV brand names (Promontoria, Beltany, Ennis) that are the Irish vehicles for international funds (Cerberus, Goldman Sachs, Cabot).",
    "SSHA": "Summary of Social Housing Assessments — the annual count of households on the social-housing waiting list in each local authority, compiled by the Housing Agency.",
    "TAA": "Travel & Accommodation Allowance — reimbursed mileage and overnight stays for TDs travelling to Leinster House. Verified against attendance records.",
    "Tánaiste": "The Deputy Prime Minister.",
    "Taoiseach": "The head of Government, equivalent to Prime Minister.",
    "TD": "Teachta Dála — a member of the Dáil, Ireland's lower house of parliament. Elected from one of 43 multi-seat constituencies.",
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
        "Corporate notices in Iris Oifigiúil",
        """
        <p>Alongside statutory instruments, <em>Iris Oifigiúil</em> carries the
        formal corporate notices that Irish company law requires to be
        published. Together they form a public record of which companies are
        being <strong>wound down, rescued, or having their loans called in</strong>.</p>
        <p>The main categories are:</p>
        <ol>
            <li><strong>Receivership.</strong> A lender — a bank, or a fund
            that has bought the loan — appoints a receiver to take control of
            a company's assets after a default and sell what's needed to
            recover the debt. The notice names the <em>appointing party</em>
            (who called in the loan) and the receiver (who executes it).</li>
            <li><strong>Examinership.</strong> A court-supervised rescue for
            an insolvent but viable company. An examiner is appointed for up
            to 100 days to negotiate a restructuring scheme with creditors
            while trading continues.</li>
            <li><strong>SCARP.</strong> A faster, out-of-court rescue route
            for small and micro companies introduced in 2021. Led by a process
            advisor rather than a court-appointed examiner.</li>
            <li><strong>Liquidation.</strong> The formal winding up of a
            company. <em>Solvent</em> (Members' Voluntary) liquidations close
            companies that can pay their debts in full; creditors' or
            court-ordered liquidations handle insolvent ones.</li>
            <li><strong>ICAV strike-offs.</strong> Closure notices for Irish
            Collective Asset-management Vehicles — corporate fund structures
            used by international asset managers based in Ireland.</li>
        </ol>
        <p>The receivership notices carry a hidden-in-plain-sight pattern. Many
        appointing parties are <strong>Special Purpose Vehicles</strong> with
        Irish brand names (<em>Promontoria, Beltany, Ennis, Pentire</em>) that
        hold loan books bought by international funds — <em>Cerberus, Goldman
        Sachs, Cabot</em> — from Irish banks after the 2010 banking crisis.
        Translating the SPV brand back to the parent fund shows who has
        actually been calling in Irish loans, and at what scale.</p>
        <p><strong>Personal insolvency notices (named individual bankruptcies)
        are excluded from this site by editorial policy.</strong> The corporate
        register is public-interest data about firms; personal bankruptcy
        listings are about private citizens and are not republished here.</p>
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
    # h3: nests under the "In depth" <h2> so screen readers get h1 → h2 → h3.
    return (
        '<section class="dt-explainer">'
        f'<h3 class="dt-explainer-title">{_h(title)}</h3>'
        f'<div class="dt-explainer-body">{body_html}</div>'
        "</section>"
    )


@page_error_boundary
def glossary_page() -> None:
    inject_css()
    # Sidebar→filter-bar migration: the sidebar was header-only; identity is
    # carried by the top-nav tab + the main hero below.
    hide_sidebar()

    hero_banner(
        kicker="REFERENCE",
        title="Glossary",
        dek=(
            "Plain-language definitions for every acronym used in Dáil Tracker. "
            "Citizen-facing: written without parliamentary jargon."
        ),
    )

    # Client-side filter. Pure presentation over the static dict — no data
    # access, so no logic-firewall concern. Lets a citizen jump straight to
    # "TAA" without scrolling the whole list on mobile.
    query = (
        st.text_input(
            "Search the glossary",
            key="glossary_search",
            placeholder="Search a term or definition — e.g. TAA, receivership, DPO",
        )
        .strip()
        .lower()
    )

    if query:
        terms = [(k, v) for k, v in GLOSSARY_TERMS.items() if search_matches(query, k, v)]
    else:
        terms = list(GLOSSARY_TERMS.items())

    st.html('<h2 class="section-heading">Terms &amp; acronyms</h2>')
    if terms:
        body = "".join(_render_term_block(k, v) for k, v in terms)
        st.html(f'<dl class="dt-glossary-list">{body}</dl>')
    else:
        st.caption(f"No term matches “{query}”. Try a shorter search, or suggest it on GitHub.")

    # The long-form explainers are reference reading; hide them while the
    # reader is searching for a specific term to keep the result in focus.
    if not query:
        st.html('<h2 class="section-heading">In depth</h2>')
        explainers_html = "".join(_render_explainer_block(title, body_html) for title, body_html in EXPLAINERS)
        st.html(explainers_html)

    st.caption(
        "Source: Houses of the Oireachtas, lobbying.ie, and the Standards "
        "in Public Office Commission. Suggest a missing term by opening an "
        "issue on GitHub."
    )
