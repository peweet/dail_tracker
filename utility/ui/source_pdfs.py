"""
Source PDF URL registry — every bronze-layer PDF served from data.oireachtas.ie.

These are the canonical source documents that feed the pipeline.
Use render_pdf_source_links() to surface them in any provenance section.
"""
from __future__ import annotations
import streamlit as st

_INTERESTS = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests"
_PSA       = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"
_OTHER     = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other"
_TAA       = "https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa"

# ── Interests ─────────────────────────────────────────────────────────────────
# Key: (house_lower, declaration_year)  Value: canonical oireachtas.ie PDF URL
INTERESTS: dict[tuple[str, int], str] = {
    ("dail",   2020): f"{_INTERESTS}/dail/2021/2021-02-25_register-of-members-interests-dail-eireann_en.pdf",
    ("dail",   2021): f"{_INTERESTS}/dail/2022/2022-02-16_register-of-members-interests-dail-eireann_en.pdf",
    ("dail",   2022): f"{_INTERESTS}/dail/2023/2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf",
    ("dail",   2023): f"{_INTERESTS}/dail/2024/2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf",
    ("dail",   2024): f"{_INTERESTS}/dail/2025/2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf",
    ("dail",   2025): f"{_INTERESTS}/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf",
    ("seanad", 2020): f"{_INTERESTS}/seanad/2021/2021-03-16_register-of-members-interests-seanad-eireann_en.pdf",
    ("seanad", 2021): f"{_INTERESTS}/seanad/2022/2022-02-25_register-of-members-interests-seanad-eireann_en.pdf",
    ("seanad", 2022): f"{_INTERESTS}/seanad/2023/2023-02-24_register-of-members-interests-seanad-eireann_en.pdf",
    ("seanad", 2023): f"{_INTERESTS}/seanad/2024/2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf",
    ("seanad", 2024): f"{_INTERESTS}/seanad/2025/2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf",
    ("seanad", 2025): f"{_INTERESTS}/seanad/2026/2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf",
}

# ── Attendance ────────────────────────────────────────────────────────────────
# Each PDF covers a specific date range (not a clean calendar year).
ATTENDANCE: list[tuple[str, str]] = [
    ("1 Jan 2026 – 28 Feb 2026",  f"{_TAA}/2026/2026-04-02_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-28-february-2026_en.pdf"),
    ("1 Jan 2026 – 31 Jan 2026",  f"{_TAA}/2026/2026-03-06_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-31-january-2026_en.pdf"),
    ("1 Feb 2025 – 30 Dec 2025",  f"{_TAA}/2026/2026-02-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-february-2025-to-30-december-2025_en.pdf"),
    ("1 Jan 2025 – 31 Jan 2025",  f"{_TAA}/2025/2025-04-09_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-31-january-2025_en.pdf"),
    ("29 Nov 2024 – 31 Dec 2024", f"{_TAA}/2025/2025-02-28_deputies-verification-of-attendance-for-the-payment-of-taa-29-november-2024-to-31-december-2024_en.pdf"),
    ("1 Jan 2024 – 8 Nov 2024",   f"{_TAA}/2025/2025-02-17_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2024-to-08-november-2024_en.pdf"),
    ("1 Jan 2023 – 31 Dec 2023",  f"{_TAA}/2024/2024-02-01_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2023-to-31-december-2023_en.pdf"),
]

# ── Payments ──────────────────────────────────────────────────────────────────
# Reverse-chronological. (label, url)
PAYMENTS: list[tuple[str, str]] = [
    ("Feb 2026", f"{_PSA}/2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"),
    ("Jan 2026", f"{_PSA}/2026/2026-03-06_parliamentary-standard-allowance-payments-to-deputies-for-january-2026_en.pdf"),
    ("Dec 2025", f"{_PSA}/2026/2026-02-16_parliamentary-standard-allowance-payments-to-deputies-for-december-2025_en.pdf"),
    ("Nov 2025", "https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul/2026/2026-01-16_parliamentary-standard-allowance-payments-to-deputies-for-november-2025_en.pdf"),
    ("Sep 2025", f"{_PSA}/2025/2025-11-18_parliamentary-standard-allowance-payments-to-deputies-for-september-2025_en.pdf"),
    ("Aug 2025", f"{_PSA}/2025/2025-10-06_parliamentary-standard-allowance-payments-to-deputies-for-august-2025_en.pdf"),
    ("Jul 2025", f"{_PSA}/2025/2025-09-03_parliamentary-standard-allowance-payments-to-deputies-for-july-2025_en.pdf"),
    ("Jun 2025", f"{_PSA}/2025/2025-08-15_parliamentary-standard-allowance-payments-to-deputies-for-june-2025_en.pdf"),
    ("May 2025", f"{_PSA}/2025/2025-07-03_parliamentary-standard-allowance-payments-to-deputies-for-may-2025_en.pdf"),
    ("Apr 2025", f"{_PSA}/2025/2025-06-10_parliamentary-standard-allowance-payments-to-deputies-for-april-2025_en.pdf"),
    ("Feb 2025", f"{_PSA}/2025/2025-04-22_parliamentary-standard-allowance-payments-to-deputies-for-february-2025_en.pdf"),
    ("Jan 2025", f"{_PSA}/2025/2025-04-09_parliamentary-standard-allowance-payments-to-deputies-for-january-2025_en.pdf"),
    ("Dec 2024", f"{_PSA}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-1-31-december-2024_en.pdf"),
    ("29–30 Nov 2024", f"{_PSA}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-29-30-november-2024_en.pdf"),
    ("1–8 Nov 2024",   f"{_PSA}/2025/2025-02-17_parliamentary-standard-allowance-payments-to-deputies-for-1-8-november-2024_en.pdf"),
    ("Oct 2024", f"{_PSA}/2024/2024-12-16_parliamentary-standard-allowance-payments-to-deputies-for-october-2024_en.pdf"),
    ("Sep 2024", f"{_PSA}/2024/2024-11-01_parliamentary-standard-allowance-payments-to-deputies-for-september-2024_en.pdf"),
    ("Aug 2024", f"{_PSA}/2024/2024-10-11_parliamentary-standard-allowance-payments-to-deputies-for-august-2024_en.pdf"),
    ("Jul 2024", f"{_PSA}/2024/2024-09-04_parliamentary-standard-allowance-payments-to-deputies-for-july-2024_en.pdf"),
    ("Jun 2024", f"{_PSA}/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"),
    ("Apr 2024", f"{_PSA}/2024/2024-06-02_parliamentary-standard-allowance-payments-to-deputies-for-april-2024_en.pdf"),
    ("Mar 2024", f"{_PSA}/2024/2024-05-02_parliamentary-standard-allowance-payments-to-deputies-for-march-2024_en.pdf"),
    ("Feb 2024", f"{_PSA}/2024/2024-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2024_en.pdf"),
    ("Jan 2024", f"{_PSA}/2024/2024-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2024_en.pdf"),
    ("Dec 2023", f"{_PSA}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-december-2023_en.pdf"),
    ("Nov 2023", f"{_PSA}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-november-2023_en.pdf"),
    ("Oct 2023", f"{_PSA}/2023/2023-12-01_parliamentary-standard-allowance-payments-to-deputies-for-october-2023_en.pdf"),
    ("Sep 2023", f"{_PSA}/2023/2023-10-27_parliamentary-standard-allowance-payments-to-deputies-for-september-2023_en.pdf"),
    ("Aug 2023", f"{_PSA}/2023/2023-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2023_en.pdf"),
    ("Jul 2023", f"{_PSA}/2023/2023-09-01_parliamentary-standard-allowance-payments-to-deputies-for-july-2023_en.pdf"),
    ("Jun 2023", f"{_PSA}/2023/2023-08-01_parliamentary-standard-allowance-payments-to-deputies-for-june-2023_en.pdf"),
    ("May 2023", f"{_PSA}/2023/2023-07-01_parliamentary-standard-allowance-payments-to-deputies-for-may-2023_en.pdf"),
    ("Apr 2023", f"{_OTHER}/2023/2023-06-01_parliamentary-standard-allowance-payments-to-deputies-for-april-2023_en.pdf"),
    ("Mar 2023", f"{_OTHER}/2023/2023-05-01_parliamentary-standard-allowance-payments-to-deputies-for-march-2023_en.pdf"),
    ("Feb 2023", f"{_OTHER}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2023_en.pdf"),
    ("Jan 2023", f"{_OTHER}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2023_en.pdf"),
    ("Dec 2022", f"{_OTHER}/2023/2023-03-13_parliamentary-standard-allowance-payments-to-deputies-for-december-2022_en.pdf"),
    ("Nov 2022", f"{_OTHER}/2023/2023-01-03_parliamentary-standard-allowance-payments-to-deputies-for-november-2022_en.pdf"),
    ("Oct 2022", f"{_PSA}/2022/2022-12-06_parliamentary-standard-allowance-payments-to-deputies-for-october-2022_en.pdf"),
    ("Sep 2022", f"{_PSA}/2022/2022-11-14_parliamentary-standard-allowance-payments-to-deputies-for-september-2022_en.pdf"),
    ("Aug 2022", f"{_PSA}/2022/2022-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2022_en.pdf"),
    ("Jul 2022", f"{_PSA}/2022/2022-09-20_parliamentary-standard-allowance-payments-to-deputies-for-july-2022_en.pdf"),
    ("Jun 2022", f"{_PSA}/2022/2022-08-02_parliamentary-standard-allowance-payments-to-deputies-for-june-2022_en.pdf"),
    ("May 2022", f"{_PSA}/2022/2022-07-11_parliamentary-standard-allowance-payments-to-deputies-for-may-2022_en.pdf"),
    ("Apr 2022", f"{_PSA}/2022/2022-06-07_parliamentary-standard-allowance-payments-to-deputies-for-april-2022_en.pdf"),
    ("Mar 2022", f"{_PSA}/2022/2022-05-25_parliamentary-standard-allowance-payments-to-deputies-for-march-2022_en.pdf"),
    ("Feb 2022", f"{_PSA}/2022/2022-04-19_parliamentary-standard-allowance-payments-to-deputies-for-february-2022_en.pdf"),
    ("Jan 2022", f"{_PSA}/2022/2022-03-16_parliamentary-standard-allowance-payments-to-deputies-for-january-2022_en.pdf"),
    ("Dec 2021", f"{_PSA}/2022/2022-03-10_parliamentary-standard-allowance-payments-to-deputies-for-december-2021_en.pdf"),
    ("Nov 2021", f"{_PSA}/2022/2022-01-13_parliamentary-standard-allowance-payments-to-deputies-for-november-2021_en.pdf"),
    ("Oct 2021", f"{_PSA}/2021/2021-12-22_parliamentary-standard-allowance-payments-to-deputies-for-october-2021_en.pdf"),
    ("Sep 2021", f"{_PSA}/2021/2021-12-10_parliamentary-standard-allowance-payments-to-deputies-for-september-2021_en.pdf"),
    ("Aug 2021", f"{_PSA}/2021/2021-10-12_parliamentary-standard-allowance-payments-to-deputies-for-august-2021_en.pdf"),
    ("Jul 2021", f"{_PSA}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-july-2021_en.pdf"),
    ("Jun 2021", f"{_PSA}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-june-2021_en.pdf"),
    ("May 2021", f"{_PSA}/2021/2021-07-12_parliamentary-standard-allowance-payments-to-deputies-for-may-2021_en.pdf"),
    ("Apr 2021", f"{_PSA}/2021/2021-06-30_parliamentary-standard-allowance-payments-to-deputies-for-april-2021_en.pdf"),
    ("Mar 2021", f"{_PSA}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-march-2021_en.pdf"),
    ("Feb 2021", f"{_PSA}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-february-2021_en.pdf"),
    ("Jan 2021", f"{_PSA}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-january-2021_en.pdf"),
    ("Dec 2020", f"{_PSA}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-december-2020_en.pdf"),
    ("Nov 2020", f"{_PSA}/2021/2021-02-12_parliamentary-standard-allowance-payments-to-deputies-for-november-2020_en.pdf"),
    ("Oct 2020", f"{_PSA}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-october-2020_en.pdf"),
    ("Sep 2020", f"{_PSA}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-september-2020_en.pdf"),
    ("Aug 2020", f"{_PSA}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-august-2020_en.pdf"),
    ("Jul 2020", f"{_PSA}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-july-2020_en.pdf"),
    ("Jun 2020", f"{_PSA}/2020/2020-08-06_parliamentary-standard-allowance-payments-to-deputies-for-june-2020_en.pdf"),
    ("May 2020", f"{_PSA}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-may-2020_en.pdf"),
    ("Apr 2020", f"{_PSA}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-april-2020_en.pdf"),
    ("Mar 2020", f"{_PSA}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.pdf"),
    ("Feb 2020", f"{_PSA}/2020/2020-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2020_en.pdf"),
    ("Jan 2020", f"{_PSA}/2020/2020-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2020_en.pdf"),
]


def render_pdf_source_links(links: list[tuple[str, str]]) -> None:
    """Render a compact list of labelled PDF source links."""
    rows = "".join(
        f'<div style="padding:0.28rem 0;border-bottom:1px solid var(--border)">'
        f'<a class="leg-source-link" href="{url}" target="_blank" rel="noopener">'
        f'↗ {label}</a></div>'
        for label, url in links
    )
    st.markdown(
        f'<div style="font-size:0.85rem;line-height:1.5">{rows}</div>',
        unsafe_allow_html=True,
    )


def provenance_expander(
    sections: list[str],
    source_caption: str = "",
    pdf_links: list[tuple[str, str]] | None = None,
) -> None:
    """Standard 'About & data provenance' expander used on all pages.

    sections       — markdown strings; a divider is inserted between each
    source_caption — public-facing data credit line (no internal paths)
    pdf_links      — optional list of (label, url) passed to render_pdf_source_links
    """
    with st.expander("About & data provenance", expanded=False):
        for i, section in enumerate(sections):
            st.markdown(section)
            if i < len(sections) - 1:
                st.divider()
        if source_caption or pdf_links:
            st.divider()
        if source_caption:
            st.caption(source_caption)
        if pdf_links:
            n = len(pdf_links)
            st.markdown(f"**Source documents** — {n} document{'s' if n != 1 else ''}")
            render_pdf_source_links(pdf_links)


def interests_pdf_url(house: str, year: int) -> str | None:
    """Return the canonical PDF URL for a given house + declaration year, or None."""
    return INTERESTS.get((house.lower(), year))
