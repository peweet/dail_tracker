"""IPAS / international-protection accommodation — source config.

Dependency-free declaration of the asylum-accommodation corpus so the source
registry (`tools/build_source_registry.py`) can see it, and so `source_health.json`
and `freshness.json` will notice when an upstream document goes stale.

WHY THIS FILE EXISTS
--------------------
`build_source_registry.py` generates the registry by reading a fixed set of code
configs. A source not declared in one of them is INVISIBLE to health/freshness —
so the corpus would silently rot: HIQA would publish new inspection reports and
IPAS would publish new weekly statistics, and nothing would tell us.

⚠️ NOT CLOUD-SAFE. `assets.gov.ie` (gov.ie's CDN, which serves the IPAS weekly
statistics, the Accommodation Strategy, the National Standards and the IGEES paper)
403s any DATACENTER IP, and `hiqa.ie` is in the same family. These sources must be
refreshed on the LOCAL/EDGE lane, never GitHub Actions (see doc/HYBRID_REFRESH_PLAN.md).
A browser User-Agent + gov.ie Referer clears the 403 from a residential IP, but ~15
rapid requests trigger a 405 — pace requests at >= 5s.

Extractors currently live in `pipeline_sandbox/new_sources/` (hand-run, NOT in
pipeline.py), so `parser_wired=False` and `status="sandbox"`. Promote by moving the
extractor into `extractors/` and flipping those flags.
"""

from __future__ import annotations

# Keys mirror what adapt_ipas() in tools/build_source_registry.py expects.
# stale_after_days is set from the UPSTREAM CADENCE, not from our run frequency:
# a weekly report that is 21 days old means the source (or our poller) has stopped.
IPAS_SOURCES: list[dict] = [
    {
        "id": "hiqa_inspection_reports",
        "name": "HIQA — IPAS centre inspection reports",
        "check_type": "index_poll",
        "listing_url": "https://www.hiqa.ie/reports-and-publications/inspection-reports",
        "grain": "inspection_report_pdf",
        "cadence": "rolling (new reports published continuously)",
        "stale_after_days": 90,
        "cloud_safe": False,
        "caveat": (
            "hiqa.ie rate-limits: sends Retry-After: 0 (floor it to your own backoff) "
            "and throttles bursts. Individual reports NAME THE PROVIDER — the only source "
            "that does. Provider names inherit the accommodation-providers public_display gate."
        ),
    },
    {
        "id": "ipas_weekly_stats",
        "name": "IPAS weekly accommodation & arrivals statistics",
        "check_type": "index_poll",
        "listing_url": "https://www.gov.ie/en/collection/ipas-statistics/",
        "grain": "weekly_statistics_pdf",
        "cadence": "weekly",
        "stale_after_days": 21,  # weekly upstream; 3 missed weeks = something broke
        "cloud_safe": False,
        "caveat": (
            "assets.gov.ie WAF: browser UA + gov.ie Referer required; datacenter IPs 403. "
            "The per-LOCAL-AUTHORITY table is the county-map source (sums exactly to its own "
            "Grand Total). NOTE: the report NEVER publishes the unaccommodated count."
        ),
    },
    {
        "id": "cag_reports_index",
        "name": "C&AG publications index (RoAPS chapters, special reports, appropriation accounts)",
        "check_type": "index_poll",
        "listing_url": "https://www.audit.gov.ie/en/find-report/publications/",
        "grain": "audit_report_pdf",
        "cadence": "annual (RoAPS) + ad-hoc special reports",
        "stale_after_days": 400,  # RoAPS is annual; >13 months means we missed one
        "cloud_safe": True,
        "caveat": "CC-BY-4.0. No RSS — poll the index for new entries.",
    },
    {
        "id": "igees_ipas_paper",
        "name": "IGEES — Managing IPAS Expenditure Pressures (analytical paper)",
        "check_type": "fixed_file",
        "listing_url": "https://assets.gov.ie/static/documents/IPAS_Analytical_Paper_03062025.pdf",
        "grain": "analytical_paper_pdf",
        "cadence": "episodic",
        "stale_after_days": None,  # a one-off paper; age is not a fault
        "cloud_safe": False,
        "caveat": "Origin of the EUR 92/night private vs EUR 34/night State-owned figures.",
    },
    {
        "id": "ipas_national_standards",
        "name": "National Standards for accommodation offered to people in the protection process",
        "check_type": "fixed_file",
        "listing_url": "https://assets.gov.ie/static/documents/national-standards.pdf",
        "grain": "standards_document",
        "cadence": "rare (2021 edition current)",
        "stale_after_days": None,
        "cloud_safe": False,
        "caveat": (
            "The 10 themes / 40 standards HIQA judges against. Per SI 649/2023 these are "
            "merely 'published on a website maintained by the Minister' — changeable without "
            "an SI, so a silent revision IS possible. Hash-watch it."
        ),
    },
    {
        "id": "ipas_accommodation_strategy",
        "name": "Comprehensive Accommodation Strategy for International Protection Applicants",
        "check_type": "fixed_file",
        "listing_url": (
            "https://assets.gov.ie/static/documents/"
            "comprehensive-accommodation-strategy-for-international-protection-applicants.pdf"
        ),
        "grain": "government_strategy",
        "cadence": "episodic",
        "stale_after_days": None,
        "cloud_safe": False,
        "caveat": "Contains the State's own admission that it cannot meet its Reg 4 obligation.",
    },
]
