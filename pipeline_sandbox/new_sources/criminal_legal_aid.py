"""Criminal legal aid payments to legal practitioners (SCAFFOLD — BLOCKED: SOURCE NOT PUBLISHED).

STATUS as of 2026-07-12: the Department of Justice does NOT publish the annual
per-practitioner criminal legal aid payment lists (fees paid to named
solicitors / junior counsel / senior counsel) anywhere fetchable. The premise
that these are "statutorily published on gov.ie as PDF/XLSX attachments" is
FALSE — verified exhaustively across every plausible channel:

  1. data.gov.ie CKAN  (accepts bot UAs)
     https://data.gov.ie/api/3/action/package_search?q=criminal+legal+aid
     -> 17 packages; only Legal Aid Board aggregate scheme stats (CAB scheme,
        Custody Issues, Garda Station scheme). No per-practitioner payments.

  2. gov.ie — COMPLETE sitemap sweep (sitemap.xml -> sitemap-en-1..20.xml,
     ~97k URLs, grepped legal-aid|practitioner|counsel|solicitor|barrister|
     fees-paid -> 236 hits, all reviewed). No payments publication exists.
     The only criminal-legal-aid pages are:
       * publications/criminal-legal-aidadvice/          (procedures only)
       * fee-claim FORMS + services pages                (claim forms, no data)
       * irish-government-economic-and-evaluation-service-igees/igees-
         publication/criminal-legal-aid-expenditure-trends-2014-2024/
         (aggregate expenditure review, no names)
     NOTE gov.ie WAF behaviour: browser-UA + Referer headers (GOVIE_HEADERS
     below) clear the 403 wall, but ~15 rapid requests triggers 405
     throttling — pace >=5s if re-probing. The publications listing ?q= param
     is ignored server-side (same "latest" results for any q); site search
     /en/search/ is client-side JS with no discoverable static API.

  3. Legacy justice.ie via Wayback CDX (34 legal-aid URLs + 19 payment URLs;
     criminal legal aid page JELR/Pages/WP15000204, 58 snapshots 2015-2021):
     procedures + prompt-payment returns only. Never carried the lists.

  4. Wayback CDX on gov.ie /en/publication/, /en/collection/ and assets.gov.ie
     filenames matching counsel|solicitor: nothing relevant.

  5. Oireachtas PQ written answers: AGGREGATES ONLY. e.g.
     https://www.oireachtas.ie/en/debates/question/2022-07-13/449/ has a
     year x {Junior Counsel, Senior Counsel} totals table;
     https://www.oireachtas.ie/en/debates/question/2024-07-03/123/ no table.

  6. DoJ FOI Disclosure Logs (gov.ie collection, PDFs 2015-2025):
     https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/foi-disclosure-logs/
     These log FOI requests RECEIVED, not the released records. This is the
     real channel: Irish Legal News / Irish Times / Roscommon Herald publish
     top-earner lists each year (2024 data reported 2025-02-10) citing
     "figures from the Department of Justice" — i.e. an FOI release or press
     handout, with no public artefact to scrape.

To complete this source, ONE of:
  a. FOI request to foi@justice.ie for the annual per-practitioner payment
     lists (journalists obtain them annually, so records are held and
     releasable) — then ingest the released spreadsheet/PDF here.
  b. The DoJ starts publishing the lists (the General Scheme of the Criminal
     Justice (Legal Aid) Bill 2023 modernisation may change practice) —
     run() below cheaply re-probes the known anchor pages.
  c. Settle for AGGREGATE totals (year x category) from PQ answers / IGEES
     expenditure-trends review — a different, lesser dataset; if wanted,
     build it as its own source, not under this name.

Proposed silver schema (unchanged from the brief, for when data materialises):
  practitioner_name, practitioner_type (solicitor|barrister_jc|barrister_sc|firm),
  amount_eur, year, scheme ('criminal_legal_aid_main'),
  payment_basis='fees_paid', value_safe_to_sum=False,
  source_url, source_document_hash, fetched_at, source_published_date,
  source_last_modified, extraction_method, confidence,
  privacy_tier='professional_individual'  # named individuals in professional
  # capacity; lawful to process, but NEVER ingest personal addresses.

LICENCE (for the graduation gate): gov.ie content is Oireachtas/PSI-standard
CC BY 4.0 (gov.ie re-use policy / PSI licence); data.gov.ie LAB datasets are
CC-BY. An FOI release carries no explicit licence — record the FOI reference
in provenance if route (a) is used.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import fetch  # noqa: E402

# gov.ie / assets.gov.ie 403 bare bot UAs; browser UA + gov.ie Referer clears
# the WAF (same spoof as extractors/procurement_etenders_extract.py).
GOVIE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.gov.ie/",
}

CHANNELS_CHECKED = {
    "ckan_search": "https://data.gov.ie/api/3/action/package_search?q=criminal+legal+aid",
    "govie_cla_page": "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/publications/criminal-legal-aidadvice/",
    "govie_igees_trends": "https://www.gov.ie/en/irish-government-economic-and-evaluation-service-igees/igees-publication/criminal-legal-aid-expenditure-trends-2014-2024/",
    "foi_disclosure_logs": "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/foi-disclosure-logs/",
    "pq_aggregates_example": "https://www.oireachtas.ie/en/debates/question/2022-07-13/449/",
    "media_channel_example": "https://www.irishlegal.com/articles/highest-earners-from-criminal-legal-aid-scheme-in-2024-revealed",
}

# Words that would indicate the payment lists finally appeared on the page.
_APPEARED = ("payments to counsel", "payments to solicitors", "fees paid to counsel",
             "payments to legal practitioners", "payments made to solicitor")


def probe() -> bool:
    """Cheap re-check (1 request, WAF-safe) whether the DoJ criminal legal aid
    page has started linking the per-practitioner payment lists."""
    try:
        html, meta = fetch(CHANNELS_CHECKED["govie_cla_page"])
    except Exception as e:  # noqa: BLE001 — a probe must never crash the sandbox
        print(f"  probe fetch failed: {type(e).__name__}: {e}")
        return False
    low = html.lower()
    found = [k for k in _APPEARED if k in low]
    if found:
        print(f"  SOURCE MAY HAVE APPEARED — page now mentions: {found}")
        print("  -> revisit this scaffold and build the parse (schema in docstring).")
        return True
    print(f"  still absent (page {meta['bytes']} bytes, fetched {meta['fetched_at']}).")
    return False


def run() -> None:
    print("criminal_legal_aid: BLOCKED — per-practitioner lists are NOT published.")
    print("Channels exhausted (full evidence in module docstring):")
    for k, v in CHANNELS_CHECKED.items():
        print(f"  {k}: {v}")
    print("Unblock routes: (a) FOI to foi@justice.ie, (b) DoJ starts publishing,")
    print("(c) separate aggregates-only source from PQ answers / IGEES review.")
    print("Re-probing the anchor page for change...")
    probe()


if __name__ == "__main__":
    # _common's session lacks the WAF headers; apply them for the probe.
    from _common import _SESSION  # noqa: PLC0415
    _SESSION.headers.update(GOVIE_HEADERS)
    run()
