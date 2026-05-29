# Company & register notices (incl. ICAV) — feature idea

Status: PARKED — low priority, deprioritised below the Public Appointments page
Drafted: 2026-05-29

## The idea

A browsable page over the non-SI Iris Oifigiúil corporate/register notices:
corporate insolvency (~31.7k), corporate notices (~3.5k), corporate rescue /
examinership (~239), and ICAV voluntary strike-offs (~654). Faceted by year and
notice type, searchable by company name, each card linking to the Iris source.

## Honest civic-value assessment (why it's parked)

This is a **business gazette**, not accountability data. It tells you which
companies are being wound up, put into receivership/examinership, or struck off
a register. The audience is fund administrators, insolvency practitioners, and
financial/legal researchers — not a citizen tracking their TD. It cuts against
the app's theyworkforyou spirit and primary-view-simplicity principle.

**ICAV specifically has near-zero value here.** A strike-off notice means a fund
is closing; the fund names are opaque and there is no politician/public-money
thread. The genuinely interesting ICAV story — funds as tax-efficient vehicles
for vulture funds holding Irish distressed mortgages/property — is about
*ownership and tax*, and **none of that is in the strike-off dataset**.
Surfacing strike-offs would imply a significance the data doesn't carry.

## Hard constraints if ever built

- **Personal insolvency is EXCLUDED** — never display individual bankruptcy
  notices (named private citizens + home addresses). See the privacy rule in
  memory. Exclude the whole `bankruptcy` category AND filter bankrupt-wording
  rows leaking into corporate buckets (~310 rows) at the view level.
- **Per-row name quality is capped by record-splitting** — ~96% of the
  remaining `entity_name` gaps are split fragments where the company name lives
  in an adjacent record. A usable page would have to group/dedupe by company and
  show the primary notice, not raw rows.
- ICAV would be a minor sub-filter at most, never a feature in its own right.

## What's already done (2026-05-29 pipeline work)

- Classification cleaned: receiver/examiner reflow, ICAV greediness guard,
  entity_name extractor rewritten (statute citations + bare forms rejected,
  "IN THE MATTER OF" primary extraction). Suspect-name rate roughly halved.
- These were worth doing regardless — the main benefit was removing
  contamination from `public_appointment` and `statutory_instrument` (the
  categories that matter), not enabling this page.

## Recommendation

Build the **Public Appointments** page first (on-mission). Revisit this only if
there's a concrete user/journalist demand for a company-insolvency browser, and
even then treat it as a secondary utility, not a headline feature.
