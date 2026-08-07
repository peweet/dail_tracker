# Dublin City planning PDF link and NEC viewer route

**Verified 2026-08-07.** The stable public source for the PDF pulled in the bounded DCC
pilot is the Dublin City Council Agile application page for planning reference `4159/23`:

`https://planning.agileapplications.ie/dublincity/application-details/?ref=4159/23`

There is no stable direct Agile PDF URL to preserve. On the application page, expand
**Documents**, follow the visible `RunThirdPartySearch` link into DCC's NEC viewer, and use the
matching row's `.viewDocument` control. That control generates a browser download. The adapter
must not guess or persist an internal document URL.

## What the scratch pull actually retrieved

The 2026-08-06 scratch artifacts were:

- `C:\tmp\dcc_solar_technical_pilot_5.parquet`
- `C:\tmp\dcc_solar_technical_pdf_pull_2.parquet`
- `C:\tmp\pull_dcc_first_pdf_samples.py`

The completed result recorded application `4159/23`, an 84,963-byte payload with a verified
`%PDF` signature, and no retrieval error. The script passed the catch-all title pattern `.` and
labelled its selection `first_public_file_row_unclassified`; it did **not** select a solar or
technical document by title.

A live logged-out listing check on 2026-08-07 showed that the first eligible row was:

`Final Grant Notice | 18/10/2023 | Final Grant Notices`

Therefore the retrieved PDF was evidence that the DCC public-file delivery route worked for a
solar-screened application. It was not evidence that a solar technical report had been found.

## Repository implementation and guardrails

- `planning/product/core/decision_docs.py:_agile_source_url()` constructs the stable application
  page.
- `_dcc_nec_list()` obtains the public-file row listing through the NEC viewer.
- `_dcc_nec_document_pdf()` follows the visible logged-out route, validates `%PDF`, and deletes
  the browser's temporary download.
- Preserve the application-page URL plus the selected row title/date/category as provenance.
- Select by an explicit document-title rule before making any claim about document content.
- A successful PDF signature proves delivery and file type only; it does not prove that the
  document is relevant, technical, solar-related, or evidentially sufficient.
