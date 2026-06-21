# TED data ingestion links

Generated: **2026-06-07**

Scope: official TED / Publications Office / EU data links for ingesting or discovering **Tenders Electronic Daily (TED)** procurement notice data.

Notes:

- TED bulk package URLs are documented URL patterns. Some endpoints may return archives directly in a normal HTTP client but show anti-bot or JavaScript-gating pages in a browser.
- For **historical batch ingest**, start with the monthly XML package links below.
- For **incremental daily ingest**, use the release calendar CSV/XLS/PDF links to get the OJ S issue number, then construct the daily package URL.
- For **query-based ingest**, use the TED Search API and TED Open Data Service.
- For **field/schema-aware ingest**, use the eForms SDK, field list, eProcurement Ontology, and TED schema archive links.

## Official source pages

| Purpose | URL |
|---|---|
| TED home | https://ted.europa.eu/en/ |
| Developers' corner for reusers | https://ted.europa.eu/en/simap/developers-corner-for-reusers |
| XML bulk download page | https://ted.europa.eu/en/simap/xml-bulk-download |
| Release calendar page | https://ted.europa.eu/en/release-calendar |
| TED Developer Docs home | https://docs.ted.europa.eu/home/index.html |
| TED Developer Portal | https://developer.ted.europa.eu/ |
| TED Help | https://ted.europa.eu/en/help |
| TED contact/helpdesk | https://ted.europa.eu/en/contact |

## Search and browse entry points

| Purpose | URL |
|---|---|
| TED advanced search | https://ted.europa.eu/en/advanced-search |
| TED expert search | https://ted.europa.eu/en/expert-search |
| Browse by business opportunity | https://ted.europa.eu/en/browse-by-business-opportunity |
| Browse by business sector / CPV | https://ted.europa.eu/en/browse-by-business-sector |
| Browse by place of performance | https://ted.europa.eu/en/browse-by-place-of-performance |
| Latest OJ S issue / search result pattern | https://ted.europa.eu/en/search/result?scope=ALL |
| Search results with an OJ S number, example S107/2026 | https://ted.europa.eu/search/result?ojs-number=107%2F2026&scope=ALL |

## TED Search API

| Purpose | URL / endpoint |
|---|---|
| TED API documentation | https://docs.ted.europa.eu/api/latest/index.html |
| TED Search API docs | https://docs.ted.europa.eu/api/latest/search.html |
| Unified Swagger | https://api.ted.europa.eu/swagger |
| Swagger UI | https://api.ted.europa.eu/swagger-ui/index.html |
| Search notices endpoint | `POST https://api.ted.europa.eu/v3/notices/search` |
| API URL pattern | `https://api.ted.europa.eu/{api-version}/{resource}/{action}` |

Minimal ingestion skeleton:

```bash
curl -X POST 'https://api.ted.europa.eu/v3/notices/search' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "publication-date >= 2024-01-01",
    "fields": ["publication-number", "publication-date", "buyer-name", "classification-cpv"],
    "page": 1,
    "limit": 100
  }'
```

Check the Swagger schema before using this skeleton in production because request-body fields and allowed values should be taken from the current API definition.

## Direct notice download URL patterns

Replace `{publication-number}` with values such as `00676595-2024` or `1-2024`, and `{lang}` with an EU language code.

| Format | URL pattern |
|---|---|
| HTML | `https://ted.europa.eu/{lang}/notice/{publication-number}/html` |
| PDF | `https://ted.europa.eu/{lang}/notice/{publication-number}/pdf` |
| Signed PDF | `https://ted.europa.eu/{lang}/notice/{publication-number}/pdfs` |
| XML | `https://ted.europa.eu/en/notice/{publication-number}/xml` |

Language codes supported by TED pages:


| Code | Language | Example notice XML/HTML base |
|---|---|---|
| `bg` | Bulgarian | `https://ted.europa.eu/bg/notice/{{publication-number}}/html` |
| `es` | Spanish | `https://ted.europa.eu/es/notice/{{publication-number}}/html` |
| `cs` | Czech | `https://ted.europa.eu/cs/notice/{{publication-number}}/html` |
| `da` | Danish | `https://ted.europa.eu/da/notice/{{publication-number}}/html` |
| `de` | German | `https://ted.europa.eu/de/notice/{{publication-number}}/html` |
| `et` | Estonian | `https://ted.europa.eu/et/notice/{{publication-number}}/html` |
| `el` | Greek | `https://ted.europa.eu/el/notice/{{publication-number}}/html` |
| `en` | English | `https://ted.europa.eu/en/notice/{{publication-number}}/html` |
| `fr` | French | `https://ted.europa.eu/fr/notice/{{publication-number}}/html` |
| `ga` | Irish | `https://ted.europa.eu/ga/notice/{{publication-number}}/html` |
| `hr` | Croatian | `https://ted.europa.eu/hr/notice/{{publication-number}}/html` |
| `it` | Italian | `https://ted.europa.eu/it/notice/{{publication-number}}/html` |
| `lv` | Latvian | `https://ted.europa.eu/lv/notice/{{publication-number}}/html` |
| `lt` | Lithuanian | `https://ted.europa.eu/lt/notice/{{publication-number}}/html` |
| `hu` | Hungarian | `https://ted.europa.eu/hu/notice/{{publication-number}}/html` |
| `mt` | Maltese | `https://ted.europa.eu/mt/notice/{{publication-number}}/html` |
| `nl` | Dutch | `https://ted.europa.eu/nl/notice/{{publication-number}}/html` |
| `pl` | Polish | `https://ted.europa.eu/pl/notice/{{publication-number}}/html` |
| `pt` | Portuguese | `https://ted.europa.eu/pt/notice/{{publication-number}}/html` |
| `ro` | Romanian | `https://ted.europa.eu/ro/notice/{{publication-number}}/html` |
| `sk` | Slovak | `https://ted.europa.eu/sk/notice/{{publication-number}}/html` |
| `sl` | Slovenian | `https://ted.europa.eu/sl/notice/{{publication-number}}/html` |
| `fi` | Finnish | `https://ted.europa.eu/fi/notice/{{publication-number}}/html` |
| `sv` | Swedish | `https://ted.europa.eu/sv/notice/{{publication-number}}/html` |


## Bulk XML packages

### URL patterns

| Package type | URL pattern | Example |
|---|---|---|
| Daily XML package | `https://ted.europa.eu/packages/daily/{yyyynnnnn}` | https://ted.europa.eu/packages/daily/202400001 |
| Monthly XML package | `https://ted.europa.eu/packages/monthly/{yyyy-n}` | https://ted.europa.eu/packages/monthly/2024-1 |

Daily package key construction:

```python
def ted_daily_package_url(year: int, ojs_number: int) -> str:
    # OJ S issue 1 in 2024 -> 202400001; issue 103 in 2026 -> 202600103
    return f"https://ted.europa.eu/packages/daily/{year}{ojs_number:05d}"
```

Current-page sample daily links visible on the TED XML bulk page on 2026-06-07:

| OJ S | Publication date | URL |
|---|---:|---|
| S103/2026 | 2026-06-01 | https://ted.europa.eu/packages/daily/202600103 |
| S104/2026 | 2026-06-02 | https://ted.europa.eu/packages/daily/202600104 |
| S105/2026 | 2026-06-03 | https://ted.europa.eu/packages/daily/202600105 |
| S106/2026 | 2026-06-04 | https://ted.europa.eu/packages/daily/202600106 |
| S107/2026 | 2026-06-05 | https://ted.europa.eu/packages/daily/202600107 |

### Generated monthly XML package links

Generated from the documented pattern `https://ted.europa.eu/packages/monthly/{yyyy-n}`. The TED XML bulk page showed years 1993–2026 and June 2026 as not yet available at generation time, so this list runs through **2026-05**.


Do **not** enumerate these by hand — they follow the documented pattern
`https://ted.europa.eu/packages/monthly/{yyyy-n}` (n = 1..12, no zero-pad), available
for every month **1993-01 through the last completed month** (June 2026 was not yet
published at last check). Generate the full list with the snippet in *Quick link
generators* below. (A ~400-row hardcoded table was removed 2026-06-21 — it was a stale
snapshot of this exact pattern and drifted out of date every month.)

## Release calendar links

Use release calendars to map publication dates to OJ S issue numbers. Then build daily package links with `https://ted.europa.eu/packages/daily/{yyyynnnnn}`.

| Year | CSV | XLS | PDF |
|---:|---|---|---|
| 1993 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1993 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1993 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1993 |
| 1994 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1994 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1994 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1994 |
| 1995 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1995 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1995 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1995 |
| 1996 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1996 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1996 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1996 |
| 1997 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1997 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1997 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1997 |
| 1998 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1998 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1998 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1998 |
| 1999 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/1999 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/1999 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/1999 |
| 2000 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2000 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2000 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2000 |
| 2001 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2001 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2001 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2001 |
| 2002 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2002 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2002 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2002 |
| 2003 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2003 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2003 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2003 |
| 2004 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2004 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2004 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2004 |
| 2005 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2005 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2005 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2005 |
| 2006 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2006 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2006 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2006 |
| 2007 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2007 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2007 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2007 |
| 2008 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2008 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2008 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2008 |
| 2009 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2009 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2009 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2009 |
| 2010 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2010 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2010 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2010 |
| 2011 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2011 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2011 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2011 |
| 2012 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2012 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2012 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2012 |
| 2013 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2013 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2013 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2013 |
| 2014 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2014 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2014 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2014 |
| 2015 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2015 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2015 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2015 |
| 2016 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2016 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2016 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2016 |
| 2017 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2017 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2017 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2017 |
| 2018 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2018 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2018 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2018 |
| 2019 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2019 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2019 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2019 |
| 2020 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2020 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2020 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2020 |
| 2021 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2021 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2021 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2021 |
| 2022 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2022 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2022 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2022 |
| 2023 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2023 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2023 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2023 |
| 2024 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2024 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2024 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2024 |
| 2025 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2025 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2025 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2025 |
| 2026 | https://ted.europa.eu/en/release-calendar/-/download/file/CSV/2026 | https://ted.europa.eu/en/release-calendar/-/download/file/XLS/2026 | https://ted.europa.eu/en/release-calendar/-/download/file/PDF/2026 |


## TED Open Data / RDF / SPARQL

| Purpose | URL |
|---|---|
| TED Open Data Service | https://data.ted.europa.eu/ |
| TED Open Data docs | https://docs.ted.europa.eu/ODS/latest/index.html |
| Querying TED Open Data | https://docs.ted.europa.eu/ODS/latest/querying/index.html |
| Connect to TED Open Data | https://docs.ted.europa.eu/ODS/latest/connecting/index.html |
| Submit a query in the TED SPARQL endpoint | https://docs.ted.europa.eu/ODS/latest/connecting/sparql.html |
| Connect using Excel | https://docs.ted.europa.eu/ODS/latest/connecting/excel.html |
| Connect using Jupyter Notebook and Python | https://docs.ted.europa.eu/ODS/latest/connecting/python.html |
| TED Open Data Service editor | https://data.ted.europa.eu/ |
| Query Library docs | https://docs.ted.europa.eu/ODS/latest/samples/index.html |
| Current data availability | https://docs.ted.europa.eu/ODS/latest/data_availability.html |
| Known issues and limitations | https://docs.ted.europa.eu/ODS/latest/querying/known_issues.html |
| TED Open Data GitHub discussions | https://github.com/OP-TED/ted-open-data/discussions |
| TED Open Data GitHub repository | https://github.com/OP-TED/ted-open-data |
| TED Open Data Explorer GitHub repository | https://github.com/OP-TED/ted-open-data-explorer |

Practical note: the current TED Open Data Service tells users to compose/run SELECT queries in the web editor and use **Copy endpoint URL** for application ingestion. Keep those copied endpoint URLs with the exact query you are using.

## data.europa.eu dataset records

| Dataset / catalogue item | URL |
|---|---|
| TED public procurement notices / XML dataset record | https://data.europa.eu/data/datasets/ted-1?locale=en |
| TED CSV subset dataset record | https://data.europa.eu/data/datasets/ted-csv?locale=en |
| Public Procurement Data Space page | https://data.europa.eu/en/PPDS |
| data.europa.eu SPARQL endpoint for catalogue metadata, not the TED notices themselves | https://data.europa.eu/data/sparql?locale=en |

## Field lists, schema, SDK, and metadata links

### Search fields and XML schema archive

| Purpose | URL |
|---|---|
| Search fields used on TED | https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html |
| Search fields PDF | https://docs.ted.europa.eu/ODS/latest/reuse/_attachments/List_of_search_fields.pdf |
| Search fields XLSX, check page if direct filename changes | https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html |
| Search fields CSV, check page if direct filename changes | https://docs.ted.europa.eu/ODS/latest/reuse/field-list.html |
| TED XML schema archive | https://docs.ted.europa.eu/ODS/latest/reuse/ftp.html |

### eForms SDK and metadata

| Purpose | URL |
|---|---|
| eForms SDK docs | https://docs.ted.europa.eu/eforms/latest/index.html |
| eForms specification | https://docs.ted.europa.eu/eforms/latest/schema/index.html |
| eForms schema usage / all-in-one | https://docs.ted.europa.eu/eforms/latest/schema/all-in-one.html |
| eForms field metadata docs | https://docs.ted.europa.eu/eforms/latest/fields/index.html |
| eForms codelists | https://docs.ted.europa.eu/eforms/latest/codelists/index.html |
| eForms notice types | https://docs.ted.europa.eu/eforms/latest/notice-types/index.html |
| eForms XML schemas | https://docs.ted.europa.eu/eforms/latest/schemas/index.html |
| eForms Schematron | https://docs.ted.europa.eu/eforms/latest/schematrons/index.html |
| eForms Metadata Reference | https://docs.ted.europa.eu/eforms/latest/reference/index.html |
| Business Terms | https://docs.ted.europa.eu/eforms/latest/reference/business-terms.html |
| Business Rules | https://docs.ted.europa.eu/eforms/latest/reference/business-rules.html |
| Codelists Reference | https://docs.ted.europa.eu/eforms/latest/reference/codelists.html |
| Active SDK versions | https://docs.ted.europa.eu/eforms-common/active-versions/index.html |
| SDK versioning | https://docs.ted.europa.eu/eforms-common/versioning/index.html |
| eForms SDK GitHub | https://github.com/OP-TED/eForms-SDK |
| eForms docs GitHub | https://github.com/OP-TED/eforms-docs |
| eForms SDK Maven Central | https://central.sonatype.com/artifact/eu.europa.ted.eforms/eforms-sdk |
| eForms SDK Explorer | https://docs.ted.europa.eu/eforms-sdk-explorer/ |

Direct CSV metadata attachments from current eForms docs:

| File | URL |
|---|---|
| Nodes | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/nodes.csv |
| Fields | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/fields.csv |
| Notice subtypes | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/notices.csv |
| All business rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/business-rules.csv |
| Forbidden rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/forbidden-rules.csv |
| Mandatory rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/mandatory-rules.csv |
| Codelist rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/codelist-rules.csv |
| Pattern rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/pattern-rules.csv |
| Interval rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/interval-rules.csv |
| Co-constraint rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/coconstraint-rules.csv |
| Repeatability rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/repeatability-rules.csv |
| Changeability rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/changeability-rules.csv |
| Lawfulness rules | https://docs.ted.europa.eu/eforms/latest/reference/_attachments/lawfulness-rules.csv |

## eProcurement Ontology / linked-data model

| Purpose | URL |
|---|---|
| eProcurement Ontology overview | https://docs.ted.europa.eu/epo-home/index.html |
| eProcurement Ontology latest docs | https://docs.ted.europa.eu/EPO/latest/index.html |
| ePO 5.2.0 docs | https://docs.ted.europa.eu/EPO/5.2.0/index.html |
| ePO 5.1.0 docs | https://docs.ted.europa.eu/EPO/5.1.0/index.html |
| ePO 5.0.0 docs | https://docs.ted.europa.eu/EPO/5.0.0/index.html |
| ePO 4.2.0 docs | https://docs.ted.europa.eu/EPO/4.2.0/index.html |
| ePO GitHub, OP-TED | https://github.com/OP-TED/ePO |
| ePO docs GitHub, OP-TED | https://github.com/OP-TED/epo-docs |
| eProcurement ontology GitHub, historical/alternate | https://github.com/eprocurementontology/eprocurementontology |
| EU Vocabularies eProcurement authority tables | https://op.europa.eu/en/web/eu-vocabularies/e-procurement/tables |

## XML-to-RDF mapping resources

| Purpose | URL |
|---|---|
| TED Open Data XML/RDF docs home | https://docs.ted.europa.eu/ODS/latest/index.html |
| Mapping Standard Form XML data to RDF | https://docs.ted.europa.eu/ODS/latest/mapping/index_sf.html |
| Mapping eForms XML data to RDF | https://docs.ted.europa.eu/ODS/latest/mapping_eforms/index.html |
| Reusing TED XML data | https://docs.ted.europa.eu/ODS/latest/reuse/index.html |
| TED XML Data Converter GitHub | https://github.com/OP-TED/ted-xml-data-converter |

## Other TED/SIMAP pages that may be useful in ingestion metadata

| Purpose | URL |
|---|---|
| eForms on TED | https://ted.europa.eu/en/simap/eforms |
| European public procurement | https://ted.europa.eu/en/simap/european-public-procurement |
| Useful links | https://ted.europa.eu/en/simap/useful-links |
| Statistics on TED notices | https://ted.europa.eu/en/simap/statistics-on-ted-notices |
| Dashboard on contracts published on TED | https://ted.europa.eu/en/simap/dashboard-on-contracts-published-on-ted |
| Contracts awarded by EU institutions | https://ted.europa.eu/en/simap/contracts-awarded-by-eu-institutions |
| Sending electronic notices | https://ted.europa.eu/en/simap/sending-electronic-notices |
| List of TED eSenders | https://ted.europa.eu/en/simap/list-of-ted-esenders |
| ESPD | https://ted.europa.eu/en/simap/european-single-procurement-document |
| eProcurement ontology on TED | https://ted.europa.eu/en/simap/eprocurement-ontology |
| News | https://ted.europa.eu/en/news |

## RSS / feed-related page

| Purpose | URL |
|---|---|
| TED RSS feeds page | https://ted.europa.eu/en/simap/rss-feed |

## Suggested ingestion ordering

1. Download monthly XML packages for the backfill period you need.
2. Store raw archive checksums and extraction manifests.
3. Parse notices using the correct schema family: legacy TED XML schema vs eForms.
4. Ingest eForms SDK metadata, fields, codelists, and business rules for schema-aware parsing.
5. Use release calendars to run daily incremental fetches.
6. Use TED Search API only for filtered/query use cases or to reconcile missing notices.
7. Use TED Open Data/SPARQL for RDF/ePO-shaped extraction or semantic joins.

## Quick link generators

```python
def ted_monthly_package_url(year: int, month: int) -> str:
    return f"https://ted.europa.eu/packages/monthly/{year}-{month}"

def ted_daily_package_url(year: int, ojs_number: int) -> str:
    return f"https://ted.europa.eu/packages/daily/{year}{ojs_number:05d}"

def ted_notice_xml_url(publication_number: str) -> str:
    return f"https://ted.europa.eu/en/notice/{publication_number}/xml"

def ted_notice_html_url(publication_number: str, lang: str = 'en') -> str:
    return f"https://ted.europa.eu/{lang}/notice/{publication_number}/html"

def ted_release_calendar_url(year: int, fmt: str = 'CSV', lang: str = 'en') -> str:
    fmt = fmt.upper()  # CSV, XLS, PDF
    return f"https://ted.europa.eu/{lang}/release-calendar/-/download/file/{fmt}/{year}"
```

## Source verification notes

These links were compiled from the official TED homepage, TED Developers' Corner, TED API docs, TED Open Data docs, TED Open Data Service, data.europa.eu TED dataset records, and OP-TED GitHub repositories.
