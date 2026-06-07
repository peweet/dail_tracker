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


| Year | Month | URL |
|---:|---:|---|
| 1993 | 01 | https://ted.europa.eu/packages/monthly/1993-1 |
| 1993 | 02 | https://ted.europa.eu/packages/monthly/1993-2 |
| 1993 | 03 | https://ted.europa.eu/packages/monthly/1993-3 |
| 1993 | 04 | https://ted.europa.eu/packages/monthly/1993-4 |
| 1993 | 05 | https://ted.europa.eu/packages/monthly/1993-5 |
| 1993 | 06 | https://ted.europa.eu/packages/monthly/1993-6 |
| 1993 | 07 | https://ted.europa.eu/packages/monthly/1993-7 |
| 1993 | 08 | https://ted.europa.eu/packages/monthly/1993-8 |
| 1993 | 09 | https://ted.europa.eu/packages/monthly/1993-9 |
| 1993 | 10 | https://ted.europa.eu/packages/monthly/1993-10 |
| 1993 | 11 | https://ted.europa.eu/packages/monthly/1993-11 |
| 1993 | 12 | https://ted.europa.eu/packages/monthly/1993-12 |
| 1994 | 01 | https://ted.europa.eu/packages/monthly/1994-1 |
| 1994 | 02 | https://ted.europa.eu/packages/monthly/1994-2 |
| 1994 | 03 | https://ted.europa.eu/packages/monthly/1994-3 |
| 1994 | 04 | https://ted.europa.eu/packages/monthly/1994-4 |
| 1994 | 05 | https://ted.europa.eu/packages/monthly/1994-5 |
| 1994 | 06 | https://ted.europa.eu/packages/monthly/1994-6 |
| 1994 | 07 | https://ted.europa.eu/packages/monthly/1994-7 |
| 1994 | 08 | https://ted.europa.eu/packages/monthly/1994-8 |
| 1994 | 09 | https://ted.europa.eu/packages/monthly/1994-9 |
| 1994 | 10 | https://ted.europa.eu/packages/monthly/1994-10 |
| 1994 | 11 | https://ted.europa.eu/packages/monthly/1994-11 |
| 1994 | 12 | https://ted.europa.eu/packages/monthly/1994-12 |
| 1995 | 01 | https://ted.europa.eu/packages/monthly/1995-1 |
| 1995 | 02 | https://ted.europa.eu/packages/monthly/1995-2 |
| 1995 | 03 | https://ted.europa.eu/packages/monthly/1995-3 |
| 1995 | 04 | https://ted.europa.eu/packages/monthly/1995-4 |
| 1995 | 05 | https://ted.europa.eu/packages/monthly/1995-5 |
| 1995 | 06 | https://ted.europa.eu/packages/monthly/1995-6 |
| 1995 | 07 | https://ted.europa.eu/packages/monthly/1995-7 |
| 1995 | 08 | https://ted.europa.eu/packages/monthly/1995-8 |
| 1995 | 09 | https://ted.europa.eu/packages/monthly/1995-9 |
| 1995 | 10 | https://ted.europa.eu/packages/monthly/1995-10 |
| 1995 | 11 | https://ted.europa.eu/packages/monthly/1995-11 |
| 1995 | 12 | https://ted.europa.eu/packages/monthly/1995-12 |
| 1996 | 01 | https://ted.europa.eu/packages/monthly/1996-1 |
| 1996 | 02 | https://ted.europa.eu/packages/monthly/1996-2 |
| 1996 | 03 | https://ted.europa.eu/packages/monthly/1996-3 |
| 1996 | 04 | https://ted.europa.eu/packages/monthly/1996-4 |
| 1996 | 05 | https://ted.europa.eu/packages/monthly/1996-5 |
| 1996 | 06 | https://ted.europa.eu/packages/monthly/1996-6 |
| 1996 | 07 | https://ted.europa.eu/packages/monthly/1996-7 |
| 1996 | 08 | https://ted.europa.eu/packages/monthly/1996-8 |
| 1996 | 09 | https://ted.europa.eu/packages/monthly/1996-9 |
| 1996 | 10 | https://ted.europa.eu/packages/monthly/1996-10 |
| 1996 | 11 | https://ted.europa.eu/packages/monthly/1996-11 |
| 1996 | 12 | https://ted.europa.eu/packages/monthly/1996-12 |
| 1997 | 01 | https://ted.europa.eu/packages/monthly/1997-1 |
| 1997 | 02 | https://ted.europa.eu/packages/monthly/1997-2 |
| 1997 | 03 | https://ted.europa.eu/packages/monthly/1997-3 |
| 1997 | 04 | https://ted.europa.eu/packages/monthly/1997-4 |
| 1997 | 05 | https://ted.europa.eu/packages/monthly/1997-5 |
| 1997 | 06 | https://ted.europa.eu/packages/monthly/1997-6 |
| 1997 | 07 | https://ted.europa.eu/packages/monthly/1997-7 |
| 1997 | 08 | https://ted.europa.eu/packages/monthly/1997-8 |
| 1997 | 09 | https://ted.europa.eu/packages/monthly/1997-9 |
| 1997 | 10 | https://ted.europa.eu/packages/monthly/1997-10 |
| 1997 | 11 | https://ted.europa.eu/packages/monthly/1997-11 |
| 1997 | 12 | https://ted.europa.eu/packages/monthly/1997-12 |
| 1998 | 01 | https://ted.europa.eu/packages/monthly/1998-1 |
| 1998 | 02 | https://ted.europa.eu/packages/monthly/1998-2 |
| 1998 | 03 | https://ted.europa.eu/packages/monthly/1998-3 |
| 1998 | 04 | https://ted.europa.eu/packages/monthly/1998-4 |
| 1998 | 05 | https://ted.europa.eu/packages/monthly/1998-5 |
| 1998 | 06 | https://ted.europa.eu/packages/monthly/1998-6 |
| 1998 | 07 | https://ted.europa.eu/packages/monthly/1998-7 |
| 1998 | 08 | https://ted.europa.eu/packages/monthly/1998-8 |
| 1998 | 09 | https://ted.europa.eu/packages/monthly/1998-9 |
| 1998 | 10 | https://ted.europa.eu/packages/monthly/1998-10 |
| 1998 | 11 | https://ted.europa.eu/packages/monthly/1998-11 |
| 1998 | 12 | https://ted.europa.eu/packages/monthly/1998-12 |
| 1999 | 01 | https://ted.europa.eu/packages/monthly/1999-1 |
| 1999 | 02 | https://ted.europa.eu/packages/monthly/1999-2 |
| 1999 | 03 | https://ted.europa.eu/packages/monthly/1999-3 |
| 1999 | 04 | https://ted.europa.eu/packages/monthly/1999-4 |
| 1999 | 05 | https://ted.europa.eu/packages/monthly/1999-5 |
| 1999 | 06 | https://ted.europa.eu/packages/monthly/1999-6 |
| 1999 | 07 | https://ted.europa.eu/packages/monthly/1999-7 |
| 1999 | 08 | https://ted.europa.eu/packages/monthly/1999-8 |
| 1999 | 09 | https://ted.europa.eu/packages/monthly/1999-9 |
| 1999 | 10 | https://ted.europa.eu/packages/monthly/1999-10 |
| 1999 | 11 | https://ted.europa.eu/packages/monthly/1999-11 |
| 1999 | 12 | https://ted.europa.eu/packages/monthly/1999-12 |
| 2000 | 01 | https://ted.europa.eu/packages/monthly/2000-1 |
| 2000 | 02 | https://ted.europa.eu/packages/monthly/2000-2 |
| 2000 | 03 | https://ted.europa.eu/packages/monthly/2000-3 |
| 2000 | 04 | https://ted.europa.eu/packages/monthly/2000-4 |
| 2000 | 05 | https://ted.europa.eu/packages/monthly/2000-5 |
| 2000 | 06 | https://ted.europa.eu/packages/monthly/2000-6 |
| 2000 | 07 | https://ted.europa.eu/packages/monthly/2000-7 |
| 2000 | 08 | https://ted.europa.eu/packages/monthly/2000-8 |
| 2000 | 09 | https://ted.europa.eu/packages/monthly/2000-9 |
| 2000 | 10 | https://ted.europa.eu/packages/monthly/2000-10 |
| 2000 | 11 | https://ted.europa.eu/packages/monthly/2000-11 |
| 2000 | 12 | https://ted.europa.eu/packages/monthly/2000-12 |
| 2001 | 01 | https://ted.europa.eu/packages/monthly/2001-1 |
| 2001 | 02 | https://ted.europa.eu/packages/monthly/2001-2 |
| 2001 | 03 | https://ted.europa.eu/packages/monthly/2001-3 |
| 2001 | 04 | https://ted.europa.eu/packages/monthly/2001-4 |
| 2001 | 05 | https://ted.europa.eu/packages/monthly/2001-5 |
| 2001 | 06 | https://ted.europa.eu/packages/monthly/2001-6 |
| 2001 | 07 | https://ted.europa.eu/packages/monthly/2001-7 |
| 2001 | 08 | https://ted.europa.eu/packages/monthly/2001-8 |
| 2001 | 09 | https://ted.europa.eu/packages/monthly/2001-9 |
| 2001 | 10 | https://ted.europa.eu/packages/monthly/2001-10 |
| 2001 | 11 | https://ted.europa.eu/packages/monthly/2001-11 |
| 2001 | 12 | https://ted.europa.eu/packages/monthly/2001-12 |
| 2002 | 01 | https://ted.europa.eu/packages/monthly/2002-1 |
| 2002 | 02 | https://ted.europa.eu/packages/monthly/2002-2 |
| 2002 | 03 | https://ted.europa.eu/packages/monthly/2002-3 |
| 2002 | 04 | https://ted.europa.eu/packages/monthly/2002-4 |
| 2002 | 05 | https://ted.europa.eu/packages/monthly/2002-5 |
| 2002 | 06 | https://ted.europa.eu/packages/monthly/2002-6 |
| 2002 | 07 | https://ted.europa.eu/packages/monthly/2002-7 |
| 2002 | 08 | https://ted.europa.eu/packages/monthly/2002-8 |
| 2002 | 09 | https://ted.europa.eu/packages/monthly/2002-9 |
| 2002 | 10 | https://ted.europa.eu/packages/monthly/2002-10 |
| 2002 | 11 | https://ted.europa.eu/packages/monthly/2002-11 |
| 2002 | 12 | https://ted.europa.eu/packages/monthly/2002-12 |
| 2003 | 01 | https://ted.europa.eu/packages/monthly/2003-1 |
| 2003 | 02 | https://ted.europa.eu/packages/monthly/2003-2 |
| 2003 | 03 | https://ted.europa.eu/packages/monthly/2003-3 |
| 2003 | 04 | https://ted.europa.eu/packages/monthly/2003-4 |
| 2003 | 05 | https://ted.europa.eu/packages/monthly/2003-5 |
| 2003 | 06 | https://ted.europa.eu/packages/monthly/2003-6 |
| 2003 | 07 | https://ted.europa.eu/packages/monthly/2003-7 |
| 2003 | 08 | https://ted.europa.eu/packages/monthly/2003-8 |
| 2003 | 09 | https://ted.europa.eu/packages/monthly/2003-9 |
| 2003 | 10 | https://ted.europa.eu/packages/monthly/2003-10 |
| 2003 | 11 | https://ted.europa.eu/packages/monthly/2003-11 |
| 2003 | 12 | https://ted.europa.eu/packages/monthly/2003-12 |
| 2004 | 01 | https://ted.europa.eu/packages/monthly/2004-1 |
| 2004 | 02 | https://ted.europa.eu/packages/monthly/2004-2 |
| 2004 | 03 | https://ted.europa.eu/packages/monthly/2004-3 |
| 2004 | 04 | https://ted.europa.eu/packages/monthly/2004-4 |
| 2004 | 05 | https://ted.europa.eu/packages/monthly/2004-5 |
| 2004 | 06 | https://ted.europa.eu/packages/monthly/2004-6 |
| 2004 | 07 | https://ted.europa.eu/packages/monthly/2004-7 |
| 2004 | 08 | https://ted.europa.eu/packages/monthly/2004-8 |
| 2004 | 09 | https://ted.europa.eu/packages/monthly/2004-9 |
| 2004 | 10 | https://ted.europa.eu/packages/monthly/2004-10 |
| 2004 | 11 | https://ted.europa.eu/packages/monthly/2004-11 |
| 2004 | 12 | https://ted.europa.eu/packages/monthly/2004-12 |
| 2005 | 01 | https://ted.europa.eu/packages/monthly/2005-1 |
| 2005 | 02 | https://ted.europa.eu/packages/monthly/2005-2 |
| 2005 | 03 | https://ted.europa.eu/packages/monthly/2005-3 |
| 2005 | 04 | https://ted.europa.eu/packages/monthly/2005-4 |
| 2005 | 05 | https://ted.europa.eu/packages/monthly/2005-5 |
| 2005 | 06 | https://ted.europa.eu/packages/monthly/2005-6 |
| 2005 | 07 | https://ted.europa.eu/packages/monthly/2005-7 |
| 2005 | 08 | https://ted.europa.eu/packages/monthly/2005-8 |
| 2005 | 09 | https://ted.europa.eu/packages/monthly/2005-9 |
| 2005 | 10 | https://ted.europa.eu/packages/monthly/2005-10 |
| 2005 | 11 | https://ted.europa.eu/packages/monthly/2005-11 |
| 2005 | 12 | https://ted.europa.eu/packages/monthly/2005-12 |
| 2006 | 01 | https://ted.europa.eu/packages/monthly/2006-1 |
| 2006 | 02 | https://ted.europa.eu/packages/monthly/2006-2 |
| 2006 | 03 | https://ted.europa.eu/packages/monthly/2006-3 |
| 2006 | 04 | https://ted.europa.eu/packages/monthly/2006-4 |
| 2006 | 05 | https://ted.europa.eu/packages/monthly/2006-5 |
| 2006 | 06 | https://ted.europa.eu/packages/monthly/2006-6 |
| 2006 | 07 | https://ted.europa.eu/packages/monthly/2006-7 |
| 2006 | 08 | https://ted.europa.eu/packages/monthly/2006-8 |
| 2006 | 09 | https://ted.europa.eu/packages/monthly/2006-9 |
| 2006 | 10 | https://ted.europa.eu/packages/monthly/2006-10 |
| 2006 | 11 | https://ted.europa.eu/packages/monthly/2006-11 |
| 2006 | 12 | https://ted.europa.eu/packages/monthly/2006-12 |
| 2007 | 01 | https://ted.europa.eu/packages/monthly/2007-1 |
| 2007 | 02 | https://ted.europa.eu/packages/monthly/2007-2 |
| 2007 | 03 | https://ted.europa.eu/packages/monthly/2007-3 |
| 2007 | 04 | https://ted.europa.eu/packages/monthly/2007-4 |
| 2007 | 05 | https://ted.europa.eu/packages/monthly/2007-5 |
| 2007 | 06 | https://ted.europa.eu/packages/monthly/2007-6 |
| 2007 | 07 | https://ted.europa.eu/packages/monthly/2007-7 |
| 2007 | 08 | https://ted.europa.eu/packages/monthly/2007-8 |
| 2007 | 09 | https://ted.europa.eu/packages/monthly/2007-9 |
| 2007 | 10 | https://ted.europa.eu/packages/monthly/2007-10 |
| 2007 | 11 | https://ted.europa.eu/packages/monthly/2007-11 |
| 2007 | 12 | https://ted.europa.eu/packages/monthly/2007-12 |
| 2008 | 01 | https://ted.europa.eu/packages/monthly/2008-1 |
| 2008 | 02 | https://ted.europa.eu/packages/monthly/2008-2 |
| 2008 | 03 | https://ted.europa.eu/packages/monthly/2008-3 |
| 2008 | 04 | https://ted.europa.eu/packages/monthly/2008-4 |
| 2008 | 05 | https://ted.europa.eu/packages/monthly/2008-5 |
| 2008 | 06 | https://ted.europa.eu/packages/monthly/2008-6 |
| 2008 | 07 | https://ted.europa.eu/packages/monthly/2008-7 |
| 2008 | 08 | https://ted.europa.eu/packages/monthly/2008-8 |
| 2008 | 09 | https://ted.europa.eu/packages/monthly/2008-9 |
| 2008 | 10 | https://ted.europa.eu/packages/monthly/2008-10 |
| 2008 | 11 | https://ted.europa.eu/packages/monthly/2008-11 |
| 2008 | 12 | https://ted.europa.eu/packages/monthly/2008-12 |
| 2009 | 01 | https://ted.europa.eu/packages/monthly/2009-1 |
| 2009 | 02 | https://ted.europa.eu/packages/monthly/2009-2 |
| 2009 | 03 | https://ted.europa.eu/packages/monthly/2009-3 |
| 2009 | 04 | https://ted.europa.eu/packages/monthly/2009-4 |
| 2009 | 05 | https://ted.europa.eu/packages/monthly/2009-5 |
| 2009 | 06 | https://ted.europa.eu/packages/monthly/2009-6 |
| 2009 | 07 | https://ted.europa.eu/packages/monthly/2009-7 |
| 2009 | 08 | https://ted.europa.eu/packages/monthly/2009-8 |
| 2009 | 09 | https://ted.europa.eu/packages/monthly/2009-9 |
| 2009 | 10 | https://ted.europa.eu/packages/monthly/2009-10 |
| 2009 | 11 | https://ted.europa.eu/packages/monthly/2009-11 |
| 2009 | 12 | https://ted.europa.eu/packages/monthly/2009-12 |
| 2010 | 01 | https://ted.europa.eu/packages/monthly/2010-1 |
| 2010 | 02 | https://ted.europa.eu/packages/monthly/2010-2 |
| 2010 | 03 | https://ted.europa.eu/packages/monthly/2010-3 |
| 2010 | 04 | https://ted.europa.eu/packages/monthly/2010-4 |
| 2010 | 05 | https://ted.europa.eu/packages/monthly/2010-5 |
| 2010 | 06 | https://ted.europa.eu/packages/monthly/2010-6 |
| 2010 | 07 | https://ted.europa.eu/packages/monthly/2010-7 |
| 2010 | 08 | https://ted.europa.eu/packages/monthly/2010-8 |
| 2010 | 09 | https://ted.europa.eu/packages/monthly/2010-9 |
| 2010 | 10 | https://ted.europa.eu/packages/monthly/2010-10 |
| 2010 | 11 | https://ted.europa.eu/packages/monthly/2010-11 |
| 2010 | 12 | https://ted.europa.eu/packages/monthly/2010-12 |
| 2011 | 01 | https://ted.europa.eu/packages/monthly/2011-1 |
| 2011 | 02 | https://ted.europa.eu/packages/monthly/2011-2 |
| 2011 | 03 | https://ted.europa.eu/packages/monthly/2011-3 |
| 2011 | 04 | https://ted.europa.eu/packages/monthly/2011-4 |
| 2011 | 05 | https://ted.europa.eu/packages/monthly/2011-5 |
| 2011 | 06 | https://ted.europa.eu/packages/monthly/2011-6 |
| 2011 | 07 | https://ted.europa.eu/packages/monthly/2011-7 |
| 2011 | 08 | https://ted.europa.eu/packages/monthly/2011-8 |
| 2011 | 09 | https://ted.europa.eu/packages/monthly/2011-9 |
| 2011 | 10 | https://ted.europa.eu/packages/monthly/2011-10 |
| 2011 | 11 | https://ted.europa.eu/packages/monthly/2011-11 |
| 2011 | 12 | https://ted.europa.eu/packages/monthly/2011-12 |
| 2012 | 01 | https://ted.europa.eu/packages/monthly/2012-1 |
| 2012 | 02 | https://ted.europa.eu/packages/monthly/2012-2 |
| 2012 | 03 | https://ted.europa.eu/packages/monthly/2012-3 |
| 2012 | 04 | https://ted.europa.eu/packages/monthly/2012-4 |
| 2012 | 05 | https://ted.europa.eu/packages/monthly/2012-5 |
| 2012 | 06 | https://ted.europa.eu/packages/monthly/2012-6 |
| 2012 | 07 | https://ted.europa.eu/packages/monthly/2012-7 |
| 2012 | 08 | https://ted.europa.eu/packages/monthly/2012-8 |
| 2012 | 09 | https://ted.europa.eu/packages/monthly/2012-9 |
| 2012 | 10 | https://ted.europa.eu/packages/monthly/2012-10 |
| 2012 | 11 | https://ted.europa.eu/packages/monthly/2012-11 |
| 2012 | 12 | https://ted.europa.eu/packages/monthly/2012-12 |
| 2013 | 01 | https://ted.europa.eu/packages/monthly/2013-1 |
| 2013 | 02 | https://ted.europa.eu/packages/monthly/2013-2 |
| 2013 | 03 | https://ted.europa.eu/packages/monthly/2013-3 |
| 2013 | 04 | https://ted.europa.eu/packages/monthly/2013-4 |
| 2013 | 05 | https://ted.europa.eu/packages/monthly/2013-5 |
| 2013 | 06 | https://ted.europa.eu/packages/monthly/2013-6 |
| 2013 | 07 | https://ted.europa.eu/packages/monthly/2013-7 |
| 2013 | 08 | https://ted.europa.eu/packages/monthly/2013-8 |
| 2013 | 09 | https://ted.europa.eu/packages/monthly/2013-9 |
| 2013 | 10 | https://ted.europa.eu/packages/monthly/2013-10 |
| 2013 | 11 | https://ted.europa.eu/packages/monthly/2013-11 |
| 2013 | 12 | https://ted.europa.eu/packages/monthly/2013-12 |
| 2014 | 01 | https://ted.europa.eu/packages/monthly/2014-1 |
| 2014 | 02 | https://ted.europa.eu/packages/monthly/2014-2 |
| 2014 | 03 | https://ted.europa.eu/packages/monthly/2014-3 |
| 2014 | 04 | https://ted.europa.eu/packages/monthly/2014-4 |
| 2014 | 05 | https://ted.europa.eu/packages/monthly/2014-5 |
| 2014 | 06 | https://ted.europa.eu/packages/monthly/2014-6 |
| 2014 | 07 | https://ted.europa.eu/packages/monthly/2014-7 |
| 2014 | 08 | https://ted.europa.eu/packages/monthly/2014-8 |
| 2014 | 09 | https://ted.europa.eu/packages/monthly/2014-9 |
| 2014 | 10 | https://ted.europa.eu/packages/monthly/2014-10 |
| 2014 | 11 | https://ted.europa.eu/packages/monthly/2014-11 |
| 2014 | 12 | https://ted.europa.eu/packages/monthly/2014-12 |
| 2015 | 01 | https://ted.europa.eu/packages/monthly/2015-1 |
| 2015 | 02 | https://ted.europa.eu/packages/monthly/2015-2 |
| 2015 | 03 | https://ted.europa.eu/packages/monthly/2015-3 |
| 2015 | 04 | https://ted.europa.eu/packages/monthly/2015-4 |
| 2015 | 05 | https://ted.europa.eu/packages/monthly/2015-5 |
| 2015 | 06 | https://ted.europa.eu/packages/monthly/2015-6 |
| 2015 | 07 | https://ted.europa.eu/packages/monthly/2015-7 |
| 2015 | 08 | https://ted.europa.eu/packages/monthly/2015-8 |
| 2015 | 09 | https://ted.europa.eu/packages/monthly/2015-9 |
| 2015 | 10 | https://ted.europa.eu/packages/monthly/2015-10 |
| 2015 | 11 | https://ted.europa.eu/packages/monthly/2015-11 |
| 2015 | 12 | https://ted.europa.eu/packages/monthly/2015-12 |
| 2016 | 01 | https://ted.europa.eu/packages/monthly/2016-1 |
| 2016 | 02 | https://ted.europa.eu/packages/monthly/2016-2 |
| 2016 | 03 | https://ted.europa.eu/packages/monthly/2016-3 |
| 2016 | 04 | https://ted.europa.eu/packages/monthly/2016-4 |
| 2016 | 05 | https://ted.europa.eu/packages/monthly/2016-5 |
| 2016 | 06 | https://ted.europa.eu/packages/monthly/2016-6 |
| 2016 | 07 | https://ted.europa.eu/packages/monthly/2016-7 |
| 2016 | 08 | https://ted.europa.eu/packages/monthly/2016-8 |
| 2016 | 09 | https://ted.europa.eu/packages/monthly/2016-9 |
| 2016 | 10 | https://ted.europa.eu/packages/monthly/2016-10 |
| 2016 | 11 | https://ted.europa.eu/packages/monthly/2016-11 |
| 2016 | 12 | https://ted.europa.eu/packages/monthly/2016-12 |
| 2017 | 01 | https://ted.europa.eu/packages/monthly/2017-1 |
| 2017 | 02 | https://ted.europa.eu/packages/monthly/2017-2 |
| 2017 | 03 | https://ted.europa.eu/packages/monthly/2017-3 |
| 2017 | 04 | https://ted.europa.eu/packages/monthly/2017-4 |
| 2017 | 05 | https://ted.europa.eu/packages/monthly/2017-5 |
| 2017 | 06 | https://ted.europa.eu/packages/monthly/2017-6 |
| 2017 | 07 | https://ted.europa.eu/packages/monthly/2017-7 |
| 2017 | 08 | https://ted.europa.eu/packages/monthly/2017-8 |
| 2017 | 09 | https://ted.europa.eu/packages/monthly/2017-9 |
| 2017 | 10 | https://ted.europa.eu/packages/monthly/2017-10 |
| 2017 | 11 | https://ted.europa.eu/packages/monthly/2017-11 |
| 2017 | 12 | https://ted.europa.eu/packages/monthly/2017-12 |
| 2018 | 01 | https://ted.europa.eu/packages/monthly/2018-1 |
| 2018 | 02 | https://ted.europa.eu/packages/monthly/2018-2 |
| 2018 | 03 | https://ted.europa.eu/packages/monthly/2018-3 |
| 2018 | 04 | https://ted.europa.eu/packages/monthly/2018-4 |
| 2018 | 05 | https://ted.europa.eu/packages/monthly/2018-5 |
| 2018 | 06 | https://ted.europa.eu/packages/monthly/2018-6 |
| 2018 | 07 | https://ted.europa.eu/packages/monthly/2018-7 |
| 2018 | 08 | https://ted.europa.eu/packages/monthly/2018-8 |
| 2018 | 09 | https://ted.europa.eu/packages/monthly/2018-9 |
| 2018 | 10 | https://ted.europa.eu/packages/monthly/2018-10 |
| 2018 | 11 | https://ted.europa.eu/packages/monthly/2018-11 |
| 2018 | 12 | https://ted.europa.eu/packages/monthly/2018-12 |
| 2019 | 01 | https://ted.europa.eu/packages/monthly/2019-1 |
| 2019 | 02 | https://ted.europa.eu/packages/monthly/2019-2 |
| 2019 | 03 | https://ted.europa.eu/packages/monthly/2019-3 |
| 2019 | 04 | https://ted.europa.eu/packages/monthly/2019-4 |
| 2019 | 05 | https://ted.europa.eu/packages/monthly/2019-5 |
| 2019 | 06 | https://ted.europa.eu/packages/monthly/2019-6 |
| 2019 | 07 | https://ted.europa.eu/packages/monthly/2019-7 |
| 2019 | 08 | https://ted.europa.eu/packages/monthly/2019-8 |
| 2019 | 09 | https://ted.europa.eu/packages/monthly/2019-9 |
| 2019 | 10 | https://ted.europa.eu/packages/monthly/2019-10 |
| 2019 | 11 | https://ted.europa.eu/packages/monthly/2019-11 |
| 2019 | 12 | https://ted.europa.eu/packages/monthly/2019-12 |
| 2020 | 01 | https://ted.europa.eu/packages/monthly/2020-1 |
| 2020 | 02 | https://ted.europa.eu/packages/monthly/2020-2 |
| 2020 | 03 | https://ted.europa.eu/packages/monthly/2020-3 |
| 2020 | 04 | https://ted.europa.eu/packages/monthly/2020-4 |
| 2020 | 05 | https://ted.europa.eu/packages/monthly/2020-5 |
| 2020 | 06 | https://ted.europa.eu/packages/monthly/2020-6 |
| 2020 | 07 | https://ted.europa.eu/packages/monthly/2020-7 |
| 2020 | 08 | https://ted.europa.eu/packages/monthly/2020-8 |
| 2020 | 09 | https://ted.europa.eu/packages/monthly/2020-9 |
| 2020 | 10 | https://ted.europa.eu/packages/monthly/2020-10 |
| 2020 | 11 | https://ted.europa.eu/packages/monthly/2020-11 |
| 2020 | 12 | https://ted.europa.eu/packages/monthly/2020-12 |
| 2021 | 01 | https://ted.europa.eu/packages/monthly/2021-1 |
| 2021 | 02 | https://ted.europa.eu/packages/monthly/2021-2 |
| 2021 | 03 | https://ted.europa.eu/packages/monthly/2021-3 |
| 2021 | 04 | https://ted.europa.eu/packages/monthly/2021-4 |
| 2021 | 05 | https://ted.europa.eu/packages/monthly/2021-5 |
| 2021 | 06 | https://ted.europa.eu/packages/monthly/2021-6 |
| 2021 | 07 | https://ted.europa.eu/packages/monthly/2021-7 |
| 2021 | 08 | https://ted.europa.eu/packages/monthly/2021-8 |
| 2021 | 09 | https://ted.europa.eu/packages/monthly/2021-9 |
| 2021 | 10 | https://ted.europa.eu/packages/monthly/2021-10 |
| 2021 | 11 | https://ted.europa.eu/packages/monthly/2021-11 |
| 2021 | 12 | https://ted.europa.eu/packages/monthly/2021-12 |
| 2022 | 01 | https://ted.europa.eu/packages/monthly/2022-1 |
| 2022 | 02 | https://ted.europa.eu/packages/monthly/2022-2 |
| 2022 | 03 | https://ted.europa.eu/packages/monthly/2022-3 |
| 2022 | 04 | https://ted.europa.eu/packages/monthly/2022-4 |
| 2022 | 05 | https://ted.europa.eu/packages/monthly/2022-5 |
| 2022 | 06 | https://ted.europa.eu/packages/monthly/2022-6 |
| 2022 | 07 | https://ted.europa.eu/packages/monthly/2022-7 |
| 2022 | 08 | https://ted.europa.eu/packages/monthly/2022-8 |
| 2022 | 09 | https://ted.europa.eu/packages/monthly/2022-9 |
| 2022 | 10 | https://ted.europa.eu/packages/monthly/2022-10 |
| 2022 | 11 | https://ted.europa.eu/packages/monthly/2022-11 |
| 2022 | 12 | https://ted.europa.eu/packages/monthly/2022-12 |
| 2023 | 01 | https://ted.europa.eu/packages/monthly/2023-1 |
| 2023 | 02 | https://ted.europa.eu/packages/monthly/2023-2 |
| 2023 | 03 | https://ted.europa.eu/packages/monthly/2023-3 |
| 2023 | 04 | https://ted.europa.eu/packages/monthly/2023-4 |
| 2023 | 05 | https://ted.europa.eu/packages/monthly/2023-5 |
| 2023 | 06 | https://ted.europa.eu/packages/monthly/2023-6 |
| 2023 | 07 | https://ted.europa.eu/packages/monthly/2023-7 |
| 2023 | 08 | https://ted.europa.eu/packages/monthly/2023-8 |
| 2023 | 09 | https://ted.europa.eu/packages/monthly/2023-9 |
| 2023 | 10 | https://ted.europa.eu/packages/monthly/2023-10 |
| 2023 | 11 | https://ted.europa.eu/packages/monthly/2023-11 |
| 2023 | 12 | https://ted.europa.eu/packages/monthly/2023-12 |
| 2024 | 01 | https://ted.europa.eu/packages/monthly/2024-1 |
| 2024 | 02 | https://ted.europa.eu/packages/monthly/2024-2 |
| 2024 | 03 | https://ted.europa.eu/packages/monthly/2024-3 |
| 2024 | 04 | https://ted.europa.eu/packages/monthly/2024-4 |
| 2024 | 05 | https://ted.europa.eu/packages/monthly/2024-5 |
| 2024 | 06 | https://ted.europa.eu/packages/monthly/2024-6 |
| 2024 | 07 | https://ted.europa.eu/packages/monthly/2024-7 |
| 2024 | 08 | https://ted.europa.eu/packages/monthly/2024-8 |
| 2024 | 09 | https://ted.europa.eu/packages/monthly/2024-9 |
| 2024 | 10 | https://ted.europa.eu/packages/monthly/2024-10 |
| 2024 | 11 | https://ted.europa.eu/packages/monthly/2024-11 |
| 2024 | 12 | https://ted.europa.eu/packages/monthly/2024-12 |
| 2025 | 01 | https://ted.europa.eu/packages/monthly/2025-1 |
| 2025 | 02 | https://ted.europa.eu/packages/monthly/2025-2 |
| 2025 | 03 | https://ted.europa.eu/packages/monthly/2025-3 |
| 2025 | 04 | https://ted.europa.eu/packages/monthly/2025-4 |
| 2025 | 05 | https://ted.europa.eu/packages/monthly/2025-5 |
| 2025 | 06 | https://ted.europa.eu/packages/monthly/2025-6 |
| 2025 | 07 | https://ted.europa.eu/packages/monthly/2025-7 |
| 2025 | 08 | https://ted.europa.eu/packages/monthly/2025-8 |
| 2025 | 09 | https://ted.europa.eu/packages/monthly/2025-9 |
| 2025 | 10 | https://ted.europa.eu/packages/monthly/2025-10 |
| 2025 | 11 | https://ted.europa.eu/packages/monthly/2025-11 |
| 2025 | 12 | https://ted.europa.eu/packages/monthly/2025-12 |
| 2026 | 01 | https://ted.europa.eu/packages/monthly/2026-1 |
| 2026 | 02 | https://ted.europa.eu/packages/monthly/2026-2 |
| 2026 | 03 | https://ted.europa.eu/packages/monthly/2026-3 |
| 2026 | 04 | https://ted.europa.eu/packages/monthly/2026-4 |
| 2026 | 05 | https://ted.europa.eu/packages/monthly/2026-5 |


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
