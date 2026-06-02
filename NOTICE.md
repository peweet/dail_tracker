# Notices & Data Source Licensing

This repository contains two distinct kinds of material with **different licences**.
Read this before reusing or redistributing anything here.

---

## 1. The software (code)

Copyright © 2026 Patrick Glynn.

The original source code in this repository — the ETL pipeline, parsers, SQL
views, and Streamlit application — is licensed under the **GNU Affero General
Public License v3.0** (see [`LICENSE`](LICENSE)).

In short: you may use, study, modify, and redistribute it, but any **hosted /
network-deployed** version that includes modifications must also make its
complete source code available under the same licence.

---

## 2. Third-party data (NOT covered by the code licence)

The datasets, derived data files (silver CSVs, gold parquet), and any extracted
content in this repository originate from public bodies and **remain subject to
their own licences and copyright**. The AGPL above does **not** apply to them and
does **not** grant you any rights over them.

### 2a. Houses of the Oireachtas — debates, divisions (votes), parliamentary
questions, bills, acts

Source: <https://api.oireachtas.ie/> and <https://data.oireachtas.ie/>

Licensed under the **Oireachtas (Open Data) PSI Licence**, which incorporates
the **Creative Commons Attribution 4.0 International (CC BY 4.0)** licence,
pursuant to the European Communities (Re-Use of Public Sector Information)
Regulations 2005–2015.

Required attribution when reusing this data:

> Contains Oireachtas information licensed under the Oireachtas (Open Data) PSI
> Licence. © Houses of the Oireachtas.

Licence: <https://www.oireachtas.ie/en/open-data/license/>
Open data policy: <https://www.oireachtas.ie/en/open-data/>

### 2b. Iris Oifigiúil — statutory instruments, public notices, official
appointments, corporate/register notices

Source: <https://www.irisoifigiuil.ie/>

Iris Oifigiúil (the official gazette of the Government of Ireland) is published
under **Government of Ireland copyright** (Copyright and Related Rights Act
2000). **It is NOT released under an open / Creative Commons licence.** Its site
terms permit download "for personal use only" and require that, where material
is issued to others, the **source (including URL) and copyright status** be
acknowledged. Third-party material appearing within notices is not covered and
requires separate permission from the rights holder.

**What this project relies on:** copyright protects expression, not facts. This
project extracts and re-expresses *factual* data (e.g. SI numbers, dates, entity
names, appointment events) rather than redistributing verbatim gazette text or
PDF layouts. Bulk reproduction of verbatim Iris text/titles is **not**
authorised by these notices and should not be assumed to be permitted.

Required acknowledgement when reusing Iris-derived data:

> Contains public sector information from Iris Oifigiúil © Government of Ireland.
> Source: https://www.irisoifigiuil.ie/

### 2c. lobbying.ie — Register of Lobbying (SIPO)

Source: <https://www.lobbying.ie/>

Maintained by the **Standards in Public Office Commission (SIPO)**. SIPO's
[Re-use of Public Sector Information policy](https://www.lobbying.ie/about-us/our-policies/reuse-of-public-sector-information/)
states: *"You may re-use the information on the website free of charge in any
format"* — including copying, publishing, and broadcasting. Reuse must reproduce
the information accurately, must not be misleading, and must acknowledge the
source and copyright of the Commission.

Required attribution:

> Contains lobbying register data © Standards in Public Office Commission,
> reused under its PSI re-use policy.

### 2d. Companies Registration Office (CRO) — companies *(sandbox)*

Source: <https://opendata.cro.ie/>

The CRO **open-data portal** publishes company records under the **Creative
Commons Attribution 4.0 International (CC BY 4.0)** licence, free to reuse and
redistribute. (This is distinct from the CRO's separate paid bulk-data licence
at <https://cro.ie/publications/fees/bulk-data/>, which this project does **not**
use.)

Required attribution:

> Contains Irish Public Sector Data licensed under a Creative Commons
> Attribution 4.0 International (CC BY 4.0) licence.

### 2e. Charities Regulator — Public Register of Charities *(sandbox)*

Source: <https://www.charitiesregulator.ie/> /
<https://data.gov.ie/dataset/register-of-charities-in-ireland>

Published under the **Creative Commons Attribution 4.0 International (CC BY 4.0)**
licence, free to reuse and redistribute with attribution.

Required attribution:

> Contains Irish Public Sector Data licensed under a Creative Commons
> Attribution 4.0 International (CC BY 4.0) licence.

### 2f. Other sources

- **Wikipedia / Wikimedia Commons** (member images / socials, via
  `wikidata_socials_etl.py` / `wiki_data.py`): each image carries its own licence
  (CC BY / CC BY-SA / public domain). Attribution metadata is captured per-image;
  only free-licence images are flagged usable. Reuse must honour the per-image
  licence and attribution.
- **CSO / data.gov.ie** and any other ingested sources retain their respective
  terms; consult each source before redistribution.

### 2g. Personal data / GDPR

A CC BY licence covers copyright and *sui generis* database rights only — it does
**not** grant any data-protection rights. Re-publishing the records of **public
figures** (TDs, Senators, ministers, registered lobbyists) rests on a public-
interest / public-task basis. Cross-referencing **named private individuals**
(CRO directors, charity trustees) is separate processing under the GDPR and is
kept in the sandbox layer pending a documented lawful basis. Personal insolvency
/ individual bankruptcy notices are excluded by policy at the enrichment level
(`corporate_notices_enrichment.py`).

---

## Summary

| Material | Licence | Can you redistribute? |
|---|---|---|
| This project's **code** | AGPL-3.0-or-later | Yes, under AGPL terms |
| **Oireachtas** data (votes, debates, PQs, bills) | CC BY 4.0 (PSI) | Yes, **with attribution** |
| **lobbying.ie** data (SIPO) | PSI re-use policy (free reuse) | Yes, **acknowledge SIPO** |
| **CRO** companies (open-data portal) | CC BY 4.0 | Yes, **with attribution** |
| **Charities Regulator** register | CC BY 4.0 | Yes, **with attribution** |
| **Iris Oifigiúil** data | Government copyright (not open) | Facts: defensible. Verbatim text: no. **Acknowledge source.** |
| **Wikimedia** images | Per-image (CC / PD) | Per the individual image licence |

Nothing in this NOTICE is legal advice.
