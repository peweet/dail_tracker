# Committee-Evidence Probe — Coverage Report

_Sandbox feasibility probe (Phase 0). Meetings since 2024-06-01. Witness-org signal = transcript headings + opening welcome. Match = exact normalised name (shared `name_norm_expr`) in our gold data._


**Committee-identity reconciliation mismatches (API vs AKN path):** 0 (should be 0 — any >0 is a conflation risk to inspect).


## Headline: witness orgs that cross-reference to our data

_`exact` = normalised-name equality. `+likely` adds token-subset matches on the small authoritative sets (departments/councils). `public-body ceiling` = share of witness orgs that are clearly real state bodies (matched or not) — the realistic upper bound once an org-identity alias map lands in Phase 1._

| Committee | Witness orgs | Exact | Exact rate | +Likely | +Likely rate | Public-body ceiling |
|---|---|---|---|---|---|---|
| pac | 37 | 5 | 14% | 10 | 27% | 43% |
| housing | 28 | 6 | 21% | 6 | 21% | 36% |

## Which datasets the matches land in

| Dataset | Tier | Witness-org matches |
|---|---|---|
| payments_payee | exact | 10 |
| payments_publisher | likely | 8 |
| payments_publisher | exact | 4 |
| lobbying_register | exact | 1 |
| councils | exact | 1 |

## Worked examples (HUMAN-VERIFY before trusting any number)

| Committee | Date | Witness org | Source | Exact | Likely |
|---|---|---|---|---|---|
| housing | 2026-06-09 | Housing Agency | heading | payments_payee |  |
| housing | 2026-05-26 | Tailte Éireann | heading | payments_payee,payments_publisher |  |
| housing | 2026-05-19 | An Coimisiún Pleanála | heading | payments_payee |  |
| housing | 2026-05-12 | Dublin City Council | welcome | payments_payee,councils |  |
| housing | 2026-03-03 | National Association of Regional Game Councils | welcome | lobbying_register |  |
| housing | 2026-02-24 | Department of Finance | welcome | payments_publisher |  |
| pac | 2026-06-11 | Department of Public Expenditure | welcome | payments_payee |  |
| pac | 2026-05-28 | Competition and Consumer Protection Commission | heading |  | payments_publisher |
| pac | 2026-05-14 | National Treasury Management Agency | welcome | payments_payee | payments_publisher |
| pac | 2026-04-30 | Children's Health Ireland and the National Paediatric Hospital Development Board | welcome |  | payments_publisher |
| pac | 2026-04-16 | Department of Education and Youth at 10 | welcome |  | payments_publisher |
| pac | 2026-04-16 | Beaumont Hospital were before the committee | welcome |  | payments_publisher |
| pac | 2026-04-16 | Education | heading | payments_payee |  |
| pac | 2026-03-19 | Office of Public Works | welcome | payments_payee,payments_publisher |  |
| pac | 2026-03-05 | Department of Social Protection at 10 | welcome |  | payments_publisher |
| pac | 2026-02-26 | Department of Education and Youth at 10 | welcome |  | payments_publisher |
| pac | 2026-02-19 | Department of Justice | welcome | payments_payee | payments_publisher |
| pac | 2026-02-12 | Office of Public Works | heading | payments_payee,payments_publisher |  |

## Unmatched but clearly a public body (Phase-1 alias-map work-list)

| Committee | Date | Witness org | Source |
|---|---|---|---|
| pac | 2026-06-18 | Department of Further and Higher Education | welcome |
| housing | 2026-06-09 | Housing Activation Office | heading |
| housing | 2026-04-21 | Department of housing | welcome |
| housing | 2026-04-28 | Residential Tenancies Board and the Department of Further and Higher Education | welcome |
| pac | 2026-06-11 | Office of Government Procurement | heading |
| housing | 2026-05-12 | Department | welcome |
| housing | 2026-04-14 | Department of Housing | welcome |
| housing | 2026-05-12 | Department of Housing | welcome |
| pac | 2026-02-26 | Office of Minister for Education and Youth | heading |
| pac | 2026-05-28 | Competition and Consumer Protection Commission at 10 | welcome |
| housing | 2026-02-17 | Home Building Finance Ireland | welcome |
| pac | 2026-03-26 | Inland Fisheries Ireland and the Department of Climate | welcome |
| housing | 2026-01-27 | Department of Housing | welcome |
| pac | 2026-06-11 | Office of the Government Chief Information Officer | heading |
| housing | 2026-04-14 | Department | welcome |
| housing | 2026-03-24 | Department of Housing | welcome |
| pac | 2026-05-07 | National Oil Reserves Agency immediately on completion of the earlier business | welcome |
| housing | 2026-02-10 | Department of Housing | welcome |
| pac | 2026-05-21 | Department of Climate | welcome |
| housing | 2026-04-21 | Department of Housing | welcome |

## Unmatched & not obviously a body (likely topic/extraction noise)

| Committee | Date | Witness org | Source |
|---|---|---|---|
| housing | 2026-02-24 | Changing Demographics, Rural Depopulation and Housing Strategy: Discussion (Resumed) | heading |
| pac | 2026-04-16 | Protecting the State's Investment in the Schools Estate | heading |
| pac | 2026-04-23 | Mater Misericordiae University Hospital and Tallaght University Hospital from 10 | welcome |
| housing | 2026-01-22 | CCMA | welcome |
| pac | 2026-05-21 | Market Cap Fund | heading |
| pac | 2026-02-26 | Control of Grant Payments to Schools | heading |
| pac | 2026-02-26 | Report of the Comptroller and Auditor General and Appropriation Accounts 2024 | heading |
| housing | 2026-02-10 | Defective Concrete Blocks | heading |
| housing | 2026-01-20 | GAA to make their opening statement on behalf of the GAA | welcome |
| pac | 2026-03-05 | Classification of Workers for PRSI Purposes | heading |
| housing | 2026-01-20 | Gaelic Athletic Association | welcome |
| pac | 2026-06-18 | Further and Higher Education, Research, Innovation and Science | heading |
| pac | 2026-05-21 | Climate Action Fund | heading |
| pac | 2026-04-23 | Hospital Insourcing Funding Arrangements | heading |
| housing | 2026-03-24 | Housing Situation in the Gaeltacht | heading |
| housing | 2026-05-12 | Dublin Inner City Flat Complex Regeneration | heading |
| pac | 2026-05-21 | Report of the Comptroller and Auditor General and Appropriation Accounts 2024 | heading |
| housing | 2026-01-27 | Special Needs Housing and Supported Housing | heading |
| pac | 2026-02-12 | National Children's Science Centre | welcome |
| pac | 2026-02-19 | Criminal Justice Operational Hub | heading |
| pac | 2026-02-12 | Report of the Comptroller and Auditor General and Appropriation Accounts 2024 | heading |
| pac | 2026-06-11 | Central Government Accounting Standards | heading |
| housing | 2026-01-22 | Construction Industry Federation | welcome |
| pac | 2026-04-16 | Report of the Comptroller and Auditor General and Appropriation Accounts 2024 | heading |
| housing | 2026-04-14 | Proposed Changes to River Shannon, Grand and Royal Canal and Barrow Navigation By-laws | heading |
| housing | 2026-01-20 | General Scheme of the Planning and Development (Amendment) (No. 2) Bill 2025 | heading |
| housing | 2026-03-03 | Sustainable Hunting of Wild Birds and Rural Pursuits | heading |
| pac | 2026-05-21 | Environment, Climate and Communications | heading |
| housing | 2026-01-22 | County and City Management Association | welcome |
| pac | 2026-06-11 | Superannuation and Retired Allowances | heading |
