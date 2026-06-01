# loan_book_fund_aliases.csv — source citations

Companion to [`loan_book_fund_aliases.csv`](loan_book_fund_aliases.csv). The CSV maps a brand /
loan-book / SPV name (as it appears in Iris Oifigiúil corporate-distress notices) to its parent
fund and a classification. Per the project's editorial rule ("cite real-world claims"), every
classification of a named entity carries a reputable source below.

Brands were seeded from a frequency scan of the 35,276 corporate-scope notices
(`corporate_insolvency` + `corporate_notice`) in `iris_notice_events_clean.csv`, then classified
against published reporting. Matching is case-insensitive substring on the notice text.

`fund_type` controlled vocabulary (must match exactly — the Corporate page colours/ranks on these):
`vulture fund`, `credit servicer`, `Irish bank`, `Irish bank (winding down)`, `Irish bank (exited)`,
`state asset manager`, `state agency`.

## Vulture funds (and their Irish SPV brands)

- **Promontoria → Cerberus** — Cerberus's family of Irish-registered SPVs (Aran, Arrow, Eagle, Acer)
  holding loans bought from Ulster Bank, NAMA and AIB.
  [Irish Times: "Judgment orders sought by Cerberus subsidiary Promontoria"](https://www.irishtimes.com/business/commercial-property/judgment-orders-sought-by-cerberus-subsidiary-promontoria-1.2982134) ·
  [Irish Times: "Cerberus companies collect €33m on Irish debts" (2024)](https://www.irishtimes.com/business/2024/10/02/cerberus-companies-collect-33m-on-irish-debts/)
- **Beltany / Ennis Property Finance / Kenmare Property Finance → Goldman Sachs** — the three Irish
  SPVs Goldman used to work through Celtic-Tiger distressed loan books.
  [Irish Times: "Beltany Property Finance an affiliate of Goldman Sachs"](https://www.irishtimes.com/business/financial-services/beltany-property-finance-an-affiliate-of-goldman-sachs-1.2571789) ·
  [The Currency: "three Irish Goldman Sachs vulture-fund units"](https://thecurrency.news/articles/214169/from-ennis-to-vanuatu-dublin-firm-to-wind-down-three-irish-goldman-sachs-vulture-fund-units/)
- **Havbell → Apollo** — Apollo (with Deutsche Bank) vehicle holding ex-PTSB loans.
  [Gript: "A closer look at the mortgage vulture funds and their Irish operations"](https://gript.ie/a-closer-look-at-the-mortgage-vulture-funds-and-their-irish-operations/)
- **Shoreline Residential / LSREF / Tanager → Lone Star** — Lone Star vehicles holding IBRC and other
  Irish mortgage books.
  [Irish Times: "Mortgage holders will not be affected by loan sale to vulture funds, says Donohoe"](https://www.irishtimes.com/business/financial-services/mortgage-holders-will-not-be-affected-by-loan-sale-to-vulture-funds-says-donohoe-1.3582880) ·
  [People Before Profit: "US vulture funds set to make another killing"](https://www.pbp.ie/us-vulture-funds-set-to-make-another-killing/)

## Credit servicers

- **Start Mortgages → Lone Star** — Lone Star-affiliated servicer/lender; bought 10,000+ PTSB loans.
  [Irish Times (Donohoe loan-sale piece, above)](https://www.irishtimes.com/business/financial-services/mortgage-holders-will-not-be-affected-by-loan-sale-to-vulture-funds-says-donohoe-1.3582880)
- **Mars Capital → Oaktree** — "formerly an affiliate of US private equity firm Oaktree"; later
  acquired by Arrow Global. Regulated Irish mortgage servicer.
  [Irish Times: "Mars Capital ramps up Irish debt servicing business"](https://www.irishtimes.com/business/economy/mars-capital-ramps-up-irish-debt-servicing-business-1.3418626)
- **Everyday Finance → Cerberus** — Link Group servicer that holds legal title for Cerberus-acquired
  AIB loan books (Projects Sycamore, Joshua, Fir).
  [Irish Times: "AIB sells problem loans portfolio to Cerberus-led group"](https://www.irishtimes.com/business/financial-services/2022/06/21/aib-sells-problem-loans-portfolio-to-cerberus-led-group-for-400m/)
- **Pepper → Pepper Advantage** — Pepper Finance Corporation (Ireland) DAC / Pepper Asset Servicing,
  regulated by the Central Bank of Ireland; services books for many lenders.
  [Central Bank of Ireland register (C37043)](http://registers.centralbank.ie/FirmDataPage.aspx?firmReferenceNumber=C37043)
- **Link ASI → Link Group** — outsourced servicer for Everyday Finance / AIB books (see Everyday above).

## Irish banks

AIB, Bank of Ireland, Permanent TSB, EBS (part of AIB) — pillar/retail banks. Ulster Bank, KBC Bank
Ireland, ACC Bank and Danske Bank have **exited or are winding down** their Irish retail operations,
which is why their crisis-era books appear in distress notices.
[RTÉ / Irish Times passim on the Ulster Bank and KBC market exits.](https://www.irishtimes.com/business/financial-services/why-irish-banks-are-now-looking-beyond-vulture-funds-on-problem-loans-1.4186967)

## State bodies

- **NAMA** — National Asset Management Agency, statutory body managing acquired bank loans.
- **IBRC** — Irish Bank Resolution Corporation (successor to Anglo Irish Bank / INBS), in special
  liquidation since 2013.

## Excluded (deliberately not classified — needs your call)

- `LSF` (1,233 hits) — too generic as a bare substring; high false-positive risk. Use specific
  vehicle names (LSREF, Tanager, Shoreline) instead.
- `Pentire`, `Summerhill`, `Launceston`, `Strategic` — appear in notices but not firmly attributable
  to a parent from public reporting; left out rather than guessed.
- `Finance Ireland`, `Dilosk`, `Capitalflow` — legitimate Irish **non-bank lenders**, not
  distressed-debt funds; they don't fit the vulture/servicer/bank/state vocabulary.
