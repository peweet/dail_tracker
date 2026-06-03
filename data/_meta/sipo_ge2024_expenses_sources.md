# sipo_expenses_fact.parquet — source citation

Companion to the (sandbox, pre-promotion) fact
`pipeline_sandbox/_sipo_output/sipo_expenses_fact.parquet`. One row per candidate
per party, from each party's **National Agent — Election Expenses Statement** for
the 2024 general election (the 34th Dáil), as published by the Standards in Public
Office Commission (SIPO). These are **election expenses** (money spent
campaigning), NOT donations — see the editorial note at the bottom.

Built by `pipeline_sandbox/sipo_expenses_paddle_etl.py` (PaddleOCR re-OCR of the
scanned returns → geometry + 43-constituency closed-set anchor; see
`doc/SIPO_OCR_INVESTIGATION.md`). OCR-derived → every figure must carry a
"verify against the official SIPO PDF (page N)" caveat; the `flag` column marks
rows needing review.

## Statutory spending limits (GE2024) — the validity bound used in the ETL

Per-candidate spending limit by constituency size. Verified **verbatim** against
the SIPO guidelines PDF (figures quoted exactly as printed):

| Constituency size | Statutory spending limit |
|---|---|
| 3-seat | €38,900 |
| 4-seat | €48,600 |
| 5-seat | €58,350 |

"Under no circumstances can total spending on a candidate exceed the statutory
spending limit for the candidate" (Electoral Act 1997 as amended, s.32). The ETL
uses these as `statutory_limit_eur` and flags any extracted expenditure above the
limit as `over_limit_verify` (an impossible value ⇒ OCR misread or wrong
constituency match).

**Cross-check:** Fianna Fáil's "amount assigned to the party" column is a flat
**40%** of these limits (€15,560 / €19,440 / €23,340 = 0.40 × 38,900 / 48,600 /
58,350) — an internal corroboration of both the figures and the assignment
reading. Assignment proportions differ per party, which is why the ETL anchors on
the statutory limit (universal) rather than any party's assigned amounts.

### Other thresholds on the same page (for the future donations dataset)

- €11,200 — maximum reimbursement of election expenses to a candidate.
- €1,000 — max donation a candidate may accept (single source, per year).
- €2,500 — max donation a political party may accept.
- €1,500 — donation declaration threshold (party).

## Source

- **SIPO, _Guidelines for the General Election to the 34th Dáil, 29 November
  2024_** — spending limits in §"Relevant dates & spending limits"; assignment
  mechanism in §1.2 "Assignment of a portion of a candidate's spending limit to
  his/her political party."
  - PDF: <https://assets.sipo.ie/media/283883/b6e53676-bb38-4bfd-8773-565b4cd95135.pdf>
  - Collection: <https://www.sipo.ie/en/collection/30d00-2024-general-election-guidelines/>
- **Per-party expenses returns** (the scanned forms this fact is OCR'd from) —
  SIPO _Dáil General Election 2024_ collection:
  <https://www.sipo.ie/en/collection/2e0c0-dail-general-election-2024/>
  - Fianna Fáil <https://assets.sipo.ie/media/283955/6261d302-2a56-49af-abfc-57e9364c13fe.pdf>
  - Fine Gael <https://assets.sipo.ie/media/283936/7be18f2f-5cdc-4333-9f77-86830b14615c.pdf>
  - Sinn Féin <https://assets.sipo.ie/media/283935/30d4c805-b333-44d4-ac5a-cf45756b0ea5.pdf>
  - Labour <https://assets.sipo.ie/media/283939/d62f2116-9ad3-47ad-b564-415de4f043cb.pdf>
  - Green Party <https://assets.sipo.ie/media/285734/0a5f2c08-0fe3-4b0f-ab73-1c012820c90e.pdf>
  - Social Democrats <https://assets.sipo.ie/media/283937/e6e5a11f-9186-4bc6-928a-12454c9690a3.pdf>
  - People Before Profit/Solidarity <https://assets.sipo.ie/media/285690/859abebd-2f42-4a0e-8c34-045cbebf00ac.pdf>
  - Aontú <https://assets.sipo.ie/media/285737/f2c55c0f-9c08-4b5a-b7c4-48e2f2bf0c87.pdf>
  - (full field incl. Independent Ireland, Independents4Change, Right to Change,
    100% Redress, Irish Freedom, National Party, Ireland First, The Irish People,
    Centre Party also on the collection page)
- Background: [Citizens Information — Election expenses](https://www.citizensinformation.ie/en/government-in-ireland/elections-and-referenda/running-for-office/election-expenses/)

## Editorial note (no-inference rule)

These figures are *spending* by a party's national agent on behalf of its
candidates. A high or low spend is not evidence of anything beyond the spend
itself. Donations (money *received*) are a separate SIPO register
(`2024_election_donations.pdf`, not yet processed) — do not conflate the two, and
never imply influence from either.
