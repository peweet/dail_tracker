# Receiver-appointers: who's calling in Irish loans — feature idea

Status: deferred — drafted 2026-05-31, scoped from the 2,620 receivership notices in the Iris corpus
Companion to: [company_register_notices_feature.md](company_register_notices_feature.md) (parked, broader), [public_appointments_feature.md](public_appointments_feature.md) (live)

## The civic question this answers

> "Who has been calling in Irish loans, at what scale, since 2016?"

The 2,620 receivership notices in Iris Oifigiúil name the **appointing party** (the bank or fund holding the loan) and the receiver they appoint. Aggregating that surfaces a hidden-in-plain-sight pattern: the SPV brand names on the notices ("Promontoria", "Beltany", "Ennis", "Pentire") are the Irish vehicles for the international funds that bought distressed Irish loan books after 2010 (Cerberus, Goldman Sachs, Cabot, etc.). Most readers don't know that translation. A view that does the translation, and totals the activity, makes it legible.

## What the data shows (scan, 2026-05-31)

Top appointing parties named in receivership notices (raw mentions across all 2,620 notices):

| Appointing party | Notices | What it is |
|---|--:|---|
| Bank of Ireland | 697 | Irish bank |
| Allied Irish Banks / AIB | 296 + 145 | Irish bank |
| Ennis | 293 | SPV brand (Cabot) |
| Promontoria | 251 | Cerberus's Irish SPV (AIB loan books) |
| Ulster Bank | 159 | Irish bank (winding down) |
| Everyday Finance | 116 | Credit servicer (Cerberus) |
| Pepper | 73 | Pepper Finance Ireland |
| KBC | 58 | (exited Ireland) |
| Pentire | 51 | SPV brand |
| Permanent TSB / PTSB | 20 + 1 | Irish bank |
| Beltany | 17 | Goldman Sachs Irish SPV |
| Goldman | 14 | parent name |

Caveat: Irish-bank receiverships (BoI 697, AIB 441 combined, Ulster 159, KBC 58, PTSB 21) dominate the volume. The vulture-fund SPV subset is a real but **minority slice**.

## Why this is worth surfacing

- **Recognition.** The brand-to-parent translation is the value. "Promontoria appointed X receivers in 2019" reads very differently from "Cerberus appointed X receivers in 2019".
- **Scale.** Aggregating receivership notices by year × appointing party shows the *receiver wave* and who carried it.
- **Public record evidence**, not investigative claim. Receivers are appointed under contract; naming the appointer is factual, not pejorative.

## Where the value is bounded (honest)

- **It's a corporate/loan-book dataset, not a parliamentary one.** Direct audience overlap with "citizens checking their TD" is roughly zero.
- **The political accountability angle only emerges via cross-reference** with other Dáil Tracker datasets (PQs, lobbying, member interests, Finance Acts that touched Section 110 / IREF / fund taxation). Standalone, this is corporate news.
- **No wrongdoing implied.** This is a register of legal acts; the story is *scale and concentration*, not malfeasance.
- **Receiver-side noise.** The entity_name extractor has known issues (liquidator-firm names captured instead of company names on ~80 rows); same pattern would affect receiver-appointer extraction unless we harden the pattern.

## Shape if built

- Source: the same `iris_notice_events_clean.csv`, filtered to `notice_subtype == "receivership"` (≈2,620 rows after the A1/A2 reclassification).
- Enrichment needed: a curated **brand → parent fund** lookup (Promontoria → Cerberus, Beltany → Goldman Sachs, Ennis → Cabot, Everyday Finance → Cerberus, etc.). Lives in `data/_meta/loan_book_fund_aliases.csv` once curated.
- View: `v_loan_book_receivers` exposing date · appointing_party_brand · parent_fund · receiver · debtor · property/security (where extractable).
- Page: a ranked "Top appointing parties" view, a year-by-year trend (the receiver wave), and a searchable feed by debtor name (the most common civic-search route: "was my company / my address subject to a receiver?").
- Provenance and bounded claims, in editorial copy.

## Path forward

- This sits alongside the parked **company/register notices** doc but is a sharper, more defensible story than the broader business-gazette idea.
- If the wider fund-taxation topic-tracker is ever scoped (see the (A) angle from session notes), this receiver feed becomes one input, not a standalone page.
- Otherwise, a one-page version focused on appointing-party recognition + scale + trend is justifiable on its own.
