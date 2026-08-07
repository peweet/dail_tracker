# CE-report leads → PublicSignal: state and next steps (2026-08-07)

Goal: get council CE-report pre-tender leads persisted and usable on publicsignal.ie.
The scrape and corpus rebuild are done. Publication is blocked on one missing measurement.

## Done this session

**Harvest widened.** `ce_report_harvest.py --all-confirmed --latest-per-council 12
--subpage-cap 14`: manifest 21 → **186 records**, 160 documents fetched, 19 already cached.
Most councils now hold a full 12 months instead of one or two.

**Corpus rebuilt.** `extractors/council_ce_reports_corpus_build.py`:
**15,227 chunks / 4,475 leads from 171 documents / 16 councils**, report months spanning
2019-01 to 2026-08 (49 distinct months). Was 540 leads from 18 documents.
Leads per council: Fingal 968, Wicklow 678, Kildare 404, Louth 370, Clare 349,
Tipperary 331, Galway County 329, Laois 245, Leitrim 173, Kilkenny 167, Westmeath 136,
Offaly 92, Dún Laoghaire-Rathdown 90, Carlow 69, South Dublin 38, Waterford 36.

`promotion_permitted` is **0 of 4,475** — expected, see the blockers below.

## Harvest gaps, sorted by what fixes them

Fix these in `ce_report_seeds.csv` / the discovery patterns, not in the fetch code.

| Council | Symptom | Fix |
|---|---|---|
| Limerick, Wexford | seed URL 404s | page moved — re-find the landing URL |
| Meath, Sligo | `SSLCertVerificationError` | server omits the intermediate cert; needs a CA bundle or an explicit exception |
| Roscommon, Kerry | listing loads, 0 links match | `_REPORT_HINT` doesn't match their link text |
| Cork County | harvested, published 0 | 3 documents need OCR, 7 have an unresolved `report_month` |
| Carlow | only 2 documents | archive appears to expose only 2; verify |

Councils never reached: the 3 `engineering` and 4 `absent` seeds were out of scope by design.

## Blockers on publication

**1. The review gate is structurally sealed — a real bug.**
`extractors/council_ce_reports_corpus_build.py:142-148` requires `reviewed_project_name`
and `reviewed_stage` from the ledger. `ce_report_lead_reviews.csv`'s header is
`lead_id,reviewer_state,relevance_status,site_relationship,reviewer,reviewed_utc,note` —
**neither required column exists**. Any row added today fails `bool(project_name)` and can
never promote. The ledger is also empty and untracked in git, so the 28 assisted review
decisions behind the currently-live 28 leads are lost.

**2. Precision has never been measured.** `CE_REPORTS_QUALITY.md:96` states this plainly —
the promoted set was deliberately conservative and "not a random sample", so it estimates
nothing about the strict net's false-positive rate.

Recall **is** measured and holds: **0.667 [CI 0.582–0.754]**, from `ce_lead_recall_labels.json`
— 60-row seeded random sample of a 951-row gap queue, two independent labellers who saw
neither the strict net's output nor each other, 98% agreement, positive only when both agreed.
Note that measurement was taken against the 540-lead corpus; re-running it against 4,475 is
worthwhile but the anchors did not change, so it is still the best available estimate.

## The design decision waiting to be made

The three corpora that already publish (`semi_state_minutes`, `pq_*`, `council_minutes_part8`)
have **no human review gate at all** — `extractors/pre_tender_leads_promote.py` validates a
schema and ships them straight to gold. CE reports are the only lane with a per-lead human
gate, which is why it has never published at scale.

Joining that pattern means emitting a `council_ce_pre_tender_signals.parquet` matching
`pre_tender_leads_promote.REQUIRED_COLUMNS` and publishing at the `Extracted` band.

**Do not flip `promotion_permitted` before measuring precision.** Swapping a human gate for
an unmeasured automatic one is the "safety rationale written before measured" failure. The
order is: sample the 4,475 with a fixed seed → label against the source quotes under a
written rubric → report precision with a Wilson CI → then choose the publication rule.

A single-labeller measurement is weaker than the two-labeller recall probe and must say so.

## Remaining steps

1. Measure precision on a seeded random sample of the 4,475 leads.
2. Fix the ledger header to carry all five gate fields.
3. Write the promote adapter → `council_ce_pre_tender_signals.parquet` → gold via
   `save_parquet` with a row floor.
4. Write the quality report with `## Completeness` and `## Recall` sections, or
   `tools/check_conventions.py` R12 fails.
5. Wire into `build_snapshot.py` and the PublicSignal early-signals view, then redeploy.

## Watch out

A parallel session is editing this lane (`opportunities.py`, `build_snapshot.py`) and the
PublicSignal worker. Check `git status` before staging, and stage explicit paths only.
