# Bid-intelligence pack — SANDBOX PROTOTYPE

**Status:** experiment. Lives entirely under `pipeline_sandbox/`. It **imports the
existing production procurement query functions read-only and composes them** — it
writes nothing to gold/silver, registers no view, adds no API route, and modifies no
production file. Safe to delete; the app is unaffected.

## What it is

The engineering proof of the MVP described in the bid-intelligence assessment and
`doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`:

> When a relevant tender appears, assemble the historical market-research pack a bid
> manager / QS would otherwise build by hand.

`build_bid_pack(conn, cpv_code=..., buyer=...)` composes, for a tender context (a CPV
category and/or a contracting authority), these blocks — **all from existing
registered views** (`dail_tracker_core/queries/procurement.py`):

| Block | Source query | Grain |
|---|---|---|
| `comparable_awards` | `awards_for_cpv` / `awards_for_authority` | AWARD (ceiling/estimate) |
| `category_benchmark` | `cpv_summary` (median / p25 / p75) | AWARD |
| `market_signal` | `bid_signal` (award band + ceiling band + median bids + single-bid % + SME win %) | AWARD + competition |
| `buyer_awards` | `authority_summary` | AWARD |
| `buyer_payments` | `payments_publisher_profile` (SPENT + COMMITTED, separate) | PAYMENT |
| `active_firms` | derived (count) from `comparable_awards`, companies only | — |
| `incumbent_payment_evidence` | `payments_for_supplier` per leading firm | PAYMENT |
| `rebid_radar` | `expiring_contracts_etenders` filtered to the context | AWARD (advertised term) |

## Files

- `bid_pack.py` — `build_bid_pack(conn, cpv_code=, buyer=)`, the composition engine.
- `render_report.py` — `render_report(pack)`, formats a pack into the client-facing
  Markdown market-research report (the reports-first artifact; presentation only, no
  new figures). The standing caveat is printed top and bottom.
- `run_demo.py` — demo + self-checks; writes `sample_pack.json` (raw structure) and
  `sample_report.md` (the rendered report) for review.

## Run it

From the repo root, with the venv python and UTF-8 forced:

```bash
PYTHONUTF8=1 .venv/Scripts/python pipeline_sandbox/bid_intelligence/run_demo.py
```

It builds packs for CPV `72000000` (IT services) and buyer `Dublin City Council`,
asserts the boundary rails below, prints a structural summary, and writes
`sample_pack.json` + `sample_report.md` next to the code for review.

## Boundary rails (verified by `run_demo.py`, and the graduation path MUST preserve)

- **Not a price.** No bid-price calculation, no win-probability. The data cannot quote
  a job (no project size/area anywhere; 4.5×–15× intra-trade spread; framework ceilings
  14×–79× above real awards — see the `bid_signal` docstring). The standing `caveat`
  states this on every pack.
- **Three money grains never blended.** eTenders awards, EU/TED awards, and public-body
  payments (SPENT vs COMMITTED) are separate, labelled fields — never summed. Demo proves
  the award band and framework-ceiling band stay separate and payment footprints stay
  per-tier. (DCC example: €0 paid / €4.06bn ordered — surfaced as two figures.)
- **No natural person profiled.** Sole-trader/individual awardees are excluded from
  `active_firms` and the payment-evidence block, and name-masked in `comparable_awards`
  (same posture as `awards_for_supplier`).
- **Honest gaps.** Unavailable/empty blocks are reported as such; a buyer with no
  payment-register name match returns a documented name-key-gap note, not a fake €0.

## Known limitations / refinements (before any graduation)

1. **National CPV coverage.** ~71% of eTenders award CPVs are null, so CPV-keyed matching
   misses much of the national side; a title/keyword (FTS) branch is the mitigation.
3. **Buyer-name key mismatch.** Award `contracting_authority` ≠ payment `publisher_name`
   ≠ TED `buyer_name` (no shared key) — `buyer_payments` matches on exact name only and
   says so when it misses. The roadmap flags this reconciliation as a blocking prerequisite
   for any buyer-keyed competition wiring.
4. **Real-terms benchmark.** Not yet applied — the CPA07 / WPM39 deflators exist in gold
   but there is no procurement real-terms view; that is a separate, owner-reviewed wiring.
5. **Live trigger feed.** The national live-tender feed (`etenders_live_tenders`) is itself
   experimental/unwired; the product premise depends on promoting it to a scheduled LIVE
   chain first.

## Graduation path (NOT started — owner sign-off required)

Per the roadmap's user-domain list, do not do these autonomously:

1. Move `build_bid_pack` into `dail_tracker_core/dossiers.py` (composition layer), add a
   `/v1/procurement/bid-pack` route + an MCP tool, with a proper test under `test/`.
2. Resolve the remaining refinements above (esp. the buyer-name reconciliation).
3. Add accounts / saved CPV+buyer watchlists / a scheduled tender-feed diff + delivery
   (the gated Phase 5 — new PII/consent/GDPR + scheduler infrastructure).
