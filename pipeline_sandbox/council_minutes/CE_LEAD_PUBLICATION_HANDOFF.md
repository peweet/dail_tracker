
---

# Electrical breakout (2026-08-08)

`dail_tracker_core/queries/procurement/opportunities.py` now has `electrical_division()`, a
composition-layer override giving electrical work its own sector. Electrical has no CPV
division of its own (it sits under 45/31/09), so the 2-digit `cpv_division` the extractors
emit cannot express it.

Prefixes: 4531, 311, 312, 315, 316, 0931, 0933. Deliberately EXCLUDED: 71.31 (general
engineering consultancy — would relabel civil/structural notices) and 45.34 (fencing and
railing; reads electrical only if you skim the code). No CPV -> keeps its fallback.
13 hand-written cases pass, including both exclusions.

Snapshot result: **Electrical = 12 notices, 10 valued, 9 closing within 30 days, 12 buyers.**
Live and verified on `/api/opportunities?sector=Electrical` — real rows (electrical
installation work at St. Ciaran's Community School and Mountmellick, TII signalling
equipment, DOJ UPS comms room, Dunleer festive illumination).

**This is a presentation-layer override.** Gold still carries the 2-digit division, so a
consumer reading `cpv_division` from the parquet will NOT see Electrical. The durable fix is
a sub-division field in `extractors/ted_ireland_tenders_extract.py` (`CPV_DIV`) and its two
siblings `ted_ireland_extract.py` / `etenders_live_tenders_extract.py`, plus
`sql_views/procurement/procurement_bid_signal.sql` — then a TED/eTenders re-ingest.

## Open at handoff

`/api/contracts` was still serving the previous snapshot (`builtAt` 22:48) while `/api/health`
reported the new one (23:05), so the sector map had not yet shown Electrical. Same Cloudflare
edge-cache lag that made the CE leads look missing; it resolved itself last time within
minutes. Re-check `/api/contracts` for a row `{"sector": "Electrical"}` before assuming a fault.

Also confirmed live: a parallel session's free-data tier is active —
`tier: "reduced"`, withholding `buyers.award_profile`, `suppliers`, `renewals` from anonymous
callers. That is theirs, not part of this work.
