---
tier: SPEC
status: LIVE
domain: procurement
updated: 2026-06-28
supersedes: []
read_when: building the tender-alert email / bid-intelligence notification system (Phase 5, owner-gated)
key: SPEC|LIVE|procurement
---

# Tender-Alert & Bid-Intelligence Email System — Design

**Date:** 2026-06-28
**Status:** DESIGN. This is the detailed build-out of **Phase 5** of
[PROCUREMENT_INTELLIGENCE_ROADMAP.md](PROCUREMENT_INTELLIGENCE_ROADMAP.md) ("Per-user persistence —
the architectural lift — GATED on owner sign-off"). Nothing here is built. It carries new
identity + PII (emails) + GDPR/consent + a transactional write-store + email-sending + a scheduler —
none of which exist today — so the **go/no-go and all user-domain wording decisions stay with the owner.**

**Product in one line:** a user saves a profile; when a new relevant tender appears, the system emails
a brief with the historical search already done — buyer history, comparable awards (inflation-adjusted),
incumbents, competition signal, and realised-payment evidence — every figure money-grain-labelled and
source-linked.

---

## 0. What already exists vs. what is greenfield

The whole value proposition — "the historical search already done" — is **reuse**. The novel work is a
thin **operational layer** (subscribers, matching state, email) bolted onto the mature analytical lake.

| Capability | Status | Reuse / Build |
|---|---|---|
| Open-tender feeds | ✅ exists | `v_procurement_live_tenders` (eTenders national), `v_procurement_ted_tenders` (TED) |
| Award/CPV/buyer/competition history | ✅ exists | `procurement_*` views; `dail_tracker_core/queries/procurement.py` |
| Supplier dossier composition | ✅ exists | `dail_tracker_core/dossiers.build_supplier_dossier()` |
| Inflation adjustment | ✅ exists | `services/deflator.py` (`Deflator.inflate`), gold `cso_cpi_deflator.parquet` |
| Name-join key | ✅ exists | `shared/name_norm.py::name_norm_expr()` |
| Read connection over all views | ✅ exists | `dail_tracker_core/connections.py::api_conn()` / `connect_with_views()` |
| Export + caveat/licence strings | ✅ exists | `api/routers/exports.py` (`/v1/data`), `utility/ui/export_controls.py::export_button` |
| Freshness / new-data detection | ✅ exists | coverage JSON `held_through`, `tools/procurement_source_poller.py`, `tools/freshness_heartbeat.py` |
| Scheduled cloud jobs | ✅ pattern | `.github/workflows/*.yml` (`schedule:`), Windows Task (`tools/*.ps1`) |
| Cookieless analytics | ✅ pattern | `utility/ui/page_analytics.py` (append-only JSONL) |
| **Email sending** | ❌ greenfield | new `services/notify/` (SMTP/provider) |
| **Subscriber identity + consent** | ❌ greenfield | new operational DB |
| **Transactional write-store** | ❌ greenfield | new SQLite (MVP) → Postgres; the lake is read-only parquet |
| **In-app scheduler** | ❌ greenfield | run via existing GH-Actions/Task cron, not APScheduler |
| **Self-serve account UI** | ❌ greenfield | deferred to v2 (MVP seeds profiles by curated CSV) |

**Architectural rule we inherit:** keep the **analytical read lake** (DuckDB over committed parquet,
firewalled, no writes) physically separate from the **operational write DB** (subscribers, matches,
sends). The lake is the enrichment source only; nothing in the alert system writes to it. This mirrors
the existing logic-firewall split (analytical vs. operational), and means the alert DB can live and be
backed up independently.

---

## 1. The money-grain frame (the dominant correctness rail)

Four grains appear in this product. They are **never summed, never differenced, never stacked** — each
renders as its own labelled block. (Extends the 3-grain rule with the PLANNED tier the live-tender views
introduce.)

| Grain | `value_kind` | Source here | Means | Summable? |
|---|---|---|---|---|
| **PLANNED** | `estimate_advertised` | the alerting tender itself | buyer's pre-award estimate | **never** |
| **AWARD / CEILING** | `contract_award_value` / `framework_or_dps_ceiling` | `procurement_awards`, `ted_ie_*` | what was awarded (ceiling) | only `value_safe_to_sum` rows |
| **PAYMENT** | `payment_actual` (SPENT) / `po_committed` (COMMITTED) | `procurement_payments_fact` | what was actually paid/ordered | within one tier only; SPENT≠COMMITTED |
| **BUDGET** | `budget_allocated` | AFS/CSO (context only) | allocation | never (different grain) |

The tender that triggers the alert is **PLANNED**. Every historical figure we attach is **AWARD** or
**PAYMENT**. The email must make it impossible to read "this €400k tender" + "€2.1m paid to incumbent"
as one number.

---

## 2. Tender ingestion

### 2.1 Where new tenders come from (both already ingested daily)

| Lane | View / file | Extractor | CPV? | Region/NUTS? | Value | Key |
|---|---|---|---|---|---|---|
| **eTenders national** | `v_procurement_live_tenders` / `etenders_live_tenders.parquet` | `extractors/etenders_live_tenders_extract.py` (Playwright; `tools/poll_live_tenders.ps1`) | **❌ none** | ❌ none | `estimated_value_eur` (PLANNED) | `resource_id`, `buyer_org_id` |
| **TED (EU journal)** | `v_procurement_ted_tenders` / `ted_ie_tenders.parquet` | `extractors/ted_ireland_tenders_extract.py` (`ted_tenders` chain) | **✅ `cpv_code` + `cpv_division`** | ❌ none in view | `estimated_value_eur` (PLANNED) | `publication_number` |

**Load-bearing asymmetry (this shapes the entire MVP):** the national eTenders lane — which is the only
source of **sub-EU-threshold** opportunities (schools, councils, water schemes) — carries **no CPV and no
region**. Only title + buyer + procedure + value + deadline. TED carries CPV but only the EU-threshold top
slice. So:
- **CPV matching works on TED today; on eTenders it must fall back to keyword/buyer** until the CPV
  enrichment in §9 lands (this is the exact "national eTenders 0/2,363 CPV-filled" prerequisite the
  roadmap flags for Phase 5).
- **Geography matching is weak on both** (no NUTS in either view) — derive an approximate region from the
  buyer via a buyer→region crosswalk, and **label it as derived** (v2 adds real NUTS from TED BT codes).

### 2.2 Cadence
Reuse the existing daily refresh — the alert job is a **post-refresh consumer**, it does not re-fetch:
1. `tools/poll_live_tenders.ps1` refreshes the eTenders snapshot (daily).
2. The `ted_tenders` pipeline chain refreshes TED tenders (daily, in the money-flow cloud workflow).
3. The **detect** job (§5 step 2) runs immediately after, diffing the refreshed views against the
   `detected_tender` ledger. Heartbeat the lane via `tools/freshness_heartbeat.py record("tender_alert", …)`
   so staleness is visible in the existing freshness dashboard.

### 2.3 Required fields (the canonical `detected_tender` record)
`source` · `source_uid` (publication_number | resource_id) · `buyer_name` · `buyer_org_id` ·
`buyer_norm` (= `name_norm_expr(buyer)`) · `title` · `cpv_code` (nullable) · `cpv_division` (nullable) ·
`estimated_value_eur` (PLANNED, nullable) · `procedure_type` · `submission_deadline` · `detail_url` ·
`retrieved_utc` · `content_hash` · `first_seen_utc` · `last_seen_utc` · `superseded` (bool).

### 2.4 Deduplication
- **Within-source:** `source_uid` is the natural key (`publication_number` / `resource_id`). Upsert on it.
- **Cross-source:** a contract over the EU threshold appears in **both** eTenders and TED. Collapse with a
  `content_hash = sha1(buyer_norm | title_norm | submission_deadline)` where `*_norm` uses
  `name_norm_expr`. When a TED row and an eTenders row share a hash, keep both rows but link them
  (`merge_group_id`) so a subscriber is alerted **once**, preferring the TED row (it carries CPV).
- **Change detection (not just new):** if `source_uid` is already known but `content_hash` changed (e.g.
  deadline extended, value revised), mark the prior row `superseded=true`, insert the new one, and emit a
  lightweight "updated" signal rather than a fresh "new tender" alert.

### 2.5 Classification
- TED: `cpv_code` + `cpv_division` are present — use directly.
- eTenders (no CPV): assign a **provisional** `cpv_division` from a keyword→sector map over `title`
  (+ the eTenders `procedure`/category text when present), and stamp `cpv_source = 'derived_keyword'`
  with `cpv_confidence = 'low'`. Never present a derived CPV as authoritative. The real fix is §9.

---

## 3. User-profile model

A profile is one saved search owned by a subscriber. Arrays stored as JSON (SQLite) / native arrays
(Postgres).

| Field | Type | Notes / matching role |
|---|---|---|
| `profile_id` | uuid | PK |
| `subscriber_id` | fk | owner (identity in §6) |
| `label` | text | "Modular school builds, Leinster" |
| `company_name` | text | the user's own firm |
| `company_cro_num` | int? | optional; lets us self-exclude and recognise the user's own incumbency |
| `trade_categories` | text[] | free-text trades → mapped to CPV families |
| `keywords` | text[] | matched against tender `title` (accent-folded substring/token) |
| `cpv_codes` | text[] | exact + **prefix** match (CPV is hierarchical: 45 → 4521 → 452151) |
| `target_buyers` | text[] | `buyer_org_id` (exact) and/or `buyer_norm` |
| `target_regions` | text[] | NUTS / county; **derived-only today** (buyer→region crosswalk) |
| `value_min_eur` | float? | PLANNED-estimate band; null = no floor |
| `value_max_eur` | float? | null = no ceiling |
| `excluded_terms` | text[] | hard veto on `title` |
| `excluded_buyers` | text[] | hard veto on buyer |
| `known_competitors` | text[] | `name_norm` keys → enrich "who usually wins here" |
| `frameworks_on` | text[] | framework/DPS names or `parent_agreement_id`s the firm sits on → flags call-off relevance |
| `email_frequency` | enum | `immediate` \| `daily` \| `weekly` |
| `min_match_band` | enum | `strong` \| `possible` \| `weak` (suppress noise) |
| `active` | bool | soft on/off |
| `created_utc` / `updated_utc` | ts | |

**Value-band caveat baked in:** because PLANNED estimates are coverage-limited, a tender with a **null
estimate is NOT excluded** by `value_min/max` — it passes with a "value not advertised" note. Filtering a
null estimate out would silently hide real opportunities.

---

## 4. Matching

All aggregation/JOINs stay in registered views or `dail_tracker_core/queries/` (logic firewall); the
matcher itself is row-wise Python over the already-materialised tender + profile records.

### 4.1 Components & scoring

```text
function match(profile, tender) -> MatchResult:
    reasons = []          # human-readable, drives "why this matched"
    veto    = false

    # ---- HARD NEGATIVE FILTERS (any hit => drop, regardless of score) ----
    title_norm = fold(tender.title)
    if any(fold(t) in title_norm for t in profile.excluded_terms):  veto = true
    if tender.buyer_org_id in profile.excluded_buyers
       or tender.buyer_norm in map(name_norm, profile.excluded_buyers): veto = true
    if veto: return MatchResult(band='excluded', score=0, reasons=['excluded by negative filter'])

    score = 0; max_score = 0

    # ---- CPV (only when the tender carries a real CPV; TED yes, eTenders usually no) ----
    if tender.cpv_code and tender.cpv_source != 'derived_keyword':
        max_score += W_CPV
        if exact_or_prefix_match(tender.cpv_code, profile.cpv_codes):
            score += W_CPV
            reasons.append(f"CPV {tender.cpv_code} ({tender.cpv_division}) matches your categories")
    # note: if CPV is derived/absent we DON'T penalise — we lean on keyword/buyer and flag coverage

    # ---- KEYWORD ----
    if profile.keywords:
        max_score += W_KW
        hits = [k for k in profile.keywords if fold(k) in title_norm]
        if hits:
            score += W_KW * min(1.0, len(hits)/len? )   # presence-weighted, capped
            reasons.append(f"Title matches keyword(s): {', '.join(hits)}")

    # ---- BUYER ----
    if profile.target_buyers:
        max_score += W_BUYER
        if tender.buyer_org_id in profile.target_buyers \
           or tender.buyer_norm in map(name_norm, profile.target_buyers):
            score += W_BUYER
            reasons.append(f"Buyer {tender.buyer_name} is on your watch list")

    # ---- VALUE BAND (PLANNED estimate; null passes with a flag) ----
    if profile.value_min_eur or profile.value_max_eur:
        max_score += W_VALUE
        if tender.estimated_value_eur is null:
            reasons.append("Estimated value not advertised (passed your band by default)")
            score += W_VALUE * 0.5        # partial — unknown, not disqualifying
        elif within(tender.estimated_value_eur, profile.value_min_eur, profile.value_max_eur):
            score += W_VALUE
            reasons.append(f"Estimated €{tender.estimated_value_eur:,.0f} is within your band (a PLANNED estimate)")
        else:
            # outside band but other signals may still carry it; no veto unless user opted in
            reasons.append("Estimated value is outside your band")

    # ---- GEOGRAPHY (DERIVED today — buyer→region; labelled, low weight) ----
    if profile.target_regions:
        max_score += W_GEO
        region = derive_region_from_buyer(tender.buyer_org_id)   # crosswalk, may be null
        if region and region in profile.target_regions:
            score += W_GEO
            reasons.append(f"Buyer is in {region} (region derived from buyer, not the notice)")

    relevance = score / max_score if max_score else 0     # 0..1 RELEVANCE — never a win-probability
    band = 'strong' if relevance >= 0.66 else 'possible' if relevance >= 0.33 else 'weak'
    return MatchResult(band, relevance, reasons)
```

`fold()` = lower + accent-strip (the same NFKD fold `name_norm_expr` uses; reuse that helper rather than
re-implementing). `name_norm` = `shared/name_norm.py::name_norm_expr` applied to the buyer/competitor
strings. Default weights (tunable, owner sign-off): `W_CPV=0.40, W_KW=0.25, W_BUYER=0.20, W_VALUE=0.10,
W_GEO=0.05`. CPV dominates **when present**; on eTenders the denominator drops to keyword+buyer+value,
which is honest about reduced confidence.

### 4.2 Confidence-score discipline
- The number is **profile relevance**, surfaced as a band (`strong`/`possible`/`weak`), never as a percent
  chance of winning. The word "probability"/"chance of winning" is on the FORBIDDEN list (§7).
- Negatives are **hard vetoes**, not score deductions — an excluded term must never be out-voted by a high
  CPV score.
- A match is recorded once per (profile, tender `merge_group_id`) in the `match` table so the same
  opportunity is never re-alerted (even if it resurfaces in the other source).

---

## 5. Background job flow

```
┌─ (existing, daily) refresh ─────────────────────────────────────────────┐
│  poll_live_tenders.ps1  →  etenders_live_tenders.parquet                 │
│  ted_tenders chain      →  ted_ie_tenders.parquet   →  views refreshed   │
└──────────────────────────────────────────────────────────────────────────┘
        │   read-only DuckDB via connect_with_views(["procurement_*.sql"])
        ▼
[2] DETECT   diff v_procurement_live_tenders + v_procurement_ted_tenders
             against detected_tender ledger → upsert (new / superseded), dedupe (§2.4)
        ▼
[3] MATCH    for each ACTIVE profile × each NEW/updated tender → match() (§4)
             write rows to `match` (skip if (profile, merge_group) already alerted)
        ▼
[4] ENRICH   for each match ≥ profile.min_match_band → build brief (§ enrichment)
             reuse queries/procurement.py + dossiers + services/deflator.py
        ▼
[5] COMPOSE  group matches per subscriber per frequency window
             immediate → one brief; daily/weekly → digest. Render → `report`
        ▼
[6] SEND     services/notify → SMTP/provider → `email_send` (status, provider id)
             honour double-opt-in + unsubscribe; back off on bounce
        ▼
[7] FEEDBACK one-click links in email (relevant? bid? won/lost?) → `feedback`
```

- **Orchestration:** reuse the existing scheduled-runner pattern — a GitHub Actions workflow with
  `schedule:` (cloud) or a Windows Task (`tools/register_*_task.ps1` precedent) for local. **No in-app
  scheduler.** Steps 2–5 run every refresh; step 6 runs on the frequency cadence (immediate after each
  run; daily/weekly digests batched).
- **Idempotency & safety:** each step is restartable; `email_send` is written `queued` → `sent` so a crash
  never double-emails. Respect the existing memory rule *no blind background python* — the job logs and
  exits with structured codes like the pollers.

---

## 6. Data persistence (the new operational DB)

A transactional store, **separate from the parquet lake**. SQLite single-file for MVP (zero infra,
backs up to R2 alongside the existing data backup); swap to Postgres when self-serve signup lands.

```sql
-- identity + consent (PII lives ONLY here)
subscriber(
  subscriber_id PK, email, email_verified bool, verify_token, verified_utc,
  consent_marketing bool, consent_utc, unsubscribe_token, status,        -- active|paused|unsubscribed
  created_utc, last_email_utc)

profile(                          -- §3; arrays as JSON in SQLite
  profile_id PK, subscriber_id FK, label, company_name, company_cro_num,
  trade_categories, keywords, cpv_codes, target_buyers, target_regions,
  value_min_eur, value_max_eur, excluded_terms, excluded_buyers,
  known_competitors, frameworks_on, email_frequency, min_match_band,
  active, created_utc, updated_utc)

detected_tender(                  -- §2.3 dedup ledger; mirrors the live views, never written to the lake
  detected_id PK, source, source_uid, merge_group_id, buyer_name, buyer_org_id, buyer_norm,
  title, cpv_code, cpv_division, cpv_source, estimated_value_eur, procedure_type,
  submission_deadline, detail_url, content_hash, first_seen_utc, last_seen_utc, superseded bool,
  UNIQUE(source, source_uid))

match(                            -- one alert decision per (profile, opportunity)
  match_id PK, profile_id FK, merge_group_id, detected_id FK,
  relevance_score, band, reasons_json, matched_utc,
  UNIQUE(profile_id, merge_group_id))

report(                           -- the generated brief (so we can resend / audit)
  report_id PK, subscriber_id FK, profile_id FK, match_ids_json,
  subject, body_html, body_text, enrichment_json, source_snapshot_utc, created_utc)

email_send(                       -- delivery ledger; never double-send
  send_id PK, report_id FK, subscriber_id FK, status,                    -- queued|sent|bounced|failed
  provider_message_id, error, queued_utc, sent_utc)

feedback(                         -- closes the loop; trains v2 scoring
  feedback_id PK, match_id FK, subscriber_id FK,
  signal,                                                                -- relevant|not_relevant|will_bid|wont_bid|won|lost
  note, created_utc)
```

Retention/GDPR: `subscriber` is the only PII table; unsubscribe purges it and cascades. `detected_tender`
holds only public procurement data (no PII). Sole-trader / individual buyer or winner names inherit the
existing quarantine — never surfaced in a brief.

---

## 7. Safety rails (mapped to enforcement)

Reuse the roadmap's Phase-0 #5 plan: a `utility/ui/safe_vocab.py` (or a `services/notify/safe_vocab.py`
sibling) holding APPROVED phrases + a FORBIDDEN list, **CI-checked against rendered email templates**.

| Rail | Enforcement |
|---|---|
| **No win-probability** | the score renders only as `strong/possible/weak` *relevance*; FORBIDDEN: "win probability", "chance of winning", "likely to win". |
| **No recommended bid price** | historical figures shown as **AWARD bands** (`p25/median/p75` from `v_procurement_cpv_summary`, deflated) labelled "what similar contracts were *awarded* — a ceiling, not a recommendation". FORBIDDEN: "you should bid", "recommended price", "target price". |
| **No causation** | single-bid rate, lobbying overlap, incumbency are **structure facts** carrying the verbatim caveat ("a single bid is a recorded fact, never a verdict"; reuse `procurement_competition` caveat string). |
| **No defamatory language** | neutral copy only; FORBIDDEN: "rigged", "corrupt", "waste", "cronyism", "influence-peddling". Sole-trader/individual privacy quarantine inherited. |
| **Money-grain labels** | every figure carries a grain badge (PLANNED estimate / AWARD ceiling / EU award / CASH paid / PO committed); the email template forbids a combined total; no stacked bars (a stack reads as a sum). |
| **Source provenance** | every figure links to its source view + the upstream `detail_url` / `source_file_url`; reuse the `api/routers/exports.py` caveat + CC-BY/TED attribution strings in the footer; coverage is stated as a **floor** ("at least N from public records — indicative, not audited"). |

---

## 8. Historical enrichment + report generation

For each alerted tender, the brief is assembled from existing analytical surfaces (no new money figures
invented — every value REPORTS a computed one). Function names below are the **verified** ones in
`dail_tracker_core/queries/procurement.py` (signatures checked 2026-06-28):

```text
function build_brief(match, tender, conn):                 # conn = read-only api_conn()/connect_with_views
    cpv = tender.cpv_code; year = current_year
    # ⚠️ resolve the tender buyer into each register's OWN naming space first (see §8.1)
    buyer_keys = resolve_buyer(tender)   # -> {authority, competition_buyer, payments_publisher}  (may be partial)

    # ── similar historical awards (AWARD grain) — median/IQR ALREADY computed ──
    cpvrow = first(r for r in queries.cpv_summary(conn, limit=None).data       # v_procurement_cpv_summary (per CPV code)
                   if r.cpv_code == cpv)                                        # exact code; CPV-FAMILY median = small new agg (§8.2)
    awards = queries.awards_for_cpv(conn, cpv).data                            # the award list (value/bids/supplier/duration)
    bands  = deflate_bands(cpvrow, to=year)   # Deflator.load().inflate(median/p25/p75, award_year, to=year) → "in {year} prices"

    # ── same-buyer history (AWARD) ──
    buyer_awards = queries.awards_for_authority(conn, buyer_keys.authority).data if buyer_keys.authority else []

    # ── competition signal (TED 2024+) — pick this buyer out of the ranking ──
    compete = first(r for r in queries.competition(conn, min_lots=0).data      # v_procurement_competition
                    if r.buyer_name == buyer_keys.competition_buyer)           # None if buyer not reconciled (§8.1)

    # ── incumbents / likely competitors (AWARD) — DERIVED from the award list (no cpv_top_parties fn today) ──
    incumbents = top_suppliers(awards, by='supplier_norm')                     # roadmap Phase-2 v_procurement_cpv_top_parties = v2
    knownco    = [c for c in map(name_norm, profile.known_competitors)
                  if c in {a.supplier_norm for a in awards}]                    # has a watched competitor won here?

    # ── expiring incumbent contract (re-tender signal) — filter the national view to this buyer/CPV ──
    expiring = [r for r in queries.expiring_contracts_etenders(conn, months_ahead=24, limit=None).data
                if r.buyer_name == buyer_keys.authority and r.cpv_code == cpv]

    # ── realised PAYMENT evidence (SPENT/COMMITTED — separate grain, labelled, NEVER summed with awards) ──
    paid = queries.payments_publisher_profile(conn, buyer_keys.payments_publisher).data \
           if buyer_keys.payments_publisher else None                         # paid_safe_eur + ordered_safe_eur side by side

    # ── SME / bid-count signals (coverage-limited) — from the award rows, not cpv_summary ──
    sme = sme_share_where_reported(awards)   # n_bids_received / SME cols live on procurement_awards, ~coverage-limited

    return Brief(tender, match.reasons, bands, buyer_awards, compete, incumbents,
                 knownco, expiring, paid, sme, caveats=STANDARD_CAVEATS)
```

`build_supplier_dossier(conn, supplier_norm)` (verified) is reused verbatim to expand any incumbent the
user clicks through to. `Deflator` is `services/deflator.py::Deflator.load().inflate(value, year, to=)`.

### 8.1 Buyer-key reconciliation — the one real integration dependency
The alerting tender's buyer is **not** stored the same way as the buyer in the history registers. Four
distinct naming spaces must be reconciled, and three of these are already flagged as open in the roadmap:
- tender (eTenders) `buyer` / `buyer_org_id`  →  `v_procurement_competition.buyer_name` (TED) — roadmap
  **must-fix #2, BLOCKING** (TED `buyer_name` ↔ eTenders `contracting_authority` mismatch).
- tender buyer  →  `v_procurement_authority_summary.contracting_authority` (eTenders authority space).
- tender buyer  →  `v_procurement_payments.publisher_name` (payments publisher space).

So `resolve_buyer()` is a **buyer crosswalk** — and this crosswalk is **already designed as a shared
artifact**: [BUYER_DOSSIER_DESIGN.md](BUYER_DOSSIER_DESIGN.md) specifies a fail-closed curated
`data/_meta/procurement_publishers/buyer_xref.csv` (`buyer_id, display_name, etenders_org_id,
ted_buyer_name_norm, payments_publisher_id, council_slug, match_tier`) plus a
`shared/buyer_clean.org_id_expr()` helper that makes **eTenders ↔ TED joinable via their shared OGP
org-id lineage** (the `_<orgid>` suffix). **Reuse status (checked 2026-06-28):** `org_id_expr(col="buyer")`
**already ships** in `shared/buyer_clean.py` (alongside `clean_name_expr` / `clean_buyer_display`) — the
eTenders↔TED half is solved. The genuinely hard, **still-unbuilt** gap is awards(OGP id) ↔
payments(`publisher_id`) — different registries, no shared key — which only `buyer_xref.csv` closes
(seeded from `publishers_seed.csv`, ~88 bodies; **the CSV does not exist yet**). **The alert system must REUSE `buyer_xref.csv`, not
invent its own**, and carry its `match_tier` through to the email: below `curated_exact`, cross-register
fusion is **SUPPRESSED, not guessed** ("no competition/payment data matched for this buyer"). Fuzzy buyer
matching is how a brief becomes defamatory (attributing one body's single-bid rate to another). This
shared crosswalk is the highest-risk dependency and must exist + be tested before any send.

> **Demonstrated against live data (2026-06-28):** the same buyer is `"University of Galway (ID 1400)"`
> in `procurement_by_authority` but `"University of Galway"` in `procurement_competition` — the org-id
> suffix is present in one register and stripped in the other, so a naive `buyer_name ==` join **silently
> returns nothing**. The crosswalk must therefore normalise the org-id/roll-number suffix
> (`name_norm_expr` already does much of this) *before* matching. Separately, `"Beaumont Hospital"` (the
> example tender's buyer) does not appear among the top contracting authorities — an HSE acute hospital
> may procure under its own name **or** under `"Health Service Executive (HSE)"`, so a hospital-level
> tender may need the parent-body record for meaningful same-buyer history. Both are coverage facts to
> disclose, not gaps to paper over with a fuzzy guess.

### 8.2 CPV granularity
`cpv_summary` / `v_procurement_cpv_summary` key on the **exact 8-digit `cpv_code`**. A profile usually
wants a **family** (e.g. `45` construction, `4521` building). Exact-code median works today; a faithful
**family median** can't be recombined from sub-medians, so it's a small new view grouped by CPV prefix
(or, MVP-acceptable, show the exact-code band + a "within CPV family 45xx" award count from
`awards_for_cpv` filtered by prefix). Label which granularity is shown.

> **Demonstrated (2026-06-28):** the example tender's CPV `45215140` (health-facility construction)
> does **not** appear in the per-code summary at all — it rolls up. Real division/group medians that
> exist today: `45000000` *Construction work* median **€494,956** (IQR €167,770–€2,043,290, n=442 valued);
> `45210000` *Building construction work* median **€976,917** (IQR €362,400–€2,986,490, n=89);
> `45200000` median **€407,253**. So an alert on a deep CPV must roll up to the nearest populated
> ancestor and **say which level it used** ("typical for CPV 45 *Construction work*"), never imply the
> band is for the exact 8-digit code.

### Report sections (each money figure grain-badged + source-linked)
subject · tender summary · **why this matched** (`match.reasons`) · buyer history · similar awards
(deflated) · competitor/incumbent analysis · inflation-adjusted benchmarks · payment evidence ·
QS notes (neutral) · caveats · source links · **CSV export link** (the API `/v1/data` resource +
`export_button`, value-safe columns + literal caveat column).

---

## 9. Example email

> **Subject:** New tender — Beaumont Hospital, building works (~€400k est.) · matches *“HSE/health
> construction, Leinster”* · closes 24 Jul

> **A new tender matches your saved profile “HSE/health construction, Leinster.”**
>
> **The opportunity** &nbsp;`PLANNED — buyer estimate, not money awarded`
> **Beaumont Hospital** — *Construction work for health facilities* (CPV 45215140)
> Estimated value **€400,000** · Procedure: **Open** · Published 25 Jun 2026 · **Closes 24 Jul 2026 (26 days)**
> [View the notice on TED →](https://ted.europa.eu/en/notice/-/detail/439546-2026)
>
> **Why we sent this**
> • CPV 45215140 (Construction → health facilities) is in your categories
> • Title matches your keyword *“hospital”*
> • Buyer region **Dublin** is in your target list *(region derived from the buyer, not stated on the notice)*
> Match: **strong** *(profile relevance — not a prediction of who will win)*
>
> **This buyer’s recent record** &nbsp;`AWARD — ceilings, never summed with the estimate above`
> Beaumont Hospital: 34 award notices recorded; you can see every one in the source links.
> *Competition signal (TED 2024+): the HSE acute-hospital cohort runs mostly open procedures.*
> *A single bidder is a recorded fact, never a verdict — often a niche/specialist supplier.*
>
> **What similar construction contracts were awarded** &nbsp;`AWARD ceiling · rolled up to CPV 45`
> CPV `45215140` has too few valued awards to summarise, so this is the nearest populated level —
> **CPV 45 *Construction work*** (442 valued national awards): median **€494,956** · typical range
> **€167,770 – €2,043,290**. *(Award ceilings, not money paid; a band for the CPV-45 family, not the exact
> 8-digit code; shown in 2026 prices via the CSO CPI deflator — a ceiling, never a recommended bid.)*
>
> **Who usually wins in this space** &nbsp;`AWARD`
> Top recorded suppliers on CPV 4521 nationally: *[Firm A], [Firm B], [Firm C]* (award counts, source-linked).
> ⚠️ One of your watched competitors, **[Competitor Ltd]**, holds prior awards with this buyer.
>
> **What this buyer has actually paid** &nbsp;`PAYMENT — realised spend, a different grain; never added to awards`
> From public payment disclosures, Beaumont/HSE-area construction suppliers have been paid **≥ €X.Xm**
> (SPENT) across N lines — *indicative floor from published records (~7% of State spend is itemised),
> not an audited total; SPENT and PO-COMMITTED are never blended.*
>
> **QS notes** — Open procedure, ~26 days to deadline. An incumbent fit-out contract at this buyer is
> projected to end ~Q4 2026 *(advertised term, not a verified event; renewals not folded in)*.
>
> **Caveats** — Estimated value is the buyer’s pre-award figure (PLANNED), never money awarded or paid.
> Award/payment figures are different money-grains and are shown separately, never summed. Coverage is
> partial; every figure links to its public source.
>
> [Download the comparable awards (CSV) →] · [This buyer as JSON →] ·
> **Was this relevant?** [Yes] [No] · **Will you bid?** [Yes] [No] · [Manage profile] · [Unsubscribe]

*(Figures bracketed/illustrative are pulled live by `build_brief`; the tender header is a real open notice
from `v_procurement_ted_tenders`.)*

---

## 10. MVP scope

**Goal: a working daily/weekly brief for a handful of seeded profiles, boundary-safe, no self-serve UI.**

- **Sources:** TED tenders (`v_procurement_ted_tenders`, full CPV/buyer/value matching) **+** eTenders
  national (`v_procurement_live_tenders`, keyword/buyer/value only — CPV via derived-keyword, flagged low).
- **Profiles:** seeded via a curated CSV/admin insert into `profile` (no account signup yet). Double
  opt-in email + unsubscribe from day one (GDPR).
- **Matching:** keyword + CPV(prefix) + buyer + value-band + negative vetoes; relevance band. Geography =
  derived-from-buyer, labelled.
- **Enrichment:** reuse `cpv_summary` (deflated bands), `awards_for_authority`, `competition`,
  incumbents (derived from `awards_for_cpv`), `expiring_contracts_etenders`,
  `payments_publisher_profile` — all existing query functions (verified). Buyer crosswalk (§8.1) is the
  one new prerequisite.
- **Delivery:** `services/notify/` over SMTP (or a single transactional provider); `daily`/`weekly`
  digest; `report` + `email_send` ledger.
- **Store:** SQLite operational DB (§6), backed up with the existing R2 job.
- **Schedule:** one GitHub Actions `schedule:` workflow (cloud) running detect→match→enrich→send after the
  daily refresh; heartbeat lane registered.
- **Safety:** `safe_vocab` FORBIDDEN-list CI check over templates; money-grain badges; provenance footer
  reusing the export caveat strings.

**Explicitly deferred from MVP:** self-serve account UI · real NUTS geography · `immediate` (real-time)
frequency · competitor real-time watch · feedback-trained scoring.

## 11. v2 features

1. **CPV-enrich the eTenders live snapshot** (the roadmap's named Phase-5 prerequisite) — backfill
   `cpv_code` onto `etenders_live_tenders.parquet` from the notice detail page, so national sub-threshold
   tenders get full CPV matching (today 0% CPV-filled).
2. **Real geography (NUTS)** from TED BT codes + a buyer→region crosswalk for eTenders → true regional
   filtering instead of derived.
3. **Self-serve signup + Streamlit profile manager** (identity + consent UI; Postgres migration).
4. **Feedback-trained relevance** — use the `feedback` table to tune weights / suppress noisy CPVs per
   subscriber.
5. **Competitor watch** — alert when a `known_competitor` *wins* (award lane), not just on new tenders.
6. **Framework-aware alerts** — match `frameworks_on` / `parent_agreement_id` so a user on a DPS is told
   about relevant call-offs specifically.
7. **Saved-search shareable URLs** (the roadmap's no-accounts substitute) and **Slack/webhook delivery**
   as an alternative channel.
8. **`immediate` frequency** with a per-source webhook/poll so high-value matches go out within the hour.

---

## 12. Boundaries & open owner decisions (do not decide autonomously)

- **Go/no-go on building any of this** — it introduces PII (emails), GDPR/consent, a write-store, an
  email sender, and a scheduler that the project deliberately does not have today.
- **Matching weights & band cut-points** (`W_*`, strong/possible/weak thresholds).
- **Whether to alert on derived-CPV eTenders rows at all**, or hold national alerts until §9.1 lands.
- **Exact money-grain badge wording** and the FORBIDDEN-vocabulary list (advisory vs CI-enforced).
- **Provider choice** for transactional email and where the operational DB is hosted.
- **Retention policy** for `detected_tender`/`report` and the unsubscribe-purge cascade.

---

## 13. Appendix — verified reuse map (checked against code 2026-06-28)

Classifies every dependency so a builder knows what is free vs. what is real work. **Reuse** = exists,
call it. **Thin wrapper** = one new firewall-safe SELECT or a client-side filter over an existing view.
**New** = genuinely new build.

| Need | Target | Verified? | Class |
|---|---|---|---|
| eTenders open tenders | `v_procurement_live_tenders` (no CPV/region) | ✅ view read | Reuse |
| TED open tenders | `v_procurement_ted_tenders` (has CPV) / `queries.ted_tenders()` | ✅ view + fn | Reuse |
| Name-join key | `shared/name_norm.py::name_norm_expr(col) -> pl.Expr` | ✅ | Reuse |
| CPV median/IQR (exact code) | `queries.cpv_summary()` → `median_award_eur/p25/p75` | ✅ cols confirmed | Reuse |
| CPV **family** median | group `v_procurement_cpv_summary` by CPV prefix | — | New (small view) |
| Award list per CPV / authority | `queries.awards_for_cpv()`, `queries.awards_for_authority()` | ✅ | Reuse |
| Incumbents / top parties per CPV | derive from `awards_for_cpv` (no `cpv_top_parties` fn) | ✅ absent confirmed | Thin (client agg) |
| Per-buyer competition (single-bid) | `queries.competition()` ranking → filter `buyer_name` | ✅ | Thin + **buyer crosswalk** |
| Expiring national contracts | `queries.expiring_contracts_etenders(months_ahead, limit)` | ✅ | Thin (filter buyer/CPV) |
| Realised payments per buyer | `queries.payments_publisher_profile(publisher_name)` (SPENT+COMMITTED) | ✅ | Thin + **buyer crosswalk** |
| Supplier payment footprint | `queries.payments_for_supplier(supplier_norm)` | ✅ | Reuse |
| Supplier dossier expansion | `dossiers.build_supplier_dossier(conn, supplier_norm)` | ✅ sig confirmed | Reuse |
| Inflation adjustment | `services/deflator.py::Deflator.load().inflate(value, year, to)` | ✅ | Reuse |
| Read connection over all views | `connections.api_conn()` / `connect_with_views(["procurement_*.sql"])` | ✅ | Reuse |
| Export + caveat/licence strings | `api/routers/exports.py` `/v1/data`, `ui/export_controls.export_button` | ✅ | Reuse |
| Freshness / new-data detection | coverage JSON `held_through`; `tools/freshness_heartbeat.py` | ✅ | Reuse pattern |
| Buyer org-id clean (eTenders↔TED half) | `shared/buyer_clean.org_id_expr()` | ✅ ships | Reuse |
| **Buyer crosswalk** (awards↔payments half) | `buyer_xref.csv` (designed in BUYER_DOSSIER_DESIGN.md) | ❌ not built | **Shared dependency — build once, reuse (§8.1)** |
| CPV on eTenders live snapshot | — (0% filled today) | roadmap Phase-5 prereq | **New (v2 §11.1)** |
| Region / NUTS on either tender feed | — (absent in both views) | — | **New (v2 §11.2)** |
| Operational DB (subscribers/matches/sends/feedback) | — (lake is read-only parquet) | ✅ greenfield confirmed | **New** |
| Email sending | — (no smtp/provider lib in deps) | ✅ greenfield confirmed | **New** |
| Identity + consent + scheduler | — (app stateless/cookieless) | ✅ greenfield confirmed | **New** |

**Reading of the map:** the entire *intelligence* half (history, comparables, deflation, incumbents,
payments, dossiers) is reuse or thin wrappers. The genuinely new work is (a) the **buyer crosswalk** —
small but correctness-critical and the gate on all same-buyer enrichment — and (b) the **operational
shell** (DB + email + identity + scheduler), which is the Phase-5 architectural lift the owner must
greenlight.

---

*Built on existing surfaces: `v_procurement_live_tenders`, `v_procurement_ted_tenders`,
`v_procurement_cpv_summary`, `v_procurement_authority_summary`, `v_procurement_competition`,
`v_procurement_expiring_contracts_etenders`, `procurement_payments_fact`,
`dail_tracker_core/queries/procurement.py`, `dail_tracker_core/dossiers.py`, `services/deflator.py`,
`shared/name_norm.py`, `api/routers/exports.py`, `tools/freshness_heartbeat.py`. Shares the buyer
crosswalk (`buyer_xref.csv` + `shared/buyer_clean.org_id_expr()`) with BUYER_DOSSIER_DESIGN.md. New:
operational DB + `services/notify/` + scheduler workflow. This is Phase 5 of
PROCUREMENT_INTELLIGENCE_ROADMAP.md.*
