# Provenance Review & Plan (API + Streamlit)

**Status:** Design doc — decision-first, no code yet. Execute the build at an ETL/feature
plateau (see `[[feedback_refactor_timing]]`).
**Date:** 2026-06-06 (rev 4 — adopts the three-tier model from comparable-project prior art)
**Scope:** the FastAPI layer (`api/`), the Streamlit UI (`utility/`), and the shared core
(`dail_tracker_core/`) that should feed both.
**Companion:** the per-page Streamlit audit lives in
[STREAMLIT_PROVENANCE_RECORD.md](STREAMLIT_PROVENANCE_RECORD.md).
**Why:** provenance — the resources that let a user validate where a number came from — is
unevenly applied on **both** surfaces, and dumping every source onto every page makes it feel
cluttered. This doc adopts the way mature projects resolve that, and proposes a single
core-owned registry that powers it across the whole product.

---

## 0. Two concepts this review keeps separate

The first draft muddied **provenance** with **freshness**. They are orthogonal axes and only one
is in scope.

| | **Provenance** *(in scope)* | **Freshness** *(out of scope)* |
|---|---|---|
| Answers | "Where did this come from? How do I verify this figure against the authoritative original?" | "How old is the data we hold? Did the pipeline run?" |
| User need | Trust / verifiability | Currency / operational health |
| Artifact | `sources{}` links, source registry, the UI PDF registries, licence | `data/_meta/freshness.json` |
| Workstream | **this review** | already owned by `[[project_freshness_architecture]]` (Unit A) |

A source is no fresher for being well-cited, and data can be perfectly cited yet a year stale.
This review is provenance only; §5 is the explicit freshness hand-off.

---

## 1. The real question: clutter vs. credibility — and how mature projects solve it

> *"The links to payment PDFs would give the app massive credibility — anyone could check the
> numbers — but it clutters up the app. The links must be shown in some way."*

This tension is universal and already solved. The resolution is that **provenance is tiered by
*where the user is*, not dumped in one place.** Clutter only happens when you put *exhaustive*
provenance where people are *scanning data*. Move exhaustiveness to a destination; keep only the
*relevant* link inline.

### 1.1 Prior art

| Project | What they do |
|---|---|
| **TheyWorkForYou** (mySociety — our lodestar, `[[project_design_principles]]`) | Every atomic record (speech, vote) has a small, quiet **"source" link** to official Hansard. Universal + low-contrast; credibility from being *always there*, not loud. |
| **Wikipedia** | Near-invisible inline `[1]` markers while scanning; full **references section + hover-cards** on demand. Collapsed by default, complete on request. |
| **Our World in Data** | A quiet one-line **"Data source: …"** under each chart + a dedicated **"Sources" tab** + source-bearing download. |
| **OpenCorporates** | "Every fact has a provenance" — yet uncluttered, because each point's source is a small low-contrast link and the *exhaustive* provenance lives on a separate data page. |
| **GOV.UK** (we score against its Service Standard, `[[project_app_design_synthesis_2026_05_26]]`) | Consistent **"Details / Methodology"** collapsible + a "Sources and related" footer. |
| **Data journalism** (FT, The Upshot, 538) | A single grey **"Source: X"** caption under every figure; methodology at the end. |
| **Court/legal DBs** (BAILII, CourtListener, GovTrack) | Per-record **deep link to the one authoritative document**. |

The one thing **nobody** does — and exactly what makes our app feel cluttered today — is list
*every* source *on every page*. That's the worst of both. The model below deletes it.

### 1.2 The three-tier model (adopted)

| Tier | What | Where it appears | Cost on the page |
|---|---|---|---|
| **T1 — Ambient credit** | one quiet `Source: … ↗` line | under *every* view, always visible | 1 line |
| **T2 — Sources destination** | *every* source + licence + the full document lists (e.g. all 72 payment PDFs) | one canonical place, one click away | none |
| **T3 — Contextual citation** | the *single* document behind the *specific* figure | only at drill-down | 1 relevant link |

This supersedes the looser "layer A / layer B" framing of earlier revisions: T1+T2 are the old
layer A done properly (a one-liner that points to a real destination, not a per-page dump), and T3
is layer B. **Anti-pattern retired:** the per-page "expander listing all sources," which most
pages either do (replicating T2 content everywhere) or skip entirely.

### 1.3 Presentation decisions (locked)

- **T3 inline citation = a quiet caption line.** A small grey `Source: Feb 2026 PSA return ↗`
  directly under the figure — always visible, lowest friction, mobile-safe (no hover dependency),
  matches the Our World in Data / data-journalism idiom. Chosen over a Wikipedia-style hover marker
  (weak on mobile) and a heavier bordered chip.
- **T1 ambient credit** uses the same quiet caption idiom, one line, linking onward to T2.
- **T2** is a single "Sources & methodology" UI page (the rendered registry) — the only place the
  full document lists live.

### 1.4 The payment-PDF example, resolved

- All **72 PDFs live on the T2 Sources page** — reachable, complete, indexed by month; never on the
  rankings.
- Drilling into a member's payments for a period shows **the one** backing PDF (T3):
  *"Source: Feb 2026 PSA return ↗"* — one link, exactly where someone verifies.
- The ranking page shows only the **T1** line: *"Source: Oireachtas payment records · all source
  documents ↗"* → the T2 page.

The links are *always shown* — just never all at once where they'd drown the data. This also
leans on the existing **two-stage member flow** (`[[project_design_principles]]`): provenance gets
richer exactly as the user narrows.

---

## 2. Current state (audited, re-tagged to the tiers)

### 2.1 API — per-endpoint

| Endpoint | Today | Tier present |
|---|---|---|
| `GET /`, `/v1/catalog` | global licence + a *promise* of "per-resource provenance" that doesn't exist ([catalog.py:89-94](../api/routers/catalog.py#L89-L94)) | weak T1 only |
| `GET /v1/members` + `…/dossier` | an identity `external_links` dict (Oireachtas profile + socials, [dossiers.py:124](../dail_tracker_core/dossiers.py#L124)) but **no source attribution on any composed data section** (attendance / payments / votes / SIs) — the **flagship**, fuses ~8 sources | weak T1 (identity only) |
| `GET /v1/legislation/{bill_id}` | `sources{}` — 3 URLs | T3 |
| `GET /v1/votes/{vote_id}` | `sources{}` — 1 URL + label | T3 |
| `/legislation`, `/statutory-instruments`, `/votes`, `/payments`, `/lobbying/*` | nothing | none |

No T2 destination exists; the two T3 `sources{}` schemas already diverge (`oireachtas_url` vs
`source_url`, different null padding) — a third endpoint would invent a third shape.

### 2.2 Streamlit — per-page (full record in [STREAMLIT_PROVENANCE_RECORD.md](STREAMLIT_PROVENANCE_RECORD.md))

- The standard `provenance_expander()` (6/15 pages) is today's stand-in for T1+T2 **mashed into a
  per-page dump** — collapsed, but replicating full source lists on each page rather than pointing
  to one destination. Plus 4 *variant* implementations (committees look-alike, corporate's
  *methodology* expander, bespoke footers on member_overview/judiciary/public_appointments/
  procurement). Five mechanisms for one idea.
- T3 per-record links exist on 6/15 pages (`source_link_html`/`render_source_links`); the
  ranking/aggregate pages where verification matters most — attendance, payments, interests, votes
  — have **none**.
- The `INTERESTS`/`ATTENDANCE`/`PAYMENTS` PDF registries (the natural T2 content) live UI-side in
  [source_pdfs.py](../utility/ui/source_pdfs.py), invisible to the API; only 3 pages render them;
  interests has one it never uses.

### 2.3 The inventory is scattered across three disconnected stores

`data/_meta/source_registry.generated.json` (107 sources, finance/PDF-weighted — **omits** the
core parliamentary sources: Oireachtas Open Data API, Statute Book, lobbying.ie register), the
UI PDF registries, and the SQL `*_sources` views. No single source of truth; none served as T2.

### 2.4 (Freshness — scoped out)

`freshness.json` + the empty `generated_at` envelope slot
([serialize.py:92-110](../dail_tracker_core/serialize.py#L92-L110)) are the freshness concern (§5).

---

## 3. Proposed model: one core registry → all three tiers, both surfaces

The spine is a **single curated source registry in the core** (`dail_tracker_core`), not in `api/`
or `utility/`. Correct under `[[project_streamlit_uncoupling]]` (Streamlit thin over a
Streamlit-free core): API and UI both become *consumers* of one provenance source of truth.

### 3.1 The shared registry

A curated allow-list of public sources (§4), each entry:

```jsonc
{
  "source_id": "oireachtas_psa_payments",
  "name": "Parliamentary Standard Allowance payments to deputies",
  "publisher": "Houses of the Oireachtas",
  "landing_url": "https://data.oireachtas.ie/.../psa",
  "licence": "CC-BY-4.0",
  "grain": "monthly_payment_return",
  "documents": [ { "label": "Feb 2026", "url": "https://data.oireachtas.ie/.../february-2026_en.pdf" }, … ],
  "caveat": null
}
```

The `documents[]` array is where the 72 payment PDFs (and the interests/attendance sets) live —
**migrated out of the UI** into the registry. No freshness field (§5).

### 3.2 How each consumer renders the tiers

| Tier | API | Streamlit |
|---|---|---|
| **T1 ambient** | per-resource `source_ids` in `/v1/catalog`; `source_ids` on list envelopes | one quiet `st.caption` per page, `source_ids` → one-liner + link to the Sources page |
| **T2 destination** | `GET /v1/sources` (+ `/{id}`) — serves the registry incl. `documents[]` | a single **"Sources & methodology"** page rendering the registry (the only home for full doc lists) |
| **T3 contextual** | `source_links` on the record (unified shape) | a quiet caption line under the specific figure (the locked presentation, §1.3) |

This replaces the 5 Streamlit footer variants with **one** T1 caption + the shared T2 page, retires
the per-page source dumps, and unifies the two divergent API `sources{}` schemas onto one core
shape. Net: API 2/11 → 11/11; UI 5 mechanisms → 1; lighter payloads *and* lighter pages.

---

## 4. The one hard part: the public-source allow-list (PII)

[catalog.py:1-7](../api/routers/catalog.py#L1-L7) and `[[feedback_personal_insolvency_privacy]]`
are explicit: some sources carry PII (SIPO donor home addresses, personal insolvency naming
private citizens). **The registry must be a curated allow-list, never an auto-dump of
`source_registry.generated.json`.** Because it now feeds *both* surfaces, the curation gates the
T2 page and every T1/T3 link.

Rules: exclude anything with `privacy_risk` set or feeding a quarantined/PII column; add the
headline parliamentary sources the generated registry omits (§2.3) and fold in the UI PDF
registries as `documents[]`; review the list like the catalog. This curation is the bulk of the
work; the plumbing is small.

---

## 5. Explicit hand-off: freshness is a separate feature

Out of scope; owned by `[[project_freshness_architecture]]` (Unit A). When surfaced: fed by
`freshness.json`; API home `GET /v1/health` or a new `/v1/status` (+ the empty `generated_at`
slot); UI home a freshness badge — **not** the source registry. Shipped on its own so the concerns
don't recontaminate.

---

## 6. Phased plan

One shared spine, then tier rollout per surface. All read-only over the registry (no ETL change).

**Spine (first):**
- **P0 — curated registry config** in core: headline parliamentary sources + safe subset of the
  generated registry + the migrated UI PDF registries as `documents[]`.
- **P1 — core provenance shape + resolver:** one Pydantic model + `resolve(source_ids)`, plus the
  unified SQL `*_sources` contract.

**T2 destination (highest leverage — the place links stop cluttering):**
- **P2a — `GET /v1/sources` (+ `/{id}`).**
- **P2b — the "Sources & methodology" Streamlit page** rendering the registry incl. all document
  lists. *Once this exists, every page's per-source dump can be deleted in favour of a T1 link.*

**T1 ambient:**
- **P3a — per-resource `source_ids` in `/v1/catalog`.**
- **P3b — one quiet `source_ids` caption on every page**, replacing the 5 footer variants and the
  per-page expander dumps. Closes the flagship `member_overview` gap.

**T3 contextual:**
- **P4a — `source_links` on API records** (migrate legislation/vote views; extend to
  member/payments/lobbying/SI).
- **P4b — quiet caption-line citations on the ranking/drill-down pages** that lack them
  (attendance, payments, interests, votes), drawing the single relevant `documents[]` entry.

**Suggested MVP:** P0 + P1 + P2a + P2b — the registry exists and has a real home on both surfaces.
That alone lets you *remove* clutter (point pages at the Sources page) while making provenance
*more* complete. T1/T3 then layer on incrementally.

### 6.1 Ready-to-do now (verified standalone, no plateau needed)

These are decoupled from the registry build — pure display wins where the data already exists.

- **W1 — wire the `INTERESTS` registry into interests.py. ✅ DONE (2026-06-06).** The 12-entry
  `INTERESTS` dict ([source_pdfs.py:25](../utility/ui/source_pdfs.py#L25)) existed but the page
  imported only `provenance_expander` and never rendered it ([interests.py:54](../utility/pages_code/interests.py#L54)).
  Added `interests_pdf_links(house)` and passed `pdf_links=` to the footer, mirroring how
  payments.py already passes `PAYMENTS` — instant T3 register-document links on a ranking page that
  had none. Pure UI, no logic-firewall surface.

---

## 7. Open questions for kickoff

1. **Registry home/format:** hand-authored `dail_tracker_core/provenance/sources.json` reviewed
   like the catalog, vs. a `public: true` flag on `build_source_registry.py`? (Leaning
   hand-authored in core — the headline sources aren't in the generated registry, curation needs
   review, and core is the shared home both surfaces need.)
2. **`source_ids` vocabulary:** mint API/UI-facing ids that map *to* internal registry ids, so the
   public contract survives extractor renames. (Leaning yes.)
3. **Member dossier:** list all ~8 contributing `source_ids` (T1), with `source_links` (T3)
   reserved for the few genuinely per-record documents. (Leaning yes.)
4. ~~T3 presentation~~ — **decided: quiet caption line (§1.3).**

---

## 8. Recommendation

Adopt the **three-tier model** (§1.2), powered by **one core-owned source registry** (§3), gated
by a **curated public-source allow-list** (§4), with **freshness handed off** (§5). It is exactly
how TheyWorkForYou, Wikipedia, Our World in Data, OpenCorporates and GOV.UK resolve the same
tension: the links are *always shown*, but exhaustive lists live on a Sources destination, a quiet
caption credits every view, and the single relevant document appears contextually at drill-down.
Result: the payment PDFs (and every other source) become a credibility asset rather than clutter,
provenance goes from bimodal to complete on both surfaces, the 5 UI footer variants collapse to
one, and it advances the core/UI uncoupling. Build at a plateau; start with the §6 MVP.
