# Public Appointments page — feature plan

Status: design brief LOCKED 2026-05-29 (via impeccable shape); enrichment v1 written, page build next
Drafted: 2026-05-29

## User question this page answers

> "Who is the government putting on State boards, into the judiciary, and as
> special advisers — appointed by whom, and when?"

This is the on-mission slice of the non-SI Iris Oifigiúil corpus: patronage and
public-appointment accountability. The closest neighbours (member_overview,
lobbying) surface none of it.

## Scope (agreed 2026-05-29)

IN: **state-board/agency appointments + special advisers + judicial appointments.**
OUT: military commissions (~105, routine/low civic signal), the ~22 residual
private-receiver/court-record contaminants, and **all personal insolvency**
(privacy rule — never applies here but the filter is shared).

Source population: `notice_category == 'public_appointment'` in
`iris_notice_events_clean.csv` (1,248 rows; ~1,140 after dropping military).

## The Irish-language finding (drives the design)

58% of notices (725/1,248) are in Irish — and it is **not arbitrary, it tracks
the appointing authority** (Article 8: Irish is the first official language, so
formal acts of the President and Government are executed in Irish):

| Language | President | Government | Minister | other |
|---|--:|--:|--:|--:|
| Irish (725) | 274 | 322 | 46 | 83 |
| English (523) | 13 | 67 | 251 | 192 |

President/Government formal acts (judicial appointments under Art. 35, Defence
Forces commissions, board appointments) → Irish, via the constitutional formula
"*Ag gníomhú dó ar chomhairle an Rialtais, tá an tUachtarán…*". Ministerial
appointments → English. So **appointing authority becomes a primary facet**, and
the language split is surfaced as civic context, not hidden.

## Translation: curated mapping, not machine translation

Decided 2026-05-29. We DO show English — produced by template-matching the ~dozen
recurring constitutional/statutory formulae, not by sending text to an MT API.

Why curated over MT (DeepL has no Irish; Google/Azure/Apertium do):
- The notices are highly formulaic — a dozen templates with names slotted in.
- The valuable content is proper nouns (person, body, role); MT garbles these,
  curated mapping extracts and preserves them.
- Deterministic + citable (matters for a civic tool).
- Trade-off: notices not matching a known template get no clean English summary —
  fall back to original Irish + flag for review (vs MT attempting every string
  less reliably). A hybrid (MT for the leftovers) is possible later.

## Pipeline enrichment (`pipeline_sandbox/` first, per sandbox rule)

`public_appointments_enrichment.py` → `data/gold/parquet/public_appointments.parquet`.

Derived fields per row:
- `appointing_authority` ∈ {President, Government, Minister} (from the formulae)
- `appointment_type` ∈ {state_board, special_adviser, judicial}
- `body` — State body/court/department appointed to (curated Irish→English map)
- `appointee` — person name (extract; improve on the 31% person_title_detected)
- `role` — position (member / chairperson / judge / special adviser)
- `portfolio` — for special advisers, the minister/department (curated map from
  "AIRE STÁIT AG AN ROINN…" → English department; reuse si_department_aliases.csv
  vocabulary where possible)
- `english_summary` — templated English sentence assembled from the above
- carry: `lang`, original title/raw_text, issue_date, iris source

Curated lookup tables (CSV in data/_meta/, like si_department_aliases.csv):
- body name Irish→English
- role/template phrases Irish→English
- portfolio/department Irish→English

Quality gate: quarantine rows where authority/type/body can't be derived; emit a
coverage summary. Exclude military + the contaminant filter here.

## Design brief (LOCKED 2026-05-29 via impeccable `shape`)

**Register / direction:** product register, editorial-accountability. Light theme
(PRODUCT-pinned). Color strategy **Restrained** — warm ink-on-paper neutrals, one
sharp accent reserved for the SpAd-spike signal + primary actions; authority gets
subtle neutral pill differentiation, not a full palette. Anchor references: The
Guardian data journalism, ProPublica data tools, a well-set parliamentary record.
No gradient accents, no hero-metric block. Uses the sanctioned project signature
(side-stripe evidence cards via `ui/components.py`, `#ffffff` card paper, `--signal-*`).

**Primary user action:** scan the most recent appointments (who appointed whom, to
which body) and pivot to "which minister has the most special advisers" without hunting.

**Confirmed decisions:**
- Special advisers = **featured lead section** (not one facet among equals).
- Default landing = **recent feed** (reverse-chronological, newspaper register).
- Bilingual = **English headline, Irish on tap** (english_summary leads; original
  Irish in an expander as provenance).

**Layout (top → bottom):**
1. Editorial hero: H1 + one-sentence dek + quiet constitutional caption (Irish =
   first official language → President/Government acts in Irish; ministerial in English).
2. **Featured: "Special advisers — who advises which minister."** Ranked
   ministers/portfolios by SpAd count + a slim year sparkline making the
   government-formation hiring spikes (2017, 2024, 2025) legible. Not a hero-metric template.
3. **Full-record search** + facet row: search (appointee/body/minister, all years) ·
   year pills (YTD-tagged 2026) · authority · type · body · minister · Gaeilge-only.
4. **Recent feed**: month/year-dividered appointment cards — authority pill · body ·
   appointee (+ "+N others") · role · date; "show original (Gaeilge)" expander.
   "Older →" pagination.
5. Detail view: full record + original Irish + Iris Oifigiúil source link.

**Going back in time (4 routes, all display-only on the loaded frame):**
search across all years · year pills jump · the trend sparkline doubles as a
clickable year scrubber · dated feed + "Older" pagination. *Point-in-time roster*
("who sat on board X on date Y") is **excluded from v1** — term-end/removal notices
are inconsistent in Iris, so a roster can't be stood behind.

**Key states:** default · filtered (chips + scoped count) · empty (civic voice, how
to widen) · partial-year 2026 (YTD tag) · appointee missing ~17% ("Appointee not
recorded in this notice", absence is data) · unknown authority 82 ("Authority not
detected", not hidden) · body-junk fallback (lead with english_summary; raw body only
when it passes a cleanliness check).

**Resolved defaults:** dedicated detail view (like SI); CSV export of filtered set
(secondary, for journalists); logic firewall = page reads `v_public_appointments` via
cached `data_access.fetch_public_appointments()`, zero business logic in the page.

**Build chain:** `sql_views/…_public_appointments.sql` → `v_public_appointments` →
cached `data_access.fetch_public_appointments()` → `utility/pages_code/public_appointments.py`
→ `st.Page` in `utility/app.py`. Mirrors statutory_instruments.py.

**Recommended impeccable refs for build:** layout.md, typeset.md, clarify.md (+ reuse
`ui/components.py`, `shared_css.py`).

## Risks / open questions (post-enrichment)

- Appointee non-SA coverage 83% (clean); ~17% genuinely unextractable (record-split /
  no-person Orders). `role` 59% (only when explicitly stated).
- **`body` junk tail** (FÓGRA, truncated names, generic headers) — pre-launch pipeline
  cleanup; page ships leading with english_summary so it isn't blocked.
- Special-adviser Orders are statutory orders — check overlap/dedup with the SI corpus.
- ~57 special advisers have no extracted portfolio; ~42 duplicate (issue_date, summary) rows.
