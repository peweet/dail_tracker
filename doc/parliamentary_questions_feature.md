# Design Brief — Parliamentary Questions on Dáil Tracker

**Scope:** UX/UI shaping for sections 1–4 only. The architectural plan (SQL views, contracts, runbooks, phasing) follows separately.

---

## 1. User questions this feature must answer

A PQ feature exists to answer **"who is doing the work of holding government to account, and on what?"** That decomposes into the following civic questions, ordered by primacy:

**Hero questions (must answer on the primary view):**
1. **Which TDs ask the most parliamentary questions?** Volume is volitional — backbench/opposition TDs choose to file PQs to extract information from ministers. High volume ≠ good, low volume ≠ bad, but the distribution is the most direct measure of who is using the most public-facing accountability instrument the parliament has.
2. **Which ministries field the most questions?** A different story — which portfolios are under most parliamentary pressure (Health, Housing, Justice typically top in Ireland).
3. **What did a TD ask about, and when?** Year-scoped activity for any individual (drives the member-overview integration).

**Secondary questions (must answer in drill-down or search):**
4. **What questions have been asked on a given topic?** Topic = `debate_section.showAs` ("Tax Code", "Garda Operations", "Housing Provision"). The only way a user can pivot from "who" to "what".
5. **What was the recent PQ activity?** A latest-questions-asked feed, useful for journalists checking what just landed.

**Dropped from the starter list (with reasoning):**
- *"How does a TD compare to peers, by ministry/topic?"* — too specialised for v1; comparison is implicit in the ranked list. Add later as a "vs peers" panel on the member profile if the need shows up.
- *"Joint-asker patterns — who co-asks with whom?"* — Network analysis is interesting but not in the civic-accountability core. Joint asks should be **disclosed transparently** (badge on the card), not visualised as a network. Park indefinitely.

**Decision:** v1 hero answers Q1 + Q2; v1 drill-down answers Q3 (via member overview) + Q4 (via topic search). Q5 lives as a sidebar widget or a small "Recently asked" strip on the primary page, not a full feed.

---

## 2. Primary view shape — `/questions`

### Hero ranking: asker-ranked, with ministry-ranked as a segmented_control sibling

**Recommendation: asker-ranked is the hero.**

Justification:
- PQ volume on the **asker** side is volitional — it tells a story (which TDs are working the accountability machinery hardest). This is what a journalist or citizen wants to see first.
- PQ volume on the **minister** side is structural — it just tracks portfolio breadth and political contestation, not effort. Useful as context, not as the headline.
- This mirrors theyworkforyou's instinct: their PQ section foregrounds individual MPs, not departments.
- The codebase already has the right pattern for this — payments.py uses `st.segmented_control` with "Rankings" + per-year tabs; the same idiom works here as **"Top askers" / "Top ministries questioned"** as the two top-level views, with year pills below.

**Layout (top-to-bottom):**

1. **Hero banner** — `hero_banner()` with kicker `"PARLIAMENTARY QUESTIONS · WRITTEN & ORAL"`, title `"Who's asking what?"`, dek explaining PQs as the chamber's main fact-extraction tool.
2. **Stat strip** — three metrics in `pay-totals-strip`-style row:
   - Total PQs in the dataset
   - Distinct askers
   - Distinct ministries questioned
3. **View switcher** — `st.segmented_control` with options: **Top askers** (default) · **Top ministries** · **Recently asked**. The third option replaces what would otherwise be a noisy bottom-of-page feed; it gets the same hierarchy as the other two.
4. **Year pill row** — `year_selector` helper, `["All years"] + DESC year integers`. Same idiom as attendance.py and payments.py.
5. **Question-type filter** — *secondary* control: a small `st.pills` with **All · Written · Oral**. PQs are dominantly written (~90%) so this filter is the difference between a routine-volume view and a chamber-noise view. Place it on the **right** of the year row (not in the sidebar) so the filter is in the same eyeline as the data.
6. **Ranked list of cards** — two-column 10+10 layout (matches payments.py rankings). Card composition:
   - **Asker card** (Top askers view): `member_card_html` with `name = td_name`, `meta = clean_meta(party, constituency)`, badge = `pay-amount-badge`-style with `{count} PQs / {year}` and a `pay-count-pill` showing `{written_count} written · {oral_count} oral`. Top three pills name the most-questioned ministries.
   - **Ministry card** (Top ministries view): kicker = ministry name, badge = total PQs that year, two pill rows showing top 3 askers + top 3 topics within that ministry.
   - Whole card is a `clickable_card_link`. Asker card → `/member-overview?member={unique_member_code}`. Ministry card → in v1, no link (no ministry page exists yet; see §4).
7. **Topic search box** — `st.text_input` placeholder `"Search PQs by topic — e.g. housing, garda, hospital waiting"` placed **below** the ranked list with its own results section. Pattern lifted from legislation.py's title search but inline rather than sidebar — topic search is core UX, not a filter modifier. Results render as a paginated card list of (question, asker, minister, date, "Open debate ↗" link).
8. **Provenance footer** — `provenance_expander` with the standard caveat block: source = Houses of the Oireachtas Open Data API; explanation that PQs are filed to a minister and answered in writing or orally; a **joint-asker disclosure note** explaining that co-asked questions count toward each named asker.
9. **CSV export** — current view as CSV (`export_button`). Same column set as the ranked list.

**Sidebar:** `sidebar_page_header("Parliamentary<br>Questions")`, then a **member filter** (so a user can jump straight to a TD's PQ activity), then notable-TD chips. No date range — year pills handle temporal scoping. No status / no type filter (those live in the eyeline as pills).

### Decision on the "Recently asked" view

The third segmented_control option ("Recently asked") is lightweight: a chronological-DESC list of the latest 50 PQs (date, ref, asker name → profile link, minister, topic, snippet, "Open debate ↗"). This is the *primary* place a journalist will land for "what just happened" — and it answers Q5 without inventing a separate page. No card hierarchy here; the rows are dense.

---

## 3. Member Overview integration (v1, kept)

### Section placement

Current `_render_stage2()` order: Headline stats → **1. Voting** → **2. Legislation** → **3. Debates (TODO)** → **4. Committees (TODO)** → **5. Lobbying**.

**Insert "Parliamentary questions" as section 3, between Legislation and Debates.** New order:

1. Voting record by issue
2. Legislation sponsored
3. **Parliamentary questions ← new**
4. Debate contributions *(pending TODO_PIPELINE_VIEW_REQUIRED)*
5. Committees *(pending)*
6. Lobbying & revolving door

Why between Legislation and Debates: PQs and bills are both *formal accountability outputs* — measurable, dated, citable. Debates are speech, which is qualitatively different. The user moves from *"what did this TD vote on / propose / probe / say"* in a coherent gradient.

**Headline stats strip update:** the existing 3-column metric row at the top of the profile (`Days in chamber`, `Votes cast`, `Payments received`) should grow to a 4-column row with **`PQs filed`** as the new fourth metric — `{count} this year · {alltime_count} total`. This raises PQ activity to the same visual level as the other big-three accountability counts.

### Section content

A `<p class="section-heading">Parliamentary questions</p>` heading, then:

1. **Year-aware sub-strip** — three small metrics:
   - PQs filed in selected year (with delta vs prior year if available)
   - Written / Oral split (e.g. "47 written · 8 oral")
   - Most-questioned ministry that year (e.g. "Health (18 PQs)")
2. **Ministry chip row** — top 5 ministries this TD has questioned in the selected year, rendered as `pay-taa-pill`-style chips with counts. Clicking a chip filters the question list below.
3. **Topic chip row** — top 5 debate-section topics (e.g. "Hospital Waiting Lists", "Rental Sector"), same chip style.
4. **Recent PQs list** — paginated card list (10 per page, `paginate()` + `pagination_controls()`) of this TD's questions in the selected year. Each card:
   - Top-left: question_ref (`[31202/26]`) as a small monospace badge
   - Top-right: question_date (formatted like `_fmt_date`)
   - Body: question_text (truncated to first 220 chars + ellipsis if longer; full text on click-to-expand)
   - Bottom-left: minister chip + topic chip
   - Bottom-right: `source_link_html(debate_section_uri, "Oireachtas debate")`
5. **CSV export** — full year of PQs for this TD.

**No `st.dataframe`** — per the project memory rule for member_overview. Every drill-down is card-based.

### Year-pill interaction

The Member Overview already has a votes-section policy-area pill row + year pill row. PQs section follows the same pattern: the year selected at the top of the votes section (`mo_vote_year`) does **not** propagate down — each section keeps its own scope. Recommend a **dedicated `mo_pq_year` pill row** at the top of the PQs section. Reason: a journalist may want to look at votes in 2024 but PQs across all years, or vice versa. Coupling them creates a hidden coupling bug.

---

## 4. Cross-page links

| From | To | Mechanism | Status |
|---|---|---|---|
| `/questions` asker card | `/member-overview?member={unique_member_code}` | `clickable_card_link` whole-card | **Ready** (silver has `unique_member_code`) |
| `/questions` ministry card | (future ministry page) | Plain text + tooltip "Ministry pages coming soon" | **Deferred** — no ministry page exists |
| `/questions` topic search result row | External Oireachtas debate URL | `source_link_html(debate_section_uri, "Open debate ↗")` | **Ready** |
| `/questions` topic search result row | `/member-overview?member={unique_member_code}` (asker name) | `member_link_html()` | **Ready** |
| `/member-overview` PQs section minister chip | (future ministry page) | Plain chip | **Deferred** |
| `/member-overview` PQs section question card | External Oireachtas debate URL | `source_link_html(debate_section_uri)` | **Ready** |
| `/member-overview` PQs section "Full PQ history →" CTA | `/questions?asker={unique_member_code}` | `entity_cta_html` | **New URL pattern** — needs `?asker=` query param wired into `/questions` page |
| `/legislation` bill detail | `/questions?topic={bill_short_title}` | None | **Reject for v1** — PQs reference bills inconsistently in `question_text`; no reliable join key. Re-evaluate if the pipeline ever extracts bill mentions from PQ text. |

**Two new URL params for `/questions`:**
- `?asker=<unique_member_code>` — pre-filters the page to a single TD's PQs (like `payments.py?member=`)
- `?ministry=<ministry_role_code>` — pre-filters by ministry. Useful for future ministry pages.

**`PAGES["questions"] = "questions"`** — register the slug in `entity_links.py` alongside the others, and add a `questions_page_url(asker_id=None, ministry=None)` helper.

**Cross-link to add elsewhere:**
- On the `/votes` page, member voting view: a small "→ View PQs by this TD" link near the existing "Full voting history" CTA. One-line addition, big payoff for journalists triangulating a TD's record.

---

## Primary-view recommendation (one paragraph for the architect)

Build `/questions` as an **asker-ranked, year-pill-driven leaderboard** in the same shape as `payments.py`'s `_render_primary`: three-segment switcher (**Top askers** default · **Top ministries** · **Recently asked**), year pills, an inline written/oral toggle, a 10+10 two-column card grid linking each TD card to `/member-overview`, and a topic-search panel below the ranking. Member Overview gets a new section between Legislation and Debates, with its own year pills and a paginated card list of the TD's recent PQs. The hero metric on the member profile grows from 3 columns to 4 to include PQ count. No `st.dataframe` anywhere on either primary or profile views; only `pay-*`-family cards and `dt-name-card` reused. Joint-asker handling defers to "count co-asks for both" with a transparency note in provenance — anything cleverer needs a primary-asker flag in the silver layer (architectural-plan TODO).

---

## Hand-off note for the architectural plan

The Plan agent should treat this brief as the **frozen UX shape** and design SQL views, contracts, and runbooks that fit it. Specifically the views need to support, in retrieval-only SQL:

- A `v_questions_member_summary` grain of *(unique_member_code, year)* with `pq_count, written_count, oral_count, top_ministry, top_ministry_count, top_topic`, ranked by `pq_count`
- A `v_questions_ministry_summary` grain of *(ministry_role_code, year)* with the same shape mirrored
- A `v_questions_member_detail` grain of *(unique_member_code, question_ref, question_date)* with full text + debate URI
- A `v_questions_topic_search` that supports `WHERE LOWER(topic) LIKE LOWER(?)` over a normalised topic column
- A `v_questions_alltime_index` for the stat strip and "Recently asked" feed
- Joint-asker dedupe on `question_ref` happens **inside the views** (count distinct `question_ref` per asker, not row count), with the `pipeline_sandbox/` route invoked only if SQL `COUNT(DISTINCT question_ref)` proves insufficient (it shouldn't).

Open architectural questions to flag in the next pass:
- Is `data/gold/parquet/questions.parquet` written by any pipeline script, or orphaned?
- Is `unique_member_code` on the silver guaranteed to match `v_member_registry.unique_member_code`? (Probably yes — it's from the same Oireachtas API — but verify.)
- Backfill horizon: what years does the bronze cover? Does it match attendance/votes (2020–present) or extend further back?
