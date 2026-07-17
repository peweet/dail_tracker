---
tier: RECORD
status: LIVE
domain: members
updated: 2026-06-22
supersedes: []
read_when: when working on the attendance/participation feature, or needing to know what shipped in the 2026-06-22 TAA-to-participation redesign
key: RECORD|LIVE|members
---

# Attendance → Participation & Absence: redesign

**Status: BUILT & SHIPPED 2026-06-22.** Full vertical slice live and tested
(239 passing). The censored TAA Hall-of-Shame is gone from the standalone page
and the member-overview panel; both now render the participation model.
**Owner notes:** see memory `project_attendance_redesign_participation`.

## BUILT — what landed
- **Pipeline (polars):** `extractors/participation_extract.py` → 4 gold tables
  (`participation_member_year`, `participation_absence_gaps`,
  `participation_presence_year`, `participation_absence_news`). Wired into
  `pipeline.py` as step `participation` (after `legislation`).
- **Office flags DERIVED from the member feed** (`flattened_members` office slots,
  no end_date) — Taoiseach/Ministers/Ministers-of-State + Ceann Comhairle /
  Leas-Cheann Comhairle / Cathaoirleach. Replaced the unreliable TAA `is_minister`
  (which was false for the Taoiseach). Party leaders curated in
  `data/_meta/oireachtas_special_roles.csv`.
- **Vindication:** curated, source-VERIFIED seed
  `data/_meta/member_absence_explanations.csv` (RBB cancer→Irish Times; Cairns
  maternity→Irish Examiner; McGrath cardiac→Tipp Mid West Radio) + live-feed
  keyword fallback. Curated wins; durable past the rolling news window.
- **Views:** `sql_views/attendance/attendance_participation_{turnout,absences,
  divergence}.sql` + `attendance_taa_compliance.sql` (registered via the page glob
  + DOMAIN_FILES). **Thin** reads over the gold.
- **Retrieval:** queries in `dail_tracker_core/queries/attendance.py`, fetchers in
  `utility/data_access/attendance_data.py`.
- **UI:** `utility/pages_code/attendance.py` rewritten. **NOT a leaderboard**
  (reframed 2026-06-22 — "attendance isn't a league table"): leads with a
  **member lookup** (theyworkforyou-style), then a short **"Patterns worth a closer
  look"** block of editorial OUTLIERS — notable absences (date-diff + sourced chip),
  "present but rarely voting" (divergence), 120-day TAA money. The full-roster
  turnout RANKING was removed (it mixed incomparable roles; per-member turnout
  lives on the member profile). Full participation table still CSV-exportable.
  Defaults to the most recent COMPLETE year. `utility/ui/attendance_panel.py`
  member-overview embed rewritten; `part-*` CSS in `shared_css.py`. Verified via
  headless Streamlit AppTest (no exceptions, no `part-turnout-card`, all sections render).
- **Tests:** `test/pipeline/test_participation.py` (14) — view contracts,
  invariants (turnout≤100, voted+missed=total, deduction≤100, divergence excludes
  office/leaders), golden cases (RBB 112+vindicated, Cairns 5.8%+leader, McGuinness
  chair, Murphy excluded, TAA below excludes office).

**Deferred (safe follow-ups):** decommissioning the orphaned old views
(`v_attendance_year_rank` etc.) — left in place; member-overview's hero says "No
rank here" so nothing queries them, but removal is a separate cleanup. TAA-date
daily-resolution absence gaps (the build uses the unfakeable vote-gap). Seanad TAA
money section (basis differs, uncurated).

---

## 1. Why the old "attendance" feature is being scrapped

The Oireachtas Travel & Accommodation Allowance (TAA) "Member Sitting Days Report"
is the source. It is **right-censored at the 120-day allowance threshold** —
recording stops once a member reaches 120 days — so the *count* is meaningless as
an attendance metric:

| Year | Members | At exactly 120 | % ≥ 120 |
|------|---------|----------------|---------|
| 2023 | 125 | 121 | 97% |
| 2025 | 155 | 125 | 81% |
| 2024 | 204 | 2 | 5% (election year, dissolution) |

Both the count AND the `sitting_days` fallback the page pivoted to
(`v_attendance_year_rank`) rank censored data and falsely label fully-compliant
TDs as "worst attenders". The old Hall-of-Shame must be decommissioned.

### The TAA count is also *soft / reconcilable* — votes are unfakeable

Ground-truth case: **Senator Niall Blaney 2024** (Donegal News / Irish Times
"Has anyone seen Niall Blaney"; source = Member Sitting Days Report). Reported:
21 of 61 sitting days Jan–Sep, no attendance Apr–Aug, no explanation given.

Our extract **disagrees**: 38 sitting days Jan–Sep incl. 8 in June + 9 in July,
TAA total = **120 (looks fully compliant)**. Almost certainly a *later reconciled*
version (the report says figures are adjusted for ill health / official duties /
extraordinary circumstances). **So the TAA count can be massaged to 120 and hide a
gap.** The vote record cannot be — you can't reconcile a vote you never cast:

- Blaney 2024: **20.2% vote turnout — worst in the Seanad** (20 of 99 divisions),
  **210-day gap between votes** (Feb 28 → Sep 25), 26-plenary-sitting-day absence
  run (Mar 7 → Jun 12).
- Blaney 2025: active (87% Seanad turnout) — not every year is the same.

**Lesson:** lead the absence signal with vote-gaps (unfakeable); use TAA-date gaps
as corroborating daily-resolution detail; never trust the TAA count.

---

## 2. The four signals (all in `dail_tracker_core`, Streamlit-free, unit-tested)

### a. `absence_gaps` — longest run of consecutive PLENARY SITTING days missed
- Calendar = distinct plenary sitting dates per (house, year) — **recess-proof**
  (no plenary sittings in August, so a summer break is not an absence). Measuring
  against the badge-in *union* instead pollutes the head with the recess crowd —
  do **not** do that (validated trap).
- Gap = run of consecutive sitting dates between two the member attended (interior
  → always real, never the trailing 120-censoring). Led by **calendar date-diff**,
  descending worst→best.
- Validated 2025: RBB 56 (illness, uncensored 34/94), Cairns 18 (maternity,
  interior), Fitzmaurice 12, then clean taper to 3–4 (ordinary missed weeks).

### b. `division_participation` — votes voted / missed / turnout %
- Off `current_{dail,seanad}_vote_history.parquet` (one row per member-division;
  absence = no row). Denominator = distinct `vote_id` per (house, year).
  2025 = 207 Dáil / 111 Seanad divisions.
- **TODO before promotion:** bound denominator by membership window
  (`member_terms`) so mid-term entrants (by-election/co-option) aren't penalised.
- Per-division turnout 2025 ran 132–172 voters (avg 150) of ~174 TDs — skipping is
  normal but bounded; the signal is *consistent* skipping + *which* votes.

### c. `presence_participation_divergence` — THE headline number (NEW)
- `taa_days_present` vs `vote_turnout_pct`. Blaney 2024 = 120 days present but 20%
  votes cast → "badged in, didn't vote." This single divergence catches what the
  old count hid. Make it first-class.

### d. `taa_allowance_compliance` — the money angle
- Reframe the existing fact: below-120 members + 1%/day allowance deduction on the
  150-day basis. The one honest use of the presence data.

---

## 3. Design decisions (locked)

- **Office-holders kept but flagged**, not excluded (user choice). Ministers /
  Ceann Comhairle / **Leas-Cheann Comhairle** / party leaders vote less via
  pairing + executive duties (Martin 49.8%, Harris 39.6%) — tag with context,
  don't hide. NOTE Naughton is Government Chief Whip / Minister of State.
  **VALIDATED FALSE POSITIVE: John McGuinness 2025 (18.4% turnout) is the
  Leas-Cheann Comhairle (Deputy Speaker, elected Feb 2025) — the chair does NOT
  vote by convention. The detector flagged him as Blaney-like; the news-check
  caught it. The LCC/CC chair roles MUST be in the structural-role table or the
  "worst" list libels the Deputy Speaker.**
- **Pairing is a real confound, invisible in the data.** Clifford-Lee paired off
  her vote for Eileen Flynn's maternity leave in 2021 (Irish Times). A low-turnout
  Seanad case can be a pairing arrangement, not absence — cannot be resolved from
  vote data alone. Caveat prominently.
- **Pairing is invisible in the data** → caveat, can't be detected.
- Present neutrally — lead with data, no green/red shame coding (house style).

## 3b. News-vindication (validated 2026-06-22) — "give honourable members their dues"

Cross-reference the top absences against news coverage to auto-separate *explained*
(honourable) from *unexplained*. Validated:

- **Richard Boyd Barrett 2025 — HONOURABLE.** Throat cancer; stepped back Apr 2025,
  returned to the Dáil **5 Nov 2025**. Our absence run ends **exactly 2025-11-05** —
  the data pinpoints his return date. Show "on medical leave", never shame.
  (Irish Times 2025-10-31 all-clear; thejournal 2025-11 "Good to be back".)
- **Holly Cairns 2025 — HONOURABLE.** Maternity (gave birth on 2024 election day;
  news 2026-06 "expecting another baby"). Interior gap May→Sep.
- **Blaney 2024 — UNEXPLAINED.** Article states no explanation was given. The clean
  contrast case.

### Blaney-detector validation run (2026-06-22) — interior vote-gap, backbenchers
Ranked by longest interior gap between divisions actually voted in (membership- &
recess-proof). Each top case news-checked:

| Member | Year | Gap (divs) | Turnout | Verdict |
|--------|------|-----------|---------|---------|
| Richard Boyd Barrett | 2025 | 112 | 18.8% | HONOURABLE — throat cancer, returned 5 Nov |
| Niall Blaney | 2024 | 61 | 20.2% | UNEXPLAINED — article confirms no explanation given |
| Lorraine Clifford-Lee | 2025 | 56 | 30.6% | UNEXPLAINED-PENDING — missed whole autumn term (Jul15→Dec16); pairing history but no 2025 arrangement found; could be private leave/pairing — treat with care |
| John McGuinness | 2025 | 31 | 18.4% | FALSE POSITIVE — Leas-Cheann Comhairle (doesn't vote) |
| Maurice Quinlivan | 2024 | 50 | 68.5% | NO EXPLANATION FOUND but single spring gap, otherwise active — weak candidate |
| Craughwell/Wilson/Daly/Keogan/Ahearn | 2024 | 34-41 | varies | LOW-NOTABILITY — shared mid-July pre-recess division burst, modest real absence |

Lessons: (1) the news-check is load-bearing — it caught the LCC false positive and
vindicated RBB/Cairns; (2) a single big gap with otherwise-high turnout ≠ a
habitual absentee — rank on BOTH gap and overall turnout; (3) several Seanad
"gaps" are a shared pre-recess burst, not individual absence — dedupe by detecting
gaps many members share on the same dates.

**Design:** reuse the `news_mentions` feed (`member_news_*` views) keyed by
`unique_member_code`. CAVEAT: that feed is a *rolling recent window* (~current
month), so it vindicates a **current** absence in real time but will NOT hold a
historical explanation (RBB's cancer coverage Apr–Nov 2025 is outside a 2026-06
snapshot). For historical vindication, either persist explanations when first seen
or run a targeted archive search. Match on illness/leave/maternity/bereavement
keywords; surface the headline + source link inline (per `feedback_cite_news_claims`).

### Confound discovered: Seanad changeover depresses 2025 turnout
The 27th Seanad was elected/nominated **Feb 2025** (old Seanad dissolved Nov 2024).
So "Seanad 2025" divisions (111) start in Feb. New senators (e.g. Nikki Bradley,
Joe Flaherty) show artificially low turnout if the denominator isn't bounded to
their membership window — **this is why the `member_terms` window denominator (§2b
TODO) is mandatory, not optional.** Also: Bradley uses a prosthetic and has spoken
about Leinster House accessibility (Irish Times 2025-06-16) — health/accessibility
context means Seanad low-turnout cases must NOT be flagged as "unexplained" without
care. Reinforces the (b) annotation need below.

## 3c. STRESS TEST (2026-06-22) — scope boundaries the feature MUST respect

Principle held throughout: **display verifiable facts (gap size, dates, turnout) +
any sourced news link; never infer a reason or a verdict.** "Unexplained" = *no
sourced explanation found*, a fact about the search, NOT an accusation.

**HARD DATA BOUNDARY — current-term only.** `current_{dail,seanad}_vote_history`
contain ONLY current (34th Dáil / 27th Seanad) members' votes, backfilled. Every
row is `dail_number=34`; Varadkar/Coveney/Ryan/Donnelly et al. have **0 rows** for
2021. So:
- Per-year voter coverage: Dáil ~98 (2016–23) → 173 (2024–26); Seanad 16–27
  (pre-2025) → 59 (2025). Earlier years are **survivor-biased** — former members
  (the likeliest absentees, having lost seats) are invisible.
- **Do NOT rank absentees before the current term.** A pre-2025 "worst" list would
  falsely exonerate everyone who left. No fuller member-level division source
  exists in the corpus (checked silver/bronze).

**2024 is a DOUBLE-DÁIL year.** 33rd Dáil sat Jan–Nov (dissolution), 34th's first
sitting was **18 Dec 2024**. ~70 new TDs show "2 votes / 162 divisions = 1.2%"
(both votes on 2024-12-18) — a pure artifact; they weren't TDs for the other 160.
**Annual turnout% is meaningless across a dissolution — split by term, not calendar
year.** (The interior-gap metric is robust here: it correctly returns 0 for them.)

**Metric robustness (validated):** interior-vote-gap = ROBUST (membership- &
recess-proof); turnout% = FRAGILE (breaks on mid-term entry, dissolution,
chair/pairing). → Lead with interior-gap; show turnout% ONLY within one clean term.

**Chair roles structurally don't vote — confirmed.** Verona Murphy: 87 votes 2024
→ **0 in 2025** (became Ceann Comhairle Dec 2024). John McGuinness = Leas-Cheann
Comhairle 2025. Both MUST be role-flagged or the "worst" list libels the Speakers.

**Guards required before any ranking:** (a) current term only; (b) ≥N votes in the
window (drop <15 to kill artifact gaps); (c) exclude CC/LCC, flag ministers/leaders;
(d) 2026 is partial → label gaps provisional/ongoing.

### Honourable (explained, sourced) — give them their dues
- RBB 2025 — throat cancer (returned 5 Nov). Holly Cairns 2025 — maternity.
- **Mattie McGrath 2026 — cardiac procedure** (Feb→Apr gap; Irish Examiner 2026-06).

### Unexplained = no sourced explanation found (display as fact, never as verdict)
- Niall Blaney 2024 (article confirms none given) · Lorraine Clifford-Lee 2025
  (whole autumn term; no 2025 pairing/leave found) · Eileen Flynn 2026 (low; only
  2021 maternity on record) — all "treat with care; could be private leave/pairing".

## 4. Open decision (handed to next context)

Legitimate, publicly-known leave (Cairns/maternity, RBB/illness) sits atop the
"worst" list. Either:
- **(a)** flat presentation + neutral "this record doesn't capture the reason"
  caveat (zero upkeep), or
- **(b)** a small curated "known reason" annotation set shown inline (more honest,
  hand-curated editorial needing sourcing + upkeep).

## 5. Remaining phases

1. Add signals (c) divergence + (d) TAA compliance to the polars base.
2. Office-holder / context flags + membership-window denominator.
3. Vet sandbox → promote to gold + registered `v_*` views.
4. Rewrite the attendance page into three honest sections (Absences / Turnout /
   TAA money) + "presence ≠ participation" explainer; rewrite the member-overview
   attendance panel; **decommission `v_attendance_year_rank`**.
5. Wire real-data invariants as tests (RBB 56, Cairns 18, Blaney 2024 20.2% +
   26-day run, turnout ≤ 100%, voted+missed = denominator).

---

## 6. FOLLOW-UP — separate task: "Skipped votes" overview on the Votes panel

> Requested 2026-06-22. Independent of the attendance redesign but shares the data.

The votes page (`utility/pages_code/votes.py`, panels in
`utility/ui/vote_explorer.py`) currently shows only members who **voted**
(Yes / No / Abstained). Add an **absent / did-not-vote** view in two places:

- **Per-division** (`render_division_panel`): list the members who were *absent*
  for that division (the anti-join: full chamber roster for that date minus the
  members with a row). Frame neutrally ("did not vote") with the same office /
  pairing caveat. Example surfaced 2025-12-17: Hildegarde Naughton skipped the
  Health (Waiting Lists) Bill, the Fox Hunting Ban Bill, and the Termination of
  Pregnancy restoration vote.
- **Per-TD** (`render_td_panel`): a "votes skipped" count + the list of divisions
  the member missed (already computable — see
  `pipeline_sandbox/participation/build_participation.py`'s anti-join logic).

Data is ready: `current_{dail,seanad}_vote_history.parquet` + a chamber roster per
date. Needs a pipeline-owned view (per-division non-voters / per-member skipped
divisions) — emit `TODO_PIPELINE_VIEW_REQUIRED` and build it the registered-view
way, not in the Streamlit page. Reuse `division_participation` from this redesign
for the denominators.
