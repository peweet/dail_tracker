# Audit — Dáil Tracker (`dail_extractor`)

**Assumptions made (please correct if wrong):**

- *Project description:* Civic-data pipeline + Streamlit dashboard for Irish parliamentary transparency — joining Oireachtas API, PDFs (attendance, payments, interests), and lobbying.ie CSVs into one analytical layer. Eight dashboard pages, ~30 SQL views, medallion bronze/silver/gold.
- *Target users:* Investigative journalists (Right to Know, The Journal Investigates, Noteworthy, Story.ie, Irish Times) and politics academics (TCD/UCD/DCU). Per `SHORT_TERM_PLAN.md`, success = "one named journalist using it."
- *Stage:* Late prototype / early alpha. Pipeline runs end-to-end locally; nothing is deployed; no CI; no users.
- *Goals:* Deploy publicly, wire automated refresh + alarms, get one named journalist using it within ~8 weeks.

---

## Executive Summary

Three things matter most:

1. **The analytics layer is much closer to "ready" than the operational layer.** SQL views, contracts, the firewall between UI and logic — this is unusually mature for a solo project. Tests, CI, deploy, monitoring, lobbying ingestion — these are essentially absent. Your own [v4 doc](doc/dail_tracker_improvements_v4.md#2-honest-readiness-picture) says 4/5 vs 1–2/5. That gap is the whole story.
2. **You have no user feedback loop yet, and you've already built eight pages.** Classic civic-tech build-trap. Every additional page before first contact compounds the risk that you ship something nobody asked for.
3. **`DATA_LIMITATIONS.md` is your single biggest asset and you are underweighting it as a product.** It is unusually rigorous (530 lines, calibrated, legally aware). Most civic-data projects don't have anything like this. Lead with it.

The 8-week plan in [SHORT_TERM_PLAN.md](doc/SHORT_TERM_PLAN.md) is well-reasoned but heavy. The fastest win is probably to send the data to one journalist *this week*, before doing any of it.

---

## What's Working

- **Genuine scope discipline.** "Current Dáil only," "2020+ practical cut-off," `pipeline_sandbox/` rule, "don't model elegance, ship rough" all visible in code and docs. This is rare.
- **The logic firewall is real.** Page contracts (`dail_tracker_bold_ui_contract_pack_v5/`), `civic-ui-review` skill, `pipeline-view` skill, the `business_logic_in_page: forbidden` flag — this enforces the thin-Streamlit principle and will let you replace the UI layer cleanly later.
- **Medallion + DuckDB + Polars is the right stack** for this size and budget. Local-first analytics with no infra cost is correct.
- **The inspiration is well-chosen and visible.** TheyWorkForYou, OpenKamer, How They Vote EU — you've internalised the lessons (provenance, methodology, public source links) instead of copying the surface.
- **Member Overview as a single-politician public accountability record** is conceptually the strongest page in the app. The two-stage flow (browse → drill down) is right.
- **The provenance vocabulary is data, not prose.** Manifests, lineage, run IDs — even if not all wired yet, the design choice is correct.

---

## What's Weak or Confusing

- **Nothing is live.** No URL, no users, no feedback signal. The single highest-leverage missing artefact in the entire project.
- **Branding is inconsistent.** [`utility/app.py:12`](utility/app.py#L12) sets `page_title="Oireachtas Explorer"` while the project, README, and roadmap call it "Dáil Tracker." Pick one. ("Oireachtas" is also slightly the wrong name — your scope is the Dáil, not the Seanad.)
- **Default landing page is Attendance** ([`utility/app.py:21`](utility/app.py#L21)). Attendance is the *weakest* dataset by your own admission — section 4 of `DATA_LIMITATIONS.md` says it doesn't capture committee work, ministerial duties, or pairing. First impression = most-disclaimed dataset. Member Overview or a story-shaped homepage would be a stronger landing.
- **No homepage / orientation.** Eight peer-level pages, no narrative entry. New users won't know where to look.
- **Lobbying is manually exported CSV.** The most analytically interesting source has the most fragile ingestion path. If you stop downloading, the pipeline silently goes stale. ([`lobby_processing.py`](lobby_processing.py), §7.1 of DATA_LIMITATIONS.)
- **Fuzzy name-join key is still primary.** Documented honestly in §11 of DATA_LIMITATIONS but no `match_method` / `match_confidence` columns surfaced in the UI. A wrong join + a public chart = a real risk.
- **PDF parsers have no golden-file tests yet.** Layout drift = silent corruption. This is the single biggest *trust* risk to the project.
- **`iris_oifiiguil_*.py` files at repo root.** Three files (`iris_oifiguil_etl.py`, `iris_oifiiguil_downloader.py`, plus a deleted `iris_oifiiguil.py`) at the top level, work-in-progress, with at least one typo'd filename ("oifiiguil" vs "oifigiuil"). Per memory, this is deferred experimental work — but it's sitting in the root, not in [`pipeline_sandbox/`](pipeline_sandbox/), and it crosses the "pipeline_sandbox rule" you set yourself.
- **The contract-pack folder is named `dail_tracker_bold_ui_contract_pack_v5`** — version-numbered folders inside a repo are usually a smell. Either commit to a version or rename.
- **Eight pages, no observable user.** Each page is technical debt waiting to happen if it doesn't survive the first user contact. Consider hiding 3–4 of them behind an "explore more" toggle until validated.
- **"Reasons we exist" in README skews architectural, not journalistic.** "Joins fragmented public data" is *how*; the reader needs *what story can I now tell?* Lead with two example questions, not the data sources.

---

## Biggest Opportunities

1. **Reframe as a journalist research tool, not a citizen-facing app.** Citizens don't open eight tabs of parliamentary metadata. Journalists do. Your roadmap already implicitly accepts this (success = one journalist). Make it explicit in positioning. This unlocks better UX choices: search-by-name first, copy-to-clipboard everywhere, "embed this chart" buttons, methodology-first homepage.
2. **The data export is the product, not the dashboard.** Many civic-data projects discover the Parquet/CSV files matter more than the UI. You already have versioned releases planned (Day 18 of the plan). Pull this earlier and make it a first-class entry point: a `/data` page with permalinks. Newsrooms will use the data; the dashboard is the *trust artefact* that proves it's clean.
3. **Story-shaped pages, not dataset-shaped pages.** "Who lobbied your TD?" "Which TDs missed the most votes in 2025?" "Which interests overlap with which committees?" — these are *questions journalists already ask*. Three story pages with embedded charts will beat eight dataset tabs.
4. **TheyWorkForYou-IE positioning.** There is no equivalent product in Ireland. "There's no TheyWorkForYou for Ireland" is a tweetable pitch. mySociety would probably link to it from their docs. You may not need to build a brand — adopt theirs.
5. **The 2029 election angle.** A platform that's been continuously running for 3+ years by election time is dramatically more credible. Start the public clock now.
6. **Academic partnerships.** Politics depts at TCD/UCD/DCU run methods modules every year that need real datasets. One conversation with one professor = ~50 students/year using your tool. Mention this in the journalist email — academics are easier first users than journalists.
7. **Defamation safety as a feature.** Most civic-data sites don't think about this; you have. Surface DATA_LIMITATIONS prominently, with per-page inline caveats, and that becomes a *differentiator* against a hypothetical journalist's spreadsheet.

---

## User Experience Feedback (first-time view)

Walking the app cold via [`utility/app.py`](utility/app.py):

- **What's the URL bar say?** "Oireachtas Explorer." I am a journalist who Googled "Irish TD lobbying." This name doesn't match my query. I might bounce.
- **First page is Attendance.** I see a list of TDs and dates. I don't know what to do. There's no orienting sentence. I do not yet know I should care.
- **No search bar at the top of every page.** I came with a TD's name. I have to figure out what page lets me search by name. (Member Overview does, but I have to find it.)
- **The "Member Overview" link is positioned second.** It is your strongest concept. It should arguably be the default and rename the app to its concept ("Find a TD" or similar).
- **No "what is this?" footer.** A journalist will need a one-line "this is built on the Oireachtas API + PDFs + lobbying.ie register, see methodology.md."
- **Provenance is currently invisible** (per [v4 §0](doc/dail_tracker_improvements_v4.md)). Plan addresses this Day 4 — pull it forward to Day 1. Without provenance, the project is not citable.

A simple test: ask one non-technical, politically-literate friend to find "the most lobbied TD in 2024" using your local Streamlit. Time them. If it's >2 minutes, the IA is wrong.

---

## Business / Strategy Feedback

- **You probably do not have a "business," and that is fine.** Civic transparency is overwhelmingly grant-funded, donation-funded, or fiscally sponsored. mySociety, OpenSpending, How They Vote EU, OpenKamer all run on this model. Trying to monetize early kills credibility.
- **Realistic funding paths, in order of plausibility:**
  1. **Fiscal sponsor** under [Right To Know](https://righttoknow.ie/) or [Open Knowledge Ireland](https://www.openknowledge.ie/) — gives you a non-profit umbrella and donation rails.
  2. **Mozilla / Code for All / OpenAire / EU civic-tech micro-grants** in the €5k–€25k range. Realistic at your maturity.
  3. **Commissioned data work for journalism orgs** (Noteworthy/Right to Know): one-off "we need a custom dataset of X" gigs. Your pipeline is the moat, not the UI.
  4. **Political consultancies / lobbying firms** would pay for clean data on their own activity vs competitors. This is the *only* commercial buyer, but it cuts against your credibility positioning. Probably not worth it.
- **Distribution: skip the dashboard, pitch the dataset.** Send a researcher a CSV with a one-paragraph note: "this is every lobbying contact reported between TD X and pharma firms 2020–2025, joined to their committee membership and votes on health-related bills. I think there's a story here." That's how civic-data projects get traction. The dashboard is the trust artefact, not the entry point.
- **Competitive landscape is thin in Ireland and that's a yellow flag.** No direct competitor + a 5-year-old, well-funded data ecosystem (lobbying.ie, Oireachtas API) means either (a) no one has tried, or (b) the data is harder to make defensible than it looks. Your DATA_LIMITATIONS doc suggests (b) is partly true. Treat the absence of competitors as both opportunity and warning.

---

## Technical / Product Feedback

- **Top technical risk: PDF parser silently breaks.** Day 7–9 of the plan addresses this with golden files. Do not deploy publicly before this is in place. A wrong attendance number is worse than no attendance number.
- **Second technical risk: lobbying delimiter parsing.** §7.6 of DATA_LIMITATIONS — `::` and `|` in free text would silently corrupt rows. Add an assertion that no field contains the delimiter character in unexpected positions, fail loud.
- **Third technical risk: Streamlit Community Cloud.** It will sleep your app, has a 1GB resource ceiling, and isn't designed for traffic spikes from a journalist's article going viral. The plan tags it as "alpha" — fine, but have an HF Spaces / Fly / Railway fallback ready before you email a journalist who might publish.
- **The Parquet-on-a-data-branch trick** (Day 2 of the plan) is clever but has GitHub LFS-shaped pain at scale. Consider [HuggingFace datasets](https://huggingface.co/docs/datasets/), Cloudflare R2, or B2 with public-read for hot data. R2 has zero egress cost, which is important if any chart goes viral.
- **DuckDB cold-start on Streamlit Cloud** can be 5–15 seconds for a fat parquet. Consider per-page cached connections or pre-warming.
- **`match_method` and `match_confidence` should be first-class columns** on every joined view, surfaced in the UI as a small chip ("name-fuzzy join, 0.92 confidence"). This converts a known weakness into a transparency feature.
- **You have a `services/` folder and a `utility/` folder and a `pipeline_sandbox/` folder and root-level scripts** ([`enrich.py`](enrich.py), [`pipeline.py`](pipeline.py), [`legislation.py`](legislation.py), etc.). The contract folder is `dail_tracker_bold_ui_contract_pack_v5/`. This is going to be confusing for any second contributor. The Week 4 consolidation step into `pipeline/sources/` is the right move — don't skip it.
- **Bus factor = 1 + a hobby + full-time pacing.** This is a real *product* risk, because if you stop, a journalist who relied on a chart loses their citation. Either explicitly mark every artefact "alpha, no SLA" or arrange a fiscal sponsor *before* the first journalist publishes anything based on the data.

---

## Prioritized Action Plan

### High priority (do before anything else)

1. **Reach a journalist now, before continuing the build.** Don't wait until Day 20. Pick the most newsworthy single insight currently extractable (e.g. "TD X had Y lobbying contacts on health policy, sat on health committee, voted Z times on health bills"). Send it as a one-paragraph email + CSV to one named person. Their reaction determines the next 40 days more than any code change. Cost: ~2 hours.
2. **Fix the landing page.** Default to Member Overview or a single-question page. Rename "Oireachtas Explorer" → "Dáil Tracker." Add a one-sentence "what is this?" header. Cost: ~1 hour.
3. **Add `match_method` / `match_confidence` to the highest-traffic views and surface them.** Turns the weakest part of your data story into a transparency feature. Cost: ~1 day.
4. **Golden-file tests for the three PDF parsers** — Days 7–9 of your plan. Don't deploy without this. Cost: per your estimate, ~3 days.
5. **Public deploy with provenance + freshness pill on Day 1, not Day 4.** The plan sequences provenance after deploy. Reverse this — every public version must have provenance from minute one.

### Medium priority

6. **Lobbying.ie auto-export** (Day 28–29 of your plan). The biggest single ingestion risk. Pull earlier if you can.
7. **Cut the dashboard to 4 pages** for the public alpha: Member Overview, Lobbying, Votes, "Methodology & Data Limitations." Hide Attendance, Payments, Interests, Legislation, Committees behind an "Explore more" link until first-user feedback says they're needed.
8. **Apply for fiscal sponsorship via Right To Know or OKI** before the first journalist publishes anything. Bus factor + libel surface.
9. **Consolidate the four `iris_oifiguil_*` root-level files into `pipeline_sandbox/iris/`.** It's already against your own rule.
10. **Per-page caveat banners** referring to specific DATA_LIMITATIONS sections.

### Low priority

11. Renaming the contract-pack folder away from `_v5`.
12. The pluggable scraper interface refactor (Track B, Week 7). Your own plan correctly says "don't do this if Track A is happening."
13. Dim/fact data modelling.
14. Mobile UX. Journalists use desktop.
15. Pre-2020 backfill. Different project.

---

## Brutally Honest Feedback

- **You have built a research project and labelled it a product.** The artifact quality is high; the audience is zero. The 8-week plan is structured to convert it to a product, but a research project that decides to become a product without first checking demand usually becomes a research project with extra steps. **Validate demand on day one, not day twenty.**
- **The contract pack and agent infrastructure may be over-engineering for a one-developer alpha.** It is genuinely useful as cognitive offload for solo work, but every line of contract YAML is also code that has to be maintained. Be honest with yourself: is this load-bearing for the *user*, or for *you*?
- **"One named journalist" is a brittle success criterion.** Journalists are busy, slow to respond, and switch beats. Plan as if your first three emails are ignored. The metric should probably be "two journalists, one academic, and one non-technical citizen group have used the tool and sent feedback within 90 days." Not "one journalist replied."
- **`DATA_LIMITATIONS.md` is so good it is also a warning.** When the disclaimer doc is the most polished artefact in the project, it usually means you (correctly) understand the data is shakier than the UI suggests. Resolve that asymmetry before going public — either by improving the data or by making the UI quieter and more cautious.
- **You're a single point of failure and the data is sensitive.** A wrong vote attribution to a named TD, made viral by a journalist, with no organisation behind you — that is a personally bad day. Sort out the umbrella before the spotlight.
- **Defaulting to Attendance suggests you have not yet asked "what's the strongest first impression?"** That's a tell that the project is still organised around what you built rather than what the user needs. Most projects at this stage have the same problem. Fix it before launch.

---

## Questions I Should Answer

1. **Who, by name, is your first user, and what story do they want to publish?** If you can't answer this in one sentence, you don't have a target — you have a hope.
2. **What's the single most damning chart your dataset enables today?** If you don't know, you don't have a launch story.
3. **If only ONE page existed, which would it be — and why are the other seven there?** Each page beyond #1 should answer a question the first one can't.
4. **Is this a product, a research project, a portfolio piece, or a public good?** All four are valid; they imply different next moves. Pick one.
5. **What happens operationally if Oireachtas changes a PDF layout while you're on holiday?** If the answer is "the dashboard quietly serves stale or wrong data for two weeks," that is a defamation risk, not just an ops risk.
6. **What's your defamation/libel insurance posture?** Or: under whose legal umbrella does the project sit? "Mine personally" is an answer, but a costly one.
7. **Do you actually want to maintain this for 5 years?** If yes, the operational layer is the bottleneck. If no, the project should plan for handover from day one (a fiscal sponsor + a contributor onboarding doc + a `handover.md`).
8. **If TheyWorkForYou launched an Ireland instance tomorrow, what would you uniquely offer?** If the answer is "lobbying integration," that's a strong differentiator — make it the headline. If it's "nothing," reconsider building from scratch vs. contributing to mySociety.
9. **Would the lobbying.ie register operator, or the Oireachtas data team, be willing to talk?** A 30-minute call with either de-risks ~3 weeks of pipeline guesswork.
10. **What's the smallest possible thing you could ship this week that gets a real user reaction?** Probably not eight pages and a refresh cron.

---

A final framing note: the project is in better shape than most civic-data efforts I've seen, *technically*. The remaining work is mostly about audience, distribution, and trust — not architecture. Your strongest leverage in the next 30 days is a single email to a single journalist, not a single line of code.
