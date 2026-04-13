# Dáil Tracker — Roadmap to Senior/Staff Standard & Licensing Guide

**Last updated: 13 Apr 2026**

---

## Part 1 — Where This Project Is Now (Honest Assessment)

This project currently sits at a **strong mid-level / early senior level**.

**What reads as senior:**
- Real engineering problems solved with thought (PDF parsing, fuzzy name matching across Unicode-variant datasets, concurrent API calls)
- Genuinely novel domain work (lobbying × member interests cross-referencing that no other Irish project does)
- Architectural self-awareness (the REARCHITECTURE doc, medallion planning, DATA_LIMITATIONS doc)
- Correct library choices (Polars for transforms, PyMuPDF over camelot after benchmarking, JSON over pickle)

**What reads as mid-level or below:**
- Doesn't run on any machine except yours (hardcoded `C:\\Users\\pglyn\\` paths)
- No single entry point — 9 scripts must run in an undocumented order
- No tests — bugs like the double-join count inflation have persisted
- No separation of concerns — scripts mix I/O, transformation, and aggregation in one flat stream
- Outputs aren't validated — nothing checks whether results are correct
- No README that tells a visitor what they're looking at or how to run it

**The gap in one sentence:** You've built something genuinely valuable but haven't packaged it so that anyone else (including a future you, or a hiring manager) can see that value.

---

## Part 2 — The Phased Roadmap

### Phase 1 — Make It Work For Anyone (~6 hours)

This is the single highest-ROI work you can do. Until someone can clone the repo and see results, everything else is invisible.

**1.1 Replace all hardcoded Windows paths with `config.py` imports (~2 hours)**

Every `C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\` becomes a relative path imported from `doc/config.py`. This is tedious but mechanical — find-and-replace across 6 files. Test by running from a fresh directory.

**1.2 Create `pipeline.py` orchestrator (~2 hours)**

A single entry point that imports each module's `main()` and runs them in the correct order. Include a `--demo` flag that limits to 10 TDs and 1 PDF for quick testing.

```
python pipeline.py              # full run
python pipeline.py --demo       # 10 TDs, 1 PDF, fast
python pipeline.py attendance   # single stage
```

**1.3 Write 5-10 critical pytest assertions (~1 hour)**

You don't need 100% coverage. You need assertions that catch the bugs you've already found:

- Every TD in the API appears in the enriched output
- No lobbying count exceeds the number of lobbying records
- Payment amounts parse to valid floats
- No duplicate join keys after normalisation
- The `most_lobbied_politician` count columns don't contain duplicated values from double-joins

**1.4 Update the README (~1 hour)**

Replace the current 12-line README with: what the project does (2 sentences), how to install (`pip install -e .`), how to run (`python pipeline.py`), what outputs it produces, and one or two headline findings from the data. Add badges for Python version and license.

**Why this phase matters:** A hiring manager will spend 30 seconds on your repo. If they see a polished README, a working install command, and passing tests, they assume competence. If they see hardcoded paths and no instructions, they move on. Phase 1 fixes the first impression.

---

### Phase 2 — Make the Lobbying Analysis the Headline (~8 hours)

This is your differentiator. No one else has built this for Ireland. Make it the centerpiece.

**2.1 Fix the 3 lobbying bugs (~1 hour)**

- `df` → `lobbying_df` at line 86 (wrong DataFrame for DPO explode)
- Remove the double-join on `most_lobbied_politician` (line 119)
- Fix `endpoint_checker` return inside loop

**2.2 Add DPO filtering and name cleaning (~2 hours)**

Implement the banned names list, title stripping, and position filtering from `dpo_integration_snippet.py`. This is what makes your lobbying counts trustworthy.

**2.3 Join lobbying data onto enriched TDs (~1 hour)**

Use `normalise_join_key` to join DPO lobbying records onto `enriched_td_attendance.csv`. This gives each TD a single row with attendance, party, payments, interests, AND lobbying contacts.

**2.4 Build the analyses that tell a story (~2 hours)**

- Period-over-period "biggest movers" — which TDs suddenly became lobbying targets?
- Shared lobbyists between TD pairs — which TDs are being targeted by the same organisations?
- Degree centrality — who is the most-connected TD in the lobbying network?
- Lobbying method breakdown — are TDs being contacted via email, meetings, or phone?

**2.5 Write a `FINDINGS.md` or Jupyter notebook (~2 hours)**

Present what the data actually reveals. This is what separates a pipeline project from a product. A staff engineer doesn't just build infrastructure — they use it to surface something interesting that creates downstream value.

Include things like: "TD X was lobbied 47 times in the last reporting period, up from 12 in the previous period. 60% of contacts were from organisations in the pharmaceutical sector. TD X holds declared shares in [pharma company]."

**Why this phase matters:** The lobbying × member interests cross-reference is the thing that could get written up in a newspaper. It's the kind of output that demonstrates not just engineering skill but judgment about what's worth building.

---

### Phase 3 — Make It Professionally Engineered (~12 hours)

These are the things a staff engineer looks for in a code review and nods at.

**3.1 Restructure into bronze/silver/gold directories (~3 hours)**

Move raw data to `bronze/` (gitignored), cleaned data to `silver/`, analytical outputs to `gold/`. Update all imports and paths. This demonstrates data engineering vocabulary and practice.

**3.2 Add DuckDB gold layer (~3 hours)**

Load all silver CSVs into DuckDB. Create analytical views (party summary, TD profile with all data sources joined, lobbying network). Write a few of the interview-ready SQL queries from the REARCHITECTURE doc.

**3.3 Add `OireachtasClient` abstraction class (~2 hours)**

Replace the repeated "build URL, call API, parse JSON" pattern with a single reusable client class that has retry logic, session reuse, and consistent error handling. Three scripts collapse to three method calls.

**3.4 Add Pandera schema validation (~2 hours)**

Define schemas for the 3-4 critical DataFrames (enriched attendance, lobbying DPOs, member interests). Schemas catch breaking changes at the point they happen, not three stages downstream.

**3.5 Set up GitHub Actions CI/CD (~1 hour)**

Run pytest on every push. Run the full pipeline weekly on a schedule. This proves the pipeline is reproducible and catches regressions automatically.

**3.6 Add type hints to core functions (~1 hour)**

Type hints on function signatures for the pipeline's main functions. Enables IDE autocompletion and shows intent. You don't need to type everything — just the public API of each module.

**Why this phase matters:** These are the markers that distinguish "I built something that works" from "I built something that's maintainable, testable, and could be handed to a team." This is the gap between mid-level and senior.

---

### Phase 4 — Staff-Level Polish (ongoing, pick what interests you)

**4.1 Conflict-of-interest detector** — match lobbying org activities against TD declared interests using keyword overlap. Flag aligned sectors. This is the feature that turns the project from "data pipeline" into "civic tool."

**4.2 Voting records integration** — add `/v1/divisions` endpoint. Cross-reference with lobbying: did a TD vote in favour of legislation after being lobbied on that topic?

**4.3 Star schema in DuckDB** — dimension tables (dim_td, dim_committee, dim_date) and fact tables (fact_attendance, fact_vote, fact_lobbying). This is Kimball modelling — the most commonly asked data engineering interview topic.

**4.4 Streamlit dashboard** — a searchable TD profile page showing all data sources on one screen. This is the lobbyieng equivalent but broader.

**4.5 Precomputed insights** — materialise expensive aggregations at pipeline time rather than query time. Write a `gold/metadata.json` with pipeline version, run timestamp, row counts, and failure notes.

---

## Part 3 — Licensing Guide for Publishing

### Your Code License

You need to choose a license for your own source code before publishing. For civic tech projects, the common choices are:

| License | What it allows | Used by |
|---|---|---|
| **MIT** | Anyone can use, modify, distribute. No restrictions except keeping the license notice. | lobbyieng, most civic tech |
| **Apache 2.0** | Same as MIT but with explicit patent grant and contributor terms. | unitedstates/congress |
| **GPL v3** | Anyone can use and modify, but derivatives must also be open source. | theyworkforyou |
| **CC0 (public domain)** | No restrictions at all. Waives all rights. | Some government data projects |

**Recommendation: MIT.** It's the simplest, most permissive, and what the comparable Irish project (lobbyieng) uses. Add a `LICENSE` file to the repo root.

To do this: create a file called `LICENSE` in your project root containing the MIT license text with your name and year.

---

### Data Source Licenses — What You Need to Comply With

Each data source you ingest has its own license terms. You don't need to "request" licenses for most of these — they're pre-granted under PSI regulations — but you do need to comply with their attribution requirements.

#### 1. Oireachtas API Data (members, legislation, questions, debates, divisions)

**License:** Oireachtas (Open Data) PSI Licence — based on Creative Commons Attribution 4.0 (CC BY 4.0).

**Source:** https://www.oireachtas.ie/en/open-data/license/

**Do you need to request permission?** No. The licence explicitly waives the requirement to formally apply for permission.

**What you must do:**
- Acknowledge the Houses of the Oireachtas as the creator of the data
- Retain their copyright notice
- Include a reference to the Oireachtas (Open Data) PSI Licence in your project
- If you publish legislation text, include a disclaimer that it is not the official text

**Suggested attribution text for your README:**
```
Parliamentary data (members, legislation, questions, debates, divisions) is sourced from
the Houses of the Oireachtas Open Data API (https://api.oireachtas.ie/) and is used under
the Oireachtas (Open Data) PSI Licence (CC BY 4.0).
© Houses of the Oireachtas.
```

**Action required:** Add attribution to README. No application needed.

---

#### 2. Oireachtas PDF Publications (attendance, payments, member interests)

**License:** Same Oireachtas (Open Data) PSI Licence as above — these PDFs are published on data.oireachtas.ie and oireachtas.ie under the same terms.

**Source:** https://www.oireachtas.ie/en/copyright-and-reuse/

**Do you need to request permission?** No.

**What you must do:** Same attribution requirements as above. Note that if you reproduce text from the Register of Members' Interests, you should note it is extracted/derived data and may contain parsing errors.

**Action required:** Add attribution to README. No application needed.

---

#### 3. Lobbying Register Data (lobbying.ie)

**License:** PSI Licence from the Standards in Public Office Commission (SIPO).

**Source:** https://www.lobbying.ie/about-us/our-policies/reuse-of-public-sector-information/

**Do you need to request permission?** No — it's available under PSI regulations. The register is explicitly public and free to view and search.

**What you must do:**
- Acknowledge the source and copyright of the Standards in Public Office Commission
- Do NOT use the data for advertising or promoting a product/service
- Do NOT use the data for illegal, immoral, fraudulent, or dishonest purposes
- Note that SIPO is not liable for any loss associated with reuse and does not guarantee the data is error-free

**Suggested attribution text:**
```
Lobbying data is sourced from the Register of Lobbying (https://www.lobbying.ie/),
maintained by the Standards in Public Office Commission under the Regulation of
Lobbying Act 2015. Used under PSI Licence terms.
© Standards in Public Office Commission.
```

**Action required:** Add attribution to README. No application needed.

---

#### 4. Companies Registration Office (CRO) — if you add this later

**License:** PSI regulations apply to CRO data, but the API (`services.cro.ie`) has its own terms of use.

**Source:** https://services.cro.ie/

**Do you need to request permission?** You should check their terms of use page before integrating. CRO data is public information but the API may have usage restrictions (rate limits, commercial vs non-commercial use).

**Action required:** Read the CRO API terms of use before integrating. May need to register for API access.

---

#### 5. Central Statistics Office (CSO) — if you add constituency demographics

**License:** Creative Commons Attribution 4.0 (CC BY 4.0) via the Irish Government's Open Data initiative.

**Source:** https://data.cso.ie/

**Do you need to request permission?** No.

**What you must do:** Attribute the CSO as the data source.

**Action required:** Add attribution to README when integrated. No application needed.

---

#### 6. Wikidata — if you add TD photos/social links

**License:** CC0 (public domain) for data. Individual images may have their own licenses (check each image's Wikimedia Commons page).

**Source:** https://www.wikidata.org/

**Do you need to request permission?** No for data. Check individual image licenses if using photos.

**Action required:** No attribution legally required for CC0 data, but good practice to acknowledge Wikidata. Check image licenses individually.

---

### GDPR and Personal Data Considerations

This project processes publicly available information about public officials acting in their public capacity. Under Irish data protection law (GDPR as implemented in the Data Protection Act 2018), there are some considerations:

**What's likely fine:**
- Names, parties, constituencies, committee memberships of TDs (public officials acting in public capacity)
- Voting records, attendance, parliamentary questions (public records)
- Declared interests (published by the Oireachtas specifically for public scrutiny)
- Lobbying records (published by SIPO specifically for public transparency)
- Payment data (published by the Oireachtas under FOI/transparency obligations)

**What to be careful about:**
- Contact details of individual lobbyists (phone numbers, personal emails) — the lobby org registry CSV contains `person_responsible_email` and `person_responsible_telephone`. Consider whether you need to include these in any published output. The data is public on lobbying.ie but republishing personal contact details in a different context may raise questions.
- Do not enrich with private/personal information about TDs beyond what they have publicly declared or what is published by official sources.

**Recommendation:** Add a brief privacy note to your README stating that all data is sourced from official public registers and APIs, and that the project processes only publicly available information about public officials acting in their official capacity. This isn't a legal requirement but it demonstrates awareness.

---

### Pre-Publishing Checklist

Before making the repo public, work through this list:

- [ ] **Choose a code license** — recommend MIT. Add a `LICENSE` file to the repo root.
- [ ] **Add data source attributions** to the README (Oireachtas CC BY 4.0, SIPO PSI Licence).
- [ ] **Remove your Windows username** from the codebase — `C:\\Users\\pglyn\\` appears in 6+ files and in the commented-out code in `oireachtas_api_service.py` (line 161).
- [ ] **Check for API keys, passwords, or credentials** in the repo or git history. Run `git log --all --full-history -- '*.py' | head` and search for anything sensitive.
- [ ] **Gitignore raw data files** — CSVs, JSONs, and PDFs should be regenerated by the pipeline, not committed. Check that `.gitignore` covers `members/`, `bills/`, `lobbyist/raw/`, `*.json`, `*.csv`, `*.pdf`.
- [ ] **Review lobby org CSV for personal contact details** — decide whether `person_responsible_email` and `person_responsible_telephone` should be dropped from any published outputs.
- [ ] **Add a disclaimer** that extracted PDF data may contain parsing errors and is not an official source.
- [ ] **Scrub git history** if it contains committed data files, personal paths, or credentials. Use `git filter-repo` (preferred) or `BFG Repo-Cleaner`. Alternatively, if the history isn't valuable, start a fresh repo with a clean initial commit.
- [ ] **Verify the repo description** matches what the project actually does — update the GitHub repo description and topics/tags.
- [ ] **Add a `CONTRIBUTING.md`** if you want others to contribute (optional but professional).
- [ ] **Add a `.env.example`** if you plan to use environment variables for configuration (optional).
