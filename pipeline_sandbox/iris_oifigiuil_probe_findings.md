# Iris Oifigiúil — Probe Findings (data-driven, v2)

**Date**: 2026-05-02 (v2)
**Author**: Claude (Patrick reviewing)
**Corpus scanned**: 467 PDFs (`data/bronze/iris_oifigiuil/IR*.pdf`, `Ir*.pdf`), spanning **2022-01-04 → 2026-03-31**.
**Companion docs**:
- `iris_oifigiuil_discovery.md` — high-level scoping (prior author)
- `iris_oifigiuil_single_pdf_findings.md` — block-by-block dump for IR310326.pdf
- Probes: `iris_oifigiuil_probe.py`, `_probe2.py`, `_probe3.py`, `_evidence.py`, `_evidence2.py`, `_value_probe.py`

---

## TL;DR — verdict (v2, after evidence rounds)

**Worth parsing? Yes — narrowly.** ~70% of every issue by character count is high-volume legal noise (winding-up, ICAV, fishing quotas, standards) that has no parliamentary signal and should be skipped or only counted.

The **30% that is high-value** unlocks data not available anywhere else in the project. Headline finds:

| Find | Linkability | Materiality |
|---|---|---|
| **Member-Interest Supplement (Sections 6 & 29 Ethics Acts)** | annual register, votes, legislation | ⭐⭐ unique — captures **mid-year additions** + **conflict-of-interest disclosures tied to specific votes**. Annual register cannot show either |
| **SI metadata layer** (department, parent Act, EU-alignment, election effect) | members, attendance, legislation | ⭐⭐ eISB has SI text, not metadata. ~30% of SIs are EU-aligned; >50% are amendments |
| **Tax Defaulters List (statutory §1086 publication)** | (new gold table), members register | ⭐⭐ **publicly nameable by statute**, quarterly, newspaper-grade content |
| **Foreshore Act licences** | (new gold table) | ⭐⭐ permitting timeline for offshore wind / port / aquaculture — directly relevant to Ireland's 10GW offshore-wind 2040 target |
| **State-board appointments** (An Bord Pleanála, Horse Racing Ireland, EPA, etc.) | members register | ⭐⭐ governance signal, often politically charged |
| **Regulated-entity attrition feed** (CB authorisation revocations) | (new gold table) | ⭐ no commercial product currently delivers this as a Tue/Fri webhook; KYC-adjacent |
| **By-election writs** | members register | ⭐ rare (1 in 5 years) but very high political signal when it does fire |
| **Section 6/29 supplement subjects** linked to specific Bills (Madigan / Russia-Ukraine sanctions example) | legislation | ⭐ rare but joinable to votes |

The other categories (winding-up, ICAV, standards, bankruptcy individuals, fishing quota tables) are not worth full parsing for *this project's audience*. Some have value as slim structured feeds; others should be dropped.

### Why no one else has done this

1. **Marginal-value story is niche**. SI text is on `irishstatutebook.ie` (eISB); aggregate insolvency is on ISI; Central Bank Register is publicly available; CRO has companies. Each domain has its silo. Iris is the only **cross-domain change-feed**, but cross-domain consolidation is hard work for a small audience.
2. **PDFs are awful** (format break 2022 → 2024, encoding issues, mis-splits) — multi-day parse work for niche ROI.
3. **EU Open Data Directive (2019/1024)** transposed by SI No. 376 of 2021 obliges publication, not transformation. The State has met its compliance bar by hosting the PDFs. No political pressure for further transformation.

The competitive moat: willing to do the dirty parse work for an audience that's small but specific (parliamentary-tracker users, journalists, civic-tech, academics). theyworkforyou-Ireland is exactly that audience.

---

## 1. Corpus inventory

| Metric | Value |
|---|---|
| Total PDFs | 467 |
| Valid (≥5KB) | **436** |
| Corrupted (146-byte 404 stubs) | 31 |
| Year span | 2022 → 2026 |
| By year suffix | `22`: 104, `23`: 104, `24`: 105, `25`: 104, `26`: 50 |
| Blocks after `_{6,}` split | **14,393** |
| Total chars extracted | ~64M |
| Schedule | Tue + Fri, Government Publications Office |
| Filename | `IR{DDMMYY}.pdf` (case-mixed: `Ir...` for older years, `IR...` for newer) |

The 31 corrupted files are 146-byte HTML 404 responses saved as `.pdf`. Re-running the downloader with suffix variants (`-2.pdf`, ` AMENDED.pdf`) might recover some but is a downloader task, not analysis.

---

## 2. Format consistency — important caveat

**The format breaks between 2022 and 2024.**

| Format | Years | Underscore-delimiters per issue | `[C-N]`/`[G-N]` block code |
|---|---|---|---|
| Old | 2022, early 2023 | ~5 (cover-page only) | absent |
| Modern | late 2023 → 2026 | 50–80 per issue | present |

Implication: a naive `re.split(r"_{6,}", text)` produces, for a 2022 issue, **one giant 100k-char block** and ~4 boilerplate stubs.

**Hybrid splitter** (cheap, no external deps):
1. Split on `_{6,}`.
2. For each resulting block, if it contains ≥2 anchors of any of these strong starts, re-split on those anchors:
   - `S\.I\. No\. \d+ of \d{4}\.?` (statutory instrument)
   - `^IN THE MATTER OF\b` (winding-up)
   - `^IRISH STANDARD` (standards)
   - `^APPOINTMENT(?:S)? (?:AS|OF)\b`
   - `^FÓGRA\b` / `^NOTICE\b`
   - `^SUPPLEMENT TO REGISTER OF INTERESTS\b`

This restores per-notice structure for old-format issues without breaking modern ones.

### Modern-format block codes

`[C-N]` = Company, `[G-N]` = Government, `[L-N]` = Local authority. Useful as a free coarse classifier but only available post-2023.

---

## 3. Block-length distribution

| Statistic | chars |
|---|---|
| min | 9 |
| median | 999 |
| p90 | 2,050 |
| max | 174,182 |
| Blocks > 15,000 chars (~5+ pages) | **253** of 14,393 (1.8%) |

**The fat tail** = old-format mis-splits + fishing-quota tables (Department of Agriculture, Food and the Marine multi-page schedules).

**Rule**: blocks > 15,000 chars → classify-then-truncate. Store category, header, first 2,000 chars, `truncated=True`. **Exception**: fishing-quota blocks should be passed through `fitz.find_tables()` instead of truncated (see §6).

---

## 4. Category taxonomy (final, evidence-driven)

### Tier 1 — Full prose parse (high value, parliamentary-relevant)

| Category | Hits | Anchor / detection | Rationale |
|---|---|---|---|
| `STATUTORY_INSTRUMENT` | 3,500 | `S\.I\. No\. \d+ of \d{4}\.?` | metadata layer eISB doesn't expose; election effect, EU-alignment %, dept distribution all measurable |
| `MEMBER_INTEREST_SUPPLEMENT` | 34 blocks / 27 PDFs | `SUPPLEMENT TO REGISTER OF INTERESTS` / `STATEMENT UNDER SECTION 6\|29 OF THE ETHICS` | **unique** — annual register cannot show mid-year additions or §29 conflict-of-interest disclosures (see §5) |
| `TAX_DEFAULTERS_LIST` ⭐NEW | 43 hits / 17 PDFs | `Section 1086 \| Defaulters Lists?` | **statutory quarterly publication, publicly nameable by law**; sample shows full table of names, addresses, settlement amounts. Newspaper-grade content |
| `STATE_BOARD_APPOINTMENT` ⭐NEW | included in 393 APPOINTMENT hits | `APPOINTMENT TO {body}` + `Government has appointed` | An Bord Pleanála, Horse Racing Ireland, EPA, Pensions Authority, etc. Sample evidence: "Mr. Peter Mullan appointed Chairperson of An Bord Pleanála 23 Jan 2024 – 22 Jan 2031" |
| `FORESHORE_LICENCE` ⭐NEW | **856 hits / 71 PDFs** | `FORESHORE ACT \| foreshore licen \| foreshore lease` | offshore wind, ports, marinas, aquaculture permits. **Highest-volume keeper** after SI |
| `AGREEMENT_INTO_FORCE` | 3 | `AGREEMENTS? WHICH (?:HAVE )?ENTERED INTO FORCE` | rare, very high signal — international treaties Ireland is bound by |
| `BILL_SIGNED_BY_PRESIDENT` | scattered | `signed by the President \| has accordingly become law` | commencement date for Acts; e.g. *"signed by the President on 16 Feb 2024 ... CORONER'S (AMENDMENT) ACT 2024 (No. 4 of 2024)"* |
| `COMMISSION_OF_INVESTIGATION` | 10 | `COMMISSION OF INVESTIGATION` (without false-positive `sole member`) | rare, very high signal |
| `EXCHEQUER_STATEMENT` | 22 | `EXCHEQUER (STATEMENT\|ACCOUNT) \| FISCAL MONITOR` | quarterly Fri issues — full revenue/expenditure breakdown |
| `BYELECTION_WRIT` ⭐NEW | 1 | `writ for the holding of a by-?election` | rare in 5 years but very high political signal |
| `REFERENDUM_OR_ELECTION` | 42 | `REFERENDUM \| TOGHCHÁN \| POLLING DAY ORDER` | turnout, polling-day orders, results |
| `POLITICAL_PARTY_REGISTRATION` | 12 | `Standards in Public Office Commission \| Electoral Acts` | direct relevance to members tracker |

### Tier 2 — Slim structured parse (single regex, free macro indicator)

| Category | Hits | What we extract | Rationale |
|---|---|---|---|
| `WINDING_UP_LIQUIDATION` | 29,707 | company name, liquidator, court, date | aggregate counts as distress signal; liquidator concentration (Aisling Meehan, Ken Fennell appear repeatedly = workload-curve indicator) |
| `IRISH_STANDARD_REVOCATION` ⭐corrected | 49 / 22 PDFs | I.S. code, supersession date | Per-revocation events for Irish-originated standards (fire, gas, electrical). Earlier "no signal" verdict was wrong — see §6 |
| `CB_AUTH_REVOCATION` ⭐reclassified | 198 hits | firm name, statute section, date | **NOT enforcement** — these are firms surrendering their own authorisation as part of liquidation. Useful as a regulated-entity attrition signal |
| `BANKRUPTCY` (counts only) | 1,776 | court, date, petition_type — **NEVER name** | quarterly counts comparable to ISI Table 7.2 + petition-type ratio (self vs creditor) that ISI doesn't publish |
| `PENSION_SCHEME_EVENTS` ⭐NEW | 102 / 36 PDFs | scheme name, action verb (registered/terminated), administrator | pension scheme registrations + terminations — identifies failing schemes |
| `CHARITY_COOP_LIFE_EVENTS` ⭐NEW | 366 / 40 PDFs | society name, registration number, action verb | Industrial and Provident Societies Acts — co-ops, water schemes, community groups (e.g. "Caher Group Water Scheme Co-Operative Society"). **Niche but unique** — only public record |
| `REVENUE_FORFEITURE` ⭐NEW | 10 / 8 PDFs | vehicle make + identification mark, statute section | Revenue Commissioners §142 Finance Act 2001 vehicle seizures — count only |

### Tier 3 — Count only

| Category | Hits | Rationale |
|---|---|---|
| `FISHING_QUOTA` (header only — body via `fitz.find_tables()`) | 14,975 | see §6 — bodies are tables, not prose. Optional second-pass parser |
| `ICAV_MIGRATION` | 12 | source jurisdictions are offshore-financial-centres (Guernsey, Cayman) **NOT London**. Earlier Brexit framing was wrong |
| `COURT_RULES_AMENDMENT` | 198 | niche legal-professional audience |
| `TRADE_UNION_REGISTRATION` | 23 | small but specific |

### Tier 4 — Drop entirely

| Category | Why |
|---|---|
| `IRISH_STANDARDS` blanket-adoption boilerplate | NSAI auto-adopts all CEN/CENELEC ENs as published; Iris re-states this each issue. No per-standard data except the Tier-2 revocations |
| `PROCESS_ADVISER_SCARP` | corporate rescue boilerplate |
| `NATURALISATION` | only 19 hits, all in unrelated SI-text contexts; Iris does not publish individual citizenship grants |
| `CORONER_APPOINTMENT` | 0 individual appointments published; only the 2024 Act amendment surfaced |
| `NATIONAL_LOTTERY` | 18 hits all inside Exchequer Statement (line item, not separate category) |
| Generic `FOGRA_NOTICE` catch-all | over-matches; absorbed by other categories |

---

## 5. Member-Interest Supplement — deep dive (the big find)

**40 individual declarations parsed; 27 (67%) have NO matching row in the existing annual-register CSV** (`silver/dail_member_interests_combined.csv`).

### Material differences from `member_interests.py`

| Question | Annual register | Iris §6/29 supplement |
|---|---|---|
| When did this TD acquire this shareholding? | No | **Yes — to the Tue/Fri week** |
| Did Minister X declare a conflict before voting on Bill Y? | No | **Yes — §29 ties disclosure to the matter under consideration** |
| What changed between this year's register and last year's? | Computed by diffing PDFs (lossy) | **Authoritative — every change publishes here as it happens** |

### Concrete cases (from 27 Iris-only declarations)

**Robert Troy (Longford-Westmeath), Iris 26 Aug 2022, §29**
- Iris: directorships disclosure under §29 ethics statement
- Annual register: residential rental income only — no directorships
- This is exactly the August 2022 property-disclosure controversy after which Troy resigned as Minister of State. **The Iris supplement IS the correction publication** — the kind of event the annual register can't show.

**Christopher O'Sullivan TD, Iris 13 Jun 2025, §6**
- Iris: declaring a new voluntary-organisation directorship (category 3)
- Annual register (2024): `"No interests declared"` for category 3
- Cleanest possible diff — Iris has a new mid-year acquisition the annual register won't show until next year's PDF.

**Senator Ollie Crowe (Galway), Iris 3 Mar 2023**
- Three back-to-back §29 declarations about *Crowe's Bar (Galway) Ltd* — shares, directorship, and connected interests
- Date-stamped pecuniary interest in a matter under House consideration on a known Friday → **joinable to votes that week**

**James Browne, Iris 2 Sep 2022, §29 (then Minister of State)**
- Property at Lower Church Street, Enniscorthy, Co. Wexford
- Allows tracking the *same person's* property holdings from Minister-of-State role through to the current full Minister seat (he is the Minister attributed in `S.I. No. 15 of 2026`)

**Josepha Madigan, Iris 3 Feb 2023, §29**
- §29 statement made in the context of the Russia/Ukraine sanctions regulations (`Council Regulation (EU) No. 833/2014 ... 2022/263 of 23 February 2022`)
- **Rare case where a §29 disclosure is directly linkable to a specific legislative matter under consideration** — the §29 trigger names what was being decided.

### Schema (final)

```
iris_member_interests
  source_pdf
  pub_date                    # Tue/Fri Iris publication date
  section_trigger             # '6' | '29'
  registration_period         # e.g. '2024-01-01_2024-12-31'
  member_raw                  # 'Richard O'Donoghue TD'
  member_uri                  # resolved via normalise_join_key (separate pass)
  interest_code               # 1..9 (matches member_interests.py INTEREST_CODE_MAP)
  interest_label              # 'Land (including property)'
  declaration_text            # full prose
  matter_under_consideration  # §29 only — extract via 'in respect of...'
```

### Block structure (mechanical to extract)

```
STATEMENT UNDER SECTION 6|29 OF THE ETHICS IN PUBLIC OFFICE ACTS, 1995 AND 2001
Notice is given herewith ... in respect of the registration period {DATE_RANGE}...
Name of Member concerned: {NAME}
Category of Registrable Interest(s) concerned:
{INTEREST_CODE} - {INTEREST_LABEL}
{numbered (i)/(ii)/(iii) sub-fields with prose}
```

Two regexes (`Name of Member concerned: ...` and `Category of Registrable Interest(s) concerned: N - LABEL`) yield one row per declaration. The body is the raw prose between the category line and the next stanza. **Same downstream consumer as the annual register** — just stamp `source = 'iris_supplement'` vs `'annual_register'`.

---

## 6. Standards & ICAV & Cancellations — corrections to v1

### Standards (corrected)

Earlier v1 verdict ("no per-standard data, blanket adoption only") was wrong. Evidence:

```
[IR011024.pdf]  I.S. 3218:2013, Fire detection and alarm systems
[IR011024.pdf]  I.S. 3218:2024, Fire detection and alarm systems    ← supersedes 2013 edition
[IR051225.pdf]  I.S. 329:2024+A1:2025, Gas distribution mains       ← amended a year later
[IR091225.pdf]  I.S. 3216:2025, Code of...                          ← supersedes I.S. 3216:2010
```

49 specific I.S. code references in 22 PDFs (~5% of corpus). 16 of those PDFs contain revocation/withdrawal language adjacent. **Pattern: Iris enumerates revocation / supersession events for native I.S. standards** (Irish-originated; not the blanket-adopted EN/ISO catalogue). Domestic safety dominates: fire, gas, electrical sockets.

**Cannot demonstrate industry-favouritism from this data alone** — that would require cross-EU comparison data (CEN catalogue lag-vs-EU-mean). The 49 events ARE useful for tracking **the pace at which Ireland updates its own domestic safety standards** (e.g. a 10-year gap I.S. 3218:2013 → I.S. 3218:2024 for fire-detection systems is itself a story — regulatory-capacity weakness).

(Note: `www.standards.ie` URL appears in the actual Iris PDF text. NSAI's current site is `nsai.ie`; standards.ie was their standards shop and now redirects. The URL in the boilerplate is just dated.)

### ICAV migrations (corrected)

Two structured ICAV migration notices in the corpus identify their source jurisdiction:

```
[IR060226.pdf]  ICAV migration inwards from Guernsey, 03/02/2026
[Ir060623.pdf]  ICAV migration inwards from The Cayman Islands, 01/06/2023
```

**Source jurisdictions: offshore-financial-centres, NOT London.** This is the **AIFMD substance-requirement** flow (post-2014, EU regulators pushing offshore funds onshore into EU-regulated domiciles), not Brexit. Earlier Brexit framing in v1 withdrawn.

### "Surrender of authorisation" (corrected)

198 cancellation/revocation notices, sample wording:

> *"REVOCATION OF AUTHORISATION OF INVESTMENT BUSINESS FIRMS: Notice is hereby given that, **at the request of the firms**, the Central Bank of Ireland has, under Section 16(1) of the Investment Intermediaries Act 1995, revoked the authorisations of: Rathkeale and District Credit Union Limited, Jahander Limited and Gleeson Curtin Limited."*

Plain explanation: Ireland's Central Bank maintains a Register of Authorised Firms. When a regulated firm winds up, the law requires the regulator to be told and the firm's authorisation must be formally cancelled. **"At the request of the firms"** is the key tell — this is the firm asking to be deleted from the register, NOT enforcement.

Useful as **an attrition signal for regulated firms (~198 deaths over 5 years)**, NOT as a regulatory-failure signal. Earlier "regulatory failure indicator" framing in v1 withdrawn.

### Fisheries (corrected)

Earlier v1 verdict ("skip the body") was wasteful. Department of Agriculture, Food and the Marine fishing-quota blocks are **structured tables, not prose**:
- Species-level quota allocations (Cod North-West ICES area VIa × vessel category × tonnes)
- Effort regimes (days-at-sea per vessel class)
- Closure orders (when a quota fills)
- Sea-bass / wild-salmon / brown-crab annual quotas (high political salience)

`fitz.find_tables()` is the right tool. Cost per PDF: a few seconds.

**Audience for fisheries data**: Sea Fisheries Protection Authority (SFPA), DAFM, BIM, Irish Times marine correspondent, MEPs on EU Fisheries Committee. Post-Brexit there's a real political constituency (Brexit Adjustment Reserve €500M, Common Fisheries Policy reform).

If fisheries observation is in scope: emit `iris_fishing_quotas` table `(pub_date, si_no, species, zone, quota_tonnes, vessel_class, period)` via a separate `iris_fishing.py` module. If not in scope, leave as a hook.

---

## 7. SI sub-classification — the metadata layer eISB doesn't expose

3,500 SI records detected. Extractable structure:

| Field | Captured | Notes |
|---|---|---|
| SI number + year | 100% | Hard anchor |
| Title (ALL-CAPS regulation name) | 77% (2,710) | Strong |
| **Department** | 46% (1,611) | Pulled from `Minister for X` clause |
| **Parent Act** | 0.6% (20) | **Regex needs work** — varied formulations |
| Subject keywords | multi-tag | Useful but taxonomy needs sharpening |

**Top 10 departments by SI volume (5 years)**:

| # | Department | SIs |
|---|---|---|
| 1 | Finance | 409 |
| 2 | Enterprise | 222 |
| 3 | Agriculture | 188 |
| 4 | Housing | 173 |
| 5 | Justice | 89 |
| 6 | Transport | 66 |
| 7 | Children | 61 |
| 8 | Further [Education] | 51 |
| 9 | Public Expenditure | 40 |
| 10 | Minister of State at Department of Agriculture | 35 |

**Subject-tag distribution** (multi-tag, keyword regex needs refinement):

```
AMENDMENT     1,823    (over half of all SIs are amendments — interesting in itself)
EU_ALIGN      1,073    (~30% of SIs implement EU directives — Irish sovereignty story)
PLANNING        537
JUSTICE         487
COMMENCEMENT    348    (commencement orders — links SIs to Bills via parent Act)
HEALTH          310
FISHING         268
ENVIRONMENT     261
FINANCE         237
EDUCATION       149
TAXATION        102
REVOCATION       70
```

**Two findings worth surfacing in UI**:
- **>50% of SIs are amendments to existing SIs** — the Statute Book is being constantly patched.
- **~30% are EU-aligned** — hard, measurable answer to "how much Irish secondary legislation is downstream of Brussels".

---

## 8. SI volume + election effect (publishable finding)

```
2024 General Election (Nov 29, 2024):
  90 days BEFORE: 234 SIs published
  90 days AFTER:  165 SIs published   ← ~30% drop

2020 General Election (Feb 8, 2020):
  No 2020 PDFs in corpus (starts 2022) — can't measure
```

**A 30% drop in SI issuance in the 90 days after an election is a real, measurable government-turnover effect.** Outgoing ministers rush regulations through before dissolution; incoming ministers spend their first quarter setting up departments. This is the Irish equivalent of "midnight regulations" / "wash-up" that political-science literature studies elsewhere. Publishable finding.

Annual SI volumes (where SI date matches publication year):

```
2022:  720    2023:  656    2024:  682    2025:  639    2026 YTD: 104 (3 months)
```

Roughly steady at 650-720/year. The 2024 figure isn't visibly suppressed annually, only locally around the November 29 cutoff.

---

## 9. Financial / fiduciary value (extracted with intent)

The strongest commercial-adjacent angle. Extractable from Iris that compliance / KYC / distressed-debt teams would care about:

| Extractable | Audience | Why they care |
|---|---|---|
| **Live change-feed of regulated-entity status** (CB authorisation revocations, ICAV strike-offs, credit union dissolutions) | KYC / AML at banks, fund administrators, payment service providers | Counterparty regulatory-status change is a Friday-morning compliance flag. **No commercial product currently delivers this as a structured Tue/Fri feed** |
| **Tax Defaulters List (§1086 quarterly)** ⭐NEW | Compliance, journalists | Statutory publication, publicly-nameable; every quarter Irish Times runs settlements stories from this |
| **Liquidator workload concentration** | Distressed-debt funds, restructuring lawyers | Tells you who's busy = where the deals are |
| **SCARP / examinership appointments** | Distressed-debt buyers, suppliers, secured lenders | First public signal of corporate-rescue scenarios; faster than press |
| **Bankruptcy aggregate by petition type** (self vs creditor) | Debt-collection firms, sentiment trackers | ISI publishes total only; petition-type ratio is leading-indicator |
| **CB "claims period" notices** | Insurance creditors, fund investors | Statutory deadline windows for filing claims against wound-up funds |
| **Foreshore Act applications** ⭐NEW | Offshore wind / port developers | Permitting timeline for marine infrastructure — directly relevant to Irish offshore-wind buildout (~10GW target by 2040) |
| **PIP authorisations** | PIP firms, debt-advisory market | Regulator-published list of who can do PIA/DSA work |
| **Pension scheme registrations + terminations** ⭐NEW | Pensions advisers, workplace-pension firms | Identifies failing schemes; affects employee outcomes |

**Most defensible commercial value**: the regulated-entity change-feed. KYC officers at any Irish bank or fund administrator legally need to know when a counterparty's status changes. Today they read PDFs. Iris parsed = a webhook. Adjacent to your project, not core.

---

## 10. Enrichment / linkability map

What each Tier-1/Tier-2 category buys, joined to existing tables:

| Iris category | Joins to | Adds |
|---|---|---|
| `STATUTORY_INSTRUMENT` | `silver/ti_oireachtas_legislation_index.csv` | the SI itself (most SIs *not* in Bills tracker), signing Minister, parent Act |
| `MEMBER_INTEREST_SUPPLEMENT` | `silver/dail_member_interests_combined.csv`, `silver/votes`, `silver/legislation` | change-log for annual register; conflict-of-interest disclosures tied to votes |
| `TAX_DEFAULTERS_LIST` | members register (politician-defaulters are flagged), CRO companies | publicly-nameable settlements/penalties data |
| `STATE_BOARD_APPOINTMENT` | `silver/flattened_members.csv` (and a future `state_appointments` table) | who got which state-body / agency role |
| `FORESHORE_LICENCE` | (new gold table) | coastal infrastructure permit pipeline; offshore-wind project tracker |
| `BILL_SIGNED_BY_PRESIDENT` | `silver/legislation` | commencement dates Bills API doesn't always carry |
| `EXCHEQUER_STATEMENT` | (new gold table) | quarterly revenue/expenditure breakdown |
| `BYELECTION_WRIT` | `silver/flattened_members.csv` | political event signal |
| `PENSION_SCHEME_EVENTS` | (new gold table) | pension-scheme attrition |
| `CHARITY_COOP_LIFE_EVENTS` | (new gold table) | only public record of community-co-op events |

---

## 11. Schema (final)

### Primary table — `iris_blocks`

| column | type | notes |
|---|---|---|
| `source_pdf` | str | filename |
| `pub_date` | date | parsed from filename |
| `block_idx` | int | position in PDF |
| `code` | str | `C-12`, `G-3`, `L-1` if present (modern format) |
| `category` | enum | one of the categories in §4 |
| `header` | str | first non-empty non-numeric line |
| `body` | str | full text if ≤15k chars, else first 2k |
| `body_truncated` | bool | true if ≥15k chars |
| `char_len` | int | original block length |

### Specialised tables (one row per parsed entity)

- `iris_si` — see §11.1
- `iris_member_interests` — see §5
- `iris_tax_defaulters` — name, address, settlement_amount, penalty, period
- `iris_state_appointments` — appointee, role, body, term_start, term_end, appointing_authority
- `iris_foreshore_licences` — applicant, licence_type, location, decision_date, conditions_link
- `iris_pension_events` — scheme_name, action, administrator, effective_date
- `iris_fishing_quotas` (optional, requires `fitz.find_tables()`) — see §6

### `iris_si`

| column | source | population |
|---|---|---|
| `si_no`, `si_year` | regex anchor | 100% |
| `pub_date`, `source_pdf` | filename | 100% |
| `title` | ALL-CAPS line after SI heading | ~77% |
| `department` | `Minister (?:for\|of)` clause | ~46% (improvable) |
| `minister_name` | when full attribution appears | low — most SIs name title only |
| `parent_act` | `made under the X Act YYYY` clause | <10% with current regex — needs work |
| `is_commencement` / `is_amendment` / `is_revocation` | title keywords | mechanical |
| `is_eu_aligned` | `European Union/Communities` trigger | ~30% |
| `body_excerpt` | first 800 chars of block | 100% |

**Don't parse the full SI body.** Link out to eISB (`https://www.irishstatutebook.ie/eli/{year}/si/{number}/made/en/print`) for canonical text.

---

## 12. UX framing — explaining SIs to end users

### What an SI is (for the page intro)

> *Acts of the Oireachtas grant Ministers the power to fill in the operational detail by signing rules called Statutory Instruments. Once an SI is signed and published in Iris Oifigiúil it has the force of law — most rules that affect citizens day-to-day come from SIs, not from Acts.*

### Concrete one-line examples (real, from corpus)

- *"S.I. 80 of 2024: Irish Aviation Authority fee schedule" — your aviation fees are set by an SI, not by an Act.*
- *"S.I. 117 of 2026: designates the United States for DNA-data sharing under the 2014 Criminal Justice Act" — Act enabled in 2014; SI in 2026 actually triggered it.*
- *"S.I. 368 of 2025: closes wild-salmon fishing in named waters" — emergency conservation order, signed in days.*

### Suggested user frames

| Frame | Use this when |
|---|---|
| **"What new rules came into force this week?"** | Default page — list view of latest SIs by date |
| **"Who's signing the most rules?"** | Department aggregation |
| **"How much of Irish law comes from Brussels?"** | EU-aligned flag — ~30% of SIs implement EU directives |
| **"What's been switched on this year?"** | `is_commencement = true` — Acts becoming enforceable |
| **"What's been amended recently?"** | `is_amendment = true` — constant patching |

### Three pages, ranked simplest first

**1. SI Directory (primary)** — searchable, sortable table:
```
Date       SI No.    Department     Title                                Tags
2026-03-31 117/2026  Justice        Criminal Justice (DNA Database)...   [commencement][EU]
```
Filters: department, year, tag, free-text search. **Link-out to eISB for full text.** No SI body shown in-page.

**2. SI Pulse (single dashboard)** — three blocks:
- Department leaderboard (5-year, this-year, this-month)
- Quarterly SI volume + flagged election dips
- EU-alignment percentage trendline

**3. Single-SI page** — only metadata, plus link to eISB. Don't render body.

### Deliberately NOT do

- Don't render full SI text in-tracker (eISB does this canonically; duplicating creates encoding-error maintenance)
- Don't editorialise which SIs matter (directory + filters lets users find their own)
- Don't summarise SI bodies with NLP (legal jargon doesn't summarise; any attempt will be wrong sometimes and create liability)
- Don't merge SIs and Acts in UI (keep legal hierarchy visible — citizens often confuse "the law" with "an Act"; surfacing SIs as parallel stream is part of educational value)

### Member-Interest UI — much simpler

Single page, list:
```
2026-04-01  Richard O'Donoghue TD       §29  Land/property — Coolruss, Bruree
2026-03-13  Christopher O'Sullivan TD   §6   Directorship (voluntary org)
```
Filter by member, by section (6 vs 29), by category (1–9). Link out to source PDF. Cross-link to same member's annual register entry so users can compare.

---

## 13. Build plan (small, focused)

**v1 — extractor only**: `pipeline_sandbox/iris_oifigiuil_extract.py` (~200 lines, fitz + pandas only). Two output tables: `iris_si.csv` + `iris_member_interests.csv`. Skip everything else for v1.

**v2 — value categories**: add Tier-1 parsers for `TAX_DEFAULTERS_LIST`, `STATE_BOARD_APPOINTMENT`, `FORESHORE_LICENCE`. Each ~50 lines; one CSV per category.

**v3 — resolvers (separate sandbox modules)**:
- `iris_member_interest_resolver.py` — fuzzy-match `member_raw` to `silver/flattened_members.csv` via `normalise_join_key`
- `iris_si_minister_resolver.py` — same for SI ministers
- `iris_tax_defaulter_member_match.py` — flag politician-defaulters

**v4 — UI**: Streamlit pages per the §12 framing.

Build v1 → v2 → v3 → v4 sequentially. Don't over-engineer.

---

## 14. Open questions / next steps

1. **Build v1 extractor** — combined fitz + pandas, hybrid splitter, two-table output.
2. **Older years** — earlier than 2022 is downloadable per discovery notes (archive goes back to ~2002). Not a blocker; current 5-year window is enough for gold tables to show trend.
3. **Recover the 31 corrupted PDFs** — re-run downloader with suffix variants (`-2.pdf`, ` AMENDED.pdf`).
4. **Commercial-side question** — keep regulated-entity feed adjacent or extract as standalone product? Adjacent is the recommended path; SaaS is out-of-scope.
5. **Fisheries decision** — in scope for theyworkforyou-Ireland (then build `iris_fishing.py`) or out of scope (then leave hook only)?

---

## Appendix — running the probes

All probe scripts in `pipeline_sandbox/`:

```bash
python pipeline_sandbox/iris_oifigiuil_probe.py             # category hit counts, block headers
python pipeline_sandbox/iris_oifigiuil_probe2.py            # [C-N]/[G-N] code analysis, minister regex, interest-hunt
python pipeline_sandbox/iris_oifigiuil_probe3.py            # block-length distribution + supplement locator
python pipeline_sandbox/iris_oifigiuil_evidence.py          # ICAV / standards / CB cancellations / member-interest diff
python pipeline_sandbox/iris_oifigiuil_evidence2.py         # standards revisited / ICAV jurisdictions / SI breakdown / election effect
python pipeline_sandbox/iris_oifigiuil_value_probe.py       # additional value categories (foreshore, tax defaulters, board appointments, etc.)
python pipeline_sandbox/iris_oifigiuil_single_pdf_dump.py   # block-by-block dump for one PDF
```

All re-runnable, idempotent, no writes outside `pipeline_sandbox/`.
