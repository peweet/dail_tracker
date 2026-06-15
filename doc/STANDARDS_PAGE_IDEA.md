# Standards & Credentials — Page Idea + Source Map

**Status:** SCOPING (concept + source research + first SI mining done; no ETL built).
**Date:** 2026-06-14.
**Owner doc.** Companion to `doc/IDEAS.md` (master map) and `doc/PROCUREMENT_MASTER.md`.

---

## 1. The idea in one line

A **supplier capability register**: for any company, surface the *certifications, statutory
registrations, accreditations and scheme memberships it provably holds* — joined to its CRO
identity and (where it exists) its public-procurement track record.

It is the **supply side** of the tendering story. The Dáil Tracker procurement pages show who the
state *buys from*; this page shows **who is provably qualified to do the work** — and pairs that
with how competitively they actually bid.

---

## 2. Why this works — the strategic asymmetry

The private tendering process we mapped has a hard wall: the **demand** side is confidential. Who
needs an electrician, who is running an RFP — never published, never obtainable. We will never get it.

But the **supply** side is public *by design*. Credential registers (Safe Electric, RGII, the
Central Bank register, EudraGMDP, NSAI) exist **precisely to be found** — that is their entire
purpose. The "discovery layer" that substitutes for eTenders in the private sector *is itself a
scrapeable public dataset.*

So we cannot build "who is buying," but we can build the thing a buyer would actually want and that
nobody publishes joined-up: **who is qualified, for what, and how they perform on public work.**

| | We can get it? | Source |
|---|---|---|
| Public-sector demand (tenders) | ✅ already have | eTenders / TED / OGP |
| Private-sector demand (RFPs) | ❌ never — confidential | — |
| **Supplier credentials (supply)** | ✅ **this page** | public credential registers |
| Supplier public track record | ✅ already have | `supplier_normalised`, `procurement_by_cpv` |

---

## 3. The closed loop — SIs supply the "demand for credentials"

The missing demand side has a substitute we **already own**: the **Statutory Instrument corpus**
(5,936 SIs, 2016+, in `data/gold/parquet/statutory_instruments.parquet`).

SIs are where the law *mandates* a credential — they stand up registers, designate competent
bodies, require approval/certification, and transpose EU standards into Irish law. So the corpus is
a machine-readable map of **which credential is legally required, under which instrument, for whom.**

```
  Statutory Instrument            Public register              Your existing data
  (the legal requirement)   ->    (qualified firms)     ->     (identity + track record)
  ────────────────────────       ──────────────────          ────────────────────────────
  "European Union (F-Gas)          F-Gas company cert          CRO number
   Designation Regs"        ->     register             ->     supplier_normalised (CPV)
                                                               procurement_by_cpv (price)
```

This is the whole value proposition: **SI (why it matters) → register (who qualifies) → CRO
(identity) → procurement (track record).** The SI corpus is the regulatory-demand map that replaces
the tender-demand we can never get.

---

## 4. SI corpus mining — evidence the demand map is real

First pass, **read-only, title + metadata only** (corpus has no body text; see caveats).
Script: `c:/tmp/mine_si_standards.py`.

- **5,936 SIs**, 2016–2026.
- **2,191 (36.9%) EU-derived** (`si_is_eu`); **31.5%** are explicit transpositions
  ("European Communities/Union …").
- **406 SIs (6.8%)** have a title that signals a **register / approval / designation / certification
  / competent-authority** action — the credential-creating instruments.
- **58 SIs** are *both* EU-derived *and* register-signalling — EU standards transposed into IE law
  that stand up a credential/register (the closed-loop sweet spot).

**Title signal keywords (of 5,936):**

| signal | hits | share |
|---|---|---|
| transposition (European Communities/Union) | 1,868 | 31.5% |
| register / registration / registered | 194 | 3.3% |
| scheme | 183 | 3.1% |
| designate / designation (competent body) | 114 | 1.9% |
| licence / licensing | 120 | 2.0% |
| fees (paywall to a credential) | 111 | 1.9% |
| certification / certified | 71 | 1.2% |
| approval / approved | 52 | 0.9% |
| standard(s) | 41 | 0.7% |

**Sector probes (title contains) — they line up with the register research:**

| sector | SI title hits | maps to register |
|---|---|---|
| financial services | 417 | Central Bank Register of Authorised Firms |
| electrical | 111 | Safe Electric (REC) |
| professional bodies (legal/acct) | 95 | Law Society, CRO Auditor Register |
| food / food safety | 93 | FSAI / DAFM / SFPA approved establishments |
| medicinal / pharma | 93 | HPRA / EudraGMDP |
| waste / environment | 72 | EPA licences, NWCPO permits |
| energy / SEAI | 68 | SEAI registered professionals |
| aquaculture / seafood | 55 | SFPA / BIM |
| organic / agriculture | 50 | IOA / Organic Trust, Bord Bia |
| data protection / GDPR | 43 | ⚠ no usable register — see caveats |
| construction / building | 38 | CIRI |
| gas installer | 32 | RGII |
| medical device | 17 | EUDAMED (SRN) |
| F-gas / fluorinated | 5 | F-Gas company cert register |

> **NEXT, body-level mining:** title mining surfaces the *domains and schemes*. The actual
> `I.S. EN …` standard citations live in the SI **body** on `eisb_url` (irishstatutebook.ie).
> Fetching bodies for the ~406 register-signalling SIs (not all 5,936) would extract the precise
> standards each instrument mandates. Deferred to a focused phase.

---

## 4b. SI BODY-level mining — results (2026-06-14/15)

Phase 1 executed. Fetched **471 SI bodies** (of 474 attempted, 3 failed) from `eisb_url`,
priority-ordered (register-signalling + EU/technical first) plus a targeted sweep of the 102-SI
CE-marking / conformity / product-safety / type-approval **standards seam**. Read-only; results in
`c:/tmp/si_body_mining/results.jsonl`. Scripts: `fetch_bodies.py`, `fetch_seam.py`.

**Finding 1 — standard citations are a thin, concentrated seam.** Only **~2% of SIs cite a standard**,
even in this technical-first ordering. **12 SIs** carry citations; **32 distinct standard codes**.
They cluster almost entirely in EU *product/conformity* law:

| Standard cluster | Citing SIs | Codes |
|---|---|---|
| Construction products | 2025/669, 2023/217 | EN 12352, EN 12676-1, EN 12899-2/3, EN 13055-1/2, EN 13139, EN 13383-1, EN 13450, EN 14188-1/2/3, EN 1423, EN 14695, EN 15322 (16 EN codes) |
| Vehicle type-approval / roadworthiness | 2022/475, 2020/556, 2017/414, 2017/280 | IS 500, IS 250, IS 100, ISO 9001, ISO 7638, ISO 10542 |
| Private security (alarms / safes / CIT) | 2023/140, 2022/299, 2018/322, 2016/343 | EN 50131-1:2006, EN 1143-1/2, IS 998:2006 |
| Dangerous goods inspection | 2017/555 | ISO/IEC 17020:2004 |

> **Implication for the build:** do NOT brute-crawl all 5,936 SIs to harvest standards — the yield is
> ~2%. The targeted seam sweep (102 SIs) captures essentially all of them. A periodic seam re-crawl is
> the right maintenance pattern.

**Finding 2 — the credential-mandate map is broad and rich.** **74% of mined SIs** carry mandate
language. Totals across 471 SIs: `approval` ×1,472, `designated_body` ×986, `shall be registered`
×289, `competent_authority` ×224, `notified_body` ×190, `conformity_assessment` ×118, `accredit` ×77,
`CE marking` ×54. **168 SIs** strongly stand up a register/designation (e.g. Nurses & Midwives
Registration Rules, Optical Registration Board, EU Basic Safety Standards (radiation), AI Act
designation, Rail Interoperability designated bodies). They reference **95 distinct EU directives** and
**174 EU regulations** — the legal backbone of the credential universe.

**Finding 3 — URL construction (answer to "can we link to the standard?").**

| Link target | Status | Pattern |
|---|---|---|
| **SI body (citation location)** | ✅ verified, already in data | `eisb_url` = `…/eli/{year}/si/{number}/made/en/html` |
| Standard's own NSAI catalogue page | ⚠ endpoint TBD | NSAI Infostore exists but `shop.standards.ie/...?q=` 404s; needs a DevTools check of the real NSAI standards-store search endpoint |
| Standard full text | ❌ not free | Paywalled NSAI/ISO PDF — purchase only, no deep link |

So we can deep-link a citation straight to the enabling SI body today; linking to the standard's
catalogue entry needs the correct NSAI Infostore endpoint (small follow-up).

**3 fetch failures** to retry (transient). Caveat: minor whitespace artifacts in extracted codes
(`ISO7638`, `EN50131-1:2006`) need normalisation before use.

---

## 5. The register map — tiered by ingestibility

Researched across six sectors (construction/engineering, pharma/health, food/agri, IT/infosec,
accountancy/legal/finance, horizontal). What matters is not how many credentials exist, but which
leave a **public, firm-keyed trail** joinable to CRO.

### Tier A — clean public registers with a real firm key (the targets)

| Register | Firm key | Why it's high-value |
|---|---|---|
| **CRO Auditor Register** — search.cro.ie/auditors | **CRO no. + ARN** | Only one natively CRO-keyed — the join spine, no fuzzy-match |
| **Central Bank Register of Authorised Firms** — registers.centralbank.ie | C-number | One consolidated, bulk-downloadable register for all regulated finance |
| **EudraGMDP** (EU) — eudragmdp.ema.europa.eu | authorisation/cert no. | Every pharma MIA/WDA/GMP/GDP site, EU-wide, no login |
| **NSAI Certified Company search** ✅ **PULLED** | company name + `store_link` | **6,103 certs / 2,358 firms** — the Type-B keystone, now a dataset (§6b) |
| **NSAI Agrément** (products + registered installers) | cert no. | Building products/systems + approved installers |
| **Repak members** — repak.ie/members/list | membership no. + **Excel export** | ~9,400 firms, literally downloadable |
| **EPA Licence Search** — epa.ie | licensee name + licence no. | IE/IPC/waste-water, back to 2004/07 |
| **NWCPO** waste permits / facility register | permit no. | C&D waste / haulage contractors |
| **CIRI** — ciri.ie | registration no. + category | Statutory; going mandatory through 2026 → approaching census-level |
| **Safe Electric / RGII** | reg no. | The cleanest trade registers (opt-in gap on Safe Electric) |
| **DAFM / SFPA approved establishments** | **approval no.** | Cleanest machine key in food; downloadable lists |
| **HIQA "Find a centre"** | provider name + centre ID | ~2,700 designated centres (nursing/disability) |
| **PSI** pharmacy register | reg no. (downloadable monthly) | Every retail/online pharmacy |
| **Law Society Find-a-Firm** | firm name (+ PII insurer) | Every solicitor practice |
| **INAB directory** | accred. no. | The accredited labs/cert bodies themselves |
| **B Corp / Great Place to Work / Origin Green / Guaranteed Irish** | company name | Public, enumerable; ethical/employer/provenance signal |
| **FSSC 22000 / SALSA** | cert no. | Food-safety certs with scopes |
| **Cyber Essentials (IASME) / CREST / PCI QSA** | cert/company | The only enumerable infosec credentials |
| **EUDAMED** (EU, medical devices) | **SRN** | Mandatory May 2026 — emerging master key for device firms |

### Tier B — public but awkward
Bord Bia farm schemes (per-herd-number *verify* only, not enumerable); Engineers Ireland / SCSI /
RIAI / ACCA / CPA / Irish Tax Institute (mostly **individual**-level — link to a firm only via a
named employee); CAI/CPA firm directories (weak search).

### Tier C — exists but no usable public trail (record, don't chase)
- **ISO 9001 / 14001 / 45001 / 27001** — *self-declared* globally (IAF CertSearch is voluntary +
  partial), BUT NSAI-*issued* Irish certs are **fully enumerable and now pulled** → these moved to
  Tier A (§5/§6b). Only certs issued by *other* bodies remain Tier C.
- **SOC 1 / SOC 2** — NDA-gated audit reports, zero register.
- **Achilles / Constructionline / EcoVadis / deep CHAS/SafeContractor** — buyer-portal paywall.
- **IiP / Q-Mark / KeepWell / Investors in Diversity** — logo galleries, not registers.
- **GDPR**: the data-controller register was **abolished in 2018**; the **DPO register is not
  public**. So despite 43 SI title hits, "GDPR compliance" is **not a linkable data point.** ⚠

---

## 6. NSAI + EU — the keystone node

**NSAI plays two roles, both useful:**
1. **Certifier** — "Search for a Certified Company" (~7,520 firms → standard). Supply side: who holds
   what. This is the *only* enumerable Irish source for the ISO 900x/1400x/4500x family, because
   those standards are otherwise self-declared (Tier C).
2. **Standards body** — adopts every EN/ISO standard as an Irish Standard (`I.S. EN …`) and publishes
   the catalogue. This is the *universe of standards* the SI bodies cite.

**EU layer underneath:** the ingestible EU-level registers are **EudraGMDP** (pharma), **EUDAMED**
(devices, SRN), **EMAS**, **ECHA/REACH**. A standard becomes binding the moment it is a harmonised
standard cited in EU law or transposed by an SI — which is exactly what §4's 1,868 transposition SIs
are doing.

---

## 6b. EXECUTED — NSAI register pulled + standards-source/linking layer (2026-06-15)

### The keystone register is now a dataset (#1 — done)
The NSAI certified-company search was reverse-engineered and pulled. It is a plain EE form POST to
`https://www.nsai.ie/certification/results/` with a per-form `XID` CSRF token + `standard_number` /
`standard_title` / `company_name` fields (no pagination — all matches in one response). Scraper:
`c:/tmp/nsai_certs/scrape_register.py`; output `nsai_certified_companies.parquet`/`.csv`.

- **6,103 certificates · 2,358 companies · 133 standards** (~81% of the ~7,520 NSAI claims; gap = cert
  schemes not in the query list + dedup — closeable by adding the missing `standard_title` areas).
- Each row: `project_file_number, company, location, standard_number, standard_code, standard_title,
  scope, store_link`.
- Top credentials: **I.S. EN ISO 9001:2015 ×1,822, ISO 45001 ×972, ISO 14001 ×931, ISO 50001 ×262,
  ISO/IEC 27001 ×165**, plus product certs (EN 206 concrete, EN 1090 steel, EN 771 masonry) and
  MDR 2017/745 ×74, FSSC 22000 ×58.
- Multi-cert firms = capability signal (Roadstone 321, An Post 113, Iarnród Éireann 76, Kilsaran 50).
- **This overturns the earlier Tier-C assumption:** Irish ISO management-system certs are NOT just
  self-declared — the NSAI-issued ones are **fully enumerable**. NSAI certified-company register is
  hereby **upgraded Tier C → Tier A** (see §5). It is the firm-linkage half of the supplier register.
- Cleanup TODO: set response encoding to UTF-8 (mojibake on fadas, e.g. "Iarnród Éireann").

### Making a standard code clickable (#2 — done)
The NSAI standards store is `shop.standards.ie` (Intertek-hosted; `infostore.nsai.ie` is dead). A
standard code is made clickable via the **search URL** (the canonical per-standard product page needs
internal doc/rec IDs not derivable from the code):

```
https://shop.standards.ie/en-ie/search/standard/?searchTerm=<CODE>&publisher=NSAI
```

Now attached per row as `store_link`. Layered link strategy:

| Link target | Source | Status |
|---|---|---|
| SI body (where a standard is cited / a register is mandated) | `eisb_url` | ✅ in data |
| Standard's catalogue entry (Irish) | `shop.standards.ie/...?searchTerm=<code>&publisher=NSAI` | ✅ working |
| Standard identity (international) | `iso.org/standard/{csnumber}.html` (needs ISO Open Data code→csnumber map); flagship topic pages `iso.org/iso-50001-energy-management.html` | partial |
| Standard full text | NSAI/ISO/CEN paywalled PDF; **ETSI is the exception — free PDFs** | ❌ (paid) / ✅ ETSI only |
| Which EU law a standard supports | EU per-legislation harmonised-standards **.xls summary lists** + EUR-Lex CELEX/ELI | ✅ free crosswalk |
| Who is authorised to certify a standard | **NANDO** notified-bodies register (NSAI = NB 0050) | ✅ public |

### NSAI → CRO join — gated fuzzy match (experiment, 2026-06-15)
Joined the 2,358 NSAI-certified firms to the 817k-row CRO master (`data/silver/cro/companies.parquet`).
Precision-first, mirroring `pipeline_sandbox/fuzzy_cro_match_probe.py`. Fuzzy accepted ONLY when
extremely likely, validated **column-by-column** (name similarity AND `location` vs CRO address):

| Tier | Rule | Matches |
|---|---|---|
| exact | suffix-stripped core equal | 1,315 |
| despace | de-spaced core equal | 61 |
| fuzzy_name | name_score ≥ 98 (near-identical) | 13 |
| fuzzy_name_loc | 92 ≤ score < 98 AND location agrees | 24 |
| **total** | | **1,413 (60%)** |

The location compare **caught real false positives** the name score alone would have passed:
`C&C Sand & Gravel`→`C&P Sand & Gravel` (96), `Patrick Donegan`→`Patrick Doogan` (95),
`Clancy Construction`→`Clanry Construction` (95) — all correctly rejected (no location corroboration).
849 near-misses logged to `nsai_cro_REJECTED_review.csv`; matches in `nsai_cro_matched_gated.csv`
(sandbox `c:/tmp/nsai_certs/`, nothing written to repo). Residual unmatched = public bodies/semi-states
(legitimately not in CRO), foreign firms, and the scrape's UTF-8 mojibake (fixable). Realistic clean
ceiling ≈ 65–70% among matchable private firms — the matcher is not the bottleneck.

### EXPERIMENTAL dataset built — `nsai_capability_register` (2026-06-15, sandbox)
End-to-end PoC built in `c:/tmp/nsai_certs/` (see `DATASET_README.md`; NOT in repo/pipeline):
NSAI certs → CRO identity+health → public payments, one row per CRO firm.
- **1,289 firms matched to a live CRO entity**; **304** received public money totalling **€2.41bn**
  (safe-to-sum, deduped per company_num).
- **31 live-but-overdue certified firms with public money** = investigable leads (€397M); **37**
  dissolved-shell matches flagged `match_review_needed` and excluded from the headline.
- Matching hardened: conservative normalisation (no geo-collapse — fixed Toyota Ireland↔Holdings),
  live-entity preference, junk filter, name-variant dedup, dissolved-match flag + confidence penalty,
  and a column-by-column (name+location) fuzzy gate. Findings are leads, not conclusions.

### Why "the law" is paywalled — and the US vs EU split (context for the page's framing)
Standards cited in law are written by **private, self-funding bodies** (CEN/CENELEC/ETSI/ISO/NSAI),
to whom governments deliberately **delegated** the technical detail of legislation (EU "New Approach",
1985). The paywall is their funding model; they hold copyright. This collides with the rule-of-law
principle that the law must be freely knowable — and is being litigated:
- **EU/IE:** CJEU *Public.Resource.Org v Commission* (C‑588/21 P, 2024 — "Malamud") — harmonised
  standards are **part of EU law and must be free**. Anchored by the Irish case *James Elliott
  Construction v Irish Asphalt* (C‑613/14, 2016 — a harmonised standard "forms part of EU law").
- **US:** *ASTM v. Public.Resource.Org* (D.C. Cir. 2023) — incorporated standards stay copyrighted
  but are **freely republishable as fair use**; *Veeck v. SBCCI* (5th Cir. 2002) — enacted codes lose
  copyright. The US also has a public index of which standards are law (**NIST SIBR**, sibr.nist.gov)
  that the EU lacks — the very artefact the SI mining reconstructs for Ireland.
- Two standard types matter differently: **harmonised/product standards** (Type A, EN 12676 — cited
  in law, *product* conformity, weak firm-linkage) vs **management-system standards** (Type B, ISO
  9001/14001/45001/50001/27001 — *companies* certify, strong firm-linkage via the NSAI register).

---

## 7. The hard problem — name→CRO join

Almost every Tier-A register is **name-keyed, not CRO-keyed** (the CRO Auditor Register is the lone
exception). Joining "Acme Electrical Ltd" on a register to its CRO record and to `supplier_normalised`
is the make-or-break engineering — and it is the project's known weak spot (the leading-number
fragmentation in `supplier_normalised`). Registers that expose their own number (C-number, ARN,
approval no., Repak no., SRN) help disambiguate but still need the CRO bridge.

**Implication:** pilot on a register that already carries strong keys and a manageable size before
committing to the fuzzy-match at scale.

---

## 8. Page concept (what the user sees)

Civic-transparency framing (not a procurement-team tool — that space is Achilles' paywalled turf):

- **Per-company credential card** — "Acme Electrical Ltd: Safe Electric registered · ISO 9001 (NSAI)
  · CIRI (Electrical) · won 7 public electrical contracts, median €X, usually competitively tendered."
- **Per-credential register browser** — "all Safe Electric contractors," filterable by county/CPV,
  each linking to the SI(s) that *mandate* the credential (the §3 loop made visible).
- **Standards-in-law lens** — driven by the SI mining: "which standards/credentials Irish law
  requires, under which instrument" — a genuinely novel public artefact.
- Honest provenance: every credential carries an "as-of" date + source register link + the
  self-declared-vs-registered distinction (Tier C must be visibly flagged).

---

## 9. Build phases

1. **SI body-level mining** (read-only, ~406 register-signalling SIs): fetch `eisb_url` bodies,
   extract `I.S. EN` citations + mandated-register language → precise standard→instrument map.
2. **Pilot one Tier-A register** end-to-end — **electrical** is the natural pilot (Safe Electric +
   CIRI Electrical; well-represented in procurement CPV 4531xxxx). Prove the name→CRO join honestly,
   measure match rate.
3. **Cross-reference** the pilot register against `supplier_normalised` → "registered AND a proven
   public winner."
4. **Decide** whether the joined view is compelling enough to generalise (Central Bank, EudraGMDP,
   Repak, CRO Auditor are the next-best by key quality).

---

## 10. Caveats / honesty

- **Titles-only mining** so far — domain/scheme map is solid; exact `I.S. EN` citations need body
  fetch (phase 1).
- **Opt-in gaps**: Safe Electric and (today) CIRI are not exhaustive — absence ≠ non-registration.
- **Self-declared ≠ registered** (Tier C) — must be visibly distinguished in any UI, per the project's
  no-inference rule.
- **Individual vs firm**: many professional registers (Engineers Ireland, ACCA, barristers) are
  individual-level; firm linkage is only via named employees.
- **GDPR is a dead end** for firm-linkage (§5) despite intuitive appeal.
- **Currency**: every credential is a point-in-time fact; the dataset is only trustworthy with an
  as-of date and a refresh cadence.
