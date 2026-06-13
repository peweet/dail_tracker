# CBI Second-Pass Reconciliation — Review

**Reviewed:** 2026-06-05
**Focus doc:** `doc/dail_tracker_cbi_second_pass_reconciliation.md`
**Action mode:** analysis-first (REVIEW_CONTEXT.md §5). No pipeline/extractor/gold edits; one read-only probe written: `pipeline_sandbox/probe_review_cbi_legal_notices.py`.

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| CBI chain exists in pipeline | `("cbi", "extractors/cbi_registers_extract.py")` | `pipeline.py:68`; label `pipeline.py:108` | confirmed |
| CBI runs late for the corporate-notices xref | runs late because xref joins gold corporate notices | `pipeline.py:65-67` comment confirms | confirmed |
| Source is the registers downloads page | `registers.centralbank.ie/downloadspage.aspx` | `cbi_registers_extract.py:50` | confirmed |
| Outputs list (4 parquet + raw + meta) | sandbox firms, 2 sandbox xrefs, gold corporate xref, meta.json | docstring `cbi_registers_extract.py:4-11`; all 5 files exist on disk (gold xref 62KB, firms 891KB, mi 7KB, lob 3KB, meta 3KB) | confirmed |
| Only corporate-notices xref is promoted to gold | true | `cbi_registers_extract.py:9,12-18,629`; `_GOLD` write only at :629 | confirmed |
| Corporate xref is load-bearing for Corporate page | badges/panel go blank without it | view source `corporate_cbi_distress.sql:42,69`; consumed `dail_tracker_core/queries/corporate.py:35,40` → `utility/data_access/corporate_data.py:38,44` → page `utility/pages_code/corporate.py:2297-2298,2436` | confirmed |
| Promoted views are `v_corporate_cbi_notice_match` / `_repeat_distress` | doc names `..._notice_match` and `v_corporate_cbi_notice_repeat_distress` | actual: `v_corporate_cbi_notice_match` (`...distress.sql:28`) and **`v_corporate_cbi_repeat_distress`** (`...distress.sql:49`) — doc's second name has an extra `notice_` | wrong (name) |
| Match is EXACT normalised name, no substring | true | `xref_corporate_notices()` inner-join on `entity_norm == firm_name_norm`, `cbi_registers_extract.py:534` | confirmed |
| CIT Providers + Designated Entities fail on direct postback | true, left out of scope | `_cbi_meta.json` download.failed = registers #57 (CIT) + #58 (Designated Entities), error `not_pdf`, 45149 bytes (HTML error body); docstring `cbi_registers_extract.py:22-24` | confirmed |
| Member-interests + lobbying xrefs are experimental/unused | true | written `cbi_registers_extract.py:621,625`; **zero non-doc/non-self consumers** (grep: only the extractor + DATA_LIMITATIONS/plan docs reference them) | confirmed |
| Register breadth is broad / many PDFs ingested | ~all register families | 57/59 postbacks cached; 20,362 firm rows, 9,642 unique names, 73 register snapshots (`_cbi_meta.json` extract block) | confirmed |
| VASP/CASP "yes if present"; ATM/CIT/Designated "partially limited" | hedged | VASP (#50), ATM Deployers (#56), Crowdfunding (#54), MiCAR Crypto/CASP (#55) all download fine (raw PDFs present); only CIT/Designated fail | stale (over-hedged) |
| SQL view header cites the extractor path | header says `pipeline_sandbox/cbi_registers_extract.py` | actual extractor is `extractors/cbi_registers_extract.py` (`pipeline.py:68`); header path is stale post-reorg | wrong (doc-string drift in `corporate_cbi_distress.sql:7`) |
| Warning notices not covered by current extractor | correct | extractor only drives the registers postback page; no news/legal-notice fetch anywhere | confirmed |
| Enforcement/prohibition/etc. not covered | correct | same — confirmed by absence | confirmed |

**Scale facts worth carrying:** corporate xref = 205 matches / 149 distinct firms (158 insolvency, 45 notice, 2 rescue). Member-interest xref = 15 matches / 6 members / 7 firms. Lobbying xref = 3 matches. The two sandbox xrefs are tiny and genuinely dead.

## Architectural Assessment

The doc's picture of *current* state is accurate: CBI today is a register-snapshot extractor plus one EXACT-name xref against Iris `corporate_notices` that is real, committed gold, and wired end-to-end into the Corporate page (badge + repeat-distress panel). The two other xrefs are vestigial. The brief is right that this is "authorised-register enrichment," not a regulated-entity product.

The proposed expansion (warning / enforcement / prohibition / revocation / Dear CEO / AML / outlook) is a **different ingestion shape** from the current extractor:

- The register extractor drives `downloadspage.aspx` ASP.NET postbacks → PDFs → heuristic SSRS table parse. It is a *firm-list snapshot* producer.
- The notice families are **event/article streams** (dated HTML articles, some with companion PDFs), not snapshots. Different cadence, different schema, different join key (event→entity, not entity→register-membership).

So the right architecture is a **new extractor** (`extractors/cbi_legal_notices_extract.py` or similar), NOT an extension of `cbi_registers_extract.py`. It can *reuse* the existing VIEWSTATE-postback fetch code (warning-notice and enforcement listings paginate via the same `__doPostBack` mechanism — verified live, see probe). It should produce its own gold table(s) and its own `sql_views/cbi_*` family, and — critically — it can **join back to the existing CBI register firm index** on `firm_name_norm` (reuse `_norm_firm`), giving "this enforced/warned/revoked entity is also on register X" for free. That join is the genuine value multiplier and it's cheap.

Parse tractability (verified live 2026-06-05 via WebFetch; see probe header):
- **Warning notices** — clean structured HTML body (firm name, website, email, phone, date, authorisation statement). Pagination = `__doPostBack`. Page advertises an RSS feed (exact path unconfirmed; `/rss/news-and-media` 404s — confirm via DevTools). **Easiest family.**
- **Enforcement actions** — the `/enforcement-actions` URL is a *hub*; records live on `/news/article/...` pages. Sampled Cantor Fitzgerald article carried fine (gross/discount/net), legal regime, multi-category breach narrative, dates — all inline HTML. Older settlements add a companion public-statement PDF, but recent article HTML is self-contained. **Tractable, firm-level, low PII (only CBI officials named).**
- **Prohibition / F&P** — PDF-centric and **names private natural persons**. High parse + high legal sensitivity.
- **Dear CEO / AML bulletins / outlook / annual reports** — born-digital narrative PDFs. These are *documents about sectors*, not rows about entities; "ingestion" here means an indexed link list + metadata, not structured extraction. Low parse cost, low structured value, and they drift toward inference if surfaced as "this sector is under heightened supervision."

## Devil's Advocate

- **Mission drift is the central risk.** Dáil Tracker is a *parliamentary/civic-accountability* app. A full "regulated-entity intelligence" / due-diligence product (the doc's framing in §1, §5, §8) is a corporate-KYC tool — a different mission. Warning + enforcement notices earn their place ONLY because they extend the existing, already-shipped Corporate (Iris) provenance frame ("this entity that appears in our parliamentary/corporate record also has a CBI regulatory event"). Everything sold purely as "due diligence" / "business reports" (the doc's repeated framing) should be rejected on mission grounds, not just effort.
- **PII / defamation — prohibition notices are the personal-insolvency case again.** The personal-insolvency precedent (`feedback_personal_insolvency_privacy.md`) suppressed individual bankruptcy notices naming private citizens; corporate stayed in. Prohibition / fitness-&-probity notices name private individuals and assert misconduct findings — exactly the suppressed category, with added defamation exposure. **Reject for surfacing.** Enforcement actions against *firms* are corporate (fine), but enforcement against *named individuals* must be stripped the same way the brief's own §3.3 sensitivity note half-concedes.
- **No-inference when surfacing "sanctioned."** A "SANCTIONED" / "ENFORCED" badge on an entity is a conclusion. The firewall and no-inference rules require source-linked facts only: render "CBI enforcement action, 25 Feb 2025, €452,790, MAR Art 16(2) — [public statement]" with the link, never a derived risk label or a "regulated-entity risk history" score. The doc's "regulatory climate" / "heightened supervisory focus" copy (§3.8) is explicit inference and must not become UI text.
- **Register name-match false positives.** The corporate xref survives only because it's EXACT-match (the doc and code both note the substring member-interest xref produces fragment noise, which is why it's dead). Any enforcement/warning→register join must hold the same EXACT-match discipline. Warning notices are *unauthorised/clone* firms by definition — matching them to the *authorised* register is mostly meaningless and primed for clone-name false positives ("HSBC Continental Europe (CLONE)" must never badge as authorised HSBC). The join should run enforcement→register (authorised entities that were sanctioned) but NOT warning→register-as-authorised.
- **Scraper decay.** These are CMS article pages + ASP.NET postback pagination + occasional PDFs — higher decay than the registers page, which is already a known-fragile heuristic. Each new family is a new maintenance surface and a new canary (`pdf_infra/pdf_endpoint_check.py` pattern). Cadence is event-driven and low-volume (a handful of notices/month), so polling is cheap, but every parser is a liability that must earn its keep.
- **Volume reality check.** Warning + enforcement are *small* datasets (tens/year). The corporate xref it would enrich is 149 firms. This is a precision feature, not a data-scale feature — judge it on civic signal per maintenance-hour, not coverage.

## Data Quality & Enrichments

- Doc's CIT/Designated claim is exactly right and now *also* true that ATM Deployers, Crowdfunding, VASP and MiCAR CASP registers DO download — so the "access-to-cash registers partly attempted" line overstates the gap; only the two postback-broken registers are missing.
- Two doc-side errors to fix in any synthesis: (1) view name `v_corporate_cbi_notice_repeat_distress` → actual `v_corporate_cbi_repeat_distress`; (2) the SQL header's `pipeline_sandbox/cbi_registers_extract.py` path is stale (post-reorg the file is under `extractors/`) — a one-line comment fix in `corporate_cbi_distress.sql:7`, not in scope here but worth flagging.
- If a notices extractor ships, it inherits the project's `value_kind` discipline: fine amounts are a NEW grain (`value_kind = enforcement_fine`, `realisation_tier` N/A) and must **never** be summed with payments/awards/budget money. They are per-event facts, displayed individually with source link.
- Reuse `_norm_firm` (already shared in concept between extractor and `corporate.py:864`) for the register join so the EXACT-match semantics stay identical.

## Build / Defer / Reject

| item | verdict | value/effort | reason |
|---|---|---|---|
| Warning notices (unauthorised/clone firms) → own gold table + view, optional EXACT join to CRO/corporate (NOT to authorised register) | **BUILD (1st)** | high value / low-med effort | Clean structured HTML, no natural persons, low decay; directly extends the existing Iris/Corporate provenance frame; postback code reusable. Surface as facts + source link only. |
| Enforcement actions against **firms** → own gold table + view, EXACT join to CBI register + corporate_notices | **BUILD (2nd)** | high value / med effort | Firm-level, fine/regime/breach all inline HTML; this is the single strongest "regulatory history" signal and it strengthens the already-shipped repeat-distress panel. Strip any individual-named enforcement rows. |
| Revocation notices → status-change events on already-tracked register entities | **BUILD-lite / DEFER (3rd)** | med value / med effort | Genuine regulated-status signal and entity-keyed, but partly already visible via "revoked" registers (#17/#32); ship only after warning+enforcement prove the notices-extractor pattern. |
| Prohibition / Fitness & Probity / disqualification notices | **REJECT** | — | Names private natural persons + asserts misconduct = personal-insolvency privacy precedent + defamation exposure. Out of mission. |
| Adverse assessments / post-assessment commentaries | REJECT | low value / high effort | PDF-heavy, individual-adjacent, niche; no parliamentary tie. |
| IFSAT tribunal decisions / inquiries | REJECT | low / high | Tiny volume, individual-naming, external tribunal docs. |
| Dear CEO / thematic-supervision letters | REJECT (as "intelligence") | low civic / drifts to inference | Sector-narrative PDFs; surfacing as "heightened supervisory focus" is inference; not entity-keyed; corporate-KYC mission, not civic. |
| AML/CFT bulletins + sector reports | REJECT | low / med | Same — sector documents, not parliamentary accountability. |
| Regulatory & Supervisory Outlook / Annual Report / APS | DEFER (link-index only, not extraction) | low / low | At most a curated source-link list in a provenance footer; no structured ingestion. |
| Consultation papers, Markets Updates, Statistics/Open Data, AnaCredit, DORA | REJECT | low / varies | Macro/policy context, no entity grain, off-mission. |
| Access-to-cash quarterly monitoring + CIT/Designated registers | DEFER | low-med / med | Only genuine "local services" angle, but CIT/Designated are postback-broken; revisit if an alternate URL/source appears. |
| Fix doc view-name + stale SQL-header path | NOTE | trivial | `v_corporate_cbi_repeat_distress`; `corporate_cbi_distress.sql:7` path. |

## Bottom Line

The doc's read of *current* CBI ingestion is essentially correct and well-grounded: the corporate-notices xref is real, committed gold, EXACT-match, and load-bearing for the Corporate page (badge + repeat-distress panel, 205 matches / 149 firms), while the member-interest (15) and lobbying (3) xrefs are genuinely written-but-dead sandbox artefacts, and CIT/Designated Entities really do fail on postback. Two doc errors: the `v_corporate_cbi_notice_repeat_distress` view name is wrong (it's `v_corporate_cbi_repeat_distress`), and the "access-to-cash/VASP partly attempted" hedge is now stale since ATM/Crowdfunding/VASP/CASP all download fine. The expansion is architecturally a *new event-stream extractor* (reusing the postback fetch + `_norm_firm` join), not an extension of the snapshot extractor. Recommend building exactly two families — **warning notices** then **firm-level enforcement actions** — both as fact-only, source-linked, EXACT-match enrichments of the existing Corporate frame, with revocation a distant third; and explicitly **reject prohibition/F&P notices** (personal-insolvency privacy precedent + defamation) and the whole Dear CEO / AML / outlook / due-diligence layer as off-mission inference bait.
