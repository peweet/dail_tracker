"""Dáil Tracker data pipeline orchestrator.

Runs domain refresh chains in the default order below. Each chain is a
self-contained ``<domain>_refresh.py`` script that orchestrates its own
step sequence (poll → extract → enrich) and prints progress to stdout.

This file is a thin dispatcher around the chains. Per-chain logs land at
``logs/runs/<run_id>/steps/NN_<slug>.log``, the manifest records each chain,
and chain-level try/except keeps one flaky source from poisoning the rest.

Default order:

    bootstrap → members → payments → attendance → seanad → interests
                                          → lobbying → iris → legislation

Cross-chain dependencies (run upstream first if you `--select` standalone):

    * every chain assumes bootstrap has refreshed flattened_members.parquet
    * iris.step_si_gold assumes members.ministerial_tenure has run
    * legislation assumes bootstrap.members_api has fetched questions/votes JSON

CLI:

    python pipeline.py                            # full run
    python pipeline.py --list                     # show chains and exit
    python pipeline.py --select iris              # only iris
    python pipeline.py --select members,iris      # subset, comma-separated
    python pipeline.py --exclude lobbying         # everything except lobbying
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import threading
from pathlib import Path

from manifest import (
    create_run_manifest,
    record_step_finished,
    record_step_started,
    run_finished_at,
)
from services.logging_setup import setup_logging
from services.run_paths import ENV_RUN_ID, make_run_id, run_dir, step_log_path

# Domain refresh chains in the default execution order. Each tuple is
# (chain_name, script_path). Chain name is used by --select/--exclude.
CHAINS: list[tuple[str, str]] = [
    ("bootstrap", "bootstrap_refresh.py"),
    ("members", "members_refresh.py"),
    # news_mentions: one Google-News RSS search PER member (current + former) ->
    # silver news_mentions, read by v_member_news_mentions on the member page.
    # Depends on bootstrap (the historic Dáil/Seanad rosters it queries by name are
    # built by bootstrap.historic_members_build). ACCUMULATES — each run appends and
    # dedups, so daily cloud execution is the intended use, not a side effect. Network
    # chain, but every per-member fetch is try/excepted (one bad member never kills the
    # run; an empty roster is the only hard stop), so a flaky run degrades, not fails.
    # news_mentions: DELISTED 2026-07-07 (per user: low relevance; the ~9 min/run per-member
    # Google-News RSS is the pipeline's slowest network chain). The silver it built stays on disk
    # and the member page's v_member_news_mentions keeps reading it (frozen, not broken);
    # participation reads it "graceful when absent". Re-enable by uncommenting this tuple + its
    # blurb in _CHAIN_BLURBS + adding it back to the guard in test_pipeline_chains.py.
    # ("news_mentions", "extractors/news_mentions_extract.py"),
    ("payments", "payments_refresh.py"),
    ("attendance", "attendance_refresh.py"),
    ("seanad", "seanad_refresh.py"),
    # member_contact: scrape each member's oireachtas.ie profile page for the office
    # address / phone(s) / @oireachtas.ie email the members API does NOT expose ->
    # silver member_contact_details, read by v_member_contact_details (Member Overview
    # "Contact" card). Runs after seanad so both flattened_members + flattened_seanad_
    # members rosters exist (it skips a missing roster file rather than failing). Each
    # profile fetch retries with backoff then logs+continues, so a transient outage
    # yields honest NULLs, never a failed chain.
    ("member_contact", "extractors/member_contact_extract.py"),
    ("interests", "interests_refresh.py"),
    ("lobbying", "lobbying_refresh.py"),
    ("iris", "iris_refresh.py"),
    ("legislation", "legislation_refresh.py"),
    # participation: the honest "Showing up" attendance model — division turnout,
    # absence gaps, presence-vs-vote divergence and 120-day TAA compliance ->
    # four gold tables read by v_attendance_participation_* + v_attendance_taa_
    # compliance. Pure deterministic transform over gold the chain already built
    # (current_{dail,seanad}_vote_history + attendance_by_td_year + the member
    # feed for office flags); MUST run after attendance/seanad/legislation so
    # those exist. Reads news_mentions if present (graceful when absent), plus the
    # curated _meta role/explanation CSVs. Headless-safe, no network.
    ("participation", "extractors/participation_extract.py"),
    # committee_evidence: committee MEETING HISTORY (the Committees-page timeline:
    # date · topics · witnesses · transcript link). Two steps — extract enumerates
    # every committee meeting via the Oireachtas /v1/debates feed in a single pass
    # and parses each AKN-XML transcript (~1k meetings / 74 committees since
    # 2024-09 → silver); promote writes the three gold tables the v_committee_meetings
    # view reads. Standalone (Oireachtas API only, no deps, headless-safe); each XML
    # fetch retries+continues so a transient outage degrades, not fails. NOTE:
    # re-fetches the full window each run (no incremental-since guard yet) — ~3-4 min.
    ("committee_evidence", "extractors/committee_witnesses_extract.py"),
    ("committee_evidence_promote", "extractors/committee_evidence_promote_gold.py"),
    # judiciary_bench: promote the validated judiciary sandbox (data/sandbox/judiciary/, pulled +
    # pressure-tested once by extractors/persist_judiciary_data.py — a manual one-off that reads
    # scratch PDFs, NOT a pipeline chain) into the gold bench/appointments/nominations/clearance/
    # waiting/courthouses tables the Judiciary page reads. Pure deterministic transform — reads the
    # committed sandbox + _meta CSVs only, no network, headless-safe. MUST run before
    # judiciary_diary_link below: that chain consumes judiciary_bench.parquet (and imports this
    # module), so without this chain the link ran against gold no chain produced. NOTE: this
    # regenerates gold from the STATIC 2026-06-04 sandbox; pulling NEW appointments still needs the
    # sandbox refreshed from Iris (out of scope here).
    ("judiciary_bench", "extractors/judiciary_bench_extract.py"),
    # legal_diary: the Courts Service daily Legal Diary is FORWARD-ACCUMULATING — the site
    # exposes only the current court day's .docx (no historical URL), so a missed day is lost
    # forever. The poller archives one .docx per diary date (returns 0 on "already held", so a
    # weekend/no-change run is a clean no-op), the extract rebuilds the privacy-tiered gold from
    # the full archive, and the link step maps surname-only diary judges to the bench roster.
    # Standalone — no deps beyond the static bench gold the link reads. Run daily to build history.
    ("legal_diary_poller", "pdf_infra/legal_diary_poller.py"),
    ("legal_diary_extract", "extractors/legal_diary_extract.py"),
    ("judiciary_diary_link", "extractors/judiciary_diary_link.py"),
    # afs: amalgamated LA Annual Financial Statements (gov.ie PDFs) -> silver
    # spend-by-service-division fact (BUDGET/SPENT-tier macro context). Standalone —
    # self-fetches + caches PDFs to bronze, no deps, headless-safe.
    ("afs", "extractors/afs_amalgamated_extract.py"),
    # cbi runs last: its corporate-notices xref joins gold corporate_notices
    # (produced by iris) against the CBI register extract. Skips re-download
    # when the source PDFs are cached, so routine runs are extract+xref only.
    ("cbi", "extractors/cbi_registers_extract.py"),
    # cro xref runs after both iris (corporate_notices gold) and lobbying (CRO
    # silver via cro_normalise): exact normalised-name join of notices to the
    # CRO company register, committed gold, read by the Corporate page badge.
    ("cro", "extractors/cro_corporate_xref_enrichment.py"),
    # corporate_receiver runs after iris (reads corporate_notices gold): precomputes
    # the receiver-appointer ranking + operator-firm concentration that the Corporate
    # page used to recompute in pandas every load (those panels describe the full
    # corpus, independent of filters — pipeline territory). Writes the notices SUPERSET
    # (corporate_notices_enriched, read by v_corporate_notices) + appointer/firm gold.
    # Pure transform, no network. Graduated out of the page per the logic-firewall audit.
    ("corporate_receiver", "extractors/corporate_receiver_enrich.py"),
    # procurement: eTenders/OGP open data -> gold awards + supplier→CRO match.
    # Self-fetches + caches the source CSV; depends on the CRO silver register
    # (same as cro), so it runs after it.
    ("procurement", "extractors/procurement_etenders_extract.py"),
    # procurement_lobbying xref runs after BOTH procurement (gold awards) and
    # lobbying (silver returns): exact normalised-name overlap of suppliers and
    # lobbying registrants/clients, committed gold, read by the Lobbying page's
    # "also a state supplier" enrichment and the (future) Procurement page.
    ("procurement_lobbying", "extractors/procurement_lobbying_xref.py"),
    # entity_xref: the organisation-360 spine. Anchored on the procurement supplier universe
    # (supplier_norm — the key the /company dossier page is entered on), LEFT-joins each
    # entity's cross-register presence — CRO identity, lobbying footprint, corporate-notice
    # count, charity status, EPA licence — fused on the CANONICAL name key (shared/name_norm),
    # bridging the divergent-normaliser gap at build time WITHOUT re-baselining any source.
    # Pure deterministic transform over committed gold; MUST run after procurement +
    # procurement_lobbying + the corporate chain (cbi/cro/corporate_receiver) above. The
    # charities_enriched + epa_supplier_compliance inputs are produced out-of-band
    # (charity/charity_enriched.py, extractors/epa_promote_to_gold.py) and read as static gold.
    # No network, headless-safe; row-floor guarded. Read by v_supplier_entity_xref (company page).
    ("entity_xref", "extractors/entity_xref_build.py"),
    # ted: TED (EU procurement journal) Irish award notices -> SILVER (cleaned, not yet
    # exposed to the UI). Zero-auth API, caches raw to bronze, depends on the CRO silver
    # register (winner->CRO match), skips gracefully on an API outage. Headless-safe.
    ("ted", "extractors/ted_ireland_extract.py"),
    # ted_tenders: TED Irish COMPETITION/tender notices (cn-standard) -> SILVER. The pre-award
    # pipeline (what's out to tender), a THIRD grain never summed with awards or payments.
    # Zero-auth API, caches raw to bronze, no deps, skips gracefully on an API outage.
    ("ted_tenders", "extractors/ted_ireland_tenders_extract.py"),
    # etenders_live_tenders: the domestic eTenders.gov.ie LIVE national tender pipeline (open
    # opportunities now accepting bids, incl. sub-EU-threshold contracts TED/OGP can't see) ->
    # SILVER etenders_live_tenders.parquet. Sits alongside ted_tenders above: its own PLANNED
    # (pre-award) grain, value_kind=estimate_advertised — a buyer ESTIMATE, NEVER summed with
    # awards or payments. Standalone, no deps. STATELESS (rebuilds the full snapshot each run)
    # with its own --min-age-hours=20 guard, so a same-day re-run from another trigger is a safe
    # no-op, not a double-scrape. Requires Playwright (the grid is JS-rendered) — the OPTIONAL
    # `scrape` extra + `playwright install chromium`, NOT part of the base `pipeline` extra, so a
    # plain local `--extra pipeline` sync will show this one chain fail (ImportError) until
    # `--extra scrape` is also installed; chain isolation keeps that from affecting the rest of
    # the run. Was previously refreshed only by .github/workflows/live_tenders_refresh.yml (daily
    # cron) + the local tools/poll_live_tenders.ps1 task — both still run; the extractor's own
    # freshness guard makes any overlap with this pipeline chain a no-op.
    ("etenders_live_tenders", "extractors/etenders_live_tenders_extract.py"),
    # public_body_payments: depts / semi-states / health / edu PO+payment disclosures over €20k
    # -> SANDBOX gold-candidate public_payments_fact (one row per source line; the consolidate
    # chain below folds it to gold). Self-fetches + caches per-publisher files to bronze (steady-
    # state runs only download newly published files), 1s/file politeness + per-publisher circuit
    # breaker, failures classified into data/_meta/fetch_failures.json. Privacy quarantine
    # enforced: likely-personal suppliers are public_display=False (refuses to write a leak).
    ("public_body_payments", "extractors/procurement_public_body_extract.py"),
    # hse_tusla_payments: HSE + Tusla bespoke column-x parse -> SANDBOX gold-candidate
    # hse_tusla_payments_fact (same schema; folded to gold by the consolidate chain below).
    # privacy_risk=high — same hard quarantine. Uses cached FOI PDFs (falls back to fetch);
    # per-year files layout-drift-gated.
    ("hse_tusla_payments", "extractors/procurement_hse_tusla_materialize.py"),
    # disclosed_bq_po: a DISCLOSED national PO/payments-over-€20k BigQuery extract (manual drop at
    # data/raw_bq/, gitignored) -> silver disclosed_bq_po_payments_fact. Phase 1 scope = HSE HISTORY
    # RECOVERY only: emits the HSE periods our PDF parse lacks (2017-Q3..2020-Q2 + 2025-Q4 +
    # 2026-Q1), inheriting ie_hse identity + payment_actual from gold. The consolidate chain folds it
    # INTO the hse_tusla source (keeping ie_hse single-source for the disjoint-publisher reconcile).
    # NO-OP-SAFE: absent raw drop -> exits 0, and the fold carry-forwards the disclosed HSE rows
    # already in gold. Runs BEFORE procurement_consolidate. (Phases 2/3 = 141 new bodies + 53
    # renames are gated on a fail-closed registry + per-body semantics; see DISCLOSED_PO_INTEGRATION_PLAN.md.)
    ("disclosed_bq_po", "extractors/disclosed_bq_po_extract.py"),
    # disclosed_bq_po_newbodies: the SAME disclosed BigQuery extract, GENUINELY-NEW bodies lane
    # (registry data/_meta/procurement_disclosed_bodies.csv) -> silver disclosed_bq_po_newbodies_fact,
    # folded into gold as its OWN disjoint SOURCE_FACTS entry. Tranche 1 = 8 councils (6 LA-config
    # recoveries w/ broken live harvest + Tipperary/Louth verified) + An Garda + EPA + Louth&Meath ETB,
    # all po_committed (source-authoritative; NO blank-PO guess). Fail-closed registry + cross-lane
    # disjointness guard. NO-OP-SAFE on absent raw drop. Runs BEFORE procurement_consolidate.
    ("disclosed_bq_po_newbodies", "extractors/disclosed_bq_po_newbodies_extract.py"),
    # la_payments: the 31 local authorities' Purchase-Orders/Payments-over-€20k (Circular
    # 07/2012) -> silver la_payments_fact (20/31 councils parse clean; no OCR). Standalone,
    # self-fetches + caches per-council files to bronze, headless-safe. Privacy-classed
    # (sole-trader/id-code quarantined as metadata). Full back-catalogue by default — a low
    # --max-files would downgrade silver to a recent slice and put gold history at risk.
    ("la_payments", "extractors/procurement_la_payments_extract.py"),
    # procurement_consolidate: folds the three payment-grain facts above (public_body + hse_tusla
    # sandbox gold-candidates + the LA silver) into the app-facing gold procurement_payments_fact.
    # Runs IN the pipeline since 2026-06-12 (was manual): the silvers regenerate every run, so a
    # manual fold left gold stale — and a fold after a degraded run nearly wiped LA history. Safe
    # now: a listing-rot carry-forward guard keeps any council that vanished from silver (bot-wall,
    # moved listing) at its existing gold rows instead of dropping it.
    ("procurement_consolidate", "extractors/procurement_payments_consolidate.py"),
    # ministerial_diaries: the DETERMINISTIC tail of the diary pipeline — classify ->
    # org_match -> lobbying_overlap -> promote_gold -> company_influence — promoted out
    # of the manual run-book. The gold it rebuilds feeds the "Who Ministers Meet" page
    # (v_ministerial_diary_*). Steps 1-4 are network-free; step 5 reads the procurement
    # gold above, so this runs AFTER procurement_consolidate. The EXTRACT + OCR upstream
    # stay manual (gov.ie WAF + off-box GPU OCR) and their working table is gitignored,
    # so the chain GUARDS on that input: absent (every cloud run) it exits 0 as a clean
    # no-op; present (a local box post-extract) it re-derives the gold.
    ("ministerial_diaries", "ministerial_diaries_refresh.py"),
    # cso: CSO PxStat tables (housing/HAP + general-government finance GFA01/GFQ01/
    # NA012) -> gold cso_<table>.parquet (the national denominators behind
    # v_gov_finance_annual). Zero-auth REST, no deps; writes any GREEN table by
    # default (--dry-run validates only). Headless-safe, fidelity-gated per table.
    ("cso", "extractors/cso_pxstat_extract.py"),
    # stateboards: DPER state-boards register (membership.stateboards.ie, static HTML,
    # ~250 boards / 20 depts) -> silver stateboards_roster + stateboards_boards -> gold
    # = silver + HAND-CURATED Wikidata identities (data/_meta/stateboards_wikidata_curated.csv).
    # No network beyond the register itself; the Wikidata candidate generator is a separate
    # un-wired tool (wikidata/stateboards_wikidata_enrich.py) whose output a human reviews.
    ("stateboards", "extractors/stateboards_roster_extract.py"),
    # ── local-government accountability cluster ("Who runs your county") ──────────
    # Three independent council-accountability feeds behind the local-government page
    # (v_la_collection_rates / v_la_derelict_sites_levy / v_la_planning_overturn).
    # Promoted out of pipeline_sandbox/ 2026-06-20. The 4th feed — the Chief Executive
    # roster — is the hand-curated data/_meta/la_chief_executives.csv (no chain, like
    # stateboards' Wikidata identities).
    #
    # noac_collection: NOAC Performance Indicator Report M2 revenue-collection tables
    # (commercial rates / rent & annuities / housing loans, 31 LAs x 2020-2024) -> gold
    # noac_m2_collection_wide. Reads the git-tracked NOAC PDF (no network), Camelot/fitz
    # parse, fidelity-gated (refuses a non-GREEN parse), min_rows floor. Standalone.
    ("noac_collection", "extractors/noac_collection_rates_extract.py"),
    # noac_scorecard: the 7 citizen-facing accountability indicators (M1 revenue balance, M3
    # settled-claims/capita, M4 overhead, C2 sickness, R1 roads-poor, F3 fire-10min, E3 litter)
    # from the same NOAC 2024 PDF -> gold noac_scorecard_wide. fitz find_tables, 31-LA guard,
    # min_rows floor. Standalone (reads the git-tracked PDF, no network).
    ("noac_scorecard", "extractors/noac_scorecard_extract.py"),
    # noac_scorecard_history: the same metrics across the 2022-2024 reports, LAYOUT-ROBUST
    # (locates each metric by column-header predicate, not a fixed page, since pagination +
    # the indicator set differ per year) -> gold noac_scorecard_history (the trend sparklines).
    # Reads whichever NOAC_PI_YYYY PDFs are present in doc/source_pdfs/ (a missing year is
    # skipped, never guessed). Standalone, no network.
    ("noac_scorecard_history", "extractors/noac_scorecard_history_extract.py"),
    # noac_indicators: the FULL per-LA indicator set (~125 series across 12 families, stored as
    # the published RAW strings + numeric_value) from the 2024 PDF -> gold noac_indicators_long
    # (the "All NOAC indicators" dossier drill-down). Standalone, min_rows floor, no network.
    ("noac_indicators", "extractors/noac_indicators_long_extract.py"),
    # derelict_sites: DHLGH Derelict Sites Act annual return -> gold derelict_sites_levy_wide
    # (per-LA levied/collected/outstanding). Reads the git-tracked XLSX cache by DEFAULT
    # (no network — re-fetch is the opt-in --download flag, gov.ie CDN 403s a bare GET);
    # reconciles per-LA sums to the file's own Total row, fidelity-gated. Standalone.
    ("derelict_sites", "extractors/derelict_sites_levy_extract.py"),
    # planning_appeal_outcomes: council planning decision vs An Coimisiún Pleanála's OWN
    # appeal decision (PC02 registry) -> silver planning_appeal_outcomes (the authoritative
    # ABP-overturn metric; the applications feed's self-reported AppealDecision is unreliable).
    # FETCHES the ACP ArcGIS FeatureServer (skips the chain on an API outage via raised
    # error → pipeline isolates it) and JOINs the COMMITTED planning_applications_silver
    # (a static input — the national planning ingest is NOT yet a chain). min_rows floor
    # guards against a degraded ArcGIS pull thinning the silver.
    ("planning_appeal_outcomes", "extractors/planning_appeal_outcomes.py"),
    # freshness runs last: it reads the silver + gold the chains above produced
    # and writes data/_meta/freshness.json (the data-age signal the Streamlit
    # badge + scheduled report read). Pure read — never mutates pipeline data.
    ("freshness", "tools/check_freshness.py"),
    # fact_cards runs after freshness: rebuilds data/_meta/fact_cards.json — the machine-readable
    # metadata index (schema/rows/year-span/grain/never-sum) every silver+gold parquet, served by
    # the MCP describe_dataset/list_datasets tools so an agent never scans a parquet to learn its
    # shape. Reads the parquet FOOTERS + freshness (just written) + the curated fact_grain.csv
    # seed. Pure read of data; must run AFTER any fact is rebuilt or its row count goes stale.
    ("fact_cards", "tools/build_fact_cards.py"),
    # source_health runs last: reads the in-code source registry + bronze and
    # writes data/_meta/source_health.json (manual-source staleness now; listing
    # reachability when DAIL_CHECK_LINKS=1). Monitoring only — always exits 0 (a
    # separate --strict run gates CI). Pure read — never mutates pipeline data.
    ("source_health", "tools/build_source_health.py"),
    # output_regressions runs last: the COMPLETENESS guard (vs freshness's recency).
    # Compares the just-built gold against data/_meta/output_baseline.json and writes
    # output_regressions.json flagging silent row-thinning / emptied tables / removed
    # columns — i.e. a PDF/layout/schema drift that parsed clean but shipped partial
    # data. Monitoring only here (always exits 0); CI/scheduled runs use --strict to
    # gate, and --update-baseline accepts an intended change. Pure read.
    ("output_regressions", "tools/check_output_regressions.py"),
    # extraction_quality runs last: the MATCH-RATE guard (vs output_regressions' row/column
    # completeness). Row count can look healthy while the extracted FIELDS silently degrade
    # (blank/garbled/unmatched) — a PDF/HTML layout drift that keeps producing roughly the
    # same row count but garbage content. Compares the matched-vs-total ratios extractors
    # already publish to data/_meta/*_coverage.json (PILOT: judiciary_diary_link,
    # entity_xref — see tools/check_extraction_quality.py ADAPTERS to extend) against
    # data/_meta/extraction_quality_baseline.json. Monitoring only here (always exits 0);
    # CI/scheduled runs use --strict to gate, --update-baseline accepts an intended change.
    # Pure read.
    ("extraction_quality", "tools/check_extraction_quality.py"),
]

_CHAIN_BLURBS: dict[str, str] = {
    "bootstrap": "shared inputs: poll PDFs + Members API + flatten members & debates",
    "members": "Wikidata socials + ministerial tenure + committees long-format",
    # news_mentions: DELISTED 2026-07-07 — re-enable this blurb alongside the CHAINS tuple.
    "payments": "Parliamentary Standard Allowance: PSA ETL + member enrichment",
    "attendance": "plenary attendance PDF extraction",
    "seanad": "Seanad parity: votes + payments + attendance + gold (reuses Dáil parsers)",
    "member_contact": "scrape oireachtas.ie profile pages -> silver member_contact_details (Member Overview)",
    "interests": "Register of Members' Interests PDF extraction",
    "lobbying": "lobbying.ie YTD + CRO + charities Tier-A + gold enrichment",
    "iris": "Iris Oifigiúil: poller + silver + SI/appointments/notices gold",
    "legislation": "bills + questions + amendments + votes + cross-dataset enrich",
    "participation": "Showing-up model: division turnout + absence gaps + presence/vote divergence + 120-day TAA compliance (gold)",
    "committee_evidence": "committee meeting history: enumerate meetings + parse transcripts (topics/witnesses) -> silver",
    "committee_evidence_promote": "promote committee meetings/witnesses silver -> gold (v_committee_meetings)",
    "judiciary_bench": "promote validated judiciary sandbox -> gold bench/appointments/clearance/waiting/courthouses (transform, no network)",
    "legal_diary_poller": "archive the Courts Service daily Legal Diary .docx (forward-accumulating, day-or-lost)",
    "legal_diary_extract": "Legal Diary archive -> privacy-tiered judiciary gold (schedule/counts/cases)",
    "judiciary_diary_link": "map surname-only diary judges to the bench roster (pipeline-owned mapping)",
    "afs": "amalgamated LA Annual Financial Statements: spend by service division (silver)",
    "cbi": "CBI register extract + corporate-notices xref (gold)",
    "cro": "CRO company register <-> corporate-notices exact-name xref (gold)",
    "corporate_receiver": "receiver-appointer ranking + operator-firm concentration (gold; notices superset)",
    "procurement": "eTenders/OGP awards + supplier->CRO match (gold); value-is-not-spend flags",
    "procurement_lobbying": "supplier <-> lobbying registrant/client overlap xref (gold)",
    "entity_xref": "organisation-360 spine: supplier x CRO/lobbying/corporate/charity/EPA (gold)",
    "ted": "TED EU award notices (Ireland) + winner->CRO match (silver); award-value-not-spend flags",
    "ted_tenders": "TED Irish competition/tender notices (cn-standard) -> silver; pre-award estimates, never summed",
    "etenders_live_tenders": "eTenders.gov.ie LIVE national tender pipeline (open opportunities, sub-EU-threshold incl.) -> silver; PLANNED-tier estimates, never summed (Playwright/scrape-extra required)",
    "public_body_payments": "public-body PO/payment disclosures over €20k -> sandbox public_payments_fact (privacy-gated, bronze-cached)",
    "hse_tusla_payments": "HSE + Tusla PO/payment PDFs -> sandbox hse_tusla_payments_fact (privacy-gated, high-risk)",
    "disclosed_bq_po": "disclosed national PO BigQuery extract -> silver disclosed_bq_po_payments_fact (Phase 1: HSE 2017-2020 + 2025Q4/2026Q1 history recovery; no-op-safe; folded into hse_tusla)",
    "disclosed_bq_po_newbodies": "disclosed national PO BigQuery extract -> silver disclosed_bq_po_newbodies_fact (Tranche 1: 8 councils + Garda + EPA + Louth&Meath ETB, all po_committed source-authoritative; fail-closed registry; own SOURCE_FACTS entry)",
    "la_payments": "31 local authorities' PO/payments-over-€20k -> silver la_payments_fact (full back-catalogue)",
    "procurement_consolidate": "fold public_body + hse_tusla + LA facts -> gold procurement_payments_fact (listing-rot guarded)",
    "ministerial_diaries": "diary classify->match->overlap->promote->company_influence (transform tail; no-op if sandbox absent)",
    "cso": "CSO PxStat housing/HAP + govt-finance (GFA01/GFQ01/NA012) -> gold denominators",
    "stateboards": "DPER state-boards register: live roster + body universe + hand-curated Wikidata identities (gold)",
    "noac_collection": "NOAC M2 per-LA revenue-collection rates (commercial/rent/housing-loan, 31 LAs) -> gold (PDF, fidelity-gated)",
    "noac_scorecard": "NOAC accountability scorecard (finance/workforce/roads/fire/litter, 31 LAs) -> gold noac_scorecard_wide (PDF, 31-LA guard)",
    "noac_scorecard_history": "NOAC scorecard across 2022-2024 reports (header-driven, layout-robust) -> gold noac_scorecard_history (trend sparklines)",
    "noac_indicators": "NOAC full indicator set (~125 series, raw values) -> gold noac_indicators_long (All-indicators drill-down)",
    "derelict_sites": "DHLGH Derelict Sites annual return: per-LA levied/collected/outstanding -> gold (cached XLSX, reconciled)",
    "planning_appeal_outcomes": "council vs An Coimisiún Pleanála appeal decisions -> silver ABP-overturn metric (ArcGIS + committed planning silver)",
    "freshness": "data-age signal per domain -> data/_meta/freshness.json",
    "fact_cards": "metadata index (schema/rows/grain/never-sum) of every parquet -> data/_meta/fact_cards.json",
    "source_health": "per-source health -> data/_meta/source_health.json (manual staleness; links opt-in)",
    "output_regressions": "completeness guard: gold row/column drop vs baseline -> data/_meta/output_regressions.json",
    "extraction_quality": "match-rate guard: coverage-JSON matched/total ratio drop vs baseline (pilot: judiciary_diary_link, entity_xref) -> data/_meta/extraction_quality_regressions.json",
}

_SUMMARY_SKIP_PREFIXES = ("warning:", "warn:", "[warn", "deprecation")


def _summarise_log(lines: list[str]) -> str | None:
    """Pick a useful summary line for the manifest.

    Walk from the end, skip blanks and obvious noise (warnings, deprecations),
    return the first remaining line.
    """
    for line in reversed(lines):
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.lower().startswith(_SUMMARY_SKIP_PREFIXES):
            continue
        return candidate[:500]
    return None


# ── Per-chain wall-clock timeout ─────────────────────────────────────────────
# A wedged network fetch otherwise stalls the whole SERIAL run — history has full
# runs hung to 400-758 min on a single chain. On expiry we kill the chain's process
# TREE (a chain script such as iris_refresh spawns its own sub-python steps, so
# proc.kill() alone would orphan the grandchildren — the Windows pileup warned about
# in feedback_no_blind_background_python) and mark the chain failed, so the run
# continues. DEFAULT covers every healthy chain with margin; the override map grants
# the legitimately-slow network harvesters more headroom. Env DAIL_CHAIN_TIMEOUT_S
# overrides the default; set it to 0 to disable timeouts entirely.
DEFAULT_CHAIN_TIMEOUT_S = int(os.environ.get("DAIL_CHAIN_TIMEOUT_S", "1200"))
_CHAIN_TIMEOUT_OVERRIDES: dict[str, int] = {
    "public_body_payments": 2700,  # harvests dozens of gov.ie / semi-state listings
    "hse_tusla_payments": 2700,
    "la_payments": 2700,  # 31 council portals, full back-catalogue
    "cbi": 1800,  # register PDFs — historically the worst hang (510 min once)
    "committee_evidence": 1200,  # re-fetches ~1k AKN XML transcripts each run
    "bootstrap": 1800,  # members API + attendance/payments/interests PDF poll (foundation chain)
}


def _chain_timeout(name: str) -> int | None:
    """Wall-clock ceiling (seconds) for a chain, or None if timeouts are disabled."""
    if DEFAULT_CHAIN_TIMEOUT_S <= 0:
        return None
    return _CHAIN_TIMEOUT_OVERRIDES.get(name, DEFAULT_CHAIN_TIMEOUT_S)


# ── Per-chain env overrides ──────────────────────────────────────────────────
# POLARS_MAX_THREADS=1 for chains whose script (or a script it further shells out
# to — env is inherited by subprocess.run() when a chain runner like
# ministerial_diaries_refresh.py doesn't pass its own env=) runs a Python
# map_elements() UDF over a large post-explode/post-concat frame. Polars 1.41.2's
# multi-threaded engine calling back into the Python interpreter (GIL) from worker
# threads intermittently segfaults on Windows (0xC0000005; reproduced 2026-07-11 in
# lobbying/lobby_processing.py, which crashed the `lobbying` chain — see
# lobbying_refresh.py's step_process() for the same fix applied at that finer grain).
# Single-threading these whole chains costs little: map_elements is a small fraction
# of each chain's runtime, well inside its timeout above.
_CHAIN_EXTRA_ENV: dict[str, dict[str, str]] = {
    "planning_appeal_outcomes": {"POLARS_MAX_THREADS": "1"},  # ~126k-row national applications frame
    "procurement_consolidate": {"POLARS_MAX_THREADS": "1"},  # 401,624-row consolidated payments fact
    "procurement": {"POLARS_MAX_THREADS": "1"},  # map_elements immediately after .explode() — same shape as the lobbying bug
    "la_payments": {"POLARS_MAX_THREADS": "1"},  # 99,412-row merged council payments
    "public_body_payments": {"POLARS_MAX_THREADS": "1"},  # 89,285-row disclosures; classify_and_flag() also imported by hse_tusla_payments
    "hse_tusla_payments": {"POLARS_MAX_THREADS": "1"},  # imports classify_and_flag() from procurement_public_body_extract.py, runs in its own process
    "cro": {"POLARS_MAX_THREADS": "1"},  # up to 51,149-row corporate notices
    "cbi": {"POLARS_MAX_THREADS": "1"},  # historic Dáil+Seanad interests backfill + notices/lobbying joins
    "ted": {"POLARS_MAX_THREADS": "1"},  # ted_enrich.enrich_winner_rows(), ~13,954-row awards
    "ministerial_diaries": {"POLARS_MAX_THREADS": "1"},  # diary_org_match/diary_lobbying_overlap/diary_promote_gold — inherited by their sub-subprocesses
    "legal_diary_extract": {"POLARS_MAX_THREADS": "1"},  # raw_case -> parties() struct map_elements; bursty, up to low-tens-of-thousands rows on catch-up runs
}


def _chain_extra_env(name: str) -> dict[str, str]:
    return _CHAIN_EXTRA_ENV.get(name, {})


def _kill_process_tree(proc: subprocess.Popen) -> None:
    """Kill the chain process AND all its descendants.

    A chain script spawns its own sub-python steps (iris_refresh, lobbying_refresh,
    members_refresh, …), so proc.kill() would reap only the top child and leave the
    grandchildren running — orphaned, hung on the network (feedback_no_blind_background_
    python). On Windows ``taskkill /T`` walks the whole tree by PID; elsewhere fall back
    to killing the child directly.
    """
    if proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        proc.kill()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def _run_subprocess(run_id: str, name: str, script: str, log_path: Path) -> tuple[int | None, str | None, bool]:
    """Run a chain script and tee its combined stdout/stderr to ``log_path``.

    Forces UTF-8 on the child's stdio so non-ASCII chars (→, é, etc.) survive on
    Windows where the console codepage is usually cp1252. A reader thread pumps the
    child's output to the log while the main thread waits with a per-chain deadline
    (see _chain_timeout), so a wedged chain is killed (whole tree) instead of stalling
    the serial run.

    Returns (exit_code, summary, timed_out); exit_code is None on a timeout kill.
    """
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    env[ENV_RUN_ID] = run_id
    env.update(_chain_extra_env(name))

    tail: list[str] = []
    timed_out = False
    timeout_s = _chain_timeout(name)
    with open(log_path, "w", encoding="utf-8", newline="") as logf:
        logf.write(f"# === {name} ({script}) ===\n")
        logf.flush()
        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        assert proc.stdout is not None

        def _pump() -> None:
            # Tee the child's combined output to our stdout + the step log. Runs in a
            # thread so the main thread can enforce the wall-clock deadline below.
            for line in proc.stdout:  # type: ignore[union-attr]
                sys.stdout.write(line)
                logf.write(line)
                logf.flush()
                tail.append(line)
                if len(tail) > 100:
                    del tail[:-100]

        reader = threading.Thread(target=_pump, name=f"tee-{name}", daemon=True)
        reader.start()
        exit_code: int | None
        try:
            exit_code = proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            timed_out = True
            _kill_process_tree(proc)
            exit_code = None
        reader.join(timeout=5)  # child stdout is EOF once killed, so the pump drains fast
        if timed_out:
            banner = f"# !!! chain '{name}' exceeded {timeout_s}s wall-clock — killed process tree\n"
            sys.stdout.write(banner)
            logf.write(banner)
            logf.flush()
    if timed_out:
        return None, f"timed out after {timeout_s}s", True
    return exit_code, _summarise_log(tail), False


def _run_chain(
    run_id: str, ordinal: int, total: int, name: str, script: str
) -> tuple[str, int | None, str | None, str | None]:
    """Returns (status, exit_code, summary, error)."""
    print(f"\n=== [{ordinal:02d}/{total}] {name} ===")
    logging.info("Pipeline chain started: %s", name)

    log_path = step_log_path(run_id, ordinal, name)
    record_step_started(run_id, ordinal, name, script, log_path)

    try:
        exit_code, summary, timed_out = _run_subprocess(run_id, name, script, log_path)
        if timed_out:
            err = f"timed out after {_chain_timeout(name)}s"
            logging.error("Pipeline chain %s failed: %s", name, err)
            return "failed", exit_code, summary, err
        if exit_code != 0:
            err = f"exit code {exit_code}"
            logging.error("Pipeline chain %s failed: %s", name, err)
            return "failed", exit_code, summary, err
        logging.info("Pipeline chain finished: %s", name)
        return "ok", exit_code, summary, None
    except Exception as e:  # noqa: BLE001 — orchestrator must isolate every failure mode
        logging.error("Pipeline chain %s failed: %s", name, e)
        return "failed", None, None, str(e)


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _filter_chains(selected: list[str], excluded: list[str]) -> list[tuple[str, str]]:
    known = {name for name, _ in CHAINS}
    for name in selected + excluded:
        if name not in known:
            raise SystemExit(f"unknown chain: {name!r} (known: {', '.join(sorted(known))})")
    if selected:
        wanted = set(selected)
        chains = [(n, s) for n, s in CHAINS if n in wanted]
    else:
        chains = list(CHAINS)
    if excluded:
        skip = set(excluded)
        chains = [(n, s) for n, s in chains if n not in skip]
    return chains


def _print_chain_list() -> None:
    print("Available chains (default run order):\n")
    width = max(len(n) for n, _ in CHAINS)
    for name, script in CHAINS:
        blurb = _CHAIN_BLURBS.get(name, "")
        print(f"  {name:<{width}}  {script:<26}  {blurb}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pipeline.py                       # full run, all chains\n"
            "  python pipeline.py --list                # show chains and exit\n"
            "  python pipeline.py --select iris         # only iris\n"
            "  python pipeline.py --select members,iris # multiple chains\n"
            "  python pipeline.py --exclude lobbying    # everything except lobbying\n"
        ),
    )
    ap.add_argument("--list", action="store_true", help="print chains and exit")
    ap.add_argument("--select", metavar="CHAINS", help="comma-separated chains to run (default: all)")
    ap.add_argument("--exclude", metavar="CHAINS", help="comma-separated chains to skip")
    args = ap.parse_args()

    # The orchestrator tees each child chain's combined stdout (which contains
    # non-ASCII progress: →, é, Σ, …) to its own stdout. When this process'
    # stdout is a pipe or file on Windows, Python picks the cp1252 ("charmap")
    # locale codec and sys.stdout.write() raises UnicodeEncodeError on the first
    # such line — crashing the parent and marking every chain "failed" with
    # exit_code=None. Force UTF-8 on our own stdio so the tee survives. (Child
    # processes already get PYTHONIOENCODING=utf-8 in _run_subprocess.)
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")

    if args.list:
        _print_chain_list()
        return 0

    selected = _parse_csv_list(args.select)
    excluded = _parse_csv_list(args.exclude)
    chains = _filter_chains(selected, excluded)
    if not chains:
        print("No chains selected after --select/--exclude. Nothing to do.", file=sys.stderr)
        return 1

    run_id = make_run_id()
    setup_logging(run_id)

    # 60-day retention of per-run log dirs — uncomment to enable.
    # from services.run_paths import prune_old_runs
    # pruned = prune_old_runs(days=60)
    # if pruned:
    #     logging.info("Pruned %d run dir(s) older than 60 days", pruned)

    create_run_manifest(run_id)
    logging.info("Pipeline run id: %s — logs at %s", run_id, run_dir(run_id))
    logging.info("Running %d chain(s): %s", len(chains), ", ".join(n for n, _ in chains))

    succeeded: list[str] = []
    broken: list[tuple[str, str]] = []
    total = len(chains)

    for ordinal, (name, script) in enumerate(chains, start=1):
        status, exit_code, summary, error = _run_chain(run_id, ordinal, total, name, script)
        record_step_finished(run_id, name, status, exit_code, summary, error)
        if status == "ok":
            succeeded.append(name)
        else:
            broken.append((name, error or "unknown"))

    run_finished_at(run_id)

    print("\n=== Pipeline summary ===")
    print(f"Run id:  {run_id}")
    print(f"Log dir: {run_dir(run_id)}")
    print(f"Succeeded ({len(succeeded)}/{total}):")
    for name in succeeded:
        print(f"  + {name}")
    if broken:
        print(f"Failed ({len(broken)}/{total}):")
        for name, error in broken:
            print(f"  - {name}: {error}")
        print("\nData processing pipeline encountered errors.")
        return 1

    print("Data processing pipeline complete. All chains executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
