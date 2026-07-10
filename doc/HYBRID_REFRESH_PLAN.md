# Hybrid unattended-refresh — corrected plan (2026-07)

**Status:** assessment complete, verified against code + an independent (Fable-5) adversarial
review. This supersedes the earlier chat sketch. **Nothing here is implemented yet** beyond the
read-only probe in §9 (`.github/workflows/procurement_ip_probe.yml`). Fix-order is deliberately
**inverted** from the first sketch: the reconciliation semantics are broken and must be fixed
*before* any hardware.

Related: `doc/CONTINUOUS_REFRESH.md` (the existing lane), `doc/DATA_BACKUP.md` /
`doc/DISASTER_RECOVERY.md` (R2), `doc/DATA_DISTRIBUTION_PLAN.md` (the git-as-CDN retirement).

---

## 1. Goal

Refresh the pipeline **unattended, laptop closed**, maximising coverage despite two obstacles:
1. **WAF/ASN block** — `gov.ie`'s CDN and ~24 county-council sites 403 any datacenter IP. GitHub-
   hosted (and AWS/any-cloud) runners cannot fetch those sources; a residential Irish IP is not blocked.
2. **Cold-start thinning** — bronze/silver working state is gitignored, so a clean runner rebuilds
   incremental sources from a thin slice unless prior state is restored.

## 2. Verified findings this plan is built on

| # | Finding | Evidence |
|---|---------|----------|
| A | **Pipeline is essentially OS-agnostic.** 65 Windows-coupling findings, 19 flagged BLOCKER; only **1** survived adversarial verification, and it doesn't crash/corrupt. The other 18 are Windows-only OCR/ops tools `pipeline.py` never invokes. | audit 2026-07-08 |
| B | **Per-chain timeout already exists** (committed `c694632`): `_kill_process_tree` os-guarded (taskkill/`proc.kill`), `DEFAULT_CHAIN_TIMEOUT_S=1200` + per-chain overrides. **Not a P0 to build.** | `pipeline.py:354-404` |
| C | **35/44 chains cloud-safe/cron-able.** Only `afs`, `public_body_payments`, `la_payments` are hard-WAF. `procurement` is uncertain (§9). `members`/`member_contact` have a soft throttle on oireachtas.ie profile HTML (skippable). `hse_tusla` is a dead source. | per-chain audit |
| D | **CI runs entirely on ubuntu-latest and is green** — the suite already passes on Linux. It guards code + gold contracts but is **blind to silver/bronze** behaviour (none in CI). | `ci.yml` (6 jobs, all ubuntu) |
| E | 🔴 **The two-lane reconciliation is DEAD as designed.** The four WAF-chain silvers are **git-tracked** (`.gitignore:367-377`); the cloud restore uses `rclone copy --ignore-existing` (`money_flow_refresh.yml:81-82`), which **skips files already in the checkout** → the box's fresh R2 silver is never applied → `procurement_consolidate` folds the **stale committed silver** daily, gate passes, ships. Silent no-op. | `git ls-files`, code |
| F | **The completeness gate is a catastrophic-loss gate, not a completeness gate**: `DEFAULT_TOLERANCE = 0.5`, gold-only, count-based. A 40% council-history loss, or a count-preserving value tamper, passes. | `check_output_regressions.py:40,43-48` |
| G | **AFS on the box would be theater**: AFS produces silver read directly by `afs_*.sql`; it is **not** in `PUBLISH_PATHS` (`publish_data.py:60-66`) → box→R2→(restore-skipped)→(unpublished) → app serves the committed copy forever. | code |
| H | **The WAF sources are quarterly/annual** (council PO ≈ quarterly Circular 07/2012; AFS annual; public-body quarterly). A *daily always-on box* optimises the cheap part while (E)/(F) go unfixed. | domain |

## 3. Corrected architecture

R2 stays the shared blackboard, but with **partitioned write authority** and a **commit protocol**
(the two things the first sketch lacked):

```
┌ EDGE WORKER (laptop wake-timer first; N100 only if flaky) ─ Irish residential IP ┐
│  weekly: pipeline.py --select <WAF chains>  → local bronze accretes → fresh silver │
│  → push silver to R2 EDGE BUCKET, then write _edge_manifest.json (sha256 +         │──┐
│     per-source row counts) LAST                                                    │  │ creds:
│  NO git. NO GitHub token. Credentials to the EDGE BUCKET ONLY.                     │  │ edge bucket
└────────────────────────────────────────────────────────────────────────────────────┘  │ only
                                                                                          ▼
                                          ┌ R2: archive bucket (bronze/silver) ┐  ┌ R2: EDGE bucket ┐
                                          │  cloud reads+writes                │  │ box writes only  │
                                          └────────────────────────────────────┘  └──────────────────┘
┌ CLOUD LANE (GitHub Actions, daily) — SOLE git-writer + publisher ─────────────────┐        ▲
│  restore archive bronze/silver  →  FORCE-OVERLAY edge silver over the checkout      │────────┘
│     (copy --update / scoped sync — NOT --ignore-existing), applying ONLY files      │  reads
│     listed in a FRESH _edge_manifest (fail loud if manifest > ~30d = box-down)      │
│  →  pipeline.py --exclude <WAF chains, hse_tusla>                                    │
│  →  gate (per-publisher row reconcile + tighter money tolerance)                     │
│  →  publish allow-listed gold to main  →  Streamlit redeploys                        │
│  →  back up refreshed bronze/silver to ARCHIVE bucket (gated on success, not always) │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

**Why partitioned buckets, not prefixes:** R2 API tokens scope per-bucket, not per-prefix. A separate
edge bucket means a compromised box (home LAN, always-on) can rewrite only 3-4 facts — not the
cloud's entire bronze/silver estate. Blast radius drops from "whole data estate" to "detectable via
the manifest + per-publisher reconcile."

## 4. runs-on split

| Bucket | Runner | Chains |
|---|---|---|
| Cloud-safe, cron now (R2 restore for stateful) | GitHub-hosted | bootstrap, seanad, lobbying, iris, legislation, committee_evidence, legal_diary_poller, cbi, cso, stateboards, planning_appeal_outcomes, ted, ted_tenders, derelict_sites *(cached)*, + all transforms |
| No-op-safe on cloud (exit 0 harmlessly) | GitHub-hosted | disclosed_bq_po×2, ministerial_diaries |
| **Edge worker (residential IP)** | laptop wake-timer → box | **afs, public_body_payments, la_payments** (+ procurement iff §9 says WAF) |
| Throttle-soft | either (pace / skip) | members, member_contact — `DAIL_SKIP_MEMBER_CONTACT=1` or route to edge |
| Exclude entirely (dead) | — | hse_tusla_payments |

## 5. Phased plan (inverted fix-order)

### Phase 0 — free, no hardware, do first
- **P0.1 Probe `procurement` from a datacenter IP** — §9 workflow. Settles whether procurement is WAF
  (→ edge) or cloud-safe (→ box scope shrinks to 3). Also the natural place to confirm CKAN reachability.
- **P0.2 Fix the one confirmed OS wart** — `procurement_etenders_extract.py:68`
  `CACHE = Path("c:/tmp/...")` → `Path(tempfile.gettempdir())/...` or `ROOT/"data/bronze/..."` (its TED
  sibling's pattern). One line; matters if procurement ever runs on Linux.

### Phase 1 — fix the reconciliation (the actually-broken part)
- **P1.1 R2 semantics.** Restore = force-overlay edge silver (drop `--ignore-existing` for edge-owned
  files); backup = `sync` (not `--ignore-existing`) **gated on pipeline success + gate pass**, never
  `if: always()`. Separate **edge bucket**; box creds scoped to it only.
- **P1.2 Manifest protocol.** Edge writes facts, then `_edge_manifest.json` (run stamp, sha256s,
  per-source row counts) **last**. Cloud applies only manifest-listed files; **fails loud if the
  manifest is older than ~30 days** (= box-down heartbeat, for free).
- **P1.3 Extend `PUBLISH_PATHS`** with `afs_amalgamated_divisions.parquet` (and audit any other
  runtime-read silver the edge refreshes) — else the AFS lane ships nothing.
- **P1.4 Gate upgrades.** Add a **per-publisher** row-count reconcile for `procurement_payments_fact`
  (consolidate already emits `procurement_payments_fact_coverage.json` — assert vs a committed
  baseline); tighten `--tolerance` for money facts below the 0.5 default.
- **P1.5 Harden** — pin the `curl | sudo bash` rclone install (`money_flow_refresh.yml:79`).

### Phase 2 — edge worker on the EXISTING laptop
- Weekly **Task Scheduler wake-timer** runs the edge script (fetch WAF chains → push to edge bucket +
  manifest). **Seed the box's bronze from the R2 archive first** (else cold-start thinning just moves
  to the box). Register a `waf_local` lane in `tools/freshness_status.py`.

### Phase 3 — buy the N100 *only if earned*
- If the wake-timer demonstrably misses runs, buy the mini-PC (Ubuntu 24.04 = x86 parity with the
  proven WSL pretest) and move the **identical** script to a systemd timer. Not before.

### Phase 4 — retire git-as-CDN (parallel track, own project)
- Daily multi-MB parquet commits grow history O(GB/yr) against GitHub's ~5GB soft cap. The repo already
  half-built the fix (`tools/fetch_runtime_data.py`, `doc/DATA_DISTRIBUTION_PLAN.md`: app rehydrates
  runtime data from R2, git carries code). Don't let this plan deepen a mechanism the project is retiring.

## 6. Security stance

- **No GitHub self-hosted runner** on this public repo (fork-PR RCE class). Edge worker = plain
  scheduled job talking only to R2.
- **Partitioned edge bucket** (§3) contains a box compromise to 3-4 facts.
- **Manifest-gated apply** + per-publisher reconcile detects thinning; note the residual **data-integrity**
  gap — a count-preserving value tamper still passes count-based gates. This is box→R2→cloud→`git push`
  into a *public* repo, so treat the edge bucket creds as sensitive.
- Pin all piped installers in `contents: write` workflows.

## 7. Failure / observability

- **Box-down** = stale manifest → cloud fails loud (P1.2). No silent weeks.
- **Alert-fatigue fix:** today the gate can "abort + open an issue **each day**" while degraded
  (`CONTINUOUS_REFRESH.md:59`) — day-40 real failure looks like day-1 noise. Distinguish "known-degraded"
  from "new failure."
- **Heartbeat coupling:** the freshness beat only records on `pipeline_exit == 0`
  (`money_flow_refresh.yml:107`), and `pipeline.py` exits 1 if *any* chain failed — so one intermittent
  chain flatlines the monitor while 95% refreshed. Consider a partial-success signal.
- **60-day auto-disable:** GitHub disables scheduled workflows after 60 days of repo inactivity; a broken
  lane + absent maintainer eventually silences its own alarm.

## 8. Open unknowns (measure, don't assume)

- Full-run end-to-end runtime on a Linux runner is **still unmeasured** vs the 350-min ceiling
  (`money_flow_refresh.yml:17-23`; the `pipeline_probe.yml` full run is prescribed, not yet performed).
- `procurement` IP-WAF status — §9 settles it.
- R2 archive is **already ~9.6GB against the 10GB free tier**, append-only by design — enable billing
  deliberately (~$0.015/GB-mo) rather than discovering failed PUTs.

## 9. Step-zero probe — is `procurement` cloud-safe?

`.github/workflows/procurement_ip_probe.yml` (read-only, `contents: read`, `workflow_dispatch` only).
It exercises the extractor's **real** `resolve_download_url()` and `GOVIE_HEADERS`, then tests the
resolved host **bare vs spoofed** so the result distinguishes the two WAF layers:

| Result | Meaning | Action |
|---|---|---|
| CKAN 200, resolved host on `data.gov.ie`, GET **200** | fully cloud-clean | `procurement` stays cloud; box scope → 3 chains |
| resolved host `assets.gov.ie`, **bare 403 / spoofed 200** | UA-WAF only (mitigable) | cloud-safe with `GOVIE_HEADERS` (already sent) |
| resolved host `assets.gov.ie`, **spoofed 403** | hard IP/ASN WAF | `procurement` → edge worker |

**To run it:** commit + push the workflow, then Actions → "Procurement IP/WAF probe" → *Run workflow*.
It downloads at most ~64 KB, writes nothing, publishes nothing. Read the job summary for the matrix.
