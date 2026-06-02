# CI/CD TODO — actions outside the repo

Companion to [CI_CD.md](CI_CD.md). The architecture and workflow contents live there; **this file lists the manual actions that can't be expressed in YAML** — GitHub UI toggles, first-push verification, and test debt surfaced during setup.

Tick each item as it's done. Delete this file once everything below is checked off (or move stragglers into [TICKETS.md](TICKETS.md) for tracking).

---

## 1. GitHub UI toggles (one-time, ~5 min)

Required after the first push of `.github/dependabot.yml` + workflow files:

- [ ] **Repo Settings → Code security and analysis → Dependabot alerts → Enable**
  Surfaces vulnerable-dep alerts in the Security tab. Free on public repos.

- [ ] **Repo Settings → Code security and analysis → Dependabot security updates → Enable**
  Auto-opens PRs for vulnerable deps *separately* from the weekly batch in `dependabot.yml`. This is the one that pays off when a CVE drops on a transitive dep over a weekend.

- [ ] **Repo Settings → Code security and analysis → Dependabot version updates → Enable**
  Should auto-enable when GitHub sees `.github/dependabot.yml` after first push. Verify it shows "Enabled by config file."

- [ ] **Repo Settings → Actions → General → Workflow permissions → Read and write permissions**
  The audit workflow needs `issues: write` to open issues on scheduled-run failures. Job-level permissions are declared in [audit.yml](../.github/workflows/audit.yml) but the repo-wide default must permit them.

---

## 2. First-push verification (~10 min)

After pushing the workflow files on a branch + opening a PR:

- [ ] PR shows two parallel checks: **`CI / lint`** and **`CI / test`**. Both green.
- [ ] If anything is red, read the failure on the Actions tab. Common causes:
  - **Case-sensitive imports** (Linux strict, Windows permissive). Grep for any `from Utility` etc.
  - **Path separators** hardcoded as `\\` instead of `pathlib.Path`.
  - **Missing CI marker** on a test that needs pipeline output.
- [ ] Manually trigger the Audit workflow once: **Actions → Audit → Run workflow → Run**. Should pass green.

---

## 3. Branch protection (do this AFTER #2 is reliable, not before)

- [ ] **Repo Settings → Branches → Add rule for `main` → Require status checks to pass before merging**:
  - Tick **`CI / lint`**
  - Tick **`CI / test`**
  - **Don't** tick `Audit` — that workflow is informational, not a gate.

> ⚠️ Doing this before CI is consistently green just blocks you from pushing fixes when CI is flaky. Wait until you've seen at least 2–3 clean runs.

---

## 4. Test debt surfaced during setup (DONE — pandera-polars API drift)

Seven tests were marked `@pytest.mark.skip` to land a green CI baseline. **All resolved** (the skip marks are removed and the tests pass):

- [x] **`test_silver_parquet.py` — 3 tests**: `PaymentTableSchema` is now defined (rejects a date leaked into `Amount` and a non-normalised `join_key`); skip marks removed.

- [x] **`test_gold_df.py` — 4 tests**: `MasterTDSchema` / `EnrichedAttendanceSchema` `@pa.dataframe_check` methods migrated to the `_df(data) = data.lazyframe.collect()` unwrap; skip marks removed.

Materialising the warehouse and running the full suite also surfaced (and fixed) several **stale** validation tests that had drifted from the current pipeline output: the attendance silver schema (per-day date columns, not per-year counts), the `master_td` count cap (174→185 for the 176-member 34th Dáil), `year_elected` dtype (now `Int64`), and a broken `normalise_df_td_name` Series/DataFrame call. Full suite now: **335 passed, 24 skipped, 0 failed** (the 24 skips are integration tests awaiting committed pipeline output).

---

## 5. Follow-up improvements (low priority, after #1–#4 are done)

These were noted in [CI_CD.md](CI_CD.md) and would extend coverage but aren't on the critical path:

- [ ] **Pre-commit hooks** ([CI_CD.md §3a](CI_CD.md#L160)) — runs ruff locally before commit, so CI catches drift not lint debt.
- [x] **Page-import smoke test** ([CI_CD.md §2c](CI_CD.md#L132)) — `test/test_page_imports.py` imports every Streamlit page. DONE: the CI `test` job now installs `--extra ui` (streamlit/altair/plotly/duckdb) so this collects and runs in CI instead of erroring on a missing import.
- [ ] **SQL view bootstrap smoke** ([CI_CD.md §2a](CI_CD.md#L76)) — requires committing tiny fixture parquets under `test/fixtures/`. Bigger task; defer until pipeline output schemas are more stable.

---

## References

- [.github/workflows/ci.yml](../.github/workflows/ci.yml) — lint + pytest on push/PR
- [.github/workflows/audit.yml](../.github/workflows/audit.yml) — pip-audit weekly + on dep-PRs
- [.github/dependabot.yml](../.github/dependabot.yml) — weekly batch + github-actions ecosystem
- [uv.lock](../uv.lock) — pinned dep resolution
- [doc/CI_CD.md](CI_CD.md) — full architecture and rationale
- [doc/TICKETS.md](TICKETS.md) — Jira-style backlog for larger work
