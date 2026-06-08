# `sql_views/` — registered DuckDB view definitions

Each `.sql` file is a `CREATE OR REPLACE VIEW v_…` statement registered onto an
in-memory DuckDB connection at runtime by
[`dail_tracker_core/db.py`](../dail_tracker_core/db.py) (`register_views`).

## Layout

Files are grouped into **per-domain subdirectories**:

| Subdir          | Loaded by glob          | Notes |
|-----------------|-------------------------|-------|
| `appointments/` | `appointments_*.sql`    | |
| `attendance/`   | `attendance_*.sql`      | also holds `v_sitting_days_by_year.sql` |
| `charity/`      | `charity_*.sql`         | |
| `committees/`   | `committees_*.sql`      | |
| `corporate/`    | `corporate_*.sql`       | |
| `debates/`      | `speech_*.sql`          | also holds `v_debate_listings.sql` |
| `judiciary/`    | `judiciary_*.sql`       | |
| `legislation/`  | `legislation_*.sql`     | includes statutory-instrument (`legislation_si_*`) views |
| `lobbying/`     | `lobbying_*.sql`        | |
| `member/`       | `member_*.sql`          | incl. `member_interests_*` and `member_zz_*` |
| `payments/`     | `payments_*.sql`        | |
| `procurement/`  | `procurement_*.sql`     | incl. `procurement_ted_*`, `procurement_payments_*` |
| `publicfinance/`| `publicfinance_*.sql`   | |
| `sipo/`         | `sipo_*.sql`            | |
| `votes/`        | `vote_*.sql`            | |

## Two rules that keep registration working

1. **Keep the domain prefix in every filename.** Views are loaded by
   prefix-glob (e.g. `procurement_*.sql`), resolved **recursively** as
   `**/procurement_*.sql`. The prefix — not the folder — is what the loader
   matches on, so a misfiled or prefix-less file silently won't load
   (registration uses `swallow_errors=True` in most callers → no crash, just a
   half-empty page). The folder is for humans; the prefix is for the loader.

2. **Within a domain, alphabetical filename order is the dependency order.**
   `register_views` loads each glob's matches via `sorted(...)`. A view that
   JOINs another must sort **after** it — hence the `zz_` prefix on dependent
   views (e.g. `legislation_si_zz_classified.sql` loads after
   `legislation_si_*`). Because every domain's files live together in one
   subdir, sorting by full path preserves the exact within-domain order the
   flat layout had.

Pathlib's `**` matches the root directory too, so a file left directly under
`sql_views/` still loads — but new files should go in the matching subdir.
