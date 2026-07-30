# support_coffee — SUPERSEDED, code removed 2026-07-27

This was the sandbox prototype for the Support page and the site footer. It
shipped into the app, so the prototype is gone and this note is what's left.

**The real implementation:**

| Concern | Lives in |
|---|---|
| The three scale figures | `utility/data_access/support_data.py` |
| HTML builders + footer | `utility/ui/components.py` (`support_*_html`, `site_footer_html`) |
| Stylesheet | `utility/shared_css.py`, the `.sup-*` family (appended last) |
| The page | `utility/pages_code/support.py` |
| Wiring | `utility/app.py` — registered under Glossary; footer after `pg.run()` |
| Tests | `test/utility/test_support_page.py` |

**Why the code was deleted rather than left here.** It had drifted into a
second, worse copy of a shipped feature: `sc-` class names that no longer match
the app's `.sup-*`, and — the reason this mattered — the Buy Me a Coffee URL and
the contact alias as **hard-coded literals**. The shipped version reads both
from environment variables (`DT_COFFEE_URL`, `DT_CONTACT_EMAIL`), both unset by
default, so a fresh checkout publishes neither a payment link nor an email
address. Keeping a copy that hard-codes them undoes that on the one axis that
actually matters.

**Recovering it:** `git show e08515b` — the prototype as committed, including
the standalone `demo_app.py` preview and the `local_app.py` harness that ran the
real app with the page patched in without touching `utility/app.py`.

Note that `coffee_ui.py` had uncommitted edits when it was removed, so that
commit holds it *without* the "Spot something wrong?" section. Nothing is lost:
that section shipped, and lives in `ui/components.py::support_help_html`.

**Still open (not code):** the Buy Me a Coffee account is unregistered and the
Proton alias does not exist. `dailtracker@proton.me` was a placeholder, never a
real address. Until both env vars are set, the Support page renders its figures
and both GitHub report routes, and asks for nothing.
