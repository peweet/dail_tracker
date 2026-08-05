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
the contact alias as **hard-coded literals**. The public coffee page is now a
checked-in default, with `DT_COFFEE_URL` retained as a deployment override and
explicit kill switch. `DT_CONTACT_EMAIL` remains unset by default, so a fresh
checkout publishes no private contact address.

**Recovering it:** `git show e08515b` — the prototype as committed, including
the standalone `demo_app.py` preview and the `local_app.py` harness that ran the
real app with the page patched in without touching `utility/app.py`.

Note that `coffee_ui.py` had uncommitted edits when it was removed, so that
commit holds it *without* the "Spot something wrong?" section. Nothing is lost:
that section shipped, and lives in `ui/components.py::support_help_html`.

**External status (2026-08-05):** the Buy Me a Coffee account is registered at
`https://buymeacoffee.com/peweet` and is the shipped default. The Proton alias
still does not exist: `dailtracker@proton.me` was a placeholder, never a real
address. Until `DT_CONTACT_EMAIL` is set, the Support page publishes no private
contact address.
