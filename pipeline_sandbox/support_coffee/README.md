# support_coffee — "buy me a coffee" surface (SANDBOX, UNWIRED)

**Status: preview only. Nothing here touches the live site.**

Per `doc/SANDBOX_MAP.md`, "it's in sandbox" is no signal about whether code is
live — so, explicitly, for this directory:

| Check | State |
|---|---|
| Registered in `utility/app.py` `st.navigation()` | **No** |
| Imported by anything under `utility/` | **No** |
| Edits to `shared_css.py` / `ui/components.py` / `app.py` | **None** |
| Reads any dataset, view or `data_access` module | **No** |
| Backs a SQL view / promote script / test | **No** |
| Safe to delete | **Yes** — nothing depends on it |

It imports *from* `utility/` (read-only: `shared_css.inject_css`,
`ui.components.hide_sidebar`) so the preview renders on the real tokens
instead of a mock palette. Imports do not mutate the app.

## Preview it

```
.venv/Scripts/python -m streamlit run pipeline_sandbox/support_coffee/demo_app.py --server.port 8599
```

## Files

- `coffee_ui.py` — scoped `sc-*` CSS + HTML builders (footer strip, hero, cost
  strip, coffee button, honesty panel). Pure strings, no Streamlit.
- `page_support.py` — the `/support` page body. Static copy only.
- `demo_app.py` — the standalone preview app.

## Blockers before any of this could ship

1. **Every figure is an unsourced placeholder** (`PLACEHOLDER_COSTS` in
   `coffee_ui.py`: `€XX`, `NNN`, `N.Nm`). Real hosting cost from an invoice,
   real counts from a registered contract — or drop the strip. Provenance is
   the user's domain; invented figures must never reach UI copy.
2. **The Buy Me a Coffee slug does not exist.** `buymeacoffee.com/dailtracker`
   is a guess in `COFFEE_URL`. Register the account (personal vs. company is an
   open decision) before the URL goes anywhere shareable — moving it later
   breaks every link anyone shared.
3. **The honesty panel is the load-bearing part**, not decoration. A
   transparency site that accepts money has to answer "who funds this"; the
   "what your money does not buy" block is that answer. It ships with the
   button or the button does not ship.

## Design notes

- **No third-party JavaScript.** The button is a plain
  `<a target="_blank" rel="noopener">`, not Buy Me a Coffee's widget script.
  Streamlit's DOMPurify pass strips `<script>` but preserves `target="_blank"`
  (`utility/app.py:118-123`), so the anchor survives. This is what keeps the app
  free of third-party cookies and consent banners.
- **The footer links inward** to `/support?from=footer`, not straight out to
  Buy Me a Coffee. The existing cookieless page-view log
  (`utility/ui/page_analytics.py`) then counts interest with no new tracking and
  no outbound-click JS — `install_spa_links` only intercepts `href^="?"`, so an
  external link could not be measured without adding script.
- **Class prefix `sc-`** so this stylesheet can never collide with a live
  component if anyone imports the module.
