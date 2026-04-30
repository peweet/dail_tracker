Review page `<PAGE_ID>` for frontend quality and API compliance.

## API compliance (check every line)

- `st.html(...)` used — not `st.markdown(..., unsafe_allow_html=True)`
- `width="stretch"` on buttons — not `use_container_width=True`
- `st.segmented_control` used — not `st.radio(horizontal=True)`
- `st.space(n)` used — not `st.write("")` spacers
- `html.escape(value)` on all dynamic text inside HTML strings
- `background: #ffffff` on cards/pills — not `var(--surface)`
- `:material/icon_name:` for icons — not emoji strings in config/headers
- All CSS in `utility/shared_css.py` — no page-local CSS blocks

## UI quality

- Looks materially different from old page (or civic/strong design if greenfield)
- Civic/editorial feel — not a generic SaaS dashboard
- Strong hero — name/title/role context above the fold
- Clear primary user question answered in the hero
- Temporal controls match the data shape (year pills, not dropdowns)
- Table usability — column_config, readable labels, sensible default sort
- Charts answer a real question — not decorative
- Empty states are human and informative (not generic "No data")
- Back navigation in main content area (not sidebar only) on profile/detail views
- Mobile ordering reasonable

## Provenance

Read the contract `source_links` section before checking provenance.
If the contract explicitly omits the provenance footer (e.g. cross-domain pages
where merging sources would be misleading), its absence is correct — do not flag.
If the contract requires provenance, verify it is a single collapsed expander
at the bottom of the page.

## Colour accessibility (mandatory)

- No colour pair relies solely on red/green distinction
- Government/opposition distinction uses blue/amber — never red/green
- Flag any new colour additions that fail deuteranopia

## Source links

- URL columns use labelled links, not raw URLs
- Only approved_url_columns from the contract are used

Return high-severity and medium-severity issues only.
