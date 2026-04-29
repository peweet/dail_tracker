# Government Source Links

Government/Oireachtas source links should be treated as evidence, not decoration.

## Use cases

- legislative PDFs
- vote/division official pages
- Oireachtas API source records
- attendance PDFs
- Register of Members' Interests PDFs
- payment PDFs
- lobbying.ie records or exports

## UI rules

- Label links clearly: “Official PDF”, “Oireachtas record”, “Source document”
- Prefer link columns configured with `st.column_config.LinkColumn`
- Show source/provenance links near the relevant table or detail view
- Include source URLs in CSV export only when the contract marks them exportable
- Avoid raw long URLs in the visible UI

## Data rules

- Render only approved URL columns
- Do not construct government URLs in Streamlit unless a contract-approved URL template exists
- Prefer pipeline-provided source URL columns
- Missing URLs should become `TODO_PIPELINE_VIEW_REQUIRED`
