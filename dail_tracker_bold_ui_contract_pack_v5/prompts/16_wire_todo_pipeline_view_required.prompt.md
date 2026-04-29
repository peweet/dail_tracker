Wire TODO_PIPELINE_VIEW_REQUIRED items.

Input:
- page contract
- page implementation
- list of TODO_PIPELINE_VIEW_REQUIRED items

Task:
Classify each TODO as:
1. missing registered view
2. missing column
3. missing filter
4. missing metric
5. missing source URL
6. missing provenance field

Then propose pipeline/view-layer changes only.

Do not implement in Streamlit.

For each TODO, return:
- target view name
- required columns
- grain
- source data
- transformation owner
- validation/check
- whether UI can gracefully degrade until implemented
