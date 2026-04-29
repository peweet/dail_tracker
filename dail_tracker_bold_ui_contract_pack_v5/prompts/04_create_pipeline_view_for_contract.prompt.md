Create or update pipeline/view-layer support for contract: `<PAGE_ID>`.

This is the only place where modelling work is allowed.

Allowed here:
- raw Parquet scans
- registered analytical view creation
- joins
- aggregations
- flags
- rankings
- source URL construction
- yearly summaries
- vote result summaries
- member profile views

Forbidden here:
- Streamlit UI changes unless explicitly requested

Read:
1. target page contract
2. relevant SQL/pipeline files only
3. manifest/provenance conventions if needed

Implement only the missing `TODO_PIPELINE_VIEW_REQUIRED` items.
