Apply shared behaviours to page `<PAGE_ID>`.

Only apply behaviours declared in `uses_patterns`.

Supported:
- ISO dates -> date-range controls
- reporting years -> year controls
- timelines -> left-to-right chronological
- CSV export -> current displayed view
- member drilldown -> approved fields only
- source links -> approved URL columns only

Do not add modelling logic.
Missing data becomes:
`TODO_PIPELINE_VIEW_REQUIRED`.
