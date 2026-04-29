Review page `<PAGE_ID>` for logic-firewall violations.

Check:
- no backend files modified
- no `read_parquet`
- no `parquet_scan`
- no persistent `.duckdb` shortcut
- no `CREATE VIEW`
- no joins/groupby used for modelling
- no pandas/polars business logic
- no API calls
- no PDF parsing
- no hardcoded absolute paths
- TODO_PIPELINE_VIEW_REQUIRED used for missing data

Run if possible:

```powershell
python tools/check_streamlit_logic_firewall.py utility/pages_code/<PAGE_FILE>.py
python tools/check_contract_shape.py utility/page_contracts/<PAGE_ID>.yaml
```
