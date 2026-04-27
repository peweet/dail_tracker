# Review logic firewall

```md
Review the changed Dáil Tracker Streamlit files against the page contract.

Reject if Streamlit contains:
- joins
- groupby/pivot/window-style modelling
- fuzzy matching
- metric definitions not allowed by contract
- raw Parquet scans
- API/scraper/PDF calls
- hardcoded absolute paths
- unparameterized SQL
- extra features outside contract

Return:
1. pass/fail
2. issues by file/function/line
3. exact fixes
```
