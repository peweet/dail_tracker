# Streamlit page guidance

- Pages render registered contracts; business queries, joins, rollups, and inference belong in `utility/data_access/` or SQL views.
- Reuse `utility/ui/` components and formatters and the shared CSS tokens. Do not create a page-local data-access or formatting fork.
- Preserve source links and uncertainty labels in the rendered result.
- Run the closest `test/utility/` test plus `uv run python tools/dev.py firewall` and `uv run python tools/dev.py conventions`.

