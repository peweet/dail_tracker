# Library Policy

## Native Streamlit first

Prefer:
- `st.dataframe`
- `st.column_config`
- `st.tabs`
- `st.container`
- `st.popover`
- `st.dialog`
- `st.fragment`
- `st.download_button`
- `st.date_input`
- `st.select_slider`

## Approved chart libraries

Use these when the chart directly answers a user question:
- Plotly
- Altair

Do not add charts for decoration.

## Optional with justification

Use only when native Streamlit cannot provide the interaction:
- `streamlit-echarts` for richer timelines, breakdowns, treemaps, or relationship charts
- `streamlit-aggrid` for researcher-grade tables with pinned columns or advanced grid controls
- `streamlit-extras` for small UI polish only
- `streamlit-community-navigation-bar` for improved app navigation

## Avoid unless explicitly approved

- custom React components
- arbitrary custom JavaScript
- one-off CSS frameworks
- large UI frameworks
- unmaintained Streamlit components
- libraries that duplicate native Streamlit features without clear benefit
