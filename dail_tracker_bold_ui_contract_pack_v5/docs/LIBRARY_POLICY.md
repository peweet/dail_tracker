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
- one-off CSS frameworks
- large UI frameworks
- unmaintained Streamlit components
- libraries that duplicate native Streamlit features without clear benefit

## Custom JavaScript policy

Bare `<script>` injection via `st.markdown(..., unsafe_allow_html=True)` is **forbidden**.

Custom JavaScript is permitted when:
1. Delivered via CCv2 (`st.components.v2.component()`) — follow the `building-streamlit-custom-components-v2` skill exactly
2. The interaction cannot be achieved with a native Streamlit widget
3. The purpose is functional, not decorative

Feature-bloat JavaScript remains forbidden regardless of delivery mechanism. The original prohibition was a feature-scope guardrail, not a blanket ban on all JS.
