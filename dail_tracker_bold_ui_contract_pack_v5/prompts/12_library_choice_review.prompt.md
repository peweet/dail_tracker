Review library choices for page `<PAGE_ID>`.

Prefer native Streamlit first.

Approved:
- Plotly
- Altair

Optional with justification:
- streamlit-echarts
- streamlit-aggrid
- streamlit-extras
- streamlit-community-navigation-bar

Avoid:
- custom JS
- custom React
- page-specific frameworks

Return:
1. current libraries used
2. whether each is justified
3. simpler native alternative if available
4. whether visual polish should be solved by CSS/helpers instead

Result contract:
- `Verdict: PASS | FAIL` (`PASS` means every non-native dependency has a demonstrated need)
- each finding with `Severity`, dependency/config `Evidence`, consequence, and required action
- checks performed and residual risk
