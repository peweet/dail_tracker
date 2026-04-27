# Review SQL performance

```md
Review SQL and data access for the page.

Rules:
- retrieval-only SQL in Streamlit
- explicit projection
- simple filters pushed into SQL
- no SELECT * unless explicitly allowed
- no large unbounded result sets
- no Python-side filtering of large frames
- respect contract limits
- use st.cache_data for DataFrame query functions

Return:
1. safety issues
2. performance issues
3. contract changes needed
4. suggested query rewrite
```
