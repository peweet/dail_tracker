"""dail_tracker_core.queries — per-domain data-retrieval functions.

Each module here exposes pure ``(conn, *params) -> QueryResult`` functions
containing ONLY retrieval SQL (SELECT / WHERE / ORDER BY / LIMIT). All joins,
aggregation, and value-gating live in the registered ``sql_views/*.sql`` (the
firewall). These functions take an explicit DuckDB connection so they are unit-
testable and free of any Streamlit/interface dependency.
"""
