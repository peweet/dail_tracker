"""Unit tests for tools/join_graph.py — the static join-graph reader.

Fixtures are inline strings, so these are fast and don't depend on live
sql_views/ or extractor files changing under them.
"""

from __future__ import annotations

from tools.join_graph import grade_key, parse_python, parse_sql


def test_grade_key_buckets():
    # canonical normalised keys
    assert grade_key("supplier_norm") == "CANON"
    assert grade_key("name_norm") == "CANON"
    assert grade_key("join_key") == "CANON"
    assert grade_key("supplier_normalised") == "CANON"
    # structured ids
    assert grade_key("unique_member_code") == "ID"
    assert grade_key("cro_number") == "ID"
    assert grade_key("rcn") == "ID"
    assert grade_key("vote_id") == "ID"
    # raw OPEN-name columns — the fragile, off-contract joins
    assert grade_key("full_name") == "RAW"
    assert grade_key("member_name") == "RAW"
    assert grade_key("minister_name") == "RAW"
    # closed-vocabulary name columns — safe by design
    assert grade_key("council") == "CONTROLLED"
    assert grade_key("constituency_name") == "CONTROLLED"
    assert grade_key("publisher_name") == "CONTROLLED"
    assert grade_key("local_authority") == "CONTROLLED"
    # dimensions / everything else
    assert grade_key("year") == "OTHER"
    assert grade_key("house") == "OTHER"


def test_sql_raw_name_join_is_flagged():
    sql = """
    SELECT * FROM roster r
    LEFT JOIN attendance_roster a ON a.full_name = r.member_name
    WHERE r.year = 2024
    """
    edges = parse_sql(sql, "fixture.sql")
    assert len(edges) == 1
    e = edges[0]
    assert e["grade"] == "RAW"
    assert {e["left"], e["right"]} == {"attendance_roster", "roster"}


def test_sql_id_join_grades_id():
    sql = """
    SELECT * FROM members u
    LEFT JOIN cov_minister cm ON cm.unique_member_code = u.unique_member_code
    """
    edges = parse_sql(sql, "fixture.sql")
    assert len(edges) == 1
    assert edges[0]["grade"] == "ID"


def test_sql_self_join_artifact_dropped():
    # both aliases resolve to the same base table; a.x = a.x must not appear
    sql = """
    SELECT * FROM real_global a
    JOIN real_global b ON a.member_name = a.member_name
    """
    assert parse_sql(sql, "fixture.sql") == []


def test_sql_comment_does_not_produce_edge():
    sql = """
    SELECT * FROM t
    -- JOIN ghost g ON g.full_name = t.full_name
    WHERE 1=1
    """
    assert parse_sql(sql, "fixture.sql") == []


def test_polars_join_detected_str_join_ignored():
    src = (
        "df = a.join(b, on='name_norm', how='left')\n"
        "s = ','.join(parts)\n"
        "p = os.path.join(root, 'x')\n"
    )
    sites = parse_python(src, "fixture.py")
    assert len(sites) == 1  # str.join / path.join carry no on=/how= kwarg
    assert sites[0]["grade"] == "CANON"
    assert sites[0]["keys"] == ["name_norm"]
    assert sites[0]["how"] == "left"
    assert sites[0]["asymmetric"] is False


def test_polars_left_right_on_raw_name_is_asymmetric_inner():
    src = "out = lob.join(idx, left_on='full_name', right_on='member_name', how='inner')\n"
    sites = parse_python(src, "fixture.py")
    assert len(sites) == 1
    assert sites[0]["grade"] == "RAW"
    assert sites[0]["keys"] == ["full_name", "member_name"]
    assert sites[0]["asymmetric"] is True  # different names each side
    assert sites[0]["how"] == "inner"      # silent row-drop on a fragile key


def test_polars_how_defaults_to_inner():
    # no how= kwarg → polars default is an inner join
    src = "df = a.join(b, on='rcn')\n"
    sites = parse_python(src, "fixture.py")
    assert sites[0]["how"] == "inner"
    assert sites[0]["grade"] == "ID"


def test_polars_validate_captured():
    src = (
        "guarded = a.join(b, on='rcn', validate='m:1')\n"
        "unguarded = a.join(b, on='rcn')\n"
    )
    sites = parse_python(src, "fixture.py")
    by_line = {s["line"]: s for s in sites}
    assert by_line[1]["validate"] == "m:1"   # guard declared
    assert by_line[2]["validate"] is None     # no guard — silent fan-out risk
