# This is a place holder .py file for now but it will be
# the starting point for writing tests for the project. We will use pytest as our testing framework and we will write tests for each of the services and utility functions in the project to ensure that they are working correctly and to catch any bugs or issues early on in the development process. We will also set up a test suite that can be run automatically as part of our CI/CD pipeline to ensure that all tests are run and that any issues are caught before they make it to production. This will help us to maintain
# a high level of code quality and to ensure that our project is reliable and robust as it evolves over time.
# TODO: write tests for the services and utility functions in the project, and set up a test suite that can be run automatically as part of our CI/CD pipeline. This will help us to maintain a high level of code quality and to ensure that our project is reliable and robust as it evolves over time. We can start by writing tests for the most critical functions and services in the project, and then expand our test coverage over time as we add more features and functionality to the project. We can also use tools like pytest-cov to measure our test coverage and to identify any areas of the code that are not being tested adequately.
# Check also pydantic, dataclass, and data expectations for future testing and validation of data structures and types across the codebase, to ensure that we are working with the correct data formats and to catch any issues early on in the development process. This will help us to maintain a high level of code quality and to ensure that our project is reliable and robust as it evolves over time.

# Take care to have the test folder mirror the productive folder to ensure ease of navigation and comprehension when writing and running tests, and to make it easier to identify which tests correspond to which services and utility functions in the project. This will help us to maintain a high level of organization and to ensure that our tests are easy to find and run as part of our development process. For example, we can have a directory structure like this:
# test/
#     attendance/
#     payments/
#     lobbying/
#     utility/


def test_td_count(): ...  # >= 127 unique TDs in attendance CSV
def test_no_null_names(): ...  # no empty identifiers
def test_max_attendance(): ...  # no TD exceeds 103 sitting days
def test_enriched_has_party(): ...  # all rows have a non-null party after join



#### `test_bronze.py` — Raw data integrity

# ```python
# import json
# from pathlib import Path

# def test_members_json_is_valid():
#     """members.json should be parseable and contain results."""
#     with open("members/members.json") as f:
#         data = json.load(f)
#     assert isinstance(data, list)
#     assert len(data) > 0

# def test_pdf_exists():
#     """The attendance PDF should exist in storage."""
#     assert Path("pdf_storage").glob("*.pdf")
# ```

# #### `test_silver.py` — Cleaned data quality

# ```python
# import pandas as pd

# def test_enriched_join_completeness():
#     """Every TD in attendance should match a member record."""
#     df = pd.read_csv("members/enriched_td_attendance.csv")
#     null_parties = df["party"].isna().sum()
#     assert null_parties == 0, f"{null_parties} TDs have no party after join"

# def test_no_duplicate_tds():
#     """No TD should appear more than once in the enriched output."""
#     df = pd.read_csv("members/enriched_td_attendance.csv")
#     dupes = df.groupby(["first_name", "last_name"]).size()
#     multi = dupes[dupes > 1]
#     # Some duplication expected from date rows — check identifiers
#     assert df["identifier"].nunique() == len(df["identifier"].dropna()), \
#         f"Duplicate identifiers found"
# ```

# #### `test_gold.py` — Database quality

# ```python
# import duckdb

# def test_attendance_count_within_limit():
#     """No TD should have more attendances than their Dáil limit."""
#     con = duckdb.connect("gold/dail_data.duckdb", read_only=True)
#     violations = con.execute("""
#         SELECT identifier, sitting_total_days
#         FROM td_attendance
#         WHERE sitting_total_days > 103
#     """).fetchall()
#     con.close()
#     assert len(violations) == 0, f"TDs exceeding limit: {violations}"
# ```

# ---
