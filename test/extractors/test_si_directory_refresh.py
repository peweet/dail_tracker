"""A failed eISB refresh must never publish empty or partial legal-state coverage."""

import json
import sys

import polars as pl
import pytest

from extractors import si_legislation_directory_extract as directory
from services.coverage_io import save_coverage
from services.parquet_io import save_parquet

ERROR_PAGE = "<html><title>403 Forbidden</title><body>Access denied</body></html>"
INDEX = """<html><title>Irish Statute Book</title><p>Updated to 24 August 2026</p>
<a href="si2025_1-50.html">1-50</a><a href="si2025_51-100.html">51-100</a></html>"""


def test_index_validation_accepts_official_2000_series_supplement_without_allowing_primary_gaps():
    complete = """<html><p>Updated to 5 September 2026</p>
    <a href="si2024_1-50.html">1-50</a>
    <a href="si2024_51-100.html">51-100</a>
    <a href="si2024_101-150.html">101-150</a>
    <a href="si2024_2001-2004.html">2001-2004</a></html>"""
    missing_primary_page = complete.replace('<a href="si2024_51-100.html">51-100</a>', "")

    assert directory._valid_directory_html(complete, f"{directory.BASE}/si2024.html")
    assert not directory._valid_directory_html(missing_primary_page, f"{directory.BASE}/si2024.html")
    assert not directory._valid_directory_html(complete.replace("si2024", "si2025"), f"{directory.BASE}/si2025.html")
    assert not directory._valid_directory_html(
        complete.replace("2001-2004", "2001-2005"), f"{directory.BASE}/si2024.html"
    )


def table(number=1, how="Not affected"):
    return f"""<html><table><tr><th>No.</th><th>Title</th><th>How Affected</th>
<th>Affecting Provision</th></tr><tr><td>{number}</td><td>Example rules</td>
<td>{how}</td><td></td></tr></table></html>"""


@pytest.fixture()
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(directory, "CACHE_DIR", tmp_path / "cache")
    monkeypatch.setattr(directory, "OUT_PARQUET", tmp_path / "state.parquet")
    monkeypatch.setattr(directory, "OUT_COVERAGE", tmp_path / "coverage.json")
    monkeypatch.setattr(directory, "GOLD", tmp_path / "gold.parquet")
    monkeypatch.setattr(directory.time, "sleep", lambda _: None)
    directory.CACHE_DIR.mkdir()
    return tmp_path


def seed_state():
    prior = pl.DataFrame(directory.parse_table(table(), 2025, f"{directory.BASE}/si2025_1-50.html", "1 July 2026"))
    save_parquet(prior, directory.OUT_PARQUET)
    save_parquet(
        pl.DataFrame({"si_id": ["2025-001"], "si_year": [2025], "si_number": [1], "si_title": ["Example rules"]}),
        directory.GOLD,
    )
    save_coverage({"directory_pages_updated_to": {"2025": "1 July 2026"}}, directory.OUT_COVERAGE)
    return prior


def test_invalid_cached_page_is_refetched_and_validated(isolated, monkeypatch):
    path = directory.CACHE_DIR / "si2025_1-50.html"
    path.write_text(ERROR_PAGE + " " * 600, encoding="utf-8")
    calls = []

    def download(url, **kwargs):
        calls.append(url)
        assert not kwargs["validate"](ERROR_PAGE.encode())
        assert kwargs["validate"](table().encode())
        return table().encode()

    monkeypatch.setattr(directory, "http_fetch_bytes", download)
    assert directory.fetch(f"{directory.BASE}/{path.name}", path.name) == table()
    assert len(calls) == 1
    assert path.read_text(encoding="utf-8") == table()


def test_failed_forced_fetch_preserves_good_cache(isolated, monkeypatch):
    path = directory.CACHE_DIR / "si2025_1-50.html"
    path.write_text(table(), encoding="utf-8")
    monkeypatch.setattr(directory, "http_fetch_bytes", lambda *args, **kwargs: ERROR_PAGE.encode())
    with pytest.raises(RuntimeError):
        directory.fetch(f"{directory.BASE}/{path.name}", path.name, force=True)
    assert path.read_text(encoding="utf-8") == table()


@pytest.mark.parametrize("cached", [False, True])
def test_offline_missing_or_invalid_page_never_fetches(isolated, monkeypatch, cached):
    if cached:
        (directory.CACHE_DIR / "si2025_index.html").write_text(ERROR_PAGE, encoding="utf-8")
    monkeypatch.setattr(directory, "http_fetch_bytes", lambda *a, **kw: pytest.fail("offline attempted HTTP"))
    with pytest.raises(RuntimeError, match="cache"):
        directory.range_urls(2025, offline=True)


@pytest.mark.parametrize("failure", ["index_error", "empty_page", "fetch_failure"])
def test_failed_refresh_preserves_parquet_and_freshness_receipt(isolated, monkeypatch, failure):
    seed_state()
    before_data = directory.OUT_PARQUET.read_bytes()
    before_coverage = directory.OUT_COVERAGE.read_bytes()

    def download(url, **kwargs):
        if url.endswith("si2025.html"):
            return (ERROR_PAGE if failure == "index_error" else INDEX).encode()
        if url.endswith("_1-50.html"):
            return table(how="Amended").encode()
        return ERROR_PAGE.encode() if failure == "empty_page" else None

    monkeypatch.setattr(directory, "http_fetch_bytes", download)
    monkeypatch.setattr(sys, "argv", ["si_directory", "--year", "2025"])
    with pytest.raises(RuntimeError):
        directory.main()
    assert directory.OUT_PARQUET.read_bytes() == before_data
    assert directory.OUT_COVERAGE.read_bytes() == before_coverage


def test_refresh_rejects_lost_existing_ids_even_when_row_count_is_unchanged(isolated):
    seed_state()
    replacement = pl.DataFrame(directory.parse_table(table(2), 2025, "official source", "24 August 2026"))
    with pytest.raises(RuntimeError, match="previously retained"):
        directory._merge_years(replacement, [2025])


def test_successful_refresh_updates_state_and_receipt_together(isolated, monkeypatch):
    seed_state()
    monkeypatch.setattr(
        directory,
        "http_fetch_bytes",
        lambda url, **kw: (
            INDEX if url.endswith("si2025.html") else table(51 if "_51-100" in url else 1, "Amended")
        ).encode(),
    )
    monkeypatch.setattr(sys, "argv", ["si_directory", "--year", "2025"])
    directory.main()
    result = pl.read_parquet(directory.OUT_PARQUET)
    assert result["si_id"].to_list() == ["2025-001", "2025-051"]
    assert result["current_state"].to_list() == ["amended", "amended"]
    receipt = json.loads(directory.OUT_COVERAGE.read_text(encoding="utf-8"))
    assert receipt["directory_pages_updated_to"] == {"2025": "24 August 2026"}
    assert receipt["gold_join_coverage_pct"] == 100.0
