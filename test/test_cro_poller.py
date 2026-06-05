"""Unit tests for cro_poller.py — the CRO bulk-register poller's safety rails.

No network. Covers the pure date/idempotency helpers and, crucially,
``extract_and_validate`` — the rail that must REJECT a bad download (wrong
schema, truncated, no CSV) by raising SourceDrift so it never overwrites a good
bronze snapshot. The network paths (resolve_resource / download_zip / main) are
exercised by the manual end-to-end run, not here.
"""

import datetime as dt
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import corporate.cro_poller as cp  # noqa: E402

HEADER = ",".join(sorted(cp.EXPECTED_COLUMNS))


def _zip_with(tmp_path: Path, *, member="companies.csv", body="", name="d.zip") -> Path:
    z = tmp_path / name
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr(member, body)
    return z


# ── date parsing ──────────────────────────────────────────────────────────────
def test_parse_ckan_date_iso_microseconds():
    assert cp._parse_ckan_date("2026-06-04T04:01:43.446029") == dt.date(2026, 6, 4)


def test_parse_ckan_date_date_only_fallback():
    assert cp._parse_ckan_date("2026-06-04 garbage trailing") == dt.date(2026, 6, 4)


def test_parse_ckan_date_none_is_today():
    assert cp._parse_ckan_date(None) == dt.date.today()


# ── idempotency: newest held snapshot ───────────────────────────────────────────
def test_latest_local_date_picks_newest(tmp_path, monkeypatch):
    d = tmp_path / "cro"
    d.mkdir()
    for fn in ("companies_20260504.csv", "companies_20260604.csv", "companies_nope.csv"):
        (d / fn).write_text("x", encoding="utf-8")
    monkeypatch.setattr(cp, "DEST_DIR", d)
    assert cp.latest_local_date() == dt.date(2026, 6, 4)


def test_latest_local_date_empty_is_none(tmp_path, monkeypatch):
    d = tmp_path / "cro"
    d.mkdir()
    monkeypatch.setattr(cp, "DEST_DIR", d)
    assert cp.latest_local_date() is None


def test_count_rows_excludes_header(tmp_path):
    p = tmp_path / "f.csv"
    p.write_text("header\na\nb\nc\n", encoding="utf-8")
    assert cp._count_rows(p) == 3


# ── extract_and_validate: the bronze-protection rail ───────────────────────────
def test_extract_validate_happy_path(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ROWS", 2)
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10)
    body = HEADER + "\n" + "r1\nr2\nr3\n"
    z = _zip_with(tmp_path, body=body)
    out = tmp_path / "out.csv"
    rows = cp.extract_and_validate(z, out)
    assert rows == 3
    assert out.exists()
    assert out.read_text(encoding="utf-8").startswith(HEADER)


def test_extract_validate_rejects_tiny_zip(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10_000_000)  # any real test zip is smaller
    z = _zip_with(tmp_path, body=HEADER + "\nr1\n")
    with pytest.raises(cp.SourceDrift, match="too small"):
        cp.extract_and_validate(z, tmp_path / "out.csv")


def test_extract_validate_rejects_no_csv_member(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10)
    z = _zip_with(tmp_path, member="readme.txt", body="not a csv")
    with pytest.raises(cp.SourceDrift, match="no .csv"):
        cp.extract_and_validate(z, tmp_path / "out.csv")


def test_extract_validate_rejects_schema_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10)
    # header missing the expected CRO columns
    z = _zip_with(tmp_path, body="foo,bar\n1,2\n")
    with pytest.raises(cp.SourceDrift, match="missing expected columns"):
        cp.extract_and_validate(z, tmp_path / "out.csv")


def test_extract_validate_rejects_row_floor(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10)
    monkeypatch.setattr(cp, "MIN_ROWS", 1_000)  # tiny csv can't meet it
    z = _zip_with(tmp_path, body=HEADER + "\nr1\nr2\n")
    out = tmp_path / "out.csv"
    with pytest.raises(cp.SourceDrift, match="row floor"):
        cp.extract_and_validate(z, out)
    assert not out.exists()  # bad download must NOT land in bronze


def test_extract_validate_failure_leaves_no_partial(tmp_path, monkeypatch):
    monkeypatch.setattr(cp, "MIN_ZIP_BYTES", 10)
    monkeypatch.setattr(cp, "MIN_ROWS", 1_000)
    z = _zip_with(tmp_path, body=HEADER + "\nr1\n")
    out = tmp_path / "out.csv"
    with pytest.raises(cp.SourceDrift):
        cp.extract_and_validate(z, out)
    assert not (out.with_suffix(".csv.partial")).exists()  # partial cleaned up


# ── resolve_resource: CKAN-structure-drift rail (no network, stubbed session) ──
class _FakeResp:
    def __init__(self, payload):
        self._p = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._p


class _FakeSession:
    def __init__(self, payload):
        self._p = payload

    def get(self, *a, **k):
        return _FakeResp(self._p)


def _pkg(resources):
    return {"success": True, "result": {"resources": resources}}


def test_resolve_resource_happy():
    s = _FakeSession(
        _pkg(
            [
                {
                    "format": "CSV",
                    "url": "https://x/companies.csv.zip",
                    "last_modified": "2026-06-04T04:01:43.446029",
                    "id": "r1",
                }
            ]
        )
    )
    res = cp.resolve_resource(s)
    assert res["url"].endswith("companies.csv.zip")
    assert res["last_modified"] == dt.date(2026, 6, 4)
    assert res["resource_id"] == "r1"


def test_resolve_resource_success_false_is_drift():
    s = _FakeSession({"success": False})
    with pytest.raises(cp.SourceDrift, match="success=false"):
        cp.resolve_resource(s)


def test_resolve_resource_no_csv_is_drift():
    s = _FakeSession(_pkg([{"format": "JSON", "url": "https://x/y.json", "id": "r9"}]))
    with pytest.raises(cp.SourceDrift, match="no CSV resource"):
        cp.resolve_resource(s)


def test_resolve_resource_csv_without_url_is_drift():
    s = _FakeSession(_pkg([{"format": "CSV", "url": "", "id": "r1"}]))
    with pytest.raises(cp.SourceDrift, match="no download url"):
        cp.resolve_resource(s)
