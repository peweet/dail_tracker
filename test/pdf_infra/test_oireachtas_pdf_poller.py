"""
Tests for oireachtas_pdf_poller.py — the discovery layer for payments,
attendance, and interests PDFs.

The poller is the single load-bearing component for new-PDF detection
across three sources. If it silently stops finding PDFs (selector drift,
filename hint mismatch, CMS redesign), the pipeline freezes without any
obvious error. These tests catch that regression in CI rather than
"three weeks later in production".

What's mocked:
  - HTTP via the `responses` library — no real network is hit.
  - The on-disk target_dir via pytest's `tmp_path` — no real PDFs touched.

What's NOT covered (deferred):
  - Live canary against oireachtas.ie. See test/HANDS_OFF_TEST_PLAN.md §3.4
    for the @pytest.mark.live shape to add when there's a scheduled
    weekly run wired up.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import json

from pdf_infra.oireachtas_pdf_poller import (
    FINGERPRINT_FILENAME,
    SOURCES,
    IndexEntry,
    PollSource,
    _exit_code,
    check_supersessions,
    download,
    fetch_index_html,
    filter_new,
    parse_index,
    run_one,
    write_supersession_log,
)

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "poller"
PAYMENTS_INDEX_FIXTURE = FIXTURES_DIR / "parliamentary-allowances.html"

# Smallest body that passes both the 10_000-byte min and the %PDF- magic-byte
# check. Used by the download + run_one tests.
_VALID_PDF_BODY = b"%PDF-1.4\n" + b"x" * 10_500

# The two valid URLs hard-coded in the payments fixture. Kept as module-level
# constants so the run_one tests don't drift from the fixture content.
_FEB_URL = (
    "https://data.oireachtas.ie/ie/oireachtas/parliamentaryBusiness/psa/"
    "2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-"
    "for-february-2026_en.pdf"
)
_JAN_URL = (
    "https://data.oireachtas.ie/ie/oireachtas/parliamentaryBusiness/psa/"
    "2026/2026-03-05_parliamentary-standard-allowance-payments-to-deputies-"
    "for-january-2026_en.pdf"
)
_FEB_FILENAME = "2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"


def _read_payments_fixture() -> str:
    return PAYMENTS_INDEX_FIXTURE.read_text(encoding="utf-8")


def _make_source(
    tmp_path: Path,
    *,
    name: str = "payments",
    topic: str = "parliamentary-allowances",
    hint: str = "parliamentary-standard-allowance-payments-to-deputies",
    file_types: frozenset = frozenset({"pdf"}),
) -> PollSource:
    """Build a test-scoped PollSource that writes inside tmp_path."""
    return PollSource(
        name=name,
        topic_slug=topic,
        target_dir=tmp_path / name,
        filename_hint=hint,
        allowed_file_types=file_types,
    )


def _entry(filename: str) -> IndexEntry:
    return IndexEntry(
        url=f"https://data.oireachtas.ie/{filename}",
        file_type="pdf",
        title=filename,
        pub_date_raw="1 Jan 2026",
        filename=filename,
    )


# ---------------------------------------------------------------------------
# _exit_code — pure unit, no fixtures
# ---------------------------------------------------------------------------


def test_exit_code_zero_when_all_sources_ok():
    results = {
        "payments": {"status": "ok", "downloads_failed": 0},
        "attendance": {"status": "ok", "downloads_failed": 0},
    }
    assert _exit_code(results) == 0


def test_exit_code_zero_when_nothing_new_downloaded():
    """status=ok with downloaded=0 is still success — nothing new published."""
    results = {"payments": {"status": "ok", "downloads_failed": 0, "downloaded": 0}}
    assert _exit_code(results) == 0


def test_exit_code_two_on_no_entries():
    """no_entries signals HTML/selector drift; needs human investigation."""
    results = {"payments": {"status": "no_entries", "downloads_failed": 0}}
    assert _exit_code(results) == 2


def test_exit_code_one_on_fetch_failure():
    results = {"payments": {"status": "fetch_failed", "downloads_failed": 0}}
    assert _exit_code(results) == 1


def test_exit_code_one_on_parse_failure():
    results = {"payments": {"status": "parse_failed", "downloads_failed": 0}}
    assert _exit_code(results) == 1


def test_exit_code_one_when_any_download_failed():
    results = {"payments": {"status": "ok", "downloads_failed": 1}}
    assert _exit_code(results) == 1


def test_exit_code_no_entries_takes_precedence_over_failed_downloads():
    # Rationale: selector drift is a higher-priority human signal than a
    # transient download failure; we must not mask it.
    results = {
        "payments": {"status": "no_entries", "downloads_failed": 0},
        "attendance": {"status": "ok", "downloads_failed": 3},
    }
    assert _exit_code(results) == 2


# ---------------------------------------------------------------------------
# parse_index — selector-drift canary
# ---------------------------------------------------------------------------


def test_parse_index_extracts_valid_payment_entries(tmp_path):
    """Most-recent valid entries must be parsed. Failure here = selector drift."""
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())

    titles = [e.title for e in entries]
    assert any("February 2026" in t for t in titles)
    assert any("January 2026" in t for t in titles)


def test_parse_index_filters_out_hint_mismatch(tmp_path):
    """The statistical-annex card has the right host + type but lacks the
    filename_hint — must be dropped.
    """
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())
    for e in entries:
        assert "statistical-annex" not in e.url


def test_parse_index_filters_out_wrong_file_type(tmp_path):
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())
    for e in entries:
        assert not e.url.endswith(".docx")
        assert e.file_type == "pdf"


def test_parse_index_filters_out_non_data_oireachtas_host(tmp_path):
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())
    for e in entries:
        assert "data.oireachtas.ie" in e.url


def test_parse_index_returns_exactly_two_valid_entries(tmp_path):
    """Fixture has 5 cards; 3 fail one of the filter rules. Net = 2."""
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())
    assert len(entries) == 2


def test_parse_index_extracts_url_title_date_filename(tmp_path):
    src = _make_source(tmp_path)
    entries = parse_index(src, _read_payments_fixture())

    february = next(e for e in entries if "February 2026" in e.title)
    assert february.url.startswith("https://data.oireachtas.ie/")
    assert february.file_type == "pdf"
    assert february.pub_date_raw == "2 April 2026"
    assert february.filename == _FEB_FILENAME


def test_parse_index_empty_html_returns_empty_list(tmp_path):
    src = _make_source(tmp_path)
    assert parse_index(src, "<html><body></body></html>") == []


def test_parse_index_returns_empty_when_no_cards_match(tmp_path):
    """Cards present but none match — returns []. This is the canary state
    that flips run_one to status='no_entries' and exit code 2.
    """
    html = """
    <html><body>
      <div class="c-publications-list__item">
        <p class="c-publications-list__view">
          <a href="https://example.com/something.pdf" data-file-type="pdf">View</a>
        </p>
      </div>
    </body></html>
    """
    src = _make_source(tmp_path)
    assert parse_index(src, html) == []


def test_parse_index_handles_card_without_link(tmp_path):
    """A card missing the view-link selector is skipped, not crashed."""
    html = """
    <html><body>
      <div class="c-publications-list__item">
        <p class="c-publications-list__title">No link card</p>
      </div>
    </body></html>
    """
    src = _make_source(tmp_path)
    assert parse_index(src, html) == []


@pytest.mark.parametrize(
    "source_name,expected_hint",
    [
        ("payments", "parliamentary-standard-allowance-payments-to-deputies"),
        ("attendance", "deputies-verification-of-attendance"),
        ("interests", "register-of-member"),
    ],
)
def test_sources_registry_has_expected_hints(source_name, expected_hint):
    """Guard against a refactor that mixes up the per-topic filename hints
    in the SOURCES dict — they're the only thing keeping each topic
    confined to its own set of entries.
    """
    assert SOURCES[source_name].filename_hint == expected_hint


# ---------------------------------------------------------------------------
# filter_new — on-disk filtering + .tmp sweep
# ---------------------------------------------------------------------------


def test_filter_new_keeps_entry_when_disk_empty(tmp_path):
    src = _make_source(tmp_path)
    entry = _entry("a.pdf")
    assert filter_new(src, [entry]) == [entry]


def test_filter_new_drops_entry_already_on_disk(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    (src.target_dir / "a.pdf").write_bytes(b"existing")
    assert filter_new(src, [_entry("a.pdf")]) == []


def test_filter_new_keeps_some_drops_others(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    (src.target_dir / "old.pdf").write_bytes(b"existing")

    new_entry = _entry("new.pdf")
    result = filter_new(src, [_entry("old.pdf"), new_entry])
    assert result == [new_entry]


def test_filter_new_creates_target_dir_if_missing(tmp_path):
    src = _make_source(tmp_path)
    assert not src.target_dir.exists()
    filter_new(src, [])
    assert src.target_dir.exists() and src.target_dir.is_dir()


def test_filter_new_sweeps_stale_tmp_files(tmp_path):
    """A previously-interrupted run leaves .tmp files. They must be removed
    before a fresh download stream reuses the name.
    """
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    stale = src.target_dir / "partial.pdf.tmp"
    stale.write_bytes(b"interrupted")

    filter_new(src, [])

    assert not stale.exists()


def test_filter_new_sweep_does_not_touch_real_files(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    keeper = src.target_dir / "real.pdf"
    keeper.write_bytes(b"real")

    filter_new(src, [])

    assert keeper.exists()


# ---------------------------------------------------------------------------
# download — atomic .tmp → final, with size + magic-byte checks
# ---------------------------------------------------------------------------


@responses.activate
def test_download_writes_atomic_file_for_valid_pdf(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("ok.pdf")
    responses.add(responses.GET, entry.url, body=_VALID_PDF_BODY, status=200)

    final = download(src, entry, requests.Session())

    assert final == src.target_dir / "ok.pdf"
    assert final.exists()
    assert final.read_bytes() == _VALID_PDF_BODY
    # No partial .tmp left behind after a successful write.
    assert not list(src.target_dir.glob("*.tmp"))


@responses.activate
def test_download_rejects_file_below_min_bytes(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("tiny.pdf")
    tiny = b"%PDF-1.4\n" + b"x" * 100  # well under 10_000
    responses.add(responses.GET, entry.url, body=tiny, status=200)

    with pytest.raises(ValueError, match="suspiciously small"):
        download(src, entry, requests.Session())

    assert not list(src.target_dir.glob("*.tmp"))
    assert not (src.target_dir / "tiny.pdf").exists()


@responses.activate
def test_download_rejects_non_pdf_magic_bytes(tmp_path):
    """An HTML error page mis-served with content-type pdf must fail the
    magic-byte check, not become a corrupt parquet downstream.
    """
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("html-disguised.pdf")
    html_body = b"<!doctype html>" + b"x" * 10_500  # large enough to pass size
    responses.add(responses.GET, entry.url, body=html_body, status=200)

    with pytest.raises(ValueError, match="not a PDF"):
        download(src, entry, requests.Session())

    assert not list(src.target_dir.glob("*.tmp"))


@responses.activate
def test_download_cleans_tmp_on_http_error(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("missing.pdf")
    responses.add(responses.GET, entry.url, status=500)

    with pytest.raises(requests.HTTPError):
        download(src, entry, requests.Session())

    assert not list(src.target_dir.glob("*.tmp"))


@responses.activate
def test_download_skips_magic_byte_check_for_non_pdf_types(tmp_path):
    """allowed_file_types is per-source — a docx source must not be rejected
    for not starting with %PDF-.
    """
    src = _make_source(tmp_path, file_types=frozenset({"docx"}))
    src.target_dir.mkdir(parents=True)
    entry = IndexEntry(
        url="https://data.oireachtas.ie/a.docx",
        file_type="docx",
        title="t",
        pub_date_raw="d",
        filename="a.docx",
    )
    docx_body = b"PK\x03\x04" + b"x" * 10_500  # zip header, not PDF
    responses.add(responses.GET, entry.url, body=docx_body, status=200)

    final = download(src, entry, requests.Session())

    assert final.read_bytes()[:4] == b"PK\x03\x04"


# ---------------------------------------------------------------------------
# fetch_index_html — UTF-8 enforcement
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_index_html_forces_utf8_decoding(tmp_path):
    """The site occasionally mis-declares encoding which would turn
    'Dáil Éireann' into mojibake. fetch_index_html overrides the
    response encoding to utf-8 — this guards that override.
    """
    src = _make_source(tmp_path)
    payload = "<html><body>Dáil Éireann</body></html>".encode()
    responses.add(
        responses.GET,
        src.index_url,
        body=payload,
        status=200,
        content_type="text/html; charset=iso-8859-1",  # intentionally wrong
    )

    html = fetch_index_html(src)

    assert "Dáil Éireann" in html


# ---------------------------------------------------------------------------
# run_one — orchestration, mocked end-to-end
# ---------------------------------------------------------------------------


@responses.activate
def test_run_one_fetch_failure_returns_fetch_failed_status(tmp_path):
    src = _make_source(tmp_path)
    responses.add(responses.GET, src.index_url, status=500)

    result = run_one(src)

    assert result["status"] == "fetch_failed"
    assert "error" in result
    assert result["downloaded"] == 0


@responses.activate
def test_run_one_zero_matching_entries_returns_no_entries(tmp_path):
    """The CMS-drift canary path: index fetches fine, parse returns 0.
    Maps to exit code 2.
    """
    src = _make_source(tmp_path)
    responses.add(responses.GET, src.index_url, body="<html></html>", status=200)

    result = run_one(src)

    assert result["status"] == "no_entries"
    assert result["downloaded"] == 0


@responses.activate
def test_run_one_downloads_new_entries_end_to_end(tmp_path):
    src = _make_source(tmp_path)
    responses.add(responses.GET, src.index_url, body=_read_payments_fixture(), status=200)
    responses.add(responses.GET, _FEB_URL, body=_VALID_PDF_BODY, status=200)
    responses.add(responses.GET, _JAN_URL, body=_VALID_PDF_BODY, status=200)

    result = run_one(src)

    assert result["status"] == "ok"
    assert result["scanned"] == 2  # 5 cards in fixture, 3 filtered before this stage
    assert result["already_on_disk"] == 0
    assert result["downloaded"] == 2
    assert result["downloads_failed"] == 0

    pdfs = sorted(p.name for p in src.target_dir.glob("*.pdf"))
    assert len(pdfs) == 2
    assert all("payments-to-deputies" in name for name in pdfs)


@responses.activate
def test_run_one_skips_entries_already_on_disk(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    (src.target_dir / _FEB_FILENAME).write_bytes(b"already here")

    responses.add(responses.GET, src.index_url, body=_read_payments_fixture(), status=200)
    responses.add(responses.GET, _JAN_URL, body=_VALID_PDF_BODY, status=200)

    result = run_one(src)

    assert result["status"] == "ok"
    assert result["already_on_disk"] == 1
    assert result["downloaded"] == 1
    # Pre-existing file is left untouched.
    assert (src.target_dir / _FEB_FILENAME).read_bytes() == b"already here"


# ---------------------------------------------------------------------------
# check_supersessions — DAIL-162: same filename, changed bytes at source
# ---------------------------------------------------------------------------


def test_supersession_baselines_unseen_file_without_network(tmp_path):
    """First time we fingerprint an on-disk file: record a baseline, NO HEAD,
    NO supersession. (Comparison only starts on the next run.) No @responses.activate
    here proves the baseline path makes zero network calls."""
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    (src.target_dir / "a.pdf").write_bytes(_VALID_PDF_BODY)
    index: dict = {}

    superseded = check_supersessions(src, [_entry("a.pdf")], requests.Session(), index)

    assert superseded == []
    assert index["a.pdf"]["bytes"] == len(_VALID_PDF_BODY)
    assert "sha256" in index["a.pdf"]


@responses.activate
def test_supersession_flagged_when_remote_size_changed(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("a.pdf")
    (src.target_dir / "a.pdf").write_bytes(b"x" * 100)
    index = {"a.pdf": {"sha256": "old", "bytes": 100, "source_url": entry.url}}
    responses.add(responses.HEAD, entry.url, headers={"Content-Length": "250"}, status=200)

    superseded = check_supersessions(src, [entry], requests.Session(), index)

    assert len(superseded) == 1
    assert superseded[0]["filename"] == "a.pdf"
    assert superseded[0]["held_bytes"] == 100
    assert superseded[0]["remote_bytes"] == 250


@responses.activate
def test_supersession_silent_when_remote_size_unchanged(tmp_path):
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("a.pdf")
    index = {"a.pdf": {"sha256": "x", "bytes": 100, "source_url": entry.url}}
    responses.add(responses.HEAD, entry.url, headers={"Content-Length": "100"}, status=200)

    assert check_supersessions(src, [entry], requests.Session(), index) == []


@responses.activate
def test_supersession_unknown_when_head_refused(tmp_path):
    """Server that 405s on HEAD -> size unknown -> conservative: NO false alarm."""
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    entry = _entry("a.pdf")
    index = {"a.pdf": {"sha256": "x", "bytes": 100, "source_url": entry.url}}
    responses.add(responses.HEAD, entry.url, status=405)

    assert check_supersessions(src, [entry], requests.Session(), index) == []


@responses.activate
def test_run_one_flags_supersession_for_held_file(tmp_path):
    """End-to-end: the held February file has a baseline whose size differs from the
    server's HEAD -> run_one surfaces it under result['superseded'], while still
    downloading the genuinely new January file."""
    src = _make_source(tmp_path)
    src.target_dir.mkdir(parents=True)
    (src.target_dir / _FEB_FILENAME).write_bytes(b"y" * 500)
    # Pre-seed a baseline for FEB with a DIFFERENT size than the server will report.
    fp = src.target_dir / FINGERPRINT_FILENAME
    fp.write_text(
        json.dumps({_FEB_FILENAME: {"sha256": "old", "bytes": 500, "source_url": _FEB_URL}}), encoding="utf-8"
    )

    responses.add(responses.GET, src.index_url, body=_read_payments_fixture(), status=200)
    responses.add(responses.HEAD, _FEB_URL, headers={"Content-Length": "9999"}, status=200)
    responses.add(responses.GET, _JAN_URL, body=_VALID_PDF_BODY, status=200)

    result = run_one(src)

    assert result["status"] == "ok"
    assert result["already_on_disk"] == 1
    assert result["downloaded"] == 1
    assert [s["filename"] for s in result["superseded"]] == [_FEB_FILENAME]


def test_write_supersession_log_writes_only_when_findings(tmp_path):
    out = tmp_path / "_meta" / "supersessions.json"
    # Clean run: nothing written, count 0.
    assert write_supersession_log({"payments": {"superseded": []}}, path=out) == 0
    assert not out.exists()
    # With a finding: file written, payload carries the detection.
    findings = {"payments": {"superseded": [{"filename": "a.pdf", "held_bytes": 1, "remote_bytes": 2}]}}
    assert write_supersession_log(findings, path=out) == 1
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["count"] == 1
    assert payload["detected"][0]["filename"] == "a.pdf"


@responses.activate
def test_run_one_counts_partial_download_failures(tmp_path):
    """If one PDF download 500s mid-batch the other still succeeds, and the
    failure count is reflected — exit_code will then return 1.
    """
    src = _make_source(tmp_path)
    responses.add(responses.GET, src.index_url, body=_read_payments_fixture(), status=200)
    responses.add(responses.GET, _FEB_URL, body=_VALID_PDF_BODY, status=200)
    responses.add(responses.GET, _JAN_URL, status=500)

    result = run_one(src)

    assert result["status"] == "ok"
    assert result["downloaded"] == 1
    assert result["downloads_failed"] == 1
