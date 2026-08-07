"""Runs the API-parity anti-drift ratchet (tools/migration/check_api_parity.py).

Wired 2026-08-01: the checker existed since the API-parity audit but had NO
automated consumer (the deptry wiring-gap pattern) — and on first wiring it had
already accumulated 5 undetected drifted functions in under two weeks, which
were absorbed into the baseline deliberately (visible in that diff). This
wrapper makes drift #6 a CI failure instead of a silent regression.

Baseline: tools/baselines/api_parity_baseline.txt — never edited by hand;
shrink it by adding a router, or regenerate with --update-baseline in a diff
that says why.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.migration import check_api_parity


def test_api_parity_ratchet_holds(capsys, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["check_api_parity.py"])
    rc = check_api_parity.main()
    out = capsys.readouterr()
    assert rc == 0, f"API parity drift:\n{out.out}\n{out.err}"


def test_analyse_resolves_nested_reexports_without_name_collisions(tmp_path, monkeypatch):
    queries = tmp_path / "queries"
    nested = queries / "nested"
    nested.mkdir(parents=True)
    (queries / "__init__.py").write_text("", encoding="utf-8")
    (queries / "alpha.py").write_text("def shared():\n    return 'alpha'\n", encoding="utf-8")
    (nested / "beta.py").write_text("def shared():\n    return 'beta'\n", encoding="utf-8")
    (nested / "__init__.py").write_text(
        "from dail_tracker_core.queries.nested.beta import shared\n",
        encoding="utf-8",
    )
    consumer = tmp_path / "consumer.py"
    consumer.write_text(
        "from dail_tracker_core.queries import nested as query\nresult = query.shared()\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(check_api_parity, "QUERIES_DIR", queries)
    monkeypatch.setattr(check_api_parity, "CONSUMER_PATHS", [consumer])

    defined, consumed, unexposed = check_api_parity.analyse()

    assert ("nested/beta", "shared", 1) in defined
    assert ("nested/beta", "shared") in consumed
    assert ("alpha", "shared", 1) in unexposed
    assert ("nested/beta", "shared", 1) not in unexposed


def test_main_reports_query_parse_errors_and_fails_closed(tmp_path, capsys, monkeypatch):
    queries = tmp_path / "queries"
    queries.mkdir()
    (queries / "broken.py").write_text("def incomplete(:\n", encoding="utf-8")
    consumer = tmp_path / "consumer.py"
    consumer.write_text("", encoding="utf-8")
    monkeypatch.setattr(check_api_parity, "QUERIES_DIR", queries)
    monkeypatch.setattr(check_api_parity, "CONSUMER_PATHS", [consumer])
    monkeypatch.setattr(check_api_parity, "BASELINE", tmp_path / "baseline.txt")
    monkeypatch.setattr(sys, "argv", ["check_api_parity.py"])

    assert check_api_parity.main() == 1
    captured = capsys.readouterr()
    assert "broken.py" in captured.err
    assert "failed closed" in captured.err


def test_parse_module_reports_decode_errors(tmp_path):
    broken = tmp_path / "broken.py"
    broken.write_bytes(b"# coding: utf-8\nvalue = '\xff'\n")

    try:
        check_api_parity.parse_module(broken)
    except check_api_parity.AnalysisError as exc:
        assert exc.path == broken
    else:
        raise AssertionError("invalid source encoding was silently accepted")


def test_private_publicsignal_composition_is_explicitly_out_of_scope():
    assert check_api_parity.module_is_excluded("procurement/opportunities")
    assert not check_api_parity.module_is_excluded("procurement/awards")
