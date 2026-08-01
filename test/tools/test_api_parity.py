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
