"""Contract for the source-fidelity ratchet — the permanent guard for the 'dropped field' class.

Every other quality gate in this repo checks whether an extracted value is CORRECT. None checks
whether a source field is being IGNORED — which is how 'Payment Currency' slipped by and USD/GBP
Enterprise Ireland payments landed in amount_eur as EUR (see feedback_source_fidelity_audit_method).

tools/audit_source_fidelity.py --check is the gate: it fails only when a source now drops a field
the committed baseline did not, i.e. a publisher added a column we capture nowhere. These tests
pin the ratchet's direction (new drop trips, accepted drop does not, captured field does not) with
pure data — no bronze corpus needed — plus a shape check on the committed baseline.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.audit_source_fidelity import BASELINE, new_drops


def test_new_dropped_field_trips_the_gate():
    base = {"pdfs.xlsx": ["Some_Old_Ignored_Col"]}
    current = {"pdfs.xlsx": ["Some_Old_Ignored_Col", "Payment Currency"]}
    assert new_drops(current, base) == {"pdfs.xlsx": ["Payment Currency"]}


def test_accepted_drop_does_not_trip():
    base = {"lobbying_csv_data.csv": ["Person primarily responsible for lobbying on this activity"]}
    current = {"lobbying_csv_data.csv": ["Person primarily responsible for lobbying on this activity"]}
    assert new_drops(current, base) == {}


def test_capturing_a_field_never_trips():
    # A field we STOP dropping (now persisted) must never fail the gate — one-way ratchet.
    base = {"src.csv": ["A", "B"]}
    current = {"src.csv": ["A"]}
    assert new_drops(current, base) == {}


def test_new_source_family_with_drops_trips():
    assert new_drops({"brandnew.csv": ["X"]}, {}) == {"brandnew.csv": ["X"]}


def test_baseline_file_exists_and_is_wellformed():
    assert BASELINE.exists(), "run: python tools/audit_source_fidelity.py --write-baseline"
    data = json.loads(BASELINE.read_text(encoding="utf-8"))
    assert isinstance(data, dict) and data
    for src, fields in data.items():
        assert src and isinstance(fields, list)
        assert all(isinstance(f, str) for f in fields)
