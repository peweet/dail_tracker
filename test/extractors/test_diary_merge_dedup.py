"""Unit tests for the diary cross-department merge dedup (extractors/diary_merge_depts.py).

The load-bearing risk here is DOUBLE-COUNTING Eamon Ryan: he held Transport AND
Climate/Environment at once and his diary is published in BOTH collections, so ~half of DECC
is byte-identical to Transport. The merge anti-joins on a NORMALISED surname key (not the raw
"Ryans"/"Ryan" filename guess) + date+time+subject. Zero coverage before — these pin both the
surname key and that the dedup actually drops cross-published rows while keeping unique ones.
"""

from __future__ import annotations

import json

import polars as pl
import pytest

from extractors.diary_merge_depts import _DEDUP_KEY, _ocr_entries, _surname_key


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ryans", "ryan"),  # possessive filename guess
        ("Ryan", "ryan"),  # canonical — must collapse to the same key
        ("O'Brien", "o'brien"),  # apostrophe kept (ASCII) — consistent both sides, so dedup still works
        ("McGrath", "mcgrath"),
        ("Ross", "ross"),  # len 4, trailing 's' is NOT a possessive → kept
        ("", ""),
        (None, ""),
    ],
)
def test_surname_key_collapses_possessive_variants(raw, expected):
    assert _surname_key(raw) == expected


def test_ryans_and_ryan_share_a_key():
    # the exact bug the normalised key fixes: an exact-string join would miss this pair
    assert _surname_key("Ryans") == _surname_key("Ryan")


def _row(minister, d, time, subj, dept):
    return {
        "entry_date": d,
        "time_slot": time,
        "subject": subj,
        "department": dept,
        "minister": minister,
        "source_pdf_url": "x",
        "ingested_date": d,
    }


def test_cross_published_ryan_entry_is_deduped():
    # Reproduce the anti-join the merge runs: a DECC row identical (on the surname key + date +
    # time + subject) to a kept TRANSPORT row must be dropped; a DECC-unique row must survive.
    keep = pl.DataFrame([_row("Ryans", "2021-03-01", "10:00", "Meeting with EirGrid", "TRANSPORT")])
    incoming = pl.DataFrame(
        [
            _row("Ryan", "2021-03-01", "10:00", "Meeting with EirGrid", "DECC"),  # cross-published dup
            _row("Ryan", "2021-03-02", "11:00", "Offshore wind briefing", "DECC"),  # DECC-unique
        ]
    )
    mk = pl.col("minister").map_elements(_surname_key, return_dtype=pl.Utf8).alias("_mk")
    keep_keys = keep.with_columns(mk).select(_DEDUP_KEY).unique()
    deduped = incoming.with_columns(mk).join(keep_keys, on=_DEDUP_KEY, how="anti").drop("_mk")
    subjects = set(deduped["subject"].to_list())
    assert "Meeting with EirGrid" not in subjects  # cross-published → dropped
    assert "Offshore wind briefing" in subjects  # unique → kept
    assert len(deduped) == 1


def test_none_then_named_minister_does_not_break_schema(tmp_path):
    # Regression: a leading minister=None (a generic denylisted file like "Minister_DFHERIS_Calendar")
    # followed by a real name must NOT make polars infer the column as Null and then fail to append
    # "O'Donovan" — the bug that SILENTLY floored the DFHERIS re-merge. infer_schema_length=None fixes it.
    recs = [
        {
            "entry_date": "2025-06-01",
            "time_slot": "10:00",
            "subject": "A",
            "department": "DFHERIS",
            "minister": None,
            "source_pdf_url": "x",
        },
        {
            "entry_date": "2024-09-01",
            "time_slot": "11:00",
            "subject": "B",
            "department": "DFHERIS",
            "minister": "O'Donovan",
            "source_pdf_url": "y",
        },
    ]
    path = tmp_path / "ocr.json"
    path.write_text(json.dumps(recs), encoding="utf-8")
    df = _ocr_entries(str(path))  # must not raise ComputeError
    assert df["minister"].to_list() == [None, "O'Donovan"]
    assert df.schema["minister"] == pl.Utf8
