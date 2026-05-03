"""
pipeline_sandbox/pydantic_manifest_example.py

Evaluation example for the pydantic-at-boundaries strategy discussed in
doc/dail_tracker_improvements_v4.md §6.5. Targets one real boundary in the
repo: data/manifests/manifest.json — a JSON list of run-manifest dicts
written by manifest.py at the end of every pipeline run.

The example is deliberately self-contained: pure stdlib + pydantic v2.
Run it directly:

    python pipeline_sandbox/pydantic_manifest_example.py

It demonstrates four things, in order:

  1. PERMISSIVE load of the real file — what production would do.
     Coerces, accepts partial entries, prints a summary.
  2. STRICT load of the same file — what a CI test would do.
     Surfaces two real producer-side bugs in manifest.py:64 (booleans
     and timedeltas being stringified before being written).
  3. Bad-fixture validation — shows the ValidationError field path you
     get when a typo or schema drift slips into a manifest.
  4. Round-trip — proves model_dump round-trips a permissive entry
     without silently dropping fields (the most common pydantic footgun:
     a misnamed field on the model causes the on-disk value to vanish).

A pytest-shaped test_* function at the bottom shows how this would
slot into test/ once the strategy is adopted.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = REPO_ROOT / "data" / "manifests" / "manifest.json"


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class RunManifestPermissive(BaseModel):
    """What a production loader would use.

    extra='ignore' so unexpected new fields don't break the pipeline mid-run.
    Optional fields with defaults so historical entries (which often only
    have run_id + started_at — the run aborted before manifest_finalise ran)
    still validate.
    """

    model_config = ConfigDict(extra="ignore")

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    time_to_run: str | None = None  # accept the on-disk str(timedelta) shape
    endpoints_ok: bool | None = None  # pydantic coerces "True" -> True


class RunManifestStrict(BaseModel):
    """What a CI test would use against a fixture we control.

    extra='forbid' catches misspellings ('endpoint_ok' vs 'endpoints_ok').
    Per-field strict on bool / timedelta catches stringified values written
    by a sloppy producer; datetime stays loose because ISO strings on the
    wire are standard JSON practice, not a bug.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    time_to_run: timedelta | None = Field(default=None, strict=True)
    endpoints_ok: bool | None = Field(default=None, strict=True)


# ---------------------------------------------------------------------------
# Demonstrations
# ---------------------------------------------------------------------------


def demo_permissive_load() -> list[RunManifestPermissive]:
    raw = json.loads(MANIFEST_PATH.read_text())
    entries = [RunManifestPermissive.model_validate(r) for r in raw]

    total = len(entries)
    finished = sum(e.finished_at is not None for e in entries)
    with_endpoints = sum(e.endpoints_ok is not None for e in entries)
    print("[1] Permissive load")
    print(f"    file:           {MANIFEST_PATH.relative_to(REPO_ROOT)}")
    print(f"    total entries:  {total}")
    print(f"    finished:       {finished} ({total - finished} aborted before finalise)")
    print(f"    endpoints_ok:   {with_endpoints} entries reported (rest are pre-section-5.4)")
    return entries


def demo_strict_load() -> None:
    raw = json.loads(MANIFEST_PATH.read_text())
    failures: list[tuple[int, list[str]]] = []
    field_counts: dict[str, int] = {}
    for i, entry in enumerate(raw):
        try:
            RunManifestStrict.model_validate(entry)
        except ValidationError as e:
            field_msgs: list[str] = []
            for err in e.errors():
                loc = ".".join(str(p) for p in err["loc"])
                field_counts[loc] = field_counts.get(loc, 0) + 1
                field_msgs.append(f"{loc} ({err['type']}, input={err.get('input')!r})")
            failures.append((i, field_msgs))

    print("\n[2] Strict load (the same file, with per-field strict + forbid)")
    if not failures:
        print("    no producer-side bugs found")
        return
    print(f"    {len(failures)} of {len(raw)} entries fail strict validation")
    print(f"    field-level error counts: {field_counts}")
    print("    sample (first failing entry, all errors):")
    idx, msgs = failures[0]
    for msg in msgs:
        print(f"      entry[{idx}]: {msg}")
    print("    -> root cause: manifest.py:64 stringifies bool and timedelta")
    print("       before json-dumping. Strict pydantic points at the exact")
    print("       field; pandera would not apply (manifest is a JSON")
    print("       list-of-dicts, not a tabular frame).")


def demo_bad_fixture() -> None:
    bad = {
        "run_id": "fixture-bad-001",
        "started_at": "2026-05-03T11:00:00+00:00",
        "endpoint_ok": True,  # typo: should be endpoints_ok
        "rows_extracted": "lots",  # nonsense extra field
    }
    print("\n[3] Bad fixture (typo'd key + junk extra) against strict model")
    try:
        RunManifestStrict.model_validate(bad)
    except ValidationError as e:
        for err in e.errors():
            loc = ".".join(str(p) for p in err["loc"])
            print(f"    {loc}: {err['msg']}  (input={err.get('input')!r})")
        print("    -> the field path tells you exactly what to fix in the producer.")


def demo_round_trip(entries: list[RunManifestPermissive]) -> None:
    raw = json.loads(MANIFEST_PATH.read_text())
    sample_idx = next(i for i, r in enumerate(raw) if "endpoints_ok" in r)
    on_disk = raw[sample_idx]
    parsed = entries[sample_idx]
    redumped = parsed.model_dump(mode="json", exclude_none=True)

    # endpoints_ok was "True" on disk; the model normalised it to True.
    # That is the *kind* of normalisation we want — but the round-trip test
    # below catches the case where a model accidentally drops a field.
    print("\n[4] Round-trip (permissive)")
    print(f"    on-disk keys:   {sorted(on_disk.keys())}")
    print(f"    re-dumped keys: {sorted(redumped.keys())}")
    missing = set(on_disk) - set(redumped)
    print(f"    fields lost:    {missing or 'none'}")
    print(f"    endpoints_ok normalised: {on_disk['endpoints_ok']!r} -> {redumped['endpoints_ok']!r}")


def _short_error(e: ValidationError) -> str:
    err = e.errors()[0]
    loc = ".".join(str(p) for p in err["loc"])
    return f"{loc} ({err['type']}, input={err.get('input')!r})"


# ---------------------------------------------------------------------------
# Pytest-shaped test (illustrative — not collected from this folder)
# ---------------------------------------------------------------------------


def test_real_manifest_validates_permissively() -> None:
    """Shape this would take in test/test_manifest_schema.py."""
    raw = json.loads(MANIFEST_PATH.read_text())
    for i, entry in enumerate(raw):
        try:
            RunManifestPermissive.model_validate(entry)
        except ValidationError as e:
            raise AssertionError(f"entry[{i}] failed permissive load: {e}") from e


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    print("=" * 72)
    print("pydantic-at-boundaries: manifest.json evaluation")
    print("=" * 72)
    entries = demo_permissive_load()
    demo_strict_load()
    demo_bad_fixture()
    demo_round_trip(entries)
    print("\nSee pipeline_sandbox/pydantic_manifest_example_findings.md for the writeup.")
