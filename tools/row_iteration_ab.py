"""Deterministic A/B for every candidate in tools/row_iteration_runbook.yaml.

CORRECTNESS GATES TIMING. Each probe's baseline and candidate must produce BYTE-IDENTICAL output —
compared as Arrow IPC bytes for polars results, not `==` — before a single duration is reported.
A fast wrong answer is a failure and exits non-zero. Equality is re-asserted on EVERY timed repeat,
not once up front, so a candidate cannot pass the gate and then drift.

Arrow IPC bytes rather than `DataFrame.equals` on purpose: `equals` can pass while dtype or
null-mask differ, and both of those reach disk.

Anti-bias measures, each of which this project has been burned by:
  - execution order alternated every repeat (a fixed A-then-B order measures the order)
  - median of N, never a single run
  - fixtures built ONCE outside the timed region, and shared by both sides so neither gets free work
  - the baseline replicates the incumbent verbatim, expensive steps included

    python tools/row_iteration_ab.py
    python tools/row_iteration_ab.py --id legal_diary_parties_unique
    python tools/row_iteration_ab.py --json
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
import pyarrow as pa
import yaml

ROOT = Path(__file__).resolve().parents[1]
RUNBOOK = ROOT / "tools/row_iteration_runbook.yaml"


def _identity_bytes(value: Any) -> bytes:
    """Deterministic byte image of a result, for equality that dtype changes cannot slip past."""
    if isinstance(value, pl.Series):
        value = value.to_frame()
    if isinstance(value, pl.DataFrame):
        # rechunk() FIRST: Arrow IPC encodes chunk boundaries, and a join returns a fragmented
        # frame (measured: a 1-chunk baseline vs a 20-chunk join result holding IDENTICAL values).
        # Comparing raw IPC bytes therefore fails on in-memory fragmentation, which never reaches
        # disk — parquet re-chunks by row group on write. Normalising it here keeps the check
        # honest about everything that DOES reach disk: values, dtypes, null masks, column names
        # and ROW ORDER are all still compared byte-for-byte, so a reordering join still fails.
        table = value.rechunk().to_arrow()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()
    # Non-frame results (lists of tuples/objects): pickle is deterministic within one process for
    # identical values and types, and unlike repr it does not collapse int/float distinctions.
    return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)


def run_candidate(entry: dict, repeats: int) -> dict:
    from tools.row_iteration_probes import PROBES

    identifier = entry["id"]
    factory = PROBES.get(identifier)
    if factory is None:
        return {"id": identifier, "status": "no-probe", "detail": "no probe registered for this id"}
    try:
        probe = factory()
    except FileNotFoundError as exc:
        return {"id": identifier, "status": "skipped", "detail": str(exc)}

    expected = _identity_bytes(probe.baseline())
    actual = _identity_bytes(probe.candidate())
    if expected != actual:
        return {
            "id": identifier,
            "status": "BYTE-MISMATCH",
            "rows": probe.rows,
            "detail": f"baseline {len(expected)}B vs candidate {len(actual)}B — output differs",
        }

    baseline_times: list[float] = []
    candidate_times: list[float] = []
    for index in range(repeats):
        pairs = (
            ((probe.baseline, baseline_times), (probe.candidate, candidate_times))
            if index % 2 == 0
            else ((probe.candidate, candidate_times), (probe.baseline, baseline_times))
        )
        for function, sink in pairs:
            start = time.perf_counter()
            result = function()
            sink.append(time.perf_counter() - start)
            # Re-assert on every repeat: passing once then drifting must not read as a win.
            if _identity_bytes(result) != expected:
                return {"id": identifier, "status": "BYTE-MISMATCH", "detail": f"drifted on repeat {index}"}

    baseline_ms = float(np.median(baseline_times)) * 1000
    candidate_ms = float(np.median(candidate_times)) * 1000
    speedup = baseline_ms / candidate_ms if candidate_ms else float("inf")
    return {
        "id": identifier,
        "status": "REGRESSION" if speedup < 1.0 else "ok",
        "rows": probe.rows,
        "baseline_ms": round(baseline_ms, 1),
        "candidate_ms": round(candidate_ms, 1),
        "speedup": round(speedup, 2),
        "tier": entry.get("tier"),
        "describes": probe.describes,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", help="run one candidate id")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    runbook = yaml.safe_load(RUNBOOK.read_text(encoding="utf-8"))
    repeats = int(runbook["benchmark"]["repeats"])
    entries = [c for c in runbook["candidates"] if not args.id or c["id"] == args.id]
    if not entries:
        raise SystemExit(f"no runbook candidate matches id={args.id!r}")

    results = [run_candidate(entry, repeats) for entry in entries]
    if args.json:
        json.dump(results, sys.stdout, indent=2)
        return

    print(f"row-iteration A/B — threshold {runbook['policy']['row_threshold']:,} rows, median of {repeats}\n")
    header = f"{'candidate':<32} {'rows':>9} {'baseline':>10} {'candidate':>10} {'speedup':>8}  status"
    print(header)
    print("-" * len(header))
    for row in results:
        # Any status without timings (skipped, no-probe, BYTE-MISMATCH) prints the reason instead.
        # A mismatch has no meaningful duration to show — and must never be rendered as a result.
        if "baseline_ms" not in row:
            detail = str(row.get("detail", ""))[:44]
            print(f"{row['id']:<32} {'-':>9} {'-':>10} {'-':>10} {'-':>8}  {row['status']}: {detail}")
            continue
        print(
            f"{row['id']:<32} {row['rows']:>9,} {row['baseline_ms']:>9.1f}ms {row['candidate_ms']:>9.1f}ms "
            f"{row['speedup']:>7.2f}x  {row['status']}"
        )

    bad = [r for r in results if r["status"] in {"BYTE-MISMATCH", "REGRESSION"}]
    graded = [r for r in results if r["status"] in {"ok", "BYTE-MISMATCH", "REGRESSION"}]
    print()
    if bad:
        for row in bad:
            print(f"  FAIL {row['id']}: {row['status']} {row.get('detail', '')}")
    elif not graded:
        print("  ⚠ NOTHING WAS GRADED — a run with no comparisons is a broken harness, not a pass")
    else:
        print(f"  all {len(graded)} candidate(s) byte-identical, no regressions")
    raise SystemExit(1 if bad or not graded else 0)


if __name__ == "__main__":
    main()
