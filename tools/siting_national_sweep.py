"""National sweep — run the full report pipeline at real points in every planning authority.

WHY THIS EXISTS (2026-07-27). The unit suite pins behaviour at a handful of benchmark points, and
`tools/siting_report_dryrun.py` pins the deliverable text at six. Neither answers "does this still
work everywhere": a change that reads a new layer can be correct in Galway and silently empty in
Leitrim, because layer coverage is per-council and the engine is designed to stay quiet where a
layer is not held. The per-council smoke of 2026-07-26 found exactly that class of defect — rural
Limerick resolving to council=None — and it found it by sweeping, not by unit test.

So this walks N real application points in each of the 31 authorities, runs
evaluate -> build_report -> render_markdown -> render_json, and records what came out plus how long
it took. It asserts nothing on its own. It writes a baseline JSON, and on a later run it DIFFS
against that baseline and reports what moved. That is the regression signal: not "did it crash",
but "did this council's report change shape without anyone intending it".

    .venv/Scripts/python tools/siting_national_sweep.py                 # sweep + diff vs baseline
    .venv/Scripts/python tools/siting_national_sweep.py --save-baseline # adopt current as baseline
    .venv/Scripts/python tools/siting_national_sweep.py --points 3      # lighter run

Memory: the point-scoped store is loaded ONCE and shared across every point (the design recorded in
memory project_siting_point_scoped_store_shipped_2026_07_23), so cost is flat in the number of
points. Private bytes are sampled, never working set — Windows trims the latter and it reads ~30 MB
while the process commits 700 (memory project_oom_root_cause_blas_threads_2026_07_26).
"""

from __future__ import annotations

# runtime_env caps the BLAS thread count and MUST stay the first import — uncapped, `import pandas`
# reserves ~650 MB of commit per process on this box. Do not let an import sorter move it.
import services.runtime_env  # noqa: F401  isort:skip

import argparse
import json
import statistics
import sys
import time
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE = REPO_ROOT / "data" / "_meta" / "siting_national_sweep_baseline.json"

# Fields compared against the baseline. Deliberately SHAPE, not prose: exact markdown is already
# pinned by the dry-run gate, and comparing it here would make every wording tweak a false alarm.
COMPARED = ("council", "fired", "context_facts", "missing_layers", "exclusions", "not_evaluated")


def _slug_points(n_per_council: int) -> dict[str, list[tuple[float, float]]]:
    """slug -> up to n real application points, chosen deterministically.

    Points come from the in-house applications register, so they are places people actually applied
    to build — a far better test set than a grid, which would put most of its points in fields and
    lakes and spend the run in the plausibility gate.
    """
    import polars as pl

    from dail_tracker_core.siting.council import SILVER, authority_to_slug

    if not SILVER.exists():
        print(f"applications register absent at {SILVER} — cannot sweep", file=sys.stderr)
        return {}
    # scan + select: the register is 495k rows and reading it whole is a needless 200 MB
    df = (
        pl.scan_parquet(SILVER)
        .select("lon", "lat", "PlanningAuthority")
        .filter(pl.col("lon").is_not_null() & pl.col("lat").is_not_null())
        .collect()
    )
    out: dict[str, list[tuple[float, float]]] = {}
    # sort first so the sample is the same on every run — an unstable sample would make the diff
    # meaningless (every run would "change")
    for row in df.sort("PlanningAuthority", "lon", "lat").iter_rows(named=True):
        slug = authority_to_slug(row["PlanningAuthority"])
        if not slug:
            continue
        pts = out.setdefault(slug, [])
        if len(pts) < n_per_council:
            pts.append((float(row["lon"]), float(row["lat"])))
    return out


def _private_mb() -> float | None:
    """This process's PRIVATE BYTES in MB, or None off Windows.

    PrivateUsage from PROCESS_MEMORY_COUNTERS_EX — deliberately not WorkingSetSize. Windows trims
    the working set, so it reads ~30 MB on a process that has committed 700; measuring it is how
    the OOM investigation of 2026-07-26 was misled for a full session. psutil is not a declared
    dependency here, so this uses ctypes rather than adding one for a measurement.
    """
    if sys.platform != "win32":
        return None
    import ctypes

    class _Counters(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    from ctypes import wintypes

    # argtypes are REQUIRED, not tidiness: GetCurrentProcess returns the pseudo-handle -1, and
    # without an explicit HANDLE argtype ctypes narrows it to 32 bits on this 64-bit build, the
    # call fails, and the helper silently returns None (which is what it did on the first run).
    kernel32, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.c_void_p, wintypes.DWORD]
    psapi.GetProcessMemoryInfo.restype = wintypes.BOOL

    counters = _Counters()
    counters.cb = ctypes.sizeof(_Counters)
    if not psapi.GetProcessMemoryInfo(kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb):
        return None
    return counters.PrivateUsage / (1024 * 1024)


def _run_point(store, lon: float, lat: float, dev: str) -> dict:
    """One point through the whole pipeline, timed per stage."""
    from dail_tracker_core.siting.engine import evaluate
    from dail_tracker_core.siting.report import build_report, render_json, render_markdown

    t0 = time.perf_counter()
    result = evaluate(lon, lat, dev, store=store)
    t1 = time.perf_counter()
    report = build_report(result, store=store, generated_on="2026-07-27")
    t2 = time.perf_counter()
    md = render_markdown(report)
    t3 = time.perf_counter()
    doc = render_json(report)
    t4 = time.perf_counter()

    ctx = report.context
    return {
        "lon": lon,
        "lat": lat,
        "council": result.council.slug,
        "not_evaluated": bool(result.not_evaluated),
        "fired": len(result.fired),
        "exclusions": len(result.exclusions),
        "context_facts": len(ctx.facts) if ctx is not None else 0,
        "missing_layers": len(ctx.missing_layers) if ctx is not None else 0,
        "boundary_mismatch": bool(ctx.boundary_mismatch) if ctx is not None else False,
        "md_chars": len(md),
        "json_keys": len(doc),
        "ms_evaluate": round((t1 - t0) * 1000, 1),
        "ms_report": round((t2 - t1) * 1000, 1),
        "ms_markdown": round((t3 - t2) * 1000, 1),
        "ms_json": round((t4 - t3) * 1000, 1),
        "ms_total": round((t4 - t0) * 1000, 1),
    }


def sweep(n_per_council: int, dev: str) -> dict:
    from dail_tracker_core.siting.layers import make_store

    points = _slug_points(n_per_council)
    if not points:
        return {}

    mem_before = _private_mb()
    t_store = time.perf_counter()
    store = make_store()
    store_load_s = time.perf_counter() - t_store
    mem_after_store = _private_mb()

    councils: dict[str, dict] = {}
    failures: list[dict] = []
    # Private bytes sampled every 10 points. The SHAPE is the finding, not the endpoint: lazy layer
    # loading gives a curve that flattens, a leak gives one that keeps climbing. A single
    # before/after pair cannot tell those apart, and this process is the template for a server one.
    mem_curve: list[tuple[int, float]] = []
    done = 0
    for slug in sorted(points):
        rows = []
        for lon, lat in points[slug]:
            try:
                rows.append(_run_point(store, lon, lat, dev))
                done += 1
                if done % 10 == 0:
                    sample = _private_mb()
                    if sample is not None:
                        mem_curve.append((done, round(sample, 1)))
            except Exception as exc:  # a crash IS the finding — record it, keep sweeping
                failures.append(
                    {
                        "council": slug,
                        "lon": lon,
                        "lat": lat,
                        "error": f"{type(exc).__name__}: {exc}",
                        "traceback": traceback.format_exc(limit=4),
                    }
                )
        if rows:
            # A point the plausibility gate blocked never reaches council resolution, so counting it
            # as "unresolved" reports a bug that is not there — the register's own pins sit on road
            # carriageways often enough that a 5-point sample hits one in ~2 counties every run, and
            # a check that cries wolf every run is a check people stop reading. Resolution is
            # therefore measured over EVALUATED points only, and blocked points get their own count.
            evaluated = [r for r in rows if not r["not_evaluated"]]
            councils[slug] = {
                "points": len(rows),
                "evaluated": len(evaluated),
                # per-council aggregates are the diffable unit; individual points are noise
                "council_resolved": sum(1 for r in evaluated if r["council"]),
                "not_evaluated": sum(1 for r in rows if r["not_evaluated"]),
                # aggregates over EVALUATED points: a blocked point contributes 0 to everything, so
                # including it would drag every range down to 0- and hide a real change
                "fired": [r["fired"] for r in evaluated],
                "context_facts": [r["context_facts"] for r in evaluated],
                "missing_layers": [r["missing_layers"] for r in evaluated],
                "exclusions": [r["exclusions"] for r in evaluated],
                "boundary_mismatch": sum(1 for r in rows if r["boundary_mismatch"]),
                "md_chars_median": int(statistics.median(r["md_chars"] for r in rows)),
                "ms_total_median": round(statistics.median(r["ms_total"] for r in rows), 1),
                "rows": rows,
            }

    all_rows = [r for c in councils.values() for r in c["rows"]]
    # COLD vs WARM matters more than the headline here. make_store() is lazy — a layer is read on
    # first touch — so the earliest points in a run pay for loading layers that every later point
    # reuses. Mixing them produces a p50 that describes neither a first request nor a steady-state
    # one. The split point is the first quarter of the run, by which time the common layers are in.
    cold_n = max(1, len(all_rows) // 4)
    warm_rows = all_rows[cold_n:]
    timings = sorted(r["ms_total"] for r in all_rows)
    warm_timings = sorted(r["ms_total"] for r in warm_rows)
    return {
        "dev_type": dev,
        "points_per_council": n_per_council,
        "councils": councils,
        "failures": failures,
        "perf": {
            "store_load_s": round(store_load_s, 2),
            "points": len(all_rows),
            "ms_p50": round(statistics.median(timings), 1) if timings else None,
            "ms_p95": round(timings[int(len(timings) * 0.95)], 1) if len(timings) > 20 else None,
            "ms_min": timings[0] if timings else None,
            "ms_max": timings[-1] if timings else None,
            "cold_points": cold_n,
            "warm_ms_p50": round(statistics.median(warm_timings), 1) if warm_timings else None,
            "warm_ms_p95": round(warm_timings[int(len(warm_timings) * 0.95)], 1) if len(warm_timings) > 20 else None,
            "ms_mean_evaluate": round(statistics.mean(r["ms_evaluate"] for r in warm_rows), 1) if warm_rows else None,
            "ms_mean_report": round(statistics.mean(r["ms_report"] for r in warm_rows), 1) if warm_rows else None,
            "ms_mean_markdown": round(statistics.mean(r["ms_markdown"] for r in warm_rows), 1) if warm_rows else None,
            "ms_mean_json": round(statistics.mean(r["ms_json"] for r in warm_rows), 1) if warm_rows else None,
            "private_mb_before": round(mem_before, 1) if mem_before else None,
            "private_mb_after_store": round(mem_after_store, 1) if mem_after_store else None,
            "private_mb_end": round(_private_mb() or 0, 1) or None,
            "private_mb_curve": mem_curve,
        },
    }


def _compare(current: dict, baseline: dict) -> list[str]:
    """Per-council shape differences against the baseline. Prose is NOT compared — see COMPARED."""
    diffs: list[str] = []
    cur_c, base_c = current.get("councils", {}), baseline.get("councils", {})
    for slug in sorted(set(cur_c) | set(base_c)):
        if slug not in base_c:
            diffs.append(f"{slug}: NEW in this run")
            continue
        if slug not in cur_c:
            diffs.append(f"{slug}: MISSING from this run (was in baseline)")
            continue
        a, b = cur_c[slug], base_c[slug]
        for field in COMPARED:
            if a.get(field) != b.get(field):
                diffs.append(f"{slug}.{field}: {b.get(field)} -> {a.get(field)}")
    return diffs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--points", type=int, default=5, help="real application points per council")
    ap.add_argument("--dev", default="one_off_house", help="development type to evaluate")
    ap.add_argument("--save-baseline", action="store_true", help="adopt this run as the baseline")
    args = ap.parse_args()

    result = sweep(args.points, args.dev)
    if not result:
        return 2

    councils, perf = result["councils"], result["perf"]
    print(f"\n{len(councils)} authorities · {perf['points']} points · dev={result['dev_type']}\n")
    print(f"{'council':38s} {'pts':>3s} {'ev':>3s} {'res':>3s} {'fired':>9s} {'ctx':>7s} {'md':>7s} {'ms':>7s}")
    for slug, c in councils.items():
        fired = f"{min(c['fired'])}-{max(c['fired'])}" if c["fired"] else "—"
        ctx = f"{min(c['context_facts'])}-{max(c['context_facts'])}" if c["context_facts"] else "—"
        print(
            f"{slug:38s} {c['points']:3d} {c['evaluated']:3d} {c['council_resolved']:3d} "
            f"{fired:>9s} {ctx:>7s} {c['md_chars_median']:7d} {c['ms_total_median']:7.1f}"
        )

    print(f"\nall points   p50 {perf['ms_p50']} ms · p95 {perf['ms_p95']} ms · "
          f"min {perf['ms_min']} · max {perf['ms_max']} ms")
    print(f"warm  (after the first {perf['cold_points']} points, layers loaded)   "
          f"p50 {perf['warm_ms_p50']} ms · p95 {perf['warm_ms_p95']} ms")
    print(f"warm stage means (ms): evaluate {perf['ms_mean_evaluate']} · report {perf['ms_mean_report']} "
          f"· markdown {perf['ms_mean_markdown']} · json {perf['ms_mean_json']}")
    if perf["private_mb_curve"]:
        curve = perf["private_mb_curve"]
        print(f"\nprivate bytes {perf['private_mb_before']} MB at start -> {perf['private_mb_end']} MB at end")
        print("  curve: " + " · ".join(f"{n}pt {mb:.0f}MB" for n, mb in curve))
        # Verdict logic, and why it is not a simple first-half/second-half comparison: private bytes
        # oscillate by ~200 MB between samples as the allocator returns and reclaims arenas, which
        # is LARGER than the trend being measured. Differencing the halves gave "FLATTENING" and
        # "STILL CLIMBING" on three consecutive runs of identical code. A verdict that flips on
        # unchanged code is worse than none, so this reports the PLATEAU BAND and only calls a leak
        # when the rise between thirds clears the oscillation amplitude.
        if len(curve) >= 6:
            values = [mb for _, mb in curve]
            third = len(values) // 3
            mid_med = statistics.median(values[third : 2 * third])
            last_med = statistics.median(values[2 * third :])
            band_lo, band_hi = min(values[third:]), max(values[third:])
            amplitude = band_hi - band_lo
            print(f"  plateau after ~{curve[third][0]} points: {band_lo:.0f}-{band_hi:.0f} MB "
                  f"(oscillation {amplitude:.0f} MB)")
            drift = last_med - mid_med
            if drift > amplitude:
                print(f"  ⚠ median rose {drift:+.0f} MB between thirds, beyond the {amplitude:.0f} MB "
                      "oscillation — investigate before serving this in a long-lived process")
            else:
                print(f"  median drift between thirds {drift:+.0f} MB, within oscillation — "
                      "no leak signal; growth is lazy layer loading")

    if result["failures"]:
        print(f"\n{len(result['failures'])} FAILURES:")
        for f in result["failures"][:10]:
            print(f"  {f['council']} ({f['lon']}, {f['lat']}): {f['error']}")

    unresolved = [s for s, c in councils.items() if c["council_resolved"] < c["evaluated"]]
    if unresolved:
        print(f"\n⚠ councils with an EVALUATED point resolving to None: {unresolved}")
    blocked = {s: c["not_evaluated"] for s, c in councils.items() if c["not_evaluated"]}
    if blocked:
        total = sum(blocked.values())
        print(f"\n{total} point(s) blocked by the plausibility gate (expected — register pins sit "
              f"on road centrelines): {blocked}")

    if args.save_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        # strip per-point rows and timings from the baseline: timings are machine/load dependent and
        # would make every run a diff, which trains people to ignore the diff
        slim = {
            "dev_type": result["dev_type"],
            "points_per_council": result["points_per_council"],
            "councils": {
                s: {k: v for k, v in c.items() if k in COMPARED or k in ("points", "council_resolved")}
                for s, c in councils.items()
            },
        }
        BASELINE.write_text(json.dumps(slim, indent=2), encoding="utf-8")
        print(f"\nbaseline written: {BASELINE.relative_to(REPO_ROOT)}")
        return 1 if result["failures"] else 0

    if BASELINE.exists():
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
        if baseline.get("points_per_council") != args.points or baseline.get("dev_type") != args.dev:
            print(f"\nbaseline was taken at points={baseline.get('points_per_council')} "
                  f"dev={baseline.get('dev_type')} — not comparable to this run")
        else:
            diffs = _compare(result, baseline)
            print(f"\n{len(diffs)} shape difference(s) vs baseline")
            for d in diffs[:40]:
                print(f"  {d}")
            if diffs:
                return 1
    else:
        print(f"\nno baseline at {BASELINE.relative_to(REPO_ROOT)} — run with --save-baseline")

    return 1 if result["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
