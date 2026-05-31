"""Offline warehouse build: run transform/enrich steps against existing bronze.

Skips the network pollers (oireachtas_pdf_poller, pdf_downloader, members API,
lobbying_poller, iris poller). Wikidata steps reuse cached bronze raws. Runs
each step in dependency order, continues on failure, records exit + duration.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable

# (label, [args]) — args[0] is script relative to ROOT
STEPS: list[tuple[str, list[str]]] = [
    ("flatten_members",        ["flatten_members_json_to_csv.py"]),
    ("flatten_debates",        ["dbsect_listings_flatten.py"]),
    ("wikidata_socials",       ["wikidata_socials_etl.py"]),
    ("ministerial_tenure",     ["ministerial_tenure_build.py"]),
    ("committees_long_format", ["committees_long_format_etl.py"]),
    ("payments_etl",           ["payments_full_psa_etl.py"]),
    ("payments_enrichment",    ["payments_member_enrichment.py"]),
    ("attendance",             ["attendance.py"]),
    ("member_interests",       ["member_interests.py"]),
    ("lobby_processing",       ["lobby_processing.py"]),
    ("lobbying_pdf_extract",   ["lobbying_pdf_extract.py"]),
    ("cro_normalise",          ["cro_normalise.py"]),
    ("charity_normalise",      ["charity_normalise.py"]),
    ("charity_resolved",       ["charity_resolved.py"]),
    ("charity_enriched",       ["charity_enriched.py"]),
    ("legislation",            ["legislation.py"]),
    ("questions",              ["questions.py"]),
    ("bill_amendments",        ["bill_amendments_flatten.py"]),
    ("transform_votes",        ["transform_votes.py"]),
    ("enrich",                 ["enrich.py"]),
    ("iris",                   ["iris_refresh.py", "--skip-poll"]),
]

PER_STEP_TIMEOUT = 1800  # seconds


def main() -> int:
    results: list[tuple[str, str, float, int | None]] = []
    overall = time.monotonic()
    for label, args in STEPS:
        print(f"\n{'='*74}\n>>> {label}  ({' '.join(args)})\n{'='*74}", flush=True)
        t = time.monotonic()
        status = "ok"
        rc: int | None = None
        try:
            r = subprocess.run([PY, *args], cwd=ROOT, timeout=PER_STEP_TIMEOUT)
            rc = r.returncode
            if rc != 0:
                status = "FAILED"
        except subprocess.TimeoutExpired:
            status = "TIMEOUT"
        except Exception as e:  # noqa: BLE001
            status = f"ERROR:{e}"
        dur = time.monotonic() - t
        results.append((label, status, dur, rc))
        print(f"<<< {label}: {status} in {dur:.1f}s (exit {rc})", flush=True)

    print(f"\n{'#'*74}\nBUILD SUMMARY  (total {time.monotonic()-overall:.1f}s)\n{'#'*74}")
    for label, status, dur, rc in results:
        print(f"  {status:>8}  {label:<24} {dur:8.1f}s  exit={rc}")
    failed = [r for r in results if r[1] != "ok"]
    print(f"\n{len(results)-len(failed)}/{len(results)} steps ok; {len(failed)} not ok")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
