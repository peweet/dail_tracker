"""tools/legal_diary_openview_health.py — live source-health canary for the Legal
Diary OpenView source (pdf_infra/legal_diary_openview_poller.py +
extractors/legal_diary_openview_extract.py).

WHY: the OpenView ingest is an HTML scrape of an IBM Domino (.nsf) app. If the Courts
Service redesigns the site — the OpenView URL params, the `clickable-row` index rows, the
`<div class="ld-content">` body, or the alfresco metadata cells — the poller/extractor
silently start returning nothing. The pipeline's own guards already fail SAFE (the poller
exits 2 on an empty index; the extractor refuses to write on 0 cases; the privacy gate
refuses on any residual name), but nothing tells a human the source DRIFTED until coverage
quietly staleens. This canary probes the live source structure and exits non-zero on drift
so the scheduled workflow can open a tracking issue.

It is READ-ONLY: fetches each jurisdiction index + a couple of detail docs, asserts the
structure still parses, and prints a summary. It archives nothing and writes no gold.

Checks per jurisdiction:
  1. the OpenView index returns at least MIN_INDEX_ROWS clickable-row sittings;
  2. a sampled detail doc still exposes the `ld-content` body and a parseable Date cell;
  3. across all jurisdictions, the parser still extracts at least one case line
     (guards a "structure present but parser broke" silent-zero regression).

Exit codes:  0 healthy · 2 drift detected (a check failed) · 1 transient network failure.

Run:  ./.venv/Scripts/python.exe tools/legal_diary_openview_health.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from urllib.parse import urljoin  # noqa: E402

from extractors.legal_diary_openview_extract import (  # noqa: E402
    JURISDICTIONS,
    court_and_meta,
    detail_lines,
    parse_detail,
    parse_meta,
)
from pdf_infra.legal_diary_openview_poller import (  # noqa: E402
    BASE,
    _get,
    _index_url,
    _session,
    parse_index_rows,
)

MIN_INDEX_ROWS = 50  # each jurisdiction holds 700–2,900 sittings; 50 is a safe floor
SAMPLE_DETAILS = 2  # detail docs probed per jurisdiction


def check() -> int:
    sess = _session()
    problems: list[str] = []
    total_cases = 0
    network_only = True  # distinguish drift (exit 2) from pure network failure (exit 1)

    for slug in JURISDICTIONS:
        iv = _get(sess, _index_url(slug))
        if iv is None:
            problems.append(f"{slug}: index unreachable (network)")
            continue
        rows = parse_index_rows(iv.text)
        if len(rows) < MIN_INDEX_ROWS:
            problems.append(
                f"{slug}: index parsed {len(rows)} rows (< {MIN_INDEX_ROWS}) — index structure may have drifted"
            )
            network_only = False
            continue
        # probe a couple of detail docs for the body + metadata structure
        ok_details = 0
        for r in rows[:SAMPLE_DETAILS]:
            dr = _get(sess, urljoin(BASE, r["data_url"]))
            if dr is None:
                continue
            lines = detail_lines(dr.text)
            meta = parse_meta(dr.text)
            court, ctx = court_and_meta(slug, meta)
            if not lines:
                problems.append(
                    f"{slug}: detail {r['unid'][:12]} has no <div class='ld-content'> body — detail structure drifted"
                )
                network_only = False
                continue
            if not ctx.get("diary_date"):
                problems.append(
                    f"{slug}: detail {r['unid'][:12]} has no parseable Date cell — metadata structure drifted"
                )
                network_only = False
                continue
            total_cases += len(parse_detail(lines, court, ctx, "canary"))
            ok_details += 1
        print(f"  {slug:24} index_rows={len(rows):5} details_ok={ok_details}/{SAMPLE_DETAILS}")

    if total_cases == 0 and not problems:
        problems.append(
            "parser extracted 0 case lines across all sampled details — structure present but parsing broke"
        )
        network_only = False

    if not problems:
        print(
            f"OpenView source healthy: all {len(JURISDICTIONS)} jurisdictions parse; {total_cases} sample cases extracted."
        )
        return 0

    print("\nDRIFT / FAILURE:")
    for p in problems:
        print(f"  - {p}")
    # if every problem was a network timeout, it's transient (exit 1), not drift (exit 2)
    return 1 if network_only else 2


if __name__ == "__main__":
    raise SystemExit(check())
