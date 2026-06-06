"""tools/build_source_health.py — per-source health from the source registry.

Reads the live source configs (via tools.build_source_registry.build_records),
checks each source, and writes ``data/_meta/source_health.json`` with one health
record per source plus a summary. Returns a nonzero exit code if any source is
``failed`` so a caller (CI gate / pipeline step) can react.

Two check tiers — and why links are opt-in
-------------------------------------------
* ``file_age`` (CRO, Charities): OFFLINE. Globs the bronze input pattern, takes
  the newest file's mtime as the "age of data we hold", and compares to the
  source's ``stale_after_days``. Always runs — no network, cheap. Covers both
  manually-dropped files and automated fetches (CRO is fetched by cro_poller.py,
  so an old snapshot means the poller stopped, not the operator).
* ``index_poll`` / ``fixed_file``: a network HEAD check of the listing/file URL.
  Runs ONLY when links are enabled (``--check-links`` or ``DAIL_CHECK_LINKS=1``),
  mirroring manifest.py's ``DAIL_CHECK_ENDPOINTS`` gate. Otherwise ``skipped``.

So a default (offline) run gives a real signal on the manual sources and marks
the online ones ``skipped`` — safe to run anywhere. CI never runs the live tool
(it would flag CRO/Charities as missing because bronze is gitignored); CI runs
the unit tests in test/tools/test_source_health.py against fixtures instead.

Status vocab: ok | warning | failed | skipped. Only ``failed`` affects exit code.

Pipeline step vs CI gate
------------------------
Like tools/check_freshness.py, the pipeline step's job is to *write* the signal,
not to gate — it always exits 0 so a naturally-aging manual source never marks a
pipeline run "partial". A separate ``--strict`` invocation (CI / scheduled
report) reads the same logic and exits nonzero on any ``failed`` source.

Usage:
    python tools/build_source_health.py                 # offline checks, write JSON (exit 0)
    python tools/build_source_health.py --check-links    # also HEAD online sources
    python tools/build_source_health.py --strict         # exit 1 if any source failed
    python tools/build_source_health.py --print          # echo result to stdout
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.build_source_registry import build_records  # noqa: E402

OK, WARNING, FAILED, SKIPPED = "ok", "warning", "failed", "skipped"


def _health(rec: dict, status: str, detail: str, **extra) -> dict:
    """One health record: the source's identity + a status/detail + check fields."""
    out = {
        "source_id": rec["source_id"],
        "group": rec["group"],
        "check_type": rec["check_type"],
        "status": status,
        "detail": detail,
    }
    out.update(extra)
    return out


def check_file_age(rec: dict, root: Path, now: datetime) -> dict:
    """Offline staleness for a bronze-glob source (manual drop OR automated fetch
    like cro_poller). Age = newest matching file's mtime. No file → failed."""
    pattern = rec.get("input_pattern")
    if not pattern:
        return _health(rec, FAILED, "manual source has no input_pattern")
    matches = sorted(root.glob(pattern))
    if not matches:
        return _health(
            rec, FAILED, f"no file matches {pattern}", input_pattern=pattern, latest_file=None, days_old=None
        )
    latest = max(matches, key=lambda p: p.stat().st_mtime)
    age_days = (now - datetime.fromtimestamp(latest.stat().st_mtime, tz=UTC)).days
    threshold = rec.get("stale_after_days")
    rel = str(latest.relative_to(root)).replace("\\", "/")
    common = dict(input_pattern=pattern, latest_file=rel, days_old=age_days, stale_after_days=threshold)
    if threshold is None:
        # no policy configured → can't assert stale; surface age as a warning
        return _health(rec, WARNING, "no stale threshold configured", **common)
    if age_days > threshold:
        return _health(rec, FAILED, f"stale: {age_days}d old > {threshold}d threshold", **common)
    return _health(rec, OK, f"{age_days}d old (<= {threshold}d)", **common)


def check_link(rec: dict, timeout: float = 20.0) -> dict:
    """Network HEAD of the source's listing URL (or first direct file). 4xx/5xx
    or transport error → failed. Imported lazily so the offline path needs no
    requests/idna."""
    url = rec.get("listing_url") or (rec.get("direct_files") or [None])[0]
    if not url:
        return _health(rec, SKIPPED, "no listing_url or direct_files to check")
    import requests  # lazy: offline runs must not require requests/idna

    try:
        r = requests.head(
            url, allow_redirects=True, timeout=timeout, headers={"User-Agent": "dail-tracker-bot/0.1 (source-health)"}
        )
        # some servers 405 on HEAD — retry a lightweight ranged GET before judging
        if r.status_code in (403, 405):
            r = requests.get(
                url,
                allow_redirects=True,
                timeout=timeout,
                stream=True,
                headers={"User-Agent": "dail-tracker-bot/0.1 (source-health)", "Range": "bytes=0-0"},
            )
        meta = dict(
            http_status=r.status_code,
            final_url=r.url,
            content_type=r.headers.get("Content-Type"),
            content_length=r.headers.get("Content-Length"),
            last_modified=r.headers.get("Last-Modified"),
        )
        if r.status_code >= 400:
            return _health(rec, FAILED, f"HTTP {r.status_code}", **meta)
        return _health(rec, OK, f"HTTP {r.status_code}", **meta)
    except Exception as e:  # noqa: BLE001 - a transport error IS the health signal
        return _health(rec, FAILED, f"{type(e).__name__}: {e}")


def _summary(health: list[dict]) -> dict:
    by = {OK: 0, WARNING: 0, FAILED: 0, SKIPPED: 0}
    for h in health:
        by[h["status"]] = by.get(h["status"], 0) + 1
    stale = sum(1 for h in health if h["check_type"] == "file_age" and h["status"] == FAILED)
    return {
        "sources_checked": len(health),
        "sources_ok": by[OK],
        "sources_warning": by[WARNING],
        "sources_failed": by[FAILED],
        "sources_skipped": by[SKIPPED],
        "stale_sources": stale,
    }


def run(
    records: list[dict] | None = None,
    *,
    check_links: bool = False,
    root: Path | None = None,
    now: datetime | None = None,
) -> dict:
    """Compute health for every source. ``records`` defaults to the live registry
    (build_records); tests inject a fixture list. Returns the full payload."""
    root = root or Path(__file__).resolve().parents[1]
    now = now or datetime.now(UTC)
    records = build_records() if records is None else records

    health: list[dict] = []
    for rec in records:
        ct = rec["check_type"]
        if ct == "file_age":
            health.append(check_file_age(rec, root, now))
        elif ct in ("index_poll", "fixed_file"):
            if check_links and rec.get("pollable", False):
                health.append(check_link(rec))
            else:
                why = "link check disabled" if not check_links else "not pollable"
                health.append(_health(rec, SKIPPED, why))
        else:  # api_canary etc. — not implemented yet
            health.append(_health(rec, SKIPPED, f"no checker for {ct}"))

    health.sort(key=lambda h: h["source_id"])
    return {
        "generated_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "links_checked": check_links,
        "summary": _summary(health),
        "sources": health,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--check-links", action="store_true", help="HEAD-check online sources (network). Also via DAIL_CHECK_LINKS=1"
    )
    ap.add_argument(
        "--strict", action="store_true", help="exit 1 if any source is failed (for CI/gate use; default exits 0)"
    )
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the health report to stdout")
    args = ap.parse_args()

    check_links = args.check_links or os.environ.get("DAIL_CHECK_LINKS") == "1"
    root = Path(__file__).resolve().parents[1]
    out_path = root / "data" / "_meta" / "source_health.json"

    payload = run(check_links=check_links, root=root)
    out_path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    s = payload["summary"]
    print(
        f"source health: {s['sources_ok']} ok / {s['sources_warning']} warn / "
        f"{s['sources_failed']} failed / {s['sources_skipped']} skipped "
        f"(links_checked={payload['links_checked']}) -> {out_path}"
    )
    if s["sources_failed"]:
        for h in payload["sources"]:
            if h["status"] == FAILED:
                print(f"  FAILED {h['source_id']}: {h['detail']}")
    if args.echo:
        sys.stdout.buffer.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        print()
    return 1 if (args.strict and s["sources_failed"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
