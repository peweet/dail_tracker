"""Fetch-failure intelligence for the self-fetching procurement extractors.

The extractors (public-body, local-authority) crawl ~90 government sites for
published PO/payment disclosures. When a download fails the old code swallowed
the reason (``except Exception: return _curl(url)``), so a run log could only say
"download failed" — useless for deciding whether the cause was our bug (malformed
URL), the publisher's WAF (rate-limit burst), or genuine source rot (listing page
replaced by a JS bot-challenge). This module gives the extractors three shared
pieces:

1. **Classifier** — maps a requests exception / response body to a stable
   ``error_class`` vocabulary (``malformed_url``, ``timeout``, ``http_403`` …,
   ``bot_challenge``, ``not_expected_filetype``), so failures are diagnosable
   in aggregate.
2. **Breaker** — a per-publisher consecutive-failure circuit breaker. A host
   that refuses 3 files in a row is refusing the run, not the file; continuing
   just hammers it (courts.ie WAF, 2026-06-11: 4 ok then 33 straight failures
   at 2 attempts each). Trip → skip the publisher's remaining files.
3. **FetchReport** — collects failures / zero-file harvests / breaker trips and
   writes ``data/_meta/fetch_failures.json`` (read-modify-write keyed by
   extractor, last-run-only — freshness.json precedent). Each failed publisher
   is enriched with ``rows_in_gold`` + ``last_period_in_gold`` from the gold
   procurement fact, so the report is directly actionable for finding an
   alternative source ("Wicklow: 410 rows to 2021-Q3 at risk, listing now serves
   a bot-challenge"). The report is consumed by the ``source_fetch_failures``
   MCP tool — its audience is a Claude session researching replacements, not CI.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = ROOT / "data/_meta/fetch_failures.json"
GOLD_FACT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
SENTINEL = ROOT / "data/_meta/last_parse_attempt.txt"

BREAKER_THRESHOLD = 3

# substrings that identify a WAF/bot interstitial page (Incapsula, Cloudflare, …)
_CHALLENGE_MARKERS = (
    b"one moment, please",
    b"just a moment",
    b"you are being redirected",
    b"_incapsula_",
    b"cf-challenge",
)


# ----------------------------------------------------------------------- classify
def classify_exception(exc: Exception) -> tuple[str, int | None]:
    """Map a requests exception to (error_class, http_status|None)."""
    if isinstance(exc, requests.exceptions.HTTPError) and exc.response is not None:
        return f"http_{exc.response.status_code}", exc.response.status_code
    if isinstance(
        exc, (requests.exceptions.InvalidURL, requests.exceptions.MissingSchema, requests.exceptions.InvalidSchema)
    ):
        return "malformed_url", None
    if isinstance(exc, (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout)):
        return "timeout", None
    if isinstance(exc, requests.exceptions.SSLError):
        return "tls_error", None
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection_error", None
    return f"error_{type(exc).__name__}", None


def classify_body(body: bytes, expected_magic: bytes | None = None) -> str | None:
    """Classify a *successfully downloaded* body that is not what we asked for.

    Returns ``bot_challenge`` for WAF interstitials, ``not_expected_filetype``
    when an expected magic prefix (e.g. ``b"%PDF"``) is absent, else None (body
    looks fine).
    """
    head = body[:2048].lower()
    if any(m in head for m in _CHALLENGE_MARKERS):
        return "bot_challenge"
    if expected_magic is not None and not body.startswith(expected_magic):
        return "not_expected_filetype"
    return None


# ------------------------------------------------------------------------ breaker
class Breaker:
    """Consecutive-failure circuit breaker, one instance per publisher."""

    def __init__(self, threshold: int = BREAKER_THRESHOLD) -> None:
        self.threshold = threshold
        self.consecutive = 0
        self.tripped = False

    def record(self, ok: bool) -> None:
        if ok:
            self.consecutive = 0
        else:
            self.consecutive += 1
            if self.consecutive >= self.threshold:
                self.tripped = True


# ------------------------------------------------------------------------- report
class FetchReport:
    """Per-run failure collector for one extractor; merges into fetch_failures.json."""

    def __init__(self, extractor: str) -> None:
        self.extractor = extractor
        self.failures: list[dict[str, Any]] = []
        self.zero_harvest: list[dict[str, Any]] = []
        self.breaker_trips: list[dict[str, Any]] = []

    def record_failure(
        self,
        *,
        publisher_id: str,
        publisher_name: str,
        url: str,
        error_class: str,
        listing_url: str = "",
        stage: str = "file",
        http_status: int | None = None,
        attempts: int = 1,
    ) -> None:
        self.failures.append(
            {
                "ts_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                "publisher_id": publisher_id,
                "publisher_name": publisher_name,
                "stage": stage,  # listing | file
                "url": url,
                "listing_url": listing_url,
                "error_class": error_class,
                "http_status": http_status,
                "attempts": attempts,
            }
        )

    def record_zero_harvest(self, *, publisher_id: str, publisher_name: str, listing_url: str) -> None:
        self.zero_harvest.append(
            {
                "ts_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                "publisher_id": publisher_id,
                "publisher_name": publisher_name,
                "listing_url": listing_url,
            }
        )

    def record_breaker_trip(self, *, publisher_id: str, publisher_name: str, files_skipped: int) -> None:
        self.breaker_trips.append(
            {
                "ts_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                "publisher_id": publisher_id,
                "publisher_name": publisher_name,
                "files_skipped": files_skipped,
            }
        )

    # -- gold enrichment: what is at risk if this source stays broken ------------
    def _gold_stakes(self) -> dict[str, dict[str, Any]]:
        pub_ids = {f["publisher_id"] for f in (*self.failures, *self.zero_harvest, *self.breaker_trips)}
        if not pub_ids or not GOLD_FACT.exists():
            return {}
        import polars as pl  # deferred: report writing is rare, polars import is not free

        gold = pl.read_parquet(GOLD_FACT, columns=["publisher_id", "period"])
        out: dict[str, dict[str, Any]] = {}
        for pid in pub_ids:
            sub = gold.filter(pl.col("publisher_id") == pid)
            if sub.height:
                out[pid] = {"rows_in_gold": sub.height, "last_period_in_gold": sub["period"].max()}
        return out

    def write(self) -> Path:
        stakes = self._gold_stakes()
        for rec in (*self.failures, *self.zero_harvest, *self.breaker_trips):
            rec.update(stakes.get(rec["publisher_id"], {}))
        existing: dict[str, Any] = {}
        if OUT_PATH.exists():
            try:
                existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                existing = {}
        sections = existing.get("extractors", {})
        sections[self.extractor] = {
            "run_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "n_failures": len(self.failures),
            "failures": self.failures,
            "zero_harvest": self.zero_harvest,
            "breaker_trips": self.breaker_trips,
        }
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUT_PATH.write_text(
            json.dumps(
                {"generated_utc": datetime.now(UTC).isoformat(timespec="seconds"), "extractors": sections},
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return OUT_PATH

    def summary_lines(self) -> list[str]:
        out = []
        if self.failures:
            out.append(f"fetch failures: {len(self.failures)}")
            by_class: dict[str, int] = {}
            for f in self.failures:
                by_class[f["error_class"]] = by_class.get(f["error_class"], 0) + 1
            out.extend(f"  {k}: {v}" for k, v in sorted(by_class.items(), key=lambda kv: -kv[1]))
        if self.zero_harvest:
            out.append(f"zero-harvest publishers (listing rot?): {[z['publisher_id'] for z in self.zero_harvest]}")
        if self.breaker_trips:
            out.append(f"breaker trips: {[t['publisher_id'] for t in self.breaker_trips]}")
        return out


def write_sentinel(extractor: str, publisher_id: str, url: str) -> None:
    """One-line crash sentinel, overwritten before every parse. If the process
    dies natively (fitz segfault — seen 2026-06-12 mid-run, untraceable from the
    log), this file names the exact file being parsed."""
    try:
        SENTINEL.parent.mkdir(parents=True, exist_ok=True)
        SENTINEL.write_text(
            f"{datetime.now(UTC).isoformat(timespec='seconds')} {extractor} {publisher_id} {url}\n", encoding="utf-8"
        )
    except OSError:
        pass
