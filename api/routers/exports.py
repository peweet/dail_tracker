"""Bulk data exports — `/v1/data` manifest + per-resource parquet/CSV download.

The bulk file IS the product for programmatic consumers (newsrooms, researchers,
bid-intelligence users): clean, refreshed, documented datasets with the value
semantics attached, instead of screen-scraping the app or paging the JSON API.

Security model is DEFAULT-DENY:
  * Only resources in ``EXPORTS`` exist — the resource name must be a dict key,
    so no path component ever reaches the filesystem.
  * Privacy filters are baked into the generated snapshot, not the docs: rows
    naming natural persons (``public_display = FALSE``, sole traders,
    ``privacy_status = 'review_personal_data'``) are EXCLUDED from the file.
  * SIPO (donor addresses), corporate notices (personal-insolvency quarantine
    lives at view level), member interests and judiciary are hard-excluded —
    they have no entry here and tests pin that.

Snapshots are materialised to ``data/_export_cache/`` (gitignored) and re-cut
whenever the source parquet's mtime advances, so a download is a cheap file
serve except right after a pipeline refresh.

Every manifest entry carries ``data_currency`` with TWO clocks: the newest
record date AND the source-file fetch time. They differ legitimately — the
eTenders open-data CSV publishes quarterly with an inherent ~6-month lag, so
"fresh pull, old newest-record" is the honest description, not a defect.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from api.contracts import ERROR_RESPONSES
from config import DATA_DIR, GOLD_PARQUET_DIR, SILVER_PARQUET_DIR
from dail_tracker_core.models.responses import ExportManifestResponse
from paths import configured_path

router = APIRouter(tags=["data"], responses=ERROR_RESPONSES)

EXPORT_CACHE_DIR = configured_path("DAIL_EXPORT_CACHE_DIR", DATA_DIR / "_export_cache")
EXPORT_SNAPSHOT_SCHEMA = 2

_SNAPSHOT_LOCKS: dict[tuple[str, str], threading.Lock] = {}
_SNAPSHOT_LOCKS_GUARD = threading.Lock()

_ETENDERS_ATTRIBUTION = "Contains Irish Public Sector Data (Office of Government Procurement) licensed under CC-BY 4.0."
_TED_ATTRIBUTION = "Contains information from TED (© European Union), reused under Decision 2011/833/EU."
_NO_PERSONS_NOTE = (
    "Rows naming natural persons (sole traders / individuals) are excluded from this export; "
    "they remain visible row-by-row in the published upstream source."
)


@dataclass(frozen=True)
class ExportSpec:
    source: Path
    description: str
    licence: str
    attribution: str
    caveat: str
    privacy_filter: str | None = None  # SQL WHERE baked into the snapshot
    privacy_note: str | None = None
    date_expr: str | None = None  # SQL aggregate yielding the newest record date


_TED_PERSON_FILTER = (
    "(supplier_class IS NULL OR supplier_class <> 'sole_trader_or_individual')"
    " AND (privacy_status IS NULL OR privacy_status <> 'review_personal_data')"
)

# The allow-list. Anything not in here does not exist as an export — adding an
# entry is a privacy decision, not a config tweak: state the filter and note.
EXPORTS: dict[str, ExportSpec] = {
    "procurement_awards": ExportSpec(
        source=GOLD_PARQUET_DIR / "procurement_awards.parquet",
        description=(
            "eTenders/OGP contract awards 2013+, one row per award×supplier, with supplier "
            "normalisation, CRO-match class, framework/DPS flags and the value taxonomy "
            "(value_kind, value_safe_to_sum)."
        ),
        licence="CC-BY-4.0",
        attribution=_ETENDERS_ATTRIBUTION,
        caveat=(
            "AWARD values are ceilings/estimates, NOT money paid. Frameworks/DPS repeat one "
            "ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even "
            "that is awarded value, never expenditure. Upstream publishes quarterly with an "
            "inherent ~6-month lag (see data_currency)."
        ),
        privacy_filter="supplier_class <> 'sole_trader_or_individual'",
        privacy_note=_NO_PERSONS_NOTE,
        date_expr="MAX(TRY_STRPTIME(\"Notice Published Date/Contract Created Date\", '%d/%m/%Y'))",
    ),
    "procurement_supplier_cro_match": ExportSpec(
        source=GOLD_PARQUET_DIR / "procurement_supplier_cro_match.parquet",
        description="Distinct eTenders suppliers (company-class) matched to CRO company numbers, with match method + confidence.",
        licence="CC-BY-4.0",
        attribution=_ETENDERS_ATTRIBUTION + " CRO data via the Companies Registration Office.",
        caveat="Name-based matches are best-effort (~53% of company-class suppliers); match_method/confidence qualify each row.",
    ),
    "procurement_payments_fact": ExportSpec(
        source=GOLD_PARQUET_DIR / "procurement_payments_fact.parquet",
        description=(
            "Public-body payments / purchase orders over €20k: 57 publishers (departments, "
            "agencies, councils, health/education bodies), unified schema with realisation_tier "
            "(SPENT vs COMMITTED), value_kind and per-row provenance."
        ),
        licence="CC-BY-4.0 (per-publisher source lists; see source_landing_url per row)",
        attribution="Compiled from official payment/PO publications of each public body (Circular 07/2012 / FOI publication schemes).",
        caveat=(
            "SPENT (paid) and COMMITTED (ordered) are different tiers — never blend them in one "
            "total. VAT basis varies by publisher and is unconfirmed for most: see the "
            "vat_matrix reference in this manifest before comparing across publishers."
        ),
        # public_display is the upstream gate, but parser drift has breached it before
        # (830 person-rows reached gold displayable, repaired 2026-06-11) — so the
        # person predicates ride along here as defense in depth.
        privacy_filter=(
            "public_display = TRUE AND supplier_class <> 'sole_trader_or_individual'"
            " AND (privacy_status IS NULL OR privacy_status <> 'review_personal_data')"
        ),
        privacy_note=_NO_PERSONS_NOTE,
        date_expr="MAX(year)",
    ),
    "procurement_lobbying_overlap": ExportSpec(
        source=GOLD_PARQUET_DIR / "procurement_lobbying_overlap.parquet",
        description="Entities appearing on BOTH the eTenders award register and the lobbying register (lobbying.ie), by normalised name.",
        licence="CC-BY-4.0",
        attribution=_ETENDERS_ATTRIBUTION + " Lobbying data via lobbying.ie.",
        caveat=(
            "CO-OCCURRENCE ONLY — never evidence that lobbying influenced any award. Rows "
            "duplicate per lobby-name match: NEVER sum awarded_value_safe_eur across rows."
        ),
    ),
    "ted_awards": ExportSpec(
        source=SILVER_PARQUET_DIR / "ted_ie_awards.parquet",
        description=(
            "TED (EU Official Journal) Irish contract awards 2024+ (eForms era): winners, award "
            "values, CRO match, and per-lot competition signals (tenders received, single-bid flag)."
        ),
        licence="EU open data (Commission Decision 2011/833/EU)",
        attribution=_TED_ATTRIBUTION,
        caveat=(
            "TED values are individual-notice context only and are never totaled; "
            "value_safe_to_sum is always false. Pan-EU framework outliers may cover many "
            "countries. Single-bid is a factual signal, never a verdict; never add TED values "
            "to payments or national awards."
        ),
        privacy_filter=_TED_PERSON_FILTER,
        privacy_note=_NO_PERSONS_NOTE,
        date_expr="MAX(dispatch_date)",
    ),
    "ted_winner_history": ExportSpec(
        source=SILVER_PARQUET_DIR / "ted_ie_winner_history.parquet",
        description=(
            "TED Irish award winners 2016–2023, recovered from per-notice XML (the TED API "
            "carries no winner data for this era) — the pre-2024 winner backlog."
        ),
        licence="EU open data (Commission Decision 2011/833/EU)",
        attribution=_TED_ATTRIBUTION,
        caveat=(
            "TED values are individual-notice context only and are never totaled; "
            "value_safe_to_sum is always false. Never add them to payments or national awards."
        ),
        privacy_filter=_TED_PERSON_FILTER,
        privacy_note=_NO_PERSONS_NOTE,
        date_expr="MAX(dispatch_date)",
    ),
    "ted_buyer_history": ExportSpec(
        source=SILVER_PARQUET_DIR / "ted_ie_buyer_history.parquet",
        description="TED Irish buyer-side award notices 2016–2023 (buyer, total value, CPV, procedure type — no winner data in this era's API).",
        licence="EU open data (Commission Decision 2011/833/EU)",
        attribution=_TED_ATTRIBUTION,
        caveat=(
            "Buyer-side grain — never union with winner-grain files. Values are individual-notice "
            "context only and value_safe_to_sum is always false."
        ),
        date_expr="MAX(dispatch_date)",
    ),
    "ted_tenders": ExportSpec(
        source=SILVER_PARQUET_DIR / "ted_ie_tenders.parquet",
        description="TED Irish OPEN tender notices (the forward pipeline): buyer, CPV, deadline, estimated value, procedure type.",
        licence="EU open data (Commission Decision 2011/833/EU)",
        attribution=_TED_ATTRIBUTION,
        caveat="PLANNED-tier estimates (estimate_advertised) — advertised expectations, never summed and never mixed with awards or payments.",
        date_expr="MAX(dispatch_date)",
    ),
}


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _snapshot_path(name: str, fmt: str) -> Path:
    return EXPORT_CACHE_DIR / f"{name}.{fmt}"


def _snapshot_metadata_path(snapshot: Path) -> Path:
    return snapshot.with_name(f"{snapshot.name}.meta.json")


def _snapshot_lock(name: str, fmt: str) -> threading.Lock:
    key = (name, fmt)
    with _SNAPSHOT_LOCKS_GUARD:
        return _SNAPSHOT_LOCKS.setdefault(key, threading.Lock())


def _policy_fingerprint(spec: ExportSpec, fmt: str) -> str:
    """Hash every policy input that can change which rows reach a snapshot."""
    payload = {
        "schema": EXPORT_SNAPSHOT_SCHEMA,
        "format": fmt,
        "privacy_filter": spec.privacy_filter or "",
        "source": str(spec.source.resolve()),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _source_identity(source: Path) -> dict[str, int]:
    stat = source.stat()
    return {"mtime_ns": stat.st_mtime_ns, "size": stat.st_size}


def _snapshot_is_current(snapshot: Path, metadata: Path, spec: ExportSpec, fmt: str) -> bool:
    if not snapshot.is_file() or not metadata.is_file():
        return False
    try:
        state = json.loads(metadata.read_text(encoding="utf-8"))
        return state == {
            "policy": _policy_fingerprint(spec, fmt),
            "source": _source_identity(spec.source),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False


def _write_metadata_atomic(path: Path, state: dict) -> None:
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(6)}.tmp")
    try:
        tmp.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            tmp.unlink()


def _ensure_snapshot(name: str, spec: ExportSpec, fmt: str) -> Path:
    """Materialise (or reuse) the privacy-filtered snapshot for one resource.

    Re-cut whenever the source identity or privacy policy changes. Generation is
    atomic: concurrent readers see either the previous complete file or the next
    complete file, never a partially written export.
    """
    out = _snapshot_path(name, fmt)
    metadata = _snapshot_metadata_path(out)
    with _snapshot_lock(name, fmt):
        if _snapshot_is_current(out, metadata, spec, fmt):
            return out

        EXPORT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        where = f" WHERE {spec.privacy_filter}" if spec.privacy_filter else ""
        select = f"SELECT * FROM read_parquet('{spec.source.as_posix()}'){where}"
        options = "FORMAT PARQUET, COMPRESSION ZSTD" if fmt == "parquet" else "FORMAT CSV, HEADER"

        # A pipeline may atomically replace the source while an API worker is
        # cutting a snapshot. Retry once rather than attaching metadata for a
        # different source generation to the copied rows.
        for _attempt in range(2):
            source_before = _source_identity(spec.source)
            tmp = out.with_name(f".{out.name}.{secrets.token_hex(6)}.tmp")
            try:
                con = _connect()
                try:
                    con.execute(f"COPY ({select}) TO '{tmp.as_posix()}' ({options})")
                finally:
                    con.close()
                source_after = _source_identity(spec.source)
                if source_after != source_before:
                    continue
                with contextlib.suppress(FileNotFoundError):
                    metadata.unlink()
                os.replace(tmp, out)
                _write_metadata_atomic(
                    metadata,
                    {"policy": _policy_fingerprint(spec, fmt), "source": source_after},
                )
                return out
            finally:
                with contextlib.suppress(FileNotFoundError):
                    tmp.unlink()
        raise RuntimeError(f"source changed while building export snapshot: {spec.source}")


def _manifest_entry(name: str, spec: ExportSpec) -> dict:
    entry: dict = {
        "name": name,
        "download": f"/v1/data/{name}",
        "formats": ["parquet", "csv"],
        "description": spec.description,
        "licence": spec.licence,
        "attribution": spec.attribution,
        "caveat": spec.caveat,
    }
    if spec.privacy_note:
        entry["privacy"] = spec.privacy_note
    if not spec.source.exists():
        entry["available"] = False
        return entry
    entry["available"] = True
    where = f" WHERE {spec.privacy_filter}" if spec.privacy_filter else ""
    con = _connect()
    try:
        count_row = con.execute(f"SELECT count(*) FROM read_parquet('{spec.source.as_posix()}'){where}").fetchone()
        n_rows = count_row[0] if count_row else 0
        latest: str | None = None
        if spec.date_expr:
            date_row = con.execute(
                f"SELECT {spec.date_expr} FROM read_parquet('{spec.source.as_posix()}'){where}"
            ).fetchone()
            val = date_row[0] if date_row else None
            latest = str(val)[:10] if val is not None else None
    finally:
        con.close()
    entry["n_rows"] = n_rows
    entry["data_currency"] = {
        "latest_record": latest,
        "source_fetched_at": datetime.fromtimestamp(spec.source.stat().st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": "Two clocks on purpose: a fresh pull of a lagging upstream shows an old latest_record honestly.",
    }
    return entry


@router.get(
    "/data",
    response_model=ExportManifestResponse,
    response_model_exclude_unset=True,
    summary="Bulk export manifest — every downloadable dataset with licence, caveats and data currency",
)
def data_manifest() -> dict:
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": (
            "Privacy filters are baked into the files (not just documented): rows naming "
            "natural persons are excluded from every export. Money grains never union — "
            "read each resource's caveat before aggregating."
        ),
        "references": {
            "vat_matrix": "data/_meta/procurement_payments_vat_matrix.json (per-publisher VAT basis for procurement_payments_fact)",
        },
        "resources": [_manifest_entry(name, spec) for name, spec in EXPORTS.items()],
    }


@router.get(
    "/data/{resource}",
    response_class=FileResponse,
    summary="Download one dataset as parquet (default) or CSV",
)
def download(
    resource: str,
    format: str = Query("parquet", pattern="^(parquet|csv)$", description="parquet (zstd) or csv"),
) -> FileResponse:
    spec = EXPORTS.get(resource)
    if spec is None:
        raise HTTPException(status_code=404, detail=f"no exportable resource '{resource}'")
    if not spec.source.exists():
        raise HTTPException(status_code=503, detail=f"resource '{resource}' source data unavailable")
    path = _ensure_snapshot(resource, spec, format)
    media = "application/vnd.apache.parquet" if format == "parquet" else "text/csv"
    return FileResponse(path, media_type=media, filename=path.name)
