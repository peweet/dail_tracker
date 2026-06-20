"""tools/build_source_registry.py — generate the unified source registry from code.

Reads the *existing* per-extractor source configs (the hand-curated bit that
cannot be auto-discovered) and normalises them into one registry record schema,
written to ``data/_meta/source_registry.generated.json``.

Why generate instead of hand-maintaining a registry
---------------------------------------------------
Source URLs already live in code, one config per extractor. Maintaining a second
hand-written URL list would silently diverge the moment an extractor's config
changes. So this tool *reads* those configs and emits the registry — the configs
stay the single source of truth (see the closing instruction in
doc/current_source_health_coverage_gaps_claude_plan.md).

The five code configs (+ manual sources)
----------------------------------------
    oireachtas_pdf_poller.SOURCES              dict[str, PollSource]
    procurement_public_body_extract.PUBLISHERS list[dict]  (cfg())
    procurement_la_payments_extract.SCHEMA_MAP list[dict]  (la())
    procurement_hse_tusla_parser.SPECS         dict        (parser geometry only)
    afs_amalgamated_extract.URLS               dict[int, str]
    + CRO / Charities                          manual bronze-glob sources

Each config has a different shape, so each gets a small pure adapter. The
adapters are deliberately import-free of the sandbox modules: ``main()`` does the
importing and passes the raw config in, so the adapters can be unit-tested with
plain fixtures and no network / no heavy deps (fitz, polars).

HSE/Tusla note
--------------
``SPECS`` holds parser column-geometry, not URLs — at runtime the parser resolves
a *file* URL from an ephemeral ``c:/tmp`` probe JSON. That probed file URL is a
discovered artifact, not config, so it does NOT belong in the registry. The
registry instead takes the HSE/Tusla *landing* URL from the committed seed
(``procurement_publishers_seed.SEEDS``), matching the registry's model: the
registry holds the index/listing URL; the poller discovers file URLs.

Usage:
    python tools/build_source_registry.py            # write the registry JSON
    python tools/build_source_registry.py --print    # also echo to stdout
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Registry record schema --------------------------------------------------------
# Every adapter emits dicts with exactly these keys (defaults filled by _record).
# check_type ∈ {index_poll, api_canary, manual_age, fixed_file}.
_RECORD_KEYS = (
    "source_id",
    "group",
    "owner_module",
    "name",
    "check_type",
    "listing_url",
    "direct_files",
    "value_semantics",
    "grain",
    "privacy_risk",
    "status",
    "pollable",
    "parser_wired",
    "include_pattern",
    "caveat",
    "input_pattern",
    "stale_after_days",
    "refresh_mode",
)


def _record(**kw) -> dict:
    """Build one normalised record, defaulting every unset key so the output
    schema is uniform across the heterogeneous source configs."""
    rec = {k: None for k in _RECORD_KEYS}
    rec["direct_files"] = []
    rec["pollable"] = False
    rec["parser_wired"] = False
    rec.update(kw)
    extra = set(kw) - set(_RECORD_KEYS)
    if extra:
        raise ValueError(f"unknown record keys: {sorted(extra)}")
    return rec


def _pattern_of(include) -> str | None:
    """Extract the source text of a compiled ``re.Pattern`` (configs store
    ``include`` as a compiled regex, which is not JSON-serialisable)."""
    return getattr(include, "pattern", None)


# Adapters (pure: take the raw config object, return list[record]) --------------
def adapt_oireachtas(sources: dict) -> list[dict]:
    """oireachtas_pdf_poller.SOURCES — dict[str, PollSource]. Each is an active,
    parser-wired index poller already running in the pipeline."""
    out = []
    for key, src in sources.items():
        out.append(
            _record(
                source_id=f"oireachtas_pdfs:{key}",
                group="oireachtas_pdfs",
                owner_module="oireachtas_pdf_poller",
                name=getattr(src, "name", key),
                check_type="index_poll",
                listing_url=getattr(src, "index_url", None),
                grain="publication_pdf",
                status="active",
                pollable=True,
                parser_wired=True,
            )
        )
    return out


def adapt_public_body(publishers: list[dict]) -> list[dict]:
    """procurement_public_body_extract.PUBLISHERS — list of cfg() dicts. Parse
    targets, not yet wired into pipeline.py (parser_wired=False)."""
    out = []
    for p in publishers:
        out.append(
            _record(
                source_id=f"public_body_payments:{p['id']}",
                group="public_body_payments",
                owner_module="procurement_public_body_extract",
                name=p["name"],
                check_type="index_poll",
                listing_url=p.get("listing_url"),
                direct_files=list(p.get("direct_files") or []),
                value_semantics=p.get("amount_semantics"),
                grain=p.get("grain"),
                privacy_risk=p.get("privacy_risk"),
                status=f"tier_{p.get('tier', '?')}",
                pollable=True,
                include_pattern=_pattern_of(p.get("include")),
                caveat=p.get("caveat") or None,
            )
        )
    return out


def adapt_la(schema_map: list[dict]) -> list[dict]:
    """procurement_la_payments_extract.SCHEMA_MAP — list of la() dicts. The
    config's own ``status`` gates pollability: NON-PUBLISHER councils never
    publish and NEEDS-RENDER ones need Playwright, so neither is pollable today
    (flagging them would be a permanent false-positive)."""
    out = []
    for c in schema_map:
        status = c.get("status", "READY")
        out.append(
            _record(
                source_id=f"local_authority_payments:{c['slug']}",
                group="local_authority_payments",
                owner_module="procurement_la_payments_extract",
                name=c["council"],
                check_type="index_poll",
                listing_url=c.get("listing_url"),
                direct_files=list(c.get("direct_files") or []),
                value_semantics=c.get("value_kind"),
                grain="payment_or_po",
                status=status,
                pollable=status in {"READY", "DIRECT"},
                include_pattern=_pattern_of(c.get("include")),
                caveat=c.get("caveat") or None,
            )
        )
    return out


def adapt_afs(urls: dict) -> list[dict]:
    """afs_amalgamated_extract.URLS — dict[int year, str url]. Fixed-file
    sources (one hardcoded PDF per year); already parser-wired."""
    out = []
    for year in sorted(urls):
        out.append(
            _record(
                source_id=f"afs_amalgamated:{year}",
                group="afs_amalgamated",
                owner_module="afs_amalgamated_extract",
                name=f"Local Authority AFS {year} (amalgamated)",
                check_type="fixed_file",
                direct_files=[urls[year]],
                grain="annual_financial_statement",
                status="fixed",
                pollable=True,
                parser_wired=True,
            )
        )
    return out


def adapt_la_afs(registry: list[dict], deferred: list[dict]) -> list[dict]:
    """la_afs_extract.REGISTRY + DEFERRED_COUNCILS — per-council audited Annual Financial
    Statements (the revenue I&E-by-division fact). Harvest councils poll their finance landing;
    the JS-rendered DEFERRED councils need Playwright to enumerate, so they are not pollable
    (flagging them would be a permanent false-positive, same rule as adapt_la)."""
    out = []
    for c in registry:
        out.append(
            _record(
                source_id=f"local_authority_afs:{c['slug']}",
                group="local_authority_afs",
                owner_module="la_afs_extract",
                name=c["council"],
                check_type="index_poll",
                listing_url=(c.get("landing") or [None])[0],
                direct_files=list(c.get("direct") or []),
                grain="annual_financial_statement",
                status="harvest",
                pollable=True,
            )
        )
    for c in deferred:
        out.append(
            _record(
                source_id=f"local_authority_afs:{c['slug']}",
                group="local_authority_afs",
                owner_module="la_afs_extract",
                name=c["council"],
                check_type="index_poll",
                listing_url=(c.get("landing") or [None])[0],
                grain="annual_financial_statement",
                status="needs_render",
                pollable=False,
            )
        )
    return out


def adapt_hse_tusla(specs: dict, seed_landing: dict[str, str]) -> list[dict]:
    """procurement_hse_tusla_parser.SPECS — parser geometry only (no URLs). The
    listing URL comes from the committed seed's landing_url; pollable only if a
    durable landing URL exists. Bespoke parser, not yet wired (parser_wired=False)."""
    out = []
    for pid, spec in specs.items():
        landing = seed_landing.get(pid)
        out.append(
            _record(
                source_id=f"hse_tusla_payments:{pid}",
                group="hse_tusla_payments",
                owner_module="procurement_hse_tusla_parser",
                name=spec.get("name", pid),
                check_type="index_poll",
                listing_url=landing,
                grain="payment",
                privacy_risk="high",
                status="landing_only" if landing else "url_unresolved",
                pollable=bool(landing),
                caveat=(
                    "listing from seed; runtime parser resolves the file URL from "
                    "an ephemeral probe JSON — promote that before wiring to pipeline"
                ),
            )
        )
    return out


def adapt_manual(specs: list[dict]) -> list[dict]:
    """Manual bronze-glob sources (e.g. Charities). A human drops a file in
    bronze; the health signal is file age (check_type=file_age), not link
    polling. CRO is no longer here — it is automated (see adapt_cro)."""
    out = []
    for s in specs:
        out.append(
            _record(
                source_id=f"file_sources:{s['id']}",
                group="file_sources",
                owner_module=s["owner_module"],
                name=s["name"],
                check_type="file_age",
                grain=s.get("grain"),
                status="manual",
                pollable=False,
                refresh_mode="manual",
                input_pattern=s["input_pattern"],
                stale_after_days=s.get("stale_after_days"),
                caveat=s.get("caveat") or None,
            )
        )
    return out


def adapt_cro(meta: dict) -> list[dict]:
    """CRO bulk register — now AUTOMATED by cro_poller.py (CKAN daily zip). Same
    file-age health signal as a manual source, but refresh_mode=automated and a
    tighter threshold: an old snapshot means the poller stopped, not the operator."""
    return [
        _record(
            source_id=f"file_sources:{meta['source_id']}",
            group="file_sources",
            owner_module=meta["owner_module"],
            name=meta["name"],
            check_type="file_age",
            listing_url=f"{meta['ckan_base']}/dataset/{meta['package_id']}",
            grain="company_register",
            status="automated",
            pollable=True,
            refresh_mode="automated",
            input_pattern=meta["input_pattern"],
            stale_after_days=meta.get("stale_after_days"),
            caveat="CKAN daily zip; fetched + validated by cro_poller.py",
        )
    ]


def adapt_cro_fs(meta: dict) -> list[dict]:
    """CRO financial-statements filing INDEX — sandbox extractor (CKAN, free).
    Health = age of the silver event log we hold; figures stay paywalled. Not yet
    wired to pipeline.py, so the stale threshold is generous (annual-ish upstream)."""
    return [
        _record(
            source_id=f"file_sources:{meta['source_id']}",
            group="file_sources",
            owner_module=meta["owner_module"],
            name=meta["name"],
            check_type="file_age",
            listing_url=f"{meta['ckan_base']}/dataset/{meta['package_id']}",
            grain="company_filing_index",
            status="automated",
            pollable=True,
            refresh_mode="automated",
            input_pattern=meta["silver_pattern"],
            stale_after_days=meta.get("stale_after_days"),
            caveat="CKAN filing index (figures paywalled); sandbox extractor, not yet pipeline-wired",
        )
    ]


def adapt_stateboards(index_url: str) -> list[dict]:
    """DPER State Boards register (membership.stateboards.ie) — pipeline-wired
    scrape (stateboards chain). Health = age of the silver roster parquet; the
    register is continuously maintained, so a stale local copy means OUR chain
    stopped, not upstream."""
    return [
        _record(
            source_id="stateboards:register",
            group="stateboards",
            owner_module="stateboards_roster_extract",
            name="Membership of State Boards (DPER register)",
            check_type="file_age",
            listing_url=index_url,
            grain="board_member_seat",
            status="automated",
            pollable=True,
            parser_wired=True,
            refresh_mode="automated",
            input_pattern="data/silver/parquet/stateboards_roster.parquet",
            stale_after_days=60,
            caveat="current roster only (no history); Wikidata identities are hand-curated (data/_meta/stateboards_wikidata_curated.csv)",
        )
    ]


# Manual-source specs (no code config exists; the plan defines these) -----------
MANUAL_SOURCES = [
    {
        "id": "charities_register",
        "owner_module": "charity_normalise",
        "name": "Charities Public Register",
        # 180d: the register is a manual/semi-manual XLSX drop and the Charities
        # Regulator refreshes the public export on a slow (≈quarterly-to-annual)
        # cadence, so a copy older than ~6 months means OUR ingest has lapsed, not
        # that upstream is unchanged. Without a threshold the source could only ever
        # 'warning', never 'failed' (build_source_health.check_file_age) — i.e. it
        # was un-gateable. 180d makes a lapsed refresh an actionable failure.
        "grain": "charity_register",
        "input_pattern": "data/bronze/charities/public_register_*.xlsx",
        "stale_after_days": 180,
        "caveat": "manual/semi-manual XLSX drop; refresh at least twice a year",
    },
]


def _seed_landing_urls(seeds: list[dict]) -> dict[str, str]:
    """publisher_id -> landing_url for any seed row that has a non-empty URL."""
    return {s["publisher_id"]: s["landing_url"] for s in seeds if s.get("landing_url")}


def build_records() -> list[dict]:
    """Import each live config and run its adapter. Each import is guarded so a
    single missing heavy dep degrades that group rather than killing the build;
    the skipped group is reported on stderr."""
    records: list[dict] = []

    def _try(group: str, fn) -> None:
        try:
            records.extend(fn())
        except Exception as e:  # noqa: BLE001 - health tool must not hard-fail
            print(f"  ! skipped {group}: {type(e).__name__}: {e}", file=sys.stderr)

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extractors"))

    def _oireachtas():
        from pdf_infra.oireachtas_pdf_poller import SOURCES

        return adapt_oireachtas(SOURCES)

    def _public_body():
        from procurement_public_body_extract import PUBLISHERS

        return adapt_public_body(PUBLISHERS)

    def _la():
        from procurement_la_payments_extract import SCHEMA_MAP

        return adapt_la(SCHEMA_MAP)

    def _afs():
        from afs_amalgamated_extract import URLS

        return adapt_afs(URLS)

    def _la_afs():
        from la_afs_extract import DEFERRED_COUNCILS, REGISTRY

        return adapt_la_afs(REGISTRY, DEFERRED_COUNCILS)

    def _hse_tusla():
        from procurement_publishers_seed import SEEDS

        seed_landing = _seed_landing_urls(SEEDS)
        try:
            from procurement_hse_tusla_parser import SPECS
        except Exception:  # noqa: BLE001 - heavy deps (fitz); fall back to ids
            SPECS = {"ie_hse": {"name": "HSE"}, "ie_tusla": {"name": "Tusla"}}
        return adapt_hse_tusla(SPECS, seed_landing)

    def _cro():
        from corporate.cro_poller import SOURCE_META

        return adapt_cro(SOURCE_META)

    def _cro_fs():
        from cro_financial_statements_extract import SOURCE_META as FS_META

        return adapt_cro_fs(FS_META)

    _try("oireachtas_pdfs", _oireachtas)
    _try("public_body_payments", _public_body)
    _try("local_authority_payments", _la)
    _try("afs_amalgamated", _afs)
    _try("local_authority_afs", _la_afs)
    _try("hse_tusla_payments", _hse_tusla)

    def _stateboards():
        from stateboards_roster_extract import INDEX_URL

        return adapt_stateboards(INDEX_URL)

    _try("file_sources:cro", _cro)
    _try("file_sources:cro_fs", _cro_fs)
    _try("file_sources:manual", lambda: adapt_manual(MANUAL_SOURCES))
    _try("stateboards", _stateboards)

    records.sort(key=lambda r: r["source_id"])
    return records


def _summary(records: list[dict]) -> dict:
    groups: dict[str, dict] = {}
    for r in records:
        g = groups.setdefault(r["group"], {"total": 0, "pollable": 0})
        g["total"] += 1
        g["pollable"] += int(r["pollable"])
    return {
        "sources_total": len(records),
        "sources_pollable": sum(int(r["pollable"]) for r in records),
        "by_group": groups,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the registry to stdout after writing")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_path = root / "data" / "_meta" / "source_registry.generated.json"

    records = build_records()
    payload = {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "note": "GENERATED by tools/build_source_registry.py from in-code source "
        "configs. Do not hand-edit; edit the owning extractor's config.",
        "summary": _summary(records),
        "sources": records,
    }
    out_path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))

    s = payload["summary"]
    print(f"source registry: {s['sources_total']} sources ({s['sources_pollable']} pollable) -> {out_path}")
    for g, gs in sorted(s["by_group"].items()):
        print(f"  {g:28s} {gs['pollable']:>2}/{gs['total']:>2} pollable")
    if args.echo:
        sys.stdout.buffer.write(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
