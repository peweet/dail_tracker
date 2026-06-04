"""Unit tests for tools/build_source_registry.py adapters.

Pure unit tests (no marker, default CI lane). They feed each adapter a tiny
fixture that mirrors the *shape* of the real in-code config — so they never
import the sandbox extractors and need no network or heavy deps (fitz, polars).
The point is to lock the normalisation contract: compiled-regex extraction,
LA status→pollable gating, dataclass duck-typing, and source_id uniqueness.
"""

import re
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tools.build_source_registry import (  # noqa: E402
    _RECORD_KEYS,
    adapt_afs,
    adapt_hse_tusla,
    adapt_la,
    adapt_manual,
    adapt_oireachtas,
    adapt_public_body,
    build_records,
)


def _has_schema(rec: dict) -> bool:
    return set(rec) == set(_RECORD_KEYS)


def test_oireachtas_adapter_uses_index_url():
    src = SimpleNamespace(name="payments", index_url="https://o.ie/?topic=x")
    [rec] = adapt_oireachtas({"payments": src})
    assert _has_schema(rec)
    assert rec["source_id"] == "oireachtas_pdfs:payments"
    assert rec["listing_url"] == "https://o.ie/?topic=x"
    assert rec["check_type"] == "index_poll"
    assert rec["pollable"] and rec["parser_wired"]


def test_public_body_adapter_extracts_compiled_regex():
    pubs = [
        {
            "id": "ie_opw",
            "name": "OPW",
            "listing_url": "https://gov.ie/opw",
            "direct_files": ["https://gov.ie/opw/q1.xlsx"],
            "amount_semantics": "payment_actual",
            "grain": "payment",
            "privacy_risk": "low",
            "tier": "A",
            "include": re.compile(r"purchase|payment", re.I),
            "caveat": "",
        }
    ]
    [rec] = adapt_public_body(pubs)
    assert _has_schema(rec)
    assert rec["source_id"] == "public_body_payments:ie_opw"
    # compiled regex must be serialised to its source text, not the object
    assert rec["include_pattern"] == "purchase|payment"
    assert rec["value_semantics"] == "payment_actual"
    assert rec["status"] == "tier_A"
    assert rec["caveat"] is None  # empty string normalised to None
    assert rec["parser_wired"] is False


def test_la_adapter_status_gates_pollable():
    rows = [
        {
            "slug": "cork_city",
            "council": "Cork City",
            "status": "READY",
            "listing_url": "https://corkcity.ie",
            "value_kind": "po_committed",
        },
        {
            "slug": "mayo",
            "council": "Mayo",
            "status": "DIRECT",
            "direct_files": ["https://mayo.ie/a.pdf"],
            "value_kind": "po_committed",
        },
        {"slug": "carlow", "council": "Carlow", "status": "NEEDS-RENDER", "value_kind": "po_committed"},
        {"slug": "dublin_city", "council": "Dublin City", "status": "NON-PUBLISHER", "value_kind": "po_committed"},
    ]
    recs = {r["source_id"]: r for r in adapt_la(rows)}
    assert recs["local_authority_payments:cork_city"]["pollable"] is True
    assert recs["local_authority_payments:mayo"]["pollable"] is True
    # NEEDS-RENDER and NON-PUBLISHER must NOT be pollable (else permanent false alarms)
    assert recs["local_authority_payments:carlow"]["pollable"] is False
    assert recs["local_authority_payments:dublin_city"]["pollable"] is False


def test_la_adapter_defaults_missing_status_to_ready():
    [rec] = adapt_la([{"slug": "x", "council": "X", "value_kind": "po_committed"}])
    assert rec["status"] == "READY"
    assert rec["pollable"] is True


def test_afs_adapter_emits_fixed_file_per_year():
    recs = adapt_afs({2023: "https://gov.ie/AFS_2023.pdf", 2016: "https://gov.ie/2016.pdf"})
    assert [r["source_id"] for r in recs] == ["afs_amalgamated:2016", "afs_amalgamated:2023"]  # sorted by year
    assert recs[1]["check_type"] == "fixed_file"
    assert recs[1]["direct_files"] == ["https://gov.ie/AFS_2023.pdf"]
    assert recs[1]["listing_url"] is None


def test_hse_tusla_listing_from_seed_landing():
    specs = {"ie_hse": {"name": "HSE"}, "ie_tusla": {"name": "Tusla"}}
    seed_landing = {"ie_hse": "https://hse.ie/proc"}  # tusla missing on purpose
    recs = {r["source_id"]: r for r in adapt_hse_tusla(specs, seed_landing)}
    hse = recs["hse_tusla_payments:ie_hse"]
    tusla = recs["hse_tusla_payments:ie_tusla"]
    assert hse["listing_url"] == "https://hse.ie/proc"
    assert hse["pollable"] is True and hse["status"] == "landing_only"
    # no durable URL -> not pollable, flagged unresolved (never invented)
    assert tusla["listing_url"] is None
    assert tusla["pollable"] is False and tusla["status"] == "url_unresolved"
    assert hse["privacy_risk"] == "high"
    assert hse["parser_wired"] is False


def test_manual_adapter_carries_glob_and_threshold():
    specs = [
        {
            "id": "cro_companies",
            "owner_module": "cro_normalise",
            "name": "CRO",
            "input_pattern": "data/bronze/cro/companies_*.csv",
            "stale_after_days": 45,
        }
    ]
    [rec] = adapt_manual(specs)
    assert rec["check_type"] == "file_age"
    assert rec["input_pattern"] == "data/bronze/cro/companies_*.csv"
    assert rec["stale_after_days"] == 45
    assert rec["pollable"] is False


def test_build_records_live_configs_are_importable_and_unique():
    """Smoke test against the REAL in-code configs: the build must import every
    config without error and produce globally-unique source_ids. Skips if a
    heavy sandbox dep is unavailable in this environment."""
    records = build_records()
    if not records:
        import pytest

        pytest.skip("no source configs importable in this environment")
    ids = [r["source_id"] for r in records]
    assert len(ids) == len(set(ids)), "source_id collision"
    assert all(_has_schema(r) for r in records)
    # the three already-running Oireachtas pollers must always be present
    groups = {r["group"] for r in records}
    assert "oireachtas_pdfs" in groups
