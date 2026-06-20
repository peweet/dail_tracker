"""Parity + registration guard for the receiver-appointer / operator-firm graduation.

The receiver-appointer ranking and operator-firm concentration were lifted out of
utility/pages_code/corporate.py into extractors/corporate_receiver_enrich.py (gold)
+ sql_views/corporate/corporate_receiver.sql (views). These tests lock that the
precomputed gold still reproduces the verbatim page logic, and that the views
register and agree with the gold.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
NOTICES = ROOT / "data" / "gold" / "parquet" / "corporate_notices.parquet"

pytestmark = pytest.mark.skipif(not NOTICES.exists(), reason="corporate_notices gold not present")


def _enrichment():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "corporate_receiver_enrich", ROOT / "extractors" / "corporate_receiver_enrich.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_gold_matches_verbatim_page_logic():
    """build_appointers/firms must agree with the reference recomputation that
    mirrors corporate._render_featured exactly (top-N, bucket mix, scalar counts)."""
    m = _enrichment()
    notices = pd.read_parquet(NOTICES)
    enriched = m.enrich_notices(notices)
    appointers = m.build_appointers(enriched)

    ref_top, ref_buckets, ref_scalars = m._reference_topn_and_buckets(enriched)
    assert appointers["parent"].head(m.FEATURED_TOP_N).tolist() == ref_top
    assert appointers.groupby("type_bucket")["n_notices"].sum().to_dict() == ref_buckets
    assert int(enriched["is_receivership"].sum()) == ref_scalars["n_recv"]
    assert int((enriched["is_receivership"] & enriched["has_parent_mention"]).sum()) == ref_scalars["n_tagged"]


def test_views_register_and_match_gold():
    """The corporate_receiver views build and the summary scalars equal the gold."""
    from dail_tracker_core.db import connect_with_views

    con = connect_with_views(["corporate_*.sql"], swallow_errors=False)
    # views exist and are non-empty where expected
    assert con.execute("SELECT COUNT(*) FROM v_corporate_receiver_appointers").fetchone()[0] >= 1
    assert con.execute("SELECT COUNT(*) FROM v_corporate_receiver_firms").fetchone()[0] >= 1

    n_recv = con.execute("SELECT n_recv FROM v_corporate_receiver_summary").fetchone()[0]
    recv_via_notices = con.execute(
        "SELECT COUNT(*) FROM v_corporate_notices WHERE is_receivership"
    ).fetchone()[0]
    assert n_recv == recv_via_notices

    # bucket mix totals the appointer notice counts
    bucket_total = con.execute("SELECT SUM(n) FROM v_corporate_receiver_bucket_mix").fetchone()[0]
    appointer_total = con.execute("SELECT SUM(n_notices) FROM v_corporate_receiver_appointers").fetchone()[0]
    assert bucket_total == appointer_total


def test_cbi_badge_b2_drops_single_token_false_positives():
    """The CBI badge uses method B2 (exact + >=2-token substring). A notice whose
    only overlap with a CBI firm is a single short token inside an ADDRESS (the
    'Donnybrook' street false positive the old page shipped) must NOT be badged;
    a genuine prefixed entity (Havbell No.2 DAC) MUST be badged."""
    from dail_tracker_core.db import connect_with_views

    con = connect_with_views(["corporate_*.sql"], swallow_errors=False)
    addr_fp = con.execute(
        "SELECT COUNT(*) FROM v_corporate_notices "
        "WHERE entity_name ILIKE '%Morehampton Road, Donnybrook%' AND cbi_register <> ''"
    ).fetchone()[0]
    assert addr_fp == 0, "Donnybrook street-address rows must not carry a CBI badge"

    havbell = con.execute(
        "SELECT COUNT(*) FROM v_corporate_notices "
        "WHERE entity_name ILIKE '%Havbell No%2%' AND cbi_register <> ''"
    ).fetchone()[0]
    assert havbell >= 1, "genuine prefixed entity (Havbell No.2 DAC) should be badged"


def test_v_corporate_notices_carries_receiver_columns():
    """Downstream/firewall contract: the notices view exposes the precomputed
    flags the page now selects instead of recomputing."""
    from dail_tracker_core.db import connect_with_views

    con = connect_with_views(["corporate_*.sql"], swallow_errors=False)
    cols = {d[0] for d in con.execute("DESCRIBE SELECT * FROM v_corporate_notices").fetchall()}
    assert {
        "is_receivership",
        "is_spv",
        "has_parent_mention",
        "receiver_firms",
        "has_receiver_firm",
        "cbi_register",
        "cbi_ref_no",
    } <= cols
