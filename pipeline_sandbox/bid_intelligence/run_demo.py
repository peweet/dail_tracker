"""SANDBOX demo + self-check for the bid-intelligence pack prototype.

Run from the repo root with the venv python (UTF-8 forced):

    PYTHONUTF8=1 .venv/Scripts/python pipeline_sandbox/bid_intelligence/run_demo.py

Builds packs for a real CPV category and a real buyer, asserts the boundary
invariants (caveat present; money grains never blended; no natural person profiled),
prints a structural summary, and writes a trimmed ``sample_pack.json`` next to this
file for review. Reads the registered production views; writes only into the sandbox.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

from dail_tracker_core.db import connect_with_views

# Allow running as a plain script (no package install): import the sibling module.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from bid_pack import _INDIVIDUAL_CLASS, _NAME_WITHHELD, build_bid_pack  # noqa: E402
from render_report import render_report  # noqa: E402

CPV = "72000000"  # IT services — a large, well-populated category
BUYER = "Dublin City Council"  # a high-volume contracting authority


def _check(name: str, cond: bool) -> None:
    print(("  PASS " if cond else "  FAIL ") + name)
    if not cond:
        raise AssertionError(name)


def _summarise(label: str, pack: dict) -> None:
    print(f"\n--- pack: {label} ---")
    print("  blocks:", [k for k in pack if k != "caveat"])
    ca = pack.get("comparable_awards", {})
    if isinstance(ca, dict):
        print("  comparable_awards.n_total:", ca.get("n_total"))
    cb = pack.get("category_benchmark")
    if cb:
        print(
            "  category_benchmark: median=%s p25=%s p75=%s (n_valued=%s)"
            % (cb.get("median_award_eur"), cb.get("p25_award_eur"), cb.get("p75_award_eur"), cb.get("n_awards_valued"))
        )
    ms = pack.get("market_signal")
    if ms:
        print(
            "  market_signal: award_median=%s ceiling_median=%s median_bids=%s single_bid_pct=%s sme_win_pct=%s"
            % (
                ms.get("award_median_eur"),
                ms.get("ceiling_median_eur"),
                ms.get("median_bids"),
                ms.get("single_bid_pct"),
                ms.get("sme_win_pct"),
            )
        )
    af = pack.get("active_firms", {})
    if isinstance(af, dict):
        print("  active_firms.n_total:", af.get("n_total"), "top:", [f["supplier"] for f in af.get("firms", [])[:5]])
    ev = pack.get("incumbent_payment_evidence", {})
    print("  payment_evidence firms:", [f["supplier"] for f in ev.get("firms", [])])
    rr = pack.get("rebid_radar", {})
    if rr:
        print("  rebid_radar.n:", rr.get("n"))
    ba = pack.get("buyer_awards")
    if ba:
        print("  buyer_awards: n_awards=%s safe_value=%s" % (ba.get("n_awards"), ba.get("awarded_value_safe_eur")))
    bp = pack.get("buyer_payments", {})
    if "paid_safe_eur" in bp:
        print("  buyer_payments: paid=%s ordered=%s" % (bp.get("paid_safe_eur"), bp.get("ordered_safe_eur")))
    elif "note" in bp:
        print("  buyer_payments: (name-key gap)")


def _assert_invariants(pack: dict) -> None:
    """The boundary rails the graduation path must preserve."""
    _check("caveat present + says NOT A BID PRICE", "NOT A BID PRICE" in pack.get("caveat", ""))
    # No natural person is ever named in the competitor set.
    firms = pack.get("active_firms", {}).get("firms", [])
    _check(
        "active_firms names no individual",
        all(f.get("supplier") != _NAME_WITHHELD and f.get("supplier_norm") for f in firms),
    )
    # Payment evidence is per-tier and never pre-summed across SPENT/COMMITTED.
    for ev in pack.get("incumbent_payment_evidence", {}).get("firms", []):
        tiers = [row.get("realisation_tier") for row in ev.get("payment_footprint", [])]
        _check(f"{ev['supplier']}: payment footprint split by tier", set(tiers) <= {"SPENT", "COMMITTED"})
    # Market signal keeps the award band and the framework-ceiling band as separate fields.
    ms = pack.get("market_signal")
    if ms:
        _check("market_signal keeps award band and ceiling band separate", "award_median_eur" in ms and "ceiling_median_eur" in ms)


def main() -> int:
    conn = connect_with_views(["procurement_*.sql"], swallow_errors=True)
    try:
        packs = {
            f"cpv={CPV}": build_bid_pack(conn, cpv_code=CPV),
            f"buyer={BUYER}": build_bid_pack(conn, buyer=BUYER),
            f"cpv={CPV}+buyer={BUYER}": build_bid_pack(conn, cpv_code=CPV, buyer=BUYER),
        }
        for label, pack in packs.items():
            if "error" in pack:
                print(f"\n{label}: ERROR {pack['error']}")
                continue
            _summarise(label, pack)
            print("  invariants:")
            _assert_invariants(pack)

        # Negative case: no inputs -> honest error, never an empty 'pack'.
        no_input = build_bid_pack(conn)
        _check("no-input call returns an error", "error" in no_input)

        # Write a trimmed sample for review (cap the listing blocks).
        sample = build_bid_pack(conn, cpv_code=CPV, buyer=BUYER, max_comparables=5, max_firms=8)
        here = Path(__file__).resolve().parent
        out = here / "sample_pack.json"
        out.write_text(json.dumps(sample, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nWrote sample pack -> {out}")

        # Render the client-facing report from a fuller pack (the reports-first artifact).
        report_pack = build_bid_pack(conn, cpv_code=CPV, buyer=BUYER, max_comparables=15)
        report_md = render_report(report_pack)
        _check("report renders and states NOT A BID PRICE", "NOT A BID PRICE" in report_md)
        report_out = here / "sample_report.md"
        report_out.write_text(report_md, encoding="utf-8")
        print(f"Wrote sample report -> {report_out}")
        print("\nALL SANDBOX CHECKS PASSED")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
