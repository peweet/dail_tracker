"""SANDBOX — render a bid-intelligence pack as a Markdown market-research report.

Pure presentation: it formats the dict from ``bid_pack.build_bid_pack`` into the
report an analyst would hand a paying client (the reports-first MVP, BI_SPINOUT
§9 #6 / §15). It derives NO new figure — every number is passed through from the
pack, and the standing caveat is printed top and bottom. Sandbox-only.
"""

from __future__ import annotations

from typing import Any


def _eur(v: Any) -> str:
    if v is None:
        return "—"
    try:
        return f"€{float(v):,.0f}"
    except (TypeError, ValueError):
        return str(v)


def _num(v: Any) -> str:
    return "—" if v is None else f"{v:,}" if isinstance(v, int) else str(v)


def _year(v: Any) -> str:
    """Years are plain integers — never thousands-separated (2019, not 2,019)."""
    return "—" if v is None else str(int(v))


def render_report(pack: dict[str, Any]) -> str:
    if "error" in pack:
        return f"# Bid-intelligence pack\n\n**Could not build:** {pack['error']}\n"

    ctx = pack.get("context", {})
    title_bits = []
    if ctx.get("cpv_code"):
        title_bits.append(f"CPV {ctx['cpv_code']}")
    if ctx.get("buyer"):
        title_bits.append(ctx["buyer"])
    title = " · ".join(title_bits) or "tender context"

    L: list[str] = []
    L.append(f"# Market-research pack — {title}")
    L.append("")
    L.append(f"> **{pack.get('caveat', '')}**")
    L.append("")

    # Category benchmark
    cb = pack.get("category_benchmark")
    if cb:
        L.append("## Typical award value (this category)")
        L.append("")
        L.append(f"- Median award: **{_eur(cb.get('median_award_eur'))}**  "
                 f"(IQR {_eur(cb.get('p25_award_eur'))} – {_eur(cb.get('p75_award_eur'))})")
        L.append(f"- Based on {_num(cb.get('n_awards_valued'))} valued awards "
                 f"({_num(cb.get('n_awards'))} awards, {_num(cb.get('n_suppliers'))} suppliers).")
        L.append("- *Award value = contract ceiling/estimate at award, not money paid. No project size/area to normalise — context, never a quote.*")
        L.append("")

    # Market signal
    ms = pack.get("market_signal")
    if ms:
        L.append("## Market signal (should-I-bid context)")
        L.append("")
        L.append(f"- **{ms.get('trade_label') or ms.get('trade_code')}** "
                 f"({_num(ms.get('n_awards_total'))} awards; {_num(ms.get('n_contract_awards'))} contract awards)")
        L.append(f"- Contract-award band: {_eur(ms.get('award_p25_eur'))} / **{_eur(ms.get('award_median_eur'))}** / {_eur(ms.get('award_p75_eur'))} (p25/median/p75)")
        L.append(f"- Framework-ceiling band (shown SEPARATELY, never the same as awards): "
                 f"{_eur(ms.get('ceiling_p25_eur'))} / {_eur(ms.get('ceiling_median_eur'))} / {_eur(ms.get('ceiling_p75_eur'))} "
                 f"({_num(ms.get('n_framework_ceilings'))} ceilings)")
        L.append(f"- Competition: median **{_num(ms.get('median_bids'))} bids**; single-bid **{ms.get('single_bid_pct')}%** "
                 f"(TED 2024+ where reported, n={_num(ms.get('n_with_bid_data'))})")
        L.append(f"- SME win rate: **{ms.get('sme_win_pct')}%** (n={_num(ms.get('n_with_sme_data'))})")
        L.append(f"- *{pack.get('market_signal_note', '')}*")
        L.append("")

    # Buyer history
    ba = pack.get("buyer_awards")
    bp = pack.get("buyer_payments", {})
    if ba or bp:
        L.append("## Buyer history")
        L.append("")
        if ba:
            L.append(f"- Awards made: **{_num(ba.get('n_awards'))}** to {_num(ba.get('n_suppliers'))} suppliers; "
                     f"sum-safe awarded value {_eur(ba.get('awarded_value_safe_eur'))} (ceilings — never money paid).")
        if "paid_safe_eur" in bp:
            L.append(f"- Public-body payments: **{_eur(bp.get('paid_safe_eur'))} paid (SPENT)** · "
                     f"**{_eur(bp.get('ordered_safe_eur'))} ordered (COMMITTED)** — different stages, never added.")
        elif bp.get("note"):
            L.append(f"- Payments: *{bp['note']}*")
        L.append("")

    # Comparable awards
    ca = pack.get("comparable_awards", {})
    awards = ca.get("awards", []) if isinstance(ca, dict) else []
    if awards:
        L.append(f"## Comparable awards ({_num(ca.get('n_total'))} total — showing {min(len(awards), 15)})")
        L.append("")
        L.append("| Date | Supplier | Buyer | Value | Kind | Bids |")
        L.append("|---|---|---|---:|---|---:|")
        for a in awards[:15]:
            L.append("| {date} | {sup} | {buyer} | {val} | {kind} | {bids} |".format(
                date=a.get("award_date") or "—",
                sup=(a.get("supplier") or "—"),
                buyer=(a.get("contracting_authority") or ctx.get("buyer") or "—"),
                val=_eur(a.get("value_eur")),
                kind=a.get("value_kind") or "—",
                bids=_num(a.get("n_bids_received")) if a.get("n_bids_received") is not None else "—",
            ))
        L.append("")
        L.append(f"- *{ca.get('note', '')}*")
        L.append("")

    # Active firms
    af = pack.get("active_firms", {})
    firms = af.get("firms", []) if isinstance(af, dict) else []
    if firms:
        L.append(f"## Firms active here ({_num(af.get('n_total'))} total)")
        L.append("")
        for f in firms:
            L.append(f"- **{f.get('supplier')}** — {_num(f.get('n_awards'))} awards")
        L.append(f"\n*{af.get('note', '')}*")
        L.append("")

    # Incumbent payment evidence
    ev = pack.get("incumbent_payment_evidence", {})
    if ev.get("firms"):
        L.append("## What the State actually paid the leading firms")
        L.append("")
        for fe in ev["firms"]:
            L.append(f"### {fe.get('supplier')}")
            for row in fe.get("payment_footprint", []):
                L.append(f"- {row.get('realisation_tier')}: {_eur(row.get('total_safe_eur'))} "
                         f"across {_num(row.get('n_publishers'))} bodies "
                         f"({_year(row.get('min_year'))}–{_year(row.get('max_year'))})"
                         + (" · mixed VAT (indicative floor)" if row.get("vat_mixed") else ""))
            L.append("")
        L.append(f"*{ev.get('note', '')}*")
        L.append("")

    # Re-bid radar
    rr = pack.get("rebid_radar", {})
    if rr.get("contracts"):
        L.append(f"## Re-bid radar — contracts ending soon ({_num(rr.get('n'))} in scope)")
        L.append("")
        L.append("| Est. end | Buyer | Contract | Incumbent | Value |")
        L.append("|---|---|---|---|---:|")
        for c in rr["contracts"][:15]:
            L.append("| {end} | {buyer} | {name} | {win} | {val} |".format(
                end=c.get("est_end_date") or "—",
                buyer=c.get("buyer_name") or "—",
                name=(c.get("contract_name") or "—")[:60],
                win=c.get("winner_display") or "—",
                val=_eur(c.get("award_value_eur")),
            ))
        L.append("")
        L.append(f"- *{rr.get('note', '')}*")
        L.append("")

    L.append("---")
    L.append(f"*{pack.get('caveat', '')}*")
    L.append("")
    return "\n".join(L)
