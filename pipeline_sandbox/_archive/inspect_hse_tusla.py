"""DIAGNOSTIC (PRE-ETL, throwaway): why is HSE's parsed sum €6.38bn, and what's
fragmenting Tusla (miss_sup=91 / dup=253)? Answers the two structural read-issues from
sample_extract_procurement_pdf.py BEFORE deciding whether they need per-source anchors.

Reuses the extractor's own functions so it sees exactly what the probe sees.
Run:  ./.venv/Scripts/python.exe pipeline_sandbox/inspect_hse_tusla.py
"""

from __future__ import annotations

import contextlib
import json
import sys
from collections import Counter
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from sample_extract_procurement_pdf import (  # noqa: E402
    TMP, clean_supplier, cluster_word_rows, fetch, find_header, header_columns,
    parse_pdf, to_eur,
)

PROBE = TMP / "procurement_publishers_probe.json"


def url_for(pid: str) -> str:
    d = json.loads(PROBE.read_text(encoding="utf-8"))
    return next(p["sample"] for p in d["publishers"] if p["publisher_id"] == pid)


def hr(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


def show_header_geometry(b: bytes) -> None:
    """Print the detected header row's word x-positions so we can see WHERE the
    column boundaries fell (and which logical columns got fused)."""
    doc = fitz.open(stream=b, filetype="pdf")
    rows = cluster_word_rows(doc[0])
    h = find_header(rows)
    doc.close()
    if not h:
        print("  no header detected on page 1")
        return
    print("  header words (x0 -> x1 : text):")
    for w in sorted(h, key=lambda w: w[0]):
        print(f"    {w[0]:6.1f} -> {w[2]:6.1f} : {w[4]!r}")
    print("  fused anchors:")
    for c in header_columns(h):
        print(f"    [{c['x0']:6.1f}-{c['x1']:6.1f}]  {c['label']!r}")


def inspect_hse() -> None:
    hr("HSE — is €6.38bn real or an amount/date-merge artifact?")
    b = fetch(url_for("ie_hse"))
    show_header_geometry(b)
    info = parse_pdf(b, None)
    recs, roles = info["records"], info["roles"]
    amt_i = roles.get("amount")
    sup_i = roles.get("supplier")
    cols = info["cols"]
    print("\n  roles: " + ", ".join(f"{k}->{cols[v]['label']!r}" for k, v in roles.items()))
    print(f"  rows={len(recs)}  amount col idx={amt_i}")
    print("\n  12 sample rows  (vendor | RAW amount-bucket | parsed €):")
    for r in recs[:12]:
        amt_cell = r[amt_i] if amt_i is not None and amt_i < len(r) else ""
        sup = clean_supplier(r[sup_i]) if sup_i is not None and sup_i < len(r) else ""
        print(f"    {sup[:34]:<34} | {amt_cell[:34]:<34} | {to_eur(amt_cell)}")

    amts = [to_eur(r[amt_i]) for r in recs if amt_i is not None and amt_i < len(r)]
    amts = [a for a in amts if a is not None]
    amts.sort(reverse=True)
    total = sum(amts)
    print(f"\n  parsed sum = €{total:,.0f}   n={len(amts)}")
    print(f"  top 8 amounts: {[f'{a:,.0f}' for a in amts[:8]]}")
    print(f"  share from rows >€1m: {sum(a for a in amts if a > 1e6)/total:.0%}")
    print(f"  share from rows >€10m: {sum(a for a in amts if a > 1e7)/total:.0%}")
    # are the giant values plausible single payments, or date-number contamination?
    print("  rows where amount-bucket also holds a date (space-separated 2+ tokens):")
    multi = [r[amt_i] for r in recs if amt_i is not None and amt_i < len(r) and len(str(r[amt_i]).split()) >= 2]
    print(f"    {len(multi)} of {len(recs)} amount cells have >=2 tokens; e.g. {multi[:4]}")


def inspect_tusla() -> None:
    hr("Tusla — what causes miss_sup=91 and dup=253?")
    b = fetch(url_for("ie_tusla"))
    show_header_geometry(b)
    info = parse_pdf(b, None)
    recs, roles = info["records"], info["roles"]
    sup_i = roles.get("supplier")
    amt_i = roles.get("amount")

    empty = [r for r in recs if sup_i is not None and (sup_i >= len(r) or not r[sup_i].strip())]
    print(f"\n  rows with EMPTY vendor bucket: {len(empty)}")
    print("  5 such full rows (all columns) — are these wrapped continuation lines?:")
    for r in empty[:5]:
        print(f"    {r}")

    seen = Counter(tuple(r) for r in recs)
    dups = [r for r, n in seen.items() if n > 1]
    print(f"\n  distinct duplicated row-tuples: {len(dups)} (accounting for {sum(n-1 for r,n in seen.items() if n>1)} dup rows)")
    print("  5 duplicated rows (are these legitimately repeated payments, or lost detail?):")
    for r in dups[:5]:
        print(f"    x{seen[r]}  {r}")
    print("\n  NOTE: each PDF row is one y-line. A vendor/description that wraps to a 2nd")
    print("  line becomes its own row; if it has no money token it's skipped, if it does")
    print("  it can collide. This is a row-stitching question, not a column question.")


def main() -> None:
    inspect_hse()
    inspect_tusla()


if __name__ == "__main__":
    main()
