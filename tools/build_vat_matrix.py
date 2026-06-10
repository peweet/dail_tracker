"""tools/build_vat_matrix.py — per-publisher VAT-basis matrix for the payments fact.

Writes ``data/_meta/procurement_payments_vat_matrix.json``: one entry per
publisher recording its VAT-status mix, row count, sum-safe value and year
span. Verified 2026-06-10: 87% of rows (145,528 of 167,190, 55 publishers)
carry ``vat_status='unknown'`` — only HSE and Tusla are documented incl-VAT.
A cross-publisher € total therefore mixes VAT bases; this matrix makes that
gap explicit per publisher instead of silent, and the export manifest +
public-payments caveat both point consumers at it.

Closing the unknowns is a per-publisher documentation chore (read each
publisher's notes page, record incl/excl here via the extractor config) —
this script only REPORTS what the gold parquet carries; it never guesses.

Usage:
    python tools/build_vat_matrix.py            # write the matrix JSON
    python tools/build_vat_matrix.py --print    # also echo to stdout
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import orjson

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import GOLD_PARQUET_DIR, PROJECT_ROOT  # noqa: E402

FACT = GOLD_PARQUET_DIR / "procurement_payments_fact.parquet"
OUTPUT_PATH = PROJECT_ROOT / "data" / "_meta" / "procurement_payments_vat_matrix.json"


def build_matrix() -> dict:
    con = duckdb.connect()
    rows = con.execute(
        f"""
        SELECT
            publisher_id,
            publisher_name,
            vat_status,
            count(*)                                                  AS n_rows,
            COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS safe_value_eur,
            MIN(year)::INT                                            AS min_year,
            MAX(year)::INT                                            AS max_year
        FROM read_parquet('{FACT.as_posix()}')
        GROUP BY 1, 2, 3
        ORDER BY publisher_name, vat_status
        """
    ).fetchall()
    con.close()

    publishers: dict[str, dict] = {}
    for pub_id, pub_name, vat, n, safe_eur, y0, y1 in rows:
        entry = publishers.setdefault(
            pub_id,
            {"publisher_name": pub_name, "vat_status_mix": {}, "n_rows": 0, "safe_value_eur": 0.0},
        )
        entry["vat_status_mix"][vat] = {"n_rows": n, "safe_value_eur": round(float(safe_eur), 2)}
        entry["n_rows"] += n
        entry["safe_value_eur"] = round(entry["safe_value_eur"] + float(safe_eur), 2)
        # year can be NULL for rows whose period failed to parse — skip those for the span
        if y0 is not None:
            prev = entry.get("min_year")
            entry["min_year"] = y0 if prev is None else min(prev, y0)
        if y1 is not None:
            prev = entry.get("max_year")
            entry["max_year"] = y1 if prev is None else max(prev, y1)

    n_unknown = sum(e["vat_status_mix"].get("unknown", {}).get("n_rows", 0) for e in publishers.values())
    n_total = sum(e["n_rows"] for e in publishers.values())
    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "data/gold/parquet/procurement_payments_fact.parquet",
        "note": (
            "VAT basis per publisher as carried on the gold payments fact. 'unknown' means the "
            "publisher's payment list does not document whether amounts include VAT — totals "
            "summed across publishers with differing/unknown bases are NOT comparable. Treat "
            "per-publisher figures as internally consistent; treat cross-publisher totals as "
            "indicative only until the unknowns are closed."
        ),
        "summary": {
            "n_publishers": len(publishers),
            "n_rows": n_total,
            "n_rows_vat_unknown": n_unknown,
            "pct_rows_vat_unknown": round(100.0 * n_unknown / n_total, 1) if n_total else None,
        },
        "publishers": dict(sorted(publishers.items())),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--print", action="store_true", dest="echo", help="echo the JSON to stdout")
    args = ap.parse_args()
    matrix = build_matrix()
    OUTPUT_PATH.write_bytes(orjson.dumps(matrix, option=orjson.OPT_INDENT_2))
    print(f"wrote {OUTPUT_PATH} ({matrix['summary']['n_publishers']} publishers, "
          f"{matrix['summary']['pct_rows_vat_unknown']}% rows vat-unknown)")
    if args.echo:
        print(orjson.dumps(matrix, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()
