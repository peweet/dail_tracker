"""Cross-match VALIDITY probe — is a normalised-name join to each dataset actually
same-entity, or a spurious collision? (Run before wiring any Phase-2 cross-ref.)

For every distinct witness org in the silver, this shows — per candidate dataset — the
RAW source record(s) the normalised key would join to, plus two false-positive signals:
  - key_ambiguity: how many DISTINCT raw source names collapse to that same key (>1 = the
    key is not a unique identifier on that side; the "match" may be the wrong entity).
  - key tokens / length: single-token or short keys match promiscuously.

We judge validity by eyeballing the raw matched names next to the witness org — not by a
match-rate number. Output: VALIDITY.md.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import polars as pl  # noqa: E402

from shared.name_norm import name_norm_expr  # noqa: E402

GOLD = _REPO / "data" / "gold" / "parquet"
SILVER = _REPO / "data" / "silver"
OUT = Path(__file__).resolve().parent / "VALIDITY.md"


def key_index(df: pl.DataFrame, raw_col: str) -> dict[str, set[str]]:
    """norm_key -> set of distinct RAW names that map to it (for ambiguity + eyeball)."""
    sub = df.select(
        pl.col(raw_col).alias("raw"),
        name_norm_expr(raw_col).alias("k"),
    ).drop_nulls().filter(pl.col("k").str.len_chars() > 0)
    idx: dict[str, set[str]] = defaultdict(set)
    for raw, k in sub.iter_rows():
        idx[k].add(raw)
    return idx


def main() -> None:
    witnesses = pl.read_parquet(SILVER / "committee_evidence" / "committee_witnesses.parquet")
    orgs = witnesses.select("witness_org").unique().sort("witness_org")["witness_org"].to_list()

    # Build raw-name indexes for each candidate dataset.
    pay = pl.read_parquet(GOLD / "procurement_payments_fact.parquet")
    idx = {
        "payments_payee": key_index(pay.select(pl.col("supplier_raw")), "supplier_raw"),
        "payments_publisher": key_index(pay.select("publisher_name"), "publisher_name"),
        "procurement_awards": key_index(pl.read_parquet(GOLD / "procurement_awards.parquet").select("supplier"), "supplier"),
        # FULL lobbying register: registrants AND clients (the risky, large set)
        "lobby_registrant": key_index(pl.read_parquet(SILVER / "lobbying" / "parquet" / "lobbyist_returns_detail.parquet").select("lobbyist_name"), "lobbyist_name"),
        "lobby_client": key_index(pl.read_parquet(SILVER / "lobbying" / "parquet" / "client_company_returns_detail.parquet").select("client_name"), "client_name"),
        "councils": key_index(pl.read_csv(_REPO / "data" / "_meta" / "la_chief_executives.csv").select("council_name"), "council_name"),
    }

    def keyfor(s: str) -> str:
        return pl.DataFrame({"n": [s]}).select(name_norm_expr("n"))["n"][0]

    lines = ["# Cross-match VALIDITY probe\n",
             "_For each witness org: the RAW records its normalised key joins to, per dataset. "
             "`amb` = # distinct raw names sharing that key (>1 ⇒ key not unique on that side). "
             "Judge same-entity by eye; flag generic/short keys._\n"]

    for org in orgs:
        k = keyfor(org)
        ntok = len(k.split())
        hits = []
        for ds, index in idx.items():
            if k in index:
                raws = sorted(index[k])
                hits.append((ds, len(raws), raws))
        if not hits:
            continue
        flag = " ⚠️SHORT/GENERIC" if (ntok < 2 or len(k) < 8) else ""
        lines.append(f"\n### {org}")
        lines.append(f"`key={k!r}` tokens={ntok}{flag}")
        for ds, amb, raws in hits:
            shown = "; ".join(raws[:4]) + (f"  …(+{len(raws) - 4})" if len(raws) > 4 else "")
            ambflag = "  ⚠️AMBIGUOUS" if amb > 1 else ""
            lines.append(f"- **{ds}** (amb={amb}{ambflag}): {shown}")

    # Summary: how many orgs match, how many only via short/generic or ambiguous keys
    matched = sum(1 for org in orgs if any(keyfor(org) in index for index in idx.values()))
    lines.insert(2, f"\n**{matched}/{len(orgs)} distinct witness orgs match ≥1 dataset.** "
                    "Validity (not count) is the question below.\n")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {OUT}  ({matched}/{len(orgs)} orgs matched)")


if __name__ == "__main__":
    main()
