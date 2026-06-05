"""PROBE (throwaway): what is the real quality distribution of entity_name on
corporate notices? This caps BOTH the CRO join (probe A) and search recall.

Tests the D2 assumption: that entity_name failures fall into a small set of
classifiable states we can badge, rather than one undifferentiated "junk" bucket.

States:
  not_extracted        - null/empty
  sentence_fragment    - captured prose, not a name ("... be wound up", "having
                         its registered office", embedded quotes, lowercase verbs)
  boilerplate          - notice formula text (matches corporate.py _JUNK_RE)
  too_short            - normalises to < 4 chars
  clean                - looks like a usable company name

Run:  .venv/Scripts/python.exe pipeline_sandbox/probe_entity_quality.py
Reads only; writes nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from shared.name_norm import name_norm_expr  # noqa: E402

NOTICES = ROOT / "data/gold/parquet/corporate_notices.parquet"

BOILERPLATE_RE = re.compile(
    r"NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET",
    re.I,
)
# sentence-fragment tells: prose verbs / clause connectors / stray quotes that a
# clean company name would never contain.
FRAGMENT_RE = re.compile(
    r"\bhaving its\b|\bbe wound up\b|\bhas been struck\b|\bregistered office\b|"
    r"\bthe \"|\bwhose\b|\bin exercise\b|\bpursuant\b|\bnotice\b|^\(|\bduly\b|\bresolved\b",
    re.I,
)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def classify(name: str | None, norm: str) -> str:
    if name is None or name.strip() == "":
        return "not_extracted"
    if BOILERPLATE_RE.search(name):
        return "boilerplate"
    if FRAGMENT_RE.search(name) or '"' in name or name.count(",") >= 2:
        return "sentence_fragment"
    if len(norm) < 4:
        return "too_short"
    return "clean"


def main() -> None:
    n = pl.read_parquet(NOTICES).with_columns(name_norm_expr("entity_name").alias("nn"))
    rows = n.select(["entity_name", "nn", "notice_subtype"]).to_dicts()
    states = [classify(r["entity_name"], r["nn"] or "") for r in rows]
    res = n.with_columns(pl.Series("quality_state", states))

    hr("ENTITY-NAME QUALITY DISTRIBUTION")
    dist = res.group_by("quality_state").len().sort("len", descending=True)
    for row in dist.iter_rows(named=True):
        print(f"  {row['quality_state']:<18} {row['len']:>7,}  ({row['len'] / res.height:.1%})")
    clean = res.filter(pl.col("quality_state") == "clean").height
    print(f"\n  CLEAN (search/join-ready): {clean:,}  ({clean / res.height:.1%})")

    hr("QUALITY BY NOTICE SUBTYPE (clean fraction)")
    by = (
        res.group_by("notice_subtype")
        .agg(pl.len().alias("n"), (pl.col("quality_state") == "clean").sum().alias("clean"))
        .with_columns((pl.col("clean") / pl.col("n")).alias("clean_frac"))
        .sort("n", descending=True)
    )
    print(by)

    hr("SAMPLE: sentence_fragment (the recoverable-with-better-extraction bucket)")
    print(res.filter(pl.col("quality_state") == "sentence_fragment").select("entity_name").head(10))

    hr("SAMPLE: clean")
    print(res.filter(pl.col("quality_state") == "clean").select("entity_name").head(8))

    hr("EXPECTATION CHECKS")
    frag = res.filter(pl.col("quality_state") == "sentence_fragment").height
    print(f"  clean fraction: {clean / res.height:.1%}")
    print(f"  [{'PASS' if clean / res.height > 0.6 else 'CHECK'}] clean > 60%")
    print(f"  recoverable (sentence_fragment) = {frag:,} — these are extraction bugs, not absent data")


if __name__ == "__main__":
    main()
