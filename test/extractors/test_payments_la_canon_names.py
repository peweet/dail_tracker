"""Guards for the local-authority publisher-name canonicaliser in the payments consolidator.

WHY THIS TEST EXISTS. Two lanes feed councils into the gold payments fact with different
spellings — la_payments_fact uses "Dublin City", disclosed_bq_po_newbodies uses "Dublin City
Council". The mismatch silently orphaned 69,715 rows (41% of all LA payments, incl. Dublin
City's 40,431) from the constituency crosswalk AND from the AFS accounts fact, whose join key
is `payments.publisher_name == afs.council`.

THE TRAP (hit for real while fixing it): resolving names by stripping "city"/"county" and
comparing the remainder collapses "Cork City" and "Cork County" onto the SAME key — and one
overwrites the other. The first implementation relabelled Cork City's 5,323 rows as Cork County.
The collision test below is the whole point of this file; do not delete it.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

pl = pytest.importorskip("polars")

from procurement_payments_consolidate import _canon_la_publisher_names  # noqa: E402

FACT = ROOT / "data" / "gold" / "parquet" / "procurement_payments_fact.parquet"
XWALK = ROOT / "data" / "_meta" / "constituency_la_crosswalk.csv"


def _canon() -> set[str]:
    with XWALK.open(encoding="utf-8") as fh:
        return {r["local_authority"] for r in csv.DictReader(fh)}


def _run(names: list[str]) -> list[str]:
    df = pl.DataFrame({"publisher_name": names, "publisher_type": ["local_authority"] * len(names)})
    return _canon_la_publisher_names(df)["publisher_name"].to_list()


# ── the collision (the reason this file exists) ────────────────────────────────────────────────
def test_city_and_county_never_collide():
    """Cork City must NEVER be relabelled as Cork County (or Galway likewise)."""
    got = _run(["Cork City", "Cork County", "Galway City", "Galway County"])
    assert got == ["Cork City", "Cork County", "Galway City", "Galway County"]


def test_city_and_county_long_forms_resolve_distinctly():
    assert _run(["Cork City Council", "Cork County Council"]) == ["Cork City", "Cork County"]
    assert _run(["Galway City Council", "Galway County Council"]) == ["Galway City", "Galway County"]


# ── the actual fix ─────────────────────────────────────────────────────────────────────────────
def test_formal_names_resolve_to_canonical():
    got = _run(
        [
            "Carlow County Council",
            "Dublin City Council",
            "Dún Laoghaire-Rathdown County Council",
            "Tipperary County Council",
        ]
    )
    assert got == ["Carlow", "Dublin City", "Dun Laoghaire-Rathdown", "Tipperary"]


def test_already_canonical_names_pass_through_untouched():
    canon = sorted(_canon())
    assert _run(canon) == canon


def test_unmappable_council_fails_loudly_rather_than_orphaning():
    with pytest.raises(SystemExit):
        _run(["Atlantis County Council"])


# ── the fact itself ────────────────────────────────────────────────────────────────────────────
@pytest.mark.skipif(not FACT.exists(), reason="gold payments fact not present")
def test_every_la_publisher_in_gold_joins_the_crosswalk():
    """The regression that started this: 8 councils were unjoinable. Must stay at zero."""
    df = pl.read_parquet(FACT, columns=["publisher_name", "publisher_type"])
    la = set(df.filter(pl.col("publisher_type") == "local_authority")["publisher_name"].unique().to_list())
    orphans = la - _canon()
    assert not orphans, f"LA publishers absent from the crosswalk (they orphan from AFS too): {sorted(orphans)}"
