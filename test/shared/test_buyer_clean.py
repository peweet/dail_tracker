"""Contract for shared.buyer_clean — the single source of truth that strips OGP org-id /
school-roll debris off public-buyer names, used by both the eTenders live-tenders lane and
the TED extractors. The rule that must never regress: identifier suffixes go, real
acronyms/place-names stay.
"""

from __future__ import annotations

import polars as pl
import pytest

from shared.buyer_clean import clean_buyer_display, clean_name_expr, org_id_expr


def _clean(names: list[str]) -> list[str]:
    df = pl.DataFrame({"buyer": names})
    return df.with_columns(clean_name_expr("buyer").alias("buyer"))["buyer"].to_list()


def _org_ids(names: list[str]) -> list[str | None]:
    df = pl.DataFrame({"buyer": names})
    return df.with_columns(org_id_expr("buyer").alias("id"))["id"].to_list()


def test_strips_org_id_suffix():
    assert _clean(["Cork County Council_424", "BirdWatch Ireland_85627"]) == [
        "Cork County Council",
        "BirdWatch Ireland",
    ]


def test_lifts_org_id_into_its_own_value():
    assert _org_ids(["Cork County Council_424", "Holy Cross National School"]) == ["424", None]


def test_strips_school_roll_number():
    assert _clean(["Scoil Ailbhe - (18030I)", "Gaelscoil Na Fuinseoige - (20487T)"]) == [
        "Scoil Ailbhe",
        "Gaelscoil Na Fuinseoige",
    ]


@pytest.mark.parametrize(
    "name",
    [
        "Health Information and Quality Authority (HIQA)",  # acronym
        "Transport Infrastructure Ireland (TII)",
        "Loreto Secondary School (Navan)",                  # place-name
        "Scoil Mhuire (Howth)",
    ],
)
def test_preserves_acronyms_and_place_names(name):
    # A parenthetical that does NOT start with a digit is a real name part, never stripped.
    assert _clean([name]) == [name]


def test_idempotent():
    once = _clean(["Roscommon County Council_424"])
    twice = _clean(once)
    assert once == twice == ["Roscommon County Council"]


def test_clean_buyer_display_is_noop_when_column_absent():
    df = pl.DataFrame({"other": ["x"]})
    assert clean_buyer_display(df, "buyer_name").equals(df)
