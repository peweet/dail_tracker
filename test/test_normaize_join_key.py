import polars as pl
from normalise_join_key import normalise_join_key


def test_normalise_join_key_handles_accents_and_apostrophes():
    assert normalise_join_key("Ó Súilleabháin") == normalise_join_key("O'Sullivan")

def test_member_id_unique():
    df = pl.read_csv("data/silver/members.csv")
    assert df["member_id"].n_unique() == len(df)