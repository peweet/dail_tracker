import unicodedata
import re
import polars as pl
import pandas as pd
# # https://docs.python.org/3/library/unicodedata.html#unicodedata.normalize
def normalise_df_td_name(df : pl.DataFrame ) -> pl.DataFrame:
    print('normalising names in dataframe...')
    if type(df) == pd.DataFrame:
       print('Pandas dataframe detected, converting to Polars DataFrame for normalization.')
       df = pl.from_pandas(df)
    if 'first_name' not in df.columns or 'last_name' not in df.columns:
        raise ValueError("DataFrame must contain 'first_name' and 'last_name' columns for normalization.")   
    # both_names = dataframe.select({'first_name', 'last_name'})
    both_names = df.with_columns(pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key').str.to_lowercase())
    full_name = both_names.with_columns(
        #convert special characters to their closest ASCII equivalent (e.g. "Ó Súilleabháin" → "O Suilleabhain")
        pl.col('join_key').str.normalize("NFKC")
        #remove all non-alphabetic characters (e.g. "O'Suilleabhain" → "OSuilleabhain")
        .str.replace_all(r"[^a-z\s]", "").alias('join_key')
        .str.replace_all(r"\s+", ""))   # Remove all whitespace
    return full_name
