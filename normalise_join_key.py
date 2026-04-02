import unicodedata
import re
import polars as pl
import pandas as pd
# # https://docs.python.org/3/library/unicodedata.html#unicodedata.normalize
def normalise_df_td_name(df : pl.DataFrame ) -> pl.DataFrame:
    docstring = """
    Normalises TD names in the given DataFrame by:
    1. Concatenating first and last names into a single 'join_key' column.
    2. Converting all characters to lowercase. 
    3. Removing all non-alphabetic characters (e.g. spaces, apostrophes).
    4. Normalizing Unicode characters to their closest ASCII equivalent (e.g. "Ó
         Súilleabháin" → "O Suilleabhain").
    5. Sorting the characters in the name alphabetically (e.g. "OSuilleabhain" →
         "Oabhiillnsuu").
    This process creates a 'join_key' that can be used to match 
    TD names across different datasets, even if there are variations in spelling, formatting, or special characters.
    """
    print('normalising names in dataframe...')
    if type(df) == pd.DataFrame:
       print('Pandas dataframe detected, converting to Polars DataFrame for normalization.')
       df = pl.from_pandas(df)
    if 'first_name' not in df.columns or 'last_name' not in df.columns:
        raise ValueError("DataFrame must contain 'first_name' and 'last_name' columns for normalization.")   
    # both_names = dataframe.select({'first_name', 'last_name'})
    both_names = df.with_columns(pl.concat_str(pl.col(['first_name', 'last_name'])).alias('join_key').str.to_lowercase())
    full_name = both_names.with_columns(
        # NFD decomposes accented chars into base letter + combining accent mark
        # e.g. á → a + U+0301, é → e + U+0301, ó → o + U+0301
        pl.col('join_key').str.normalize("NFD")
        # [\u0300-\u036f] is the Unicode "Combining Diacritical Marks" block.
        # After NFD, accents become separate code points in this range.
        # Stripping them converts á→a, é→e, ó→o etc. while keeping the base letter.
        # NFKC alone would DELETE accented chars via the [^a-z] step, losing letters entirely.
        .str.replace_all(r"[\u0300-\u036f]", "")
        .str.replace_all(r"[^a-z\s]", "").alias('join_key')
        .str.replace_all(r"\s+", "")# Remove all whitespace
        .str.extract_all(r".")    # Extract individual characters into a list       
        .list.sort() # Sort the list of characters alphabetically (e.g. "OSuilleabhain" → "Oabhiillnsuu")
        .list.join("") # Join the sorted characters back into a string
        )   
    return full_name
