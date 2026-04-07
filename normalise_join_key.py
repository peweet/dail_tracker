import polars as pl
import pandas as pd
import logging
# # https://docs.python.org/3/library/unicodedata.html#unicodedata.normalize
def normalise_df_td_name(df : pl.DataFrame | pl.Series) -> pl.DataFrame| pl.Series:
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
    It is an intermediate step in the data enrichment pipeline, used to join attendance records with member metadata.
    """
    logging.info('Normalising names in dataframe...')
    if type(df) == pd.DataFrame:
       logging.info('Pandas dataframe detected, converting to Polars DataFrame for normalization.')
       df = pl.from_pandas(df)
    elif 'first_name' not in df.columns or 'last_name' not in df.columns:
        raise ValueError("DataFrame must contain 'first_name' and 'last_name' columns for normalization.")   
    elif df is None or df.is_empty():
        logging.warning('Input DataFrame is empty or None. Returning empty DataFrame.')
        return pl.DataFrame()  # Return an empty DataFrame if input is empty or None
    elif type(df) == pl.Series:
        logging.info('Polars Series detected, converting to DataFrame for normalization.')
        df = df.to_frame()
    # both_names = dataframe.select({'first_name', 'last_name'})
    both_names = df.with_columns(
        pl.concat_str(
        pl.col(['first_name', 'last_name'])
        ).alias('join_key')
        )
    full_name = both_names.with_columns(
        # NFD converts accented chars into base letter + combining accent mark to make it easier to join
        pl.col('join_key')
        .str.to_lowercase()
        .str.replace_all(r"[\x27\u2019]", "") #remove apostrophes as they cause too many issues with joining names across datasets (e.g. O'Sullivan becomes OSullivan, which is easier to match with the same name in another dataset that doesn't include the apostrophe)
        .str.replace_all(r"[\u0300-\u036f]", "") # remove accents and fadas as it becomes too difficult to join names with special characters otherwise (e.g. O'Sullivan, Ó Súilleabháin)
        .str.replace_all(r"[^a-z\s]", "")
        .str.normalize("NFD").alias('join_key')
        .str.replace_all(r"\s+", "")# Remove all whitespace
        .str.extract_all(r".")    # Extract individual characters into a list       
        .list.sort() # Sort the list of characters alphabetically (e.g. "OSuilleabhain" → "Oabhiillnsuu")
        .list.join("") # Join the sorted characters back into a string
        )
    return full_name
