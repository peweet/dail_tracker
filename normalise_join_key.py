import polars as pl
import logging
import pandas as pd
import re
# # https://docs.python.org/3/library/unicodedata.html#unicodedata.normalize

def normalise_df_td_name(df: pl.DataFrame, col_name: str) -> pl.Series:
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
    elif df is None or df.is_empty():
        logging.warning('Input DataFrame is empty or None. Returning empty DataFrame.')
        return pl.DataFrame()  # Return an empty DataFrame if input is empty or None
    elif type(df) == pl.Series:
        logging.info('Polars Series detected, converting to DataFrame for normalization.')
        df = df.to_frame()
    else:
        logging.info('Polars DataFrame detected, proceeding with normalization.')   
    full_name = df.with_columns(
        # NFD converts accented chars into base letter + combining accent mark to make it easier to join
        pl.col(col_name)
        .str.to_lowercase()
        .str.normalize("NFD") # Normalize Unicode characters to their closest ASCII equivalent (e.g. "Ó Súilleabháin" → "O Suilleabhain")
        .str.replace_all(r"[\u0300-\u036f]", "")
        .str.replace_all(r"[\x27\u2018\u2019\u02BC\u02B9\u0060\u00B4\uFF07]", "")
        .str.replace_all(r"[^a-z\s]", "") # remove any remaining non-alphabetic characters (e.g. spaces, hyphens, etc.) as they cause issues with joining names across datasets
        .str.replace_all(r"^\s*(dr|prof|rev|fr|sr|mr|mrs|ms|miss|br)\s+", "") # remove honorifics as they cause issues with joining names across datasets (e.g. Dr. John Smith becomes John Smith, which is easier to match with the same name in another dataset that doesn't include the honorific)
        .str.replace_all(r"\s+", "")# Remove all whitespace
        .str.extract_all(r".")    # Extract individual characters into a list       
        .list.sort() # Sort the list of characters alphabetically (e.g. "OSuilleabhain" → "Oabhiillnsuu")
        .list.join("").alias('join_key') # Join the sorted characters back into a string
        )
    return full_name

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Testing normalise_df_td_name function with sample data...")
    sample_data = pl.DataFrame({
        'Full_Name': ['Ó Súilleabháin', "O'Sullivan", 'Sullivan', 'Smith', 'Richard Boyd Barrett', 'Barrett Richard Boyd', 'Dr. John Smith', 'John Smith', 'Richard,Boyd Barrett']
    })
    logging.info(f"Sample data:\n{sample_data}")
    normalized_df = normalise_df_td_name(sample_data, 'Full_Name')
    logging.info(f"Normalized DataFrame:\n{normalized_df}")
