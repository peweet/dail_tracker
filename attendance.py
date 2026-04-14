# import os
import re                
import fitz  # PyMuPDF
import pandas as pd
from numpy import nan
import os
from pathlib import Path                                                                 
import glob    
from config import MEMBERS_DIR
# --- Dail Eireann 2023, 2024, 2025, 2026 attendance PDF ---

#TODO: this code is currently in the attendance.py file, but it should be refactored into a separate module (e.g. pdf_processing.py) that can be imported and used in the main pipeline script (e.g. main.py) to process the scanned PDFs of TD attendance data, extract the relevant information, and create structured CSV files for analysis. This will help to keep the code organized and modular, and make it easier to maintain and extend in the future as we add more functionality to the pipeline. The pdf_processing module can contain functions for processing different types of PDFs (e.g. attendance, payments, etc.) and can be called from the main pipeline script to perform the necessary processing steps for each type of PDF data.
# def process_attendance_pdfs():
# """Process scanned PDFs of TD attendance data, extract relevant information, and create structured CSV files for analysis."""


dataframes = []
IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")
EXCLUDE_CASES = re.compile(r"^(Member|Sitting|Totals|Total)")
os.chdir(MEMBERS_DIR / "pdf_storage") 
if not Path(MEMBERS_DIR / "aggregated_td_tables.csv").is_file():   
    print("Aggregated payment tables CSV not found. Starting PDF processing to create it...")    
    for pdf in list(glob.glob('*.pdf')): 
        print(f"Processing {pdf}...")                                      
        doc = fitz.open(pdf)  # Open the PDF document using PyMuPDF
        print(f"Number of pages in {pdf}: {doc.page_count}")  # Debug: print the number of pages
        for page in doc: 
            print(f"Processing page {page.number} of {pdf}...")  # Debug: print the current page number
            text = page.get_text("text")
            lines = text.split('\n')
            for line in lines:
                if IRISH_NAME_REGEX.search(line) and not EXCLUDE_CASES.search(line):    
                    names = line.split(maxsplit=1)
                    first_name = names[-1]
                    last_name = " ".join(names[:-1])
                    identifier = line.replace(' ', '_')
            tabs = page.find_tables()
            if len(tabs.tables) == 0:   # no tables found on this page
                continue                # skip footer/disclaimer pages
            for tab in tabs.tables:
                df = tab.to_pandas()
                df.insert(0, "identifier", identifier)  # tag every row with the TD name
                df.insert(1, "first_name", first_name)  # add first_name column
                df.insert(2, "last_name", last_name)    # add last_name column
                dataframes.append(df)  # append this DataFrame to the list of DataFrames
    df = pd.concat(dataframes).drop(['Col1', 'Col2','Col3', 'Col4', 'Col5'], axis=1)  # make concatenated DataFrame and drop the 2 empty columns
    # take only the first 5 columns (date + 4 attendance types); drop any extra columns that may have been created by camelot's parsing artifacts
    df = df.iloc[:, :5].fillna(nan) 
    df = df.replace('', nan).rename(columns={'Sitting days attendance recorded on system': 'sitting_days_attendance', 'Other days attendance recorded on system *' : 'other_days_attendance'}) 
    df = df.dropna(subset=['sitting_days_attendance', 'other_days_attendance'], how='all')
    df.to_csv(MEMBERS_DIR / "aggregated_td_tables.csv", index=False) 
else:
    print("Aggregated payment tables CSV already exists. Skipping PDF processing.")
    print('Final DataFrame with attendance counts:')
df = pd.read_csv(MEMBERS_DIR / "aggregated_td_tables.csv")

# Extract year from both columns using slicing and splitting, then fill missing year values prioritizing sitting_days_attendance, then fallback to other_days_attendance, and add 'Missing' if both are missing
year_from_sitting = df['sitting_days_attendance'].str.split('/', n=3).str[-1]
year_from_other = df['other_days_attendance'].str.split('/', n=3).str[-1]

# Fill year prioritizing sitting_days_attendance, then fallback, add missing if both are missing
df['year'] = year_from_sitting.fillna(year_from_other).fillna('Missing')

#TODO: much better counts but it is still counting null occurences as 0, 
# which is not ideal. Need to add a check to only count non-empty 
# attendance entries, and add a separate column counting empty entries 
# so we can filter them out in future steps of the pipeline if needed. 
# This is because some TDs have no attendance records for certain years, 
# and we want to be able to distinguish between 0 attendance and missing data.
result = (
    df.groupby(['identifier', 'year'])[['sitting_days_attendance', 'other_days_attendance']]
    .count()
    .reset_index()
    .rename(columns={
        'sitting_days_attendance': 'sitting_days_count',
        'other_days_attendance': 'other_days_count'
    })
)   # Add a column counting non-empty attendance entries
df = pd.merge(df, result, on=['identifier', 'year'], how='left') # Join the counts back to the original DataFrame
df = df.drop('identifier', axis=1) # Drop the identifier column as it's no longer needed

df['sitting_total_days'] = df['sitting_days_count'] + df['other_days_count']

#ISO format the date columns to make them easier to work with in future steps of the pipeline
#TODO: create ISO datetime formatter for all dates for the pipeline along with a check
df['iso_sitting_days_attendance'] = pd.to_datetime(
    df['sitting_days_attendance'],
    format='%d/%m/%Y',
    errors='coerce')
df['iso_other_days_attendance'] = pd.to_datetime(
    df['other_days_attendance'],
    format='%d/%m/%Y',
    errors='coerce')
# df = df.drop(['sitting_days_attendance', 'other_days_attendance', 'identifier'], axis=1)
df.to_csv(MEMBERS_DIR / "aggregated_td_tables.csv", index=False) 
print("TD attendance CSV created successfully.")
if __name__ == "__main__":    
    print("TD attendance CSV created successfully and saved to aggregated_td_tables.csv.")