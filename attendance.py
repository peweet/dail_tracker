# import os
from ast import pattern
import re                
import fitz  # PyMuPDF
import pandas as pd
from numpy import nan
import os
from pathlib import Path                                                                 
import glob    
from config import DATA_DIR
# --- Dail Eireann 2023, 2024, 2025, 2026 attendance PDF ---

#TODO: this code is currently in the attendance.py file, but it should be refactored into a separate module (e.g. pdf_processing.py) that can be imported and used in the main pipeline script (e.g. main.py) to process the scanned PDFs of TD attendance data, extract the relevant information, and create structured CSV files for analysis. This will help to keep the code organized and modular, and make it easier to maintain and extend in the future as we add more functionality to the pipeline. The pdf_processing module can contain functions for processing different types of PDFs (e.g. attendance, payments, etc.) and can be called from the main pipeline script to perform the necessary processing steps for each type of PDF data.
# def process_attendance_pdfs():
# """Process scanned PDFs of TD attendance data, extract relevant information, and create structured CSV files for analysis."""

dataframes = []
IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")
EXCLUDE_CASES = re.compile(r"^(Member|Sitting|Totals|Total)")
DATE_RANGE = re.compile(r"(\d{1,2}-[a-zA-Z]+-\d{4})-to-(\d{1,2}-[a-zA-Z]+-\d{4})")
date_range = ''

# PDF_DIR = Path("bronze/pdf/attendance")

os.chdir(DATA_DIR /"attendance"/ "pdf_storage") 
if not Path(DATA_DIR / "silver"/ "aggregated_td_tables.csv").is_file():   
    print("Aggregated payment tables CSV not found. Starting PDF processing to create it...")    
    for pdf in list(glob.glob('*.pdf')): 
        #for now below snippet is experimental to test dange ranges extracted from PDF titles, but it can be rolled into a more robust 
        # function that extracts date ranges from PDF titles and 
        # tags the resulting CSVs with the date range for easier tracking and 
        # analysis of attendance patterns over time. 
        # This will allow us to analyze attendance data in relation to specific time periods, 
        # and identify any trends or patterns in attendance that may be relevant for our 
        # analysis of TD behavior and potential correlations with other factors such as 
        # payments, lobbying activities, and member metadata.
        ############################################################
        pdf_path = Path(pdf)
        print(pdf_path.stem.title())
        match = re.search(DATE_RANGE, pdf_path.stem.lower())
        if match:
              date_range = f"{match.group(1)}_to_{match.group(2)}"
        else:
            date_range = "unknown"
        #############################################################
        
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
    year_from_sitting = df['sitting_days_attendance'].str.split('/', n=3).str[-1] 
    year_from_other = df['other_days_attendance'].str.split('/', n=3).str[-1]
    df['year'] = year_from_sitting.fillna(year_from_other).fillna('Missing')
    df['iso_sitting_days_attendance'] = pd.to_datetime(
        df['sitting_days_attendance'],
        format='%d/%m/%Y',
        errors='coerce')
    df['iso_other_days_attendance'] = pd.to_datetime(
        df['other_days_attendance'],
        format='%d/%m/%Y',
        errors='coerce')
    df.to_csv(DATA_DIR / "silver"/ "aggregated_td_tables.csv", index=False) 
else:
    print("Aggregated payment tables CSV already exists. Skipping PDF processing.")
    print('Final DataFrame with attendance counts:')
df = pd.read_csv(DATA_DIR / "silver"/ "aggregated_td_tables.csv")

df['sitting_flag'] = df['iso_sitting_days_attendance'].notna().astype(int)
df['other_flag'] = df['iso_other_days_attendance'].notna().astype(int)

df['sitting_days_count'] = df.groupby(['identifier', 'year'])['sitting_flag'].transform('sum')
df['other_days_count'] = df.groupby(['identifier', 'year'])['other_flag'].transform('sum')

df['sitting_total_days'] = df['sitting_days_count'] + df['other_days_count']

df = df.drop(['sitting_flag', 'other_flag'], axis=1)
df.to_csv(DATA_DIR / "silver"/ "aggregated_td_tables.csv", index=False) 
print("date range extracted from title:", date_range)
print("TD attendance CSV created successfully.")
if __name__ == "__main__":    
    print("TD attendance CSV created successfully and saved to aggregated_td_tables.csv.")