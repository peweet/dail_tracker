# import os
import re                
import fitz  # PyMuPDF
import pandas as pd
from numpy import nan
import os                                                                   
import glob    
from config_dummy import MEMBERS_DIR
# --- Dail Eireann 2023, 2024, 2025 attendance PDF ---
dataframes = []
IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")
EXCLUDE_CASES = re.compile(r"^(Member|Sitting|Totals|Total)")
os.chdir(MEMBERS_DIR / "pdf_storage")         
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
            dataframes.append(df)  # append this DataFrame

df = pd.concat(dataframes).drop(['Col1', 'Col2','Col3', 'Col4', 'Col5'], axis=1)  # make concatenated DataFrame and drop the 2 empty columns
# take only the first 5 columns (date + 4 attendance types); drop any extra columns that may have been created by camelot's parsing artifacts
df = df.iloc[:, :5].fillna(nan) 
df = df.replace('', nan).rename(columns={'Sitting days attendance recorded on system': 'sitting_days_attendance', 'Other days attendance recorded on system *' : 'other_days_attendance'}) 
df = df.dropna(subset=['sitting_days_attendance', 'other_days_attendance'], how='all')
result = (
    df.groupby('identifier')[['sitting_days_attendance', 'other_days_attendance']]
    .count()
    .reset_index()
    .rename(columns={
        'sitting_days_attendance': 'sitting_days_count',
        'other_days_attendance': 'other_days_count'
    })
)   # Add a column counting non-empty attendance entries
df = pd.merge(df, result, on='identifier', how='left') # Join the counts back to the original DataFrame
df = df.drop('identifier', axis=1) # Drop the identifier column as it's no longer needed
print('Final DataFrame with attendance counts:')
df['sitting_total_days'] = df['sitting_days_count'] + df['other_days_count']
df.to_csv(MEMBERS_DIR / "aggregated_td_tables.csv", index=False) 
print("TD attendance CSV created successfully.")
if __name__ == "__main__":    
    print("TD attendance CSV created successfully and saved to aggregated_td_tables.csv.")