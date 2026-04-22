# import os
import glob
import os
import re
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd
from numpy import nan

from config import ATTENDANCE_PDF_DIR, DATA_DIR

# --- Dail Eireann 2023, 2024, 2025, 2026 attendance PDFs ---

dataframes = []
IRISH_NAME_REGEX = re.compile(r"^[A-ZÁÉÍÓÚ][a-zA-ZáéíóúÁÉÍÓÚ'\s\-]+$")
EXCLUDE_CASES = re.compile(r"^(Member|Sitting|Totals|Total)")
DATE_RANGE = re.compile(r"(\d{1,2}-[a-zA-Z]+-\d{4})-to-(\d{1,2}-[a-zA-Z]+-\d{4})")
date_range = ""

# PDF_DIR = Path("bronze/pdf/attendance")

os.chdir(ATTENDANCE_PDF_DIR)
csv_path = DATA_DIR / "silver" / "aggregated_td_tables.csv"
if not Path(csv_path).is_file():
    print("Aggregated payment tables CSV not found. Starting PDF processing to create it...")
    for pdf in list(glob.glob("*.pdf")):
        pdf_path = Path(pdf)
        print(pdf_path.stem.title())
        match = re.search(DATE_RANGE, pdf_path.stem.lower())
        if match:
            date_range = f"{match.group(1)}_to_{match.group(2)}"
        else:
            date_range = "unknown"
        print(f"Processing {pdf}...")
        doc = fitz.open(pdf)
        print(f"Number of pages in {pdf}: {doc.page_count}")
        for page in doc:
            print(f"Processing page {page.number} of {pdf}...")
            text = page.get_text("text")
            lines = text.split("\n")
            for line in lines:
                if IRISH_NAME_REGEX.search(line) and not EXCLUDE_CASES.search(line):
                    names = line.split(maxsplit=1)
                    first_name = names[-1]
                    last_name = " ".join(names[:-1])
                    identifier = line.replace(" ", "_")
            tabs = page.find_tables()
            if len(tabs.tables) == 0:
                continue
            for tab in tabs.tables:
                df = tab.to_pandas()
                df.insert(0, "identifier", identifier)
                df.insert(1, "first_name", first_name)
                df.insert(2, "last_name", last_name)
                dataframes.append(df)
    # --- ORIGINAL BLOCK (for easy reversion) ---
    # df = pd.concat(dataframes).drop(['Col1', 'Col2','Col3', 'Col4', 'Col5'], axis=1)
    # df = df.iloc[:, :5].fillna(nan)
    # df = df.replace('', nan).rename(columns={'Sitting days attendance recorded on system': 'sitting_days_attendance', 'Other days attendance recorded on system *' : 'other_days_attendance'})
    # df = df.dropna(subset=['sitting_days_attendance', 'other_days_attendance'], how='all')
    # --- FIXED BLOCK: robust to missing columns ---
    df = pd.concat(dataframes).drop(["Col1", "Col2", "Col3", "Col4", "Col5"], axis=1, errors="ignore")
    df = df.iloc[:, :5].fillna(nan)
    df = df.replace("", nan).rename(
        columns={
            "Sitting days attendance recorded on system": "sitting_days_attendance",
            "Other days attendance recorded on system *": "other_days_attendance",
            "Sitting days attendance": "sitting_days_attendance",
            "Other days attendance": "other_days_attendance",
        }
    )
    drop_cols = [c for c in ["sitting_days_attendance", "other_days_attendance"] if c in df.columns]
    if drop_cols:
        df = df.dropna(subset=drop_cols, how="all")
    year_from_sitting = df["sitting_days_attendance"].str.split("/", n=3).str[-1]
    year_from_other = df["other_days_attendance"].str.split("/", n=3).str[-1]
    df["year"] = year_from_sitting.fillna(year_from_other).fillna("Missing")
    df["iso_sitting_days_attendance"] = pd.to_datetime(
        df["sitting_days_attendance"], format="%d/%m/%Y", errors="coerce"
    )
    df["iso_other_days_attendance"] = pd.to_datetime(df["other_days_attendance"], format="%d/%m/%Y", errors="coerce")
    df.to_csv(csv_path, index=False)
else:
    print(f"Aggregated payment tables CSV already exists at {csv_path}. Skipping PDF processing.")
df = pd.read_csv(DATA_DIR / "silver" / "aggregated_td_tables.csv")

df["sitting_flag"] = df["iso_sitting_days_attendance"].notna().astype(int)
df["other_flag"] = df["iso_other_days_attendance"].notna().astype(int)

df["sitting_days_count"] = df.groupby(["identifier", "year"])["sitting_flag"].transform("sum")
df["other_days_count"] = df.groupby(["identifier", "year"])["other_flag"].transform("sum")

df["sitting_total_days"] = df["sitting_days_count"] + df["other_days_count"]

drop_cols = [
    c for c in ["sitting_flag", "other_flag", "sitting_days_attendance", "other_days_attendance"] if c in df.columns
]
if drop_cols:
    df = df.drop(drop_cols, axis=1)
df.to_csv(DATA_DIR / "silver" / "aggregated_td_tables.csv", index=False)

print(f"date range extracted from title: {date_range}")
print("TD attendance CSV created successfully.")
if __name__ == "__main__":
    print("TD attendance CSV created successfully and saved to aggregated_td_tables.csv.")
