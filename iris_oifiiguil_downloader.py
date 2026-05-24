
import calendar
import contextlib
import logging
import os
from datetime import date
import fitz  # PyMuPDF
import requests
from pathlib import Path
# from config import BRONZE_PDF_DIR
# https://irisoifigiuil.ie/archive/2025/january/IR030125.pdf 

IRIS_OIFIGUIL_BASE_URL = "https://irisoifigiuil.ie/archive/"
# range_start = "2025/january/"
# range_end = "2025/december/"


# TODO: replace hardcoded path with `BRONZE_DIR / "iris_oifigiuil"` from config when promoting out of experimental
dest = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")
#IR030625.pdf
session = requests.Session()
#https://registers.centralbank.ie/DownloadsPage.aspx

def all_weekdays_in_year(year: int, weekday: int) -> list[date]:
    dates = []
    for month in range(1, 13):
        weeks = calendar.monthcalendar(year, month)
        days = []
        for week in weeks:
            day = week[weekday]
            if day:
                days.append(day)
        for day in days:
            dates.append(date(year, month, day))
    return dates

def to_numeric(d: date) -> str:
    return d.strftime("%d%m%y")
collected_dates = []
for year in range(2016, 2020 + 1):
    print(f"Calculating Tuesdays and Fridays for {year}...")
    tue_fri = sorted(
        all_weekdays_in_year(year, calendar.TUESDAY) +
        all_weekdays_in_year(year, calendar.FRIDAY)
    )
    collected_dates.extend(tue_fri)
print(f"tue_fri: {collected_dates[:5]} ... {collected_dates[-5:]} (total: {len(collected_dates)})")
download_path = dest 
numeric_dates = [to_numeric(d) for d in collected_dates]
ir_years_to_replace = ["2020", "2021", "2022", "2023"]
iris_oifigul_full_url = []
for d in collected_dates:
    year_str = str(d.year)
    month_name = calendar.month_name[d.month].lower()
    numeric = to_numeric(d)
    prefix = "Ir" if year_str in ir_years_to_replace else "IR"
    full_url = f"{IRIS_OIFIGUIL_BASE_URL}{year_str}/{month_name}/{prefix}{numeric}.pdf"
    iris_oifigul_full_url.append(full_url)
# print(iris_oifigul_full_url)
# logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
# with contextlib.suppress(FileExistsError):
#     os.makedirs(download_path, exist_ok=True)
for url in iris_oifigul_full_url:
    destination = download_path / url.split("/")[-1]
    try:
        response = session.get(url, stream=True, timeout=30)
        print(f"{response.status_code} {url}")
        if response.status_code == 404:
            url = url.replace("/Ir", "/IR") if "/Ir" in url else url.replace("/IR", "/Ir")
            response = session.get(url, stream=True, timeout=30)
            print(f"retry {response.status_code} {url}")
        if response.status_code == 200:
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded: {destination}")
        else:
            logging.warning(f"Skipped {response.status_code}: {url}")
    except Exception as e:
        print(f"ERROR {url}: {e}")
