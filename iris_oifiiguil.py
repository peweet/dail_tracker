
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
for year in range(2022, 2024 + 1):
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
        if response.status_code == 200:
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded: {destination}")
        elif response.status_code == 404:
            logging.warning(f"Skipped {response.status_code}: {url}")
            url = url.replace("Ir", "IR")
            print(url)
            response = session.get(url, stream=True, timeout=30)
            with open(destination, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded with corrected case: {destination}")
        else:
            logging.warning(f"Skipped {response.status_code}: {url}")
    except Exception as e:
        print(f"ERROR {url}: {e}")


# path=fitz.open("https://irisoifigiuil.ie/archive/2025/january/IR030125.pdf")



#exclusion cases = IN THE MATTER OF THE COMPANIES ACT 2014
#idea count the bankruptcies, VOLUNTARY LIQUIDATION

#split conditions 
#__________ = most consistent breaking
#S.I. No. 15 of 2026*

#NAME PATTERN = Mr James Browne T.D
#AGREEMENTS WHICH ENTERED INTO FORCE
#criteria

#SI: Statutory Instrument is a form of secondary legislation in Ireland that allows a government Minister to make detailed laws—such as orders, regulations, or rules—without passing a new Act through the Oireachtas
#Fishing
#Bankruptcy
#Standards ie IRISH STANDARDS
#FÓGRA(NOTICE) law signed?

#IGNORE IN FULL = All notices and advertisements are published in Iris Oifigiúil for general information purposes only, at the risk of
# the advertiser and at the discretion of the Commissioners of Public Works in Ireland (‘‘the
# Commissioners’’).While the Commissioners utilise their best endeavours to ensure that the publication is made in
# accordance with the advertiser’s requirements, the Commissioners make no representations or warranties about
# any of the information in any notice or advertisement and accept no responsibility for the accuracy of any
# information contained in a notice or advertisement. To the fullest extent permitted by applicable law, the
# Commissioners, their servants and agents shall not be liable for loss or damage arising out of, or in connection
# with, the use of, or the inability to use, the information contained in any notice or advertisement or arising out of,
# or in connection with, a failure to meet any requirements of any advertiser or arising out of, or in connection with,
# any inaccuracy, error or omission contained in any notice or advertisement or in respect of those requirements
# even if the Commissioners have been advised of the possibility of such loss or damage, or such loss or damage
# was reasonably foreseeable. The Commissioners reserve the rights not to publish any notice or advertisement and
# to make reasonable changes (such as formatting and proofing) to the content of any notice, or advertisement at
# their sole discretion. Use of Iris Oifigiúil is subject to the above and by using Iris Oifigiúil, the user is signifying
# his or her agreement to the above. If any of the above shall be invalid or unenforceable, that part shall be deemed
# # severable and shall not affect the validity and enforceability of the remaining provisions.
# Ba cheart comhfhreagras maidir leis an Iris Oifigiúil a sheoladh chuig: An tEagarthóir, Iris Oifigiúil, Oifig an
# tSoláthair, Bóthar Bhaile Uí Bheoláin, Baile Átha Cliath 8, D08 XA06. Teil.: 046 942 3413, ríomhphost:
# info@irisoifigiuil.ie. Ní foláir fógraí le cur isteach san Iris Oifigiúil bheith faighte ag Oifig an tSoláthair ar 2.00 p.m.
# ar a dhéanaíar an lá roimh fhoilsiú. Is iad na rátaí ná €20 ar ghearrfhógraí (15 líne nó níos lú). €40 ar fhógraí ceathrú
# leathanach, €80 ar leathleathanach, €120 ar 3/4 leathanach, agus €160 ar leathanach iomlán.
# ______________________________________________
# Communications relating to Iris Oifigiúil should be addressed to The Editor, Iris Oifigiúil, Government Publications
# Office, Mountshannon Road, Dublin 8, D08 XA06. Tel.: 046 942 3413, email: info@irisoifigiuil.ie. Notices for
# insertion in Iris Oifigiúil must reach the Government Publications Office not later than 2 p.m. on the day preceding
# publication. The rates are €20 for short notices (15 lines or fewer). €40 for quarter page notices, €80 for half page,
# €120 for 3/4 page, and €160 for full page.
# Dé Máirt agus Dé hAoine
# _________________________
# BAILE ÁTHA CLIATH
# Le ceannach díreach ó
# FOILSEACHÁIN RIALTAIS,
# BÓTHAR BHAILE UÍ BHEOLÁIN, BAILE ÁTHA CLIATH 8.
# D08 XA06
# (Teil: 046 942 3100 nó 1890 213434)
# nó trí aon díoltóir leabhar.
# _________________________
# Praghas: €5.71
# Tuesday and Friday
# _________________________
# DUBLIN
# To be purchased from
# GOVERNMENT PUBLICATIONS,
# MOUNTSHANNON ROAD, DUBLIN 8.
# D08 XA06
# (Tel: 046 942 3100 or 1890 213434)
# or through any bookseller.
# _________________________