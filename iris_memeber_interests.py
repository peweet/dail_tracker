import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import fitz
import pandas as pd


from config import BRONZE_DIR  # noqa: E402
#https://www.irisoifigiuil.ie/archive/2025/june/IR130625.pdf
#https://www.irisoifigiuil.ie/archive/2023/march/Ir030323.pdf
# text =

# Name of Member concerned: Christopher O’Sullivan TD
# Category of Registrable Interest(s) concerned:
# 3 – DIRECTORSHIPS
# (i) Nature of directorship (e.g. whether chair, executive, non-
# executive, de facto or shadow)
# Director – Voluntary Organisation
# (ii) Name and registered address of company in which the
# directorship was held by you
# Clonakilty Community Hall, Association Company Limited
# by Guarantee, 13 Rossa St., Clonakilty, Co. Cork P85 EY65
# (iii) Nature of business of the company
# Community Hall in Clonakilty
# (iv) Other information that you believe may be relevant (Note:
# Completion of this heading is entirely voluntary)
# PETER FINNEGAN
# Cléireach Dháil Éireann
# (Clerk of Dáil Éireann
# An 13 Meitheamh 2025 This 13 day of June 2025
# [G-11]

# Notice is given herewith that a statement of registrable interests has
# been made in respect of the registration period 1st January 2021 to
# 31st December 2021 in accordance with the provisions of section 29
# of the above-mentioned Acts as follows:

#https://opendata.cro.ie/dataset/companies/resource/e64eb540-fb97-44c2-b461-766f2babbdf6
# That law defines an SI as being “an order, regulation, rule, scheme or bye-law made in exercise of a power conferred by statute”. It’s that last bit that we’re concerned with here: the ‘conferred by statute’ part.

#https://opendata.cro.ie/dataset/companies

#eg: https://www.irishstatutebook.ie/eli/2026/si/80/made/en/print
#https://www.irishstatutebook.ie/eli/2025/si
PDF_DIR = BRONZE_DIR / "iris_oifigiuil"
# OUT_CSV = Path(__file__).with_name("iris_member_interests.csv")
pdfs_that_have_member_interest_info = []
for pdf_path in PDF_DIR.glob("*.pdf"):
    fitz_doc = fitz.open(pdf_path)
    for page in fitz_doc:
        text = page.get_text("text")
        if "Name of Member concerned:" in text:
            print(f"Processing {pdf_path}...")
            name_match = re.search(r"Name of Member concerned:\s*(.+)", text)
            if name_match:
                print("Found match for member name in text.")
                member_name = name_match.group(1).strip()
                pdfs_that_have_member_interest_info.append((pdf_path.name, member_name))
            else:
                print(f"Could not extract member name from {pdf_path}")
with open("iris_member_interests.csv", "w") as f:
    f.write("pdf_filename,member_name\n")
    for pdf_filename, member_name in pdfs_that_have_member_interest_info:
        f.write(f"{pdf_filename},{member_name}\n")