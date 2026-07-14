"""Fetch the IPAS weekly statistics PDF embedded in the C&AG chapter and test it
for COUNTY-level IP-applicant data — the real source behind Fig 10.2's per-county
choropleth (which the C&AG published only as bands). SANDBOX ONLY."""
import re
import fitz
import requests
from _common import BRONZE

URL = "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.gov.ie/"}

COUNTIES = ["Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin", "Galway", "Kerry",
            "Kildare", "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth",
            "Mayo", "Meath", "Monaghan", "Offaly", "Roscommon", "Sligo", "Tipperary",
            "Waterford", "Westmeath", "Wexford", "Wicklow"]

d = BRONZE / "ipas_weekly"
d.mkdir(parents=True, exist_ok=True)
p = d / "29122024-ipas-stats-weekly-report.pdf"
if not p.exists():
    r = requests.get(URL, headers=UA, timeout=45)
    r.raise_for_status()
    p.write_bytes(r.content)
    print(f"fetched {len(r.content)/1024:.0f} KB")

doc = fitz.open(p)
text = "".join(pg.get_text("text") for pg in doc)
print(f"pages: {doc.page_count}, chars: {len(text)}")

# does it break down by county?
found = [c for c in COUNTIES if re.search(rf"\b{c}\b", text)]
print(f"\ncounties named in the PDF: {len(found)}/26 -> {found}")

# show the lines around county mentions to see if there are numbers attached
lines = text.splitlines()
print("\n--- county-context lines (first 40) ---")
shown = 0
for i, ln in enumerate(lines):
    if any(re.search(rf"\b{c}\b", ln) for c in COUNTIES) and shown < 40:
        ctx = " | ".join(x.strip() for x in lines[i:i+3] if x.strip())
        print(f"  {ctx[:120]}")
        shown += 1

# any per-1000 / population / per capita language?
for kw in ("per 1,000", "per 1000", "per capita", "population", "county"):
    n = len(re.findall(kw, text, re.I))
    print(f"  '{kw}': {n} hits")
