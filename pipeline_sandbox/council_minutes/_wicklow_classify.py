import requests, re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

SEED = "https://www.wicklow.ie/Living/Your-Council/Council-Meetings/Minutes-Agendas"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
       "Accept-Encoding": "gzip, deflate"}

r = requests.get(SEED, headers=HDR, timeout=40)
soup = BeautifulSoup(r.text, "html.parser")
pdfs = []
for a in soup.find_all("a", href=True):
    u = urljoin(SEED, a["href"])
    if u.lower().endswith(".pdf"):
        t = a.get_text(" ", strip=True)
        pdfs.append((t, u))

# categorize by keyword
cats = {}
for t,u in pdfs:
    tl = t.lower()
    if "agenda" in tl: c="AGENDA"
    elif "minute" in tl: c="MINUTES"
    elif "transcript" in tl: c="TRANSCRIPT"
    else: c="OTHER"
    cats.setdefault(c, []).append((t,u))

for c in ("AGENDA","MINUTES","TRANSCRIPT","OTHER"):
    print(f"=== {c}: {len(cats.get(c,[]))}")

print("\n--- AGENDA docs (full) ---")
for t,u in cats.get("AGENDA", []):
    print(repr(t))

print("\n--- OTHER docs (first 30) ---")
for t,u in cats.get("OTHER", [])[:30]:
    print(repr(t), "|", u)
