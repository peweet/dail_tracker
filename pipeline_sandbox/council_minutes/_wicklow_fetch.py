import requests, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

SEED = "https://www.wicklow.ie/Living/Your-Council/Council-Meetings/Minutes-Agendas"
HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
       "Accept-Encoding": "gzip, deflate"}

r = requests.get(SEED, headers=HDR, timeout=40)
print("STATUS", r.status_code, "LEN", len(r.text))
soup = BeautifulSoup(r.text, "html.parser")

# Collect all links
links = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(" ", strip=True)
    full = urljoin(SEED, href)
    links.append((txt, full))

# PDFs
pdfs = [(t,u) for t,u in links if u.lower().endswith(".pdf")]
print("\n=== PDF LINKS:", len(pdfs))
for t,u in pdfs[:60]:
    print(repr(t), "|", u)

# Candidate sub-pages (year / meeting related)
print("\n=== CANDIDATE SUBPAGES:")
seen=set()
for t,u in links:
    low = (t+" "+u).lower()
    if any(k in low for k in ["minute","agenda","meeting","2024","2025","2026","council"]) and not u.lower().endswith(".pdf"):
        if u not in seen and u.startswith("http") and "wicklow.ie" in u:
            seen.add(u)
            print(repr(t[:60]), "|", u)
