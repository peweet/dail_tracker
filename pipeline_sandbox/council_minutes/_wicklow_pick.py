import requests, re, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import datetime as dt

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

MONTHS = {m.lower():i for i,m in enumerate(
    ["January","February","March","April","May","June","July","August",
     "September","October","November","December"], 1)}

def parse_date(t):
    # patterns like "12th January 2026" or "9th February 2026"
    m = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})', t)
    if m:
        d, mon, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        if mon in MONTHS:
            try: return dt.date(y, MONTHS[mon], d)
            except: return None
    # patterns like "11 01 2021" or "28.11.2022" or "12042021"
    m = re.search(r'(\d{2})[ ./](\d{2})[ ./](\d{4})', t)
    if m:
        try: return dt.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except: return None
    return None

agendas = []
for t,u in pdfs:
    if "agenda" not in t.lower(): continue
    d = parse_date(t)
    if d is None: continue
    if d.year < 2024: continue
    tl = t.lower()
    # full council = ordinary, annual, budget, special, lpt, agm — exclude none, but prefer ordinary
    agendas.append((d, t, u))

agendas.sort(key=lambda x: x[0], reverse=True)
# dedupe by date keeping first
seen=set(); picked=[]
for d,t,u in agendas:
    if d in seen: continue
    seen.add(d); picked.append((d,t,u))

print("Total 2024+ agendas:", len(agendas))
print("\nTop recent (all types):")
for d,t,u in picked[:12]:
    print(d, "|", t)

# Choose 6 most recent ORDINARY full-council meetings (the standard monthly full council)
ordinary = [(d,t,u) for d,t,u in picked if "ordinary" in t.lower() or re.match(r'agenda\s+\d', t.lower()) or ("special" not in t.lower() and "annual" not in t.lower() and "budget" not in t.lower() and "lpt" not in t.lower() and "agm" not in t.lower())]
print("\nChosen 6 most-recent full-council:")
chosen = picked[:6]
for d,t,u in chosen:
    print(d, "|", t, "|", u)

with open("pipeline_sandbox/council_minutes/_wicklow_chosen.json","w",encoding="utf-8") as f:
    json.dump([{"date":d.isoformat(),"title":t,"url":u} for d,t,u in chosen], f, indent=2)
print("\nSaved _wicklow_chosen.json")
