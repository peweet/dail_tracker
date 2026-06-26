"""Collect recent full County Council meetings from the SDCC meetings portal."""
import re
import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://meetings.southdublin.ie"


def get(url):
    return requests.get(url, headers=H, timeout=40)


def parse_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            date = tds[0].get_text(" ", strip=True)
            meeting = tds[1].get_text(" ", strip=True)
            mtype = tds[2].get_text(" ", strip=True)
            link = None
            for a in tr.find_all("a", href=True):
                if "Agenda" in a["href"]:
                    link = a["href"]
            out.append((date, meeting, mtype, link))
    return out


def main():
    # try a few likely pagination params on the home page
    r0 = get(BASE + "/")
    rows = parse_rows(r0.text)
    print("home rows:", len(rows))
    cc = [row for row in rows if row[2].strip().lower() == "county council"]
    print("County Council rows on home page:")
    for row in cc:
        print(row)

    # Try pagination via querystring guesses
    for param in ["?page=2", "/Home?page=2", "?Page=2"]:
        try:
            rr = get(BASE + "/" + param.lstrip("/"))
            rws = parse_rows(rr.text)
            firstdate = rws[1][0] if len(rws) > 1 else None
            print(f"param {param!r}: status {rr.status_code} rows {len(rws)} first {firstdate}")
        except Exception as e:
            print(f"param {param!r}: ERR {e}")


if __name__ == "__main__":
    main()
