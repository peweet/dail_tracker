import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def get(url):
    return requests.get(url, headers=H, timeout=40, allow_redirects=True)


def main():
    base = "https://meetings.southdublin.ie/"
    r = get(base)
    print("STATUS", r.status_code, "FINAL", r.url, "LEN", len(r.text))
    soup = BeautifulSoup(r.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    print("TITLE", title)
    print("--- links ---")
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href in seen:
            continue
        seen.add(href)
        t = a.get_text(" ", strip=True)
        low = (t + " " + href).lower()
        if any(k in low for k in ["committee", "meeting", "calendar", "council", "agenda",
                                  "minute", "ielist", "iedoc", "mgconvert", "ielisting",
                                  "iemeeting", "browse"]):
            print(repr(t[:55]), "->", href)


if __name__ == "__main__":
    main()
