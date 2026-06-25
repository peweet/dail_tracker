import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def get(url):
    return requests.get(url, headers=H, timeout=40)


def main():
    url = "https://meetings.southdublin.ie/Home/Agenda/2753"
    r = get(url)
    print("STATUS", r.status_code, "LEN", len(r.text))
    soup = BeautifulSoup(r.text, "html.parser")
    print("TITLE", soup.title.get_text(strip=True) if soup.title else "")
    print("=== body text (first 5000) ===")
    body = soup.find("body")
    print(body.get_text("\n", strip=True)[:5000])
    print("=== document/pdf links on agenda page ===")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        low = href.lower()
        if any(k in low for k in ["pdf", "document", "doc", "mgconvert", "agenda", "minute", "/home/"]):
            print(repr(a.get_text(" ", strip=True)[:60]), "->", href)


if __name__ == "__main__":
    main()
