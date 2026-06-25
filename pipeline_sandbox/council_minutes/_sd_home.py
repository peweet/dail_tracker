import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def get(url):
    return requests.get(url, headers=H, timeout=40)


def main():
    base = "https://meetings.southdublin.ie/"
    r = get(base)
    soup = BeautifulSoup(r.text, "html.parser")
    # Print structured rows: look for tables or list items containing agenda links
    print("=== TABLES ===")
    for ti, table in enumerate(soup.find_all("table")):
        print(f"--- table {ti} ---")
        for tr in table.find_all("tr"):
            cells = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
            links = [a["href"] for a in tr.find_all("a", href=True) if "Agenda" in a["href"]]
            if cells or links:
                print(cells, "LINKS:", links)
    print("=== full text dump of body (first 4000) ===")
    body = soup.find("body")
    print(body.get_text("\n", strip=True)[:4000])


if __name__ == "__main__":
    main()
