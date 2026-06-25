import re
import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def get(url):
    return requests.get(url, headers=H, timeout=40)


def main():
    url = "https://www.sdcc.ie/en/services/our-council/council-meetings/"
    r = get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    print("--- all urls mentioning meetings/agenda/minutes/portal ---")
    seen = set()
    for m in re.findall(r"https?://[^\"'<> ]+", r.text):
        low = m.lower()
        if any(k in low for k in ["moderngov", "councilmeeting", "ecouncil", "cmis",
                                  "agenda", "minute", "meeting", "documents"]):
            if m not in seen:
                seen.add(m)
                print(m)
    print("--- iframes ---")
    for f in soup.find_all("iframe"):
        print("IFRAME", f.get("src"))
    print("--- 'Meetings Online' page ---")
    r2 = get("https://www.sdcc.ie/en/services/our-council/council-meetings/meetings-online/")
    soup2 = BeautifulSoup(r2.text, "html.parser")
    for a in soup2.find_all("a", href=True):
        href = a["href"]
        low = (a.get_text(" ", strip=True) + " " + href).lower()
        if any(k in low for k in ["agenda", "minute", "meeting", "document", "2024", "2025", "2026", "pdf", "watch", "youtube", "video"]):
            if "mailto" not in href:
                print(repr(a.get_text(" ", strip=True)[:60]), "->", href)


if __name__ == "__main__":
    main()
