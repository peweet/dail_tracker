import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://meetings.southdublin.ie"


def parse_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue
            link = next((a["href"] for a in tr.find_all("a", href=True) if "Agenda" in a["href"]), None)
            out.append((tds[0].get_text(" ", strip=True), tds[1].get_text(" ", strip=True),
                        tds[2].get_text(" ", strip=True), link))
    return out


def main():
    s = requests.Session()
    s.headers.update(H)
    r = s.get(BASE + "/", timeout=40)
    soup = BeautifulSoup(r.text, "html.parser")
    sel = soup.find("select", attrs={"name": "Options.MeetingTypeId"})
    cc_id = None
    print("=== Meeting type options ===")
    for o in sel.find_all("option"):
        label = o.get_text(" ", strip=True)
        val = o.get("value")
        if label == "County Council":
            cc_id = val
        if "council" in label.lower():
            print(f"  id={val} label={label!r}")
    print("County Council id =", cc_id)

    token = soup.find("input", attrs={"name": "__RequestVerificationToken"}).get("value")
    # POST filter
    data = {
        "Options.MeetingTypeId": cc_id,
        "PageTitle": "Meetings",
        "__RequestVerificationToken": token,
    }
    rp = s.post(BASE + "/", data=data, timeout=40)
    rows = parse_rows(rp.text)
    print(f"=== POST filter County Council: status {rp.status_code}, {len(rows)} rows ===")
    for row in rows[:12]:
        print(row)
    # check page count text
    import re
    m = re.search(r"Page 1 of (\d+) \((\d+) meetings found\)", rp.text)
    print("page-info:", m.group(0) if m else "n/a")


if __name__ == "__main__":
    main()
