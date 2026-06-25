import requests
from bs4 import BeautifulSoup

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
BASE = "https://meetings.southdublin.ie"


def get(url):
    return requests.get(url, headers=H, timeout=40)


def main():
    r = get(BASE + "/")
    soup = BeautifulSoup(r.text, "html.parser")
    for fi, form in enumerate(soup.find_all("form")):
        print(f"=== FORM {fi} action={form.get('action')} method={form.get('method')} ===")
        for inp in form.find_all(["input", "select", "button"]):
            name = inp.get("name")
            typ = inp.name + ":" + (inp.get("type") or "")
            val = inp.get("value")
            if inp.name == "select":
                opts = [o.get("value") for o in inp.find_all("option")][:5]
                print(f"  {typ} name={name} options(sample)={opts}")
            else:
                print(f"  {typ} name={name} value={val}")
    # also dump pager links
    print("=== pager links ===")
    for a in soup.find_all("a", href=True):
        t = a.get_text(" ", strip=True)
        if t in ("First", "Previous", "Next", "Last") or t.isdigit():
            print(repr(t), "->", a["href"], "data:", {k: v for k, v in a.attrs.items() if k.startswith("data")})


if __name__ == "__main__":
    main()
