import sys, re, io
import fitz
import urllib.request

def get_text(src):
    if src.startswith("http"):
        req = urllib.request.Request(src, headers={
            "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept":"application/pdf,*/*"})
        data = urllib.request.urlopen(req, timeout=60).read()
        doc = fitz.open(stream=data, filetype="pdf")
    else:
        doc = fitz.open(src)
    out=[]
    for p in doc:
        out.append(p.get_text())
    return "\n".join(out)

def show(src, terms):
    try:
        t = get_text(src)
    except Exception as e:
        print("ERROR", src, repr(e)); return
    lines = [l.strip() for l in t.splitlines()]
    print("=== chars:", len(t))
    # print lines containing money/rate keywords
    pat = re.compile("|".join(terms), re.I)
    prev=""
    for i,l in enumerate(lines):
        if pat.search(l):
            print(f"[{i}] {l}")

if __name__=="__main__":
    src = sys.argv[1]
    terms = sys.argv[2].split("|") if len(sys.argv)>2 else ["€","per square","total of contribution","per sq","residential","non-residential","commercial","m2","m²","effective","2020","2021","2022","2023","2024","2025","2026"]
    show(src, terms)
