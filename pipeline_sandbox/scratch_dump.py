import sys
import urllib.request

import fitz

src = sys.argv[1]
out = sys.argv[2]
req = urllib.request.Request(
    src,
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "application/pdf,*/*",
    },
)
data = urllib.request.urlopen(req, timeout=120).read()
doc = fitz.open(stream=data, filetype="pdf")
txt = "\n".join(p.get_text() for p in doc)
with open(out, "w", encoding="utf-8") as fh:
    fh.write(txt)
print("chars", len(txt), "->", out)
