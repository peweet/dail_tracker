import requests, json, os, re
import fitz  # PyMuPDF

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
       "Accept-Encoding": "gzip, deflate"}
BASE = "pipeline_sandbox/council_minutes"
OUT = os.path.join(BASE, "wicklow_pdfs")
os.makedirs(OUT, exist_ok=True)

chosen = json.load(open(os.path.join(BASE,"_wicklow_chosen.json"), encoding="utf-8"))

for i, m in enumerate(chosen):
    url = m["url"]
    fn = os.path.join(OUT, f"{i:02d}_{m['date']}.pdf")
    if not os.path.exists(fn):
        r = requests.get(url, headers=HDR, timeout=60)
        with open(fn, "wb") as f:
            f.write(r.content)
    doc = fitz.open(fn)
    total_text = ""
    for p in doc:
        total_text += p.get_text()
    print(f"\n{'='*70}\n[{i}] {m['date']} | {m['title']}")
    print(f"pages={doc.page_count} text_len={len(total_text)}")
    print("-"*70)
    # print first ~3500 chars to inspect structure
    print(total_text[:3500])
    doc.close()
