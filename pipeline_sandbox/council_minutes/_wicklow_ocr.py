import json, os, re
import fitz
import numpy as np
from rapidocr_onnxruntime import RapidOCR

BASE = "pipeline_sandbox/council_minutes"
OUT = os.path.join(BASE, "wicklow_pdfs")
chosen = json.load(open(os.path.join(BASE,"_wicklow_chosen.json"), encoding="utf-8"))

ocr = RapidOCR()

results = {}
for i, m in enumerate(chosen):
    fn = os.path.join(OUT, f"{i:02d}_{m['date']}.pdf")
    doc = fitz.open(fn)
    lines_all = []
    for pno, page in enumerate(doc):
        pix = page.get_pixmap(matrix=fitz.Matrix(2.5, 2.5))  # ~180-220 dpi upscale
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        if pix.n == 4:
            img = img[:, :, :3]
        res, _ = ocr(img)
        if res:
            for box, txt, score in res:
                # keep y0 for ordering: box is 4 points
                ys = min(p[1] for p in box)
                xs = min(p[0] for p in box)
                lines_all.append((pno, ys, xs, txt, float(score)))
    doc.close()
    # order by page, then y, then x
    lines_all.sort(key=lambda r:(r[0], round(r[1]/8), r[2]))
    txtlines = [l[3] for l in lines_all]
    results[i] = {"date": m["date"], "title": m["title"], "url": m["url"], "lines": txtlines}
    print(f"\n{'='*70}\n[{i}] {m['date']} | {m['title']} | n_lines={len(txtlines)}")
    for ln in txtlines:
        print("  ", ln)

with open(os.path.join(BASE,"_wicklow_ocr.json"),"w",encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print("\nSaved _wicklow_ocr.json")
