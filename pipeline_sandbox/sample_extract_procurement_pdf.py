"""PHASE-3b SAMPLE EXTRACTION (PRE-ETL): ingest the DIGITAL-PDF leads and see what turns up.

Widens sample_extract_procurement.py (tabular xlsx/csv) to the digital-PDF supplier-level
leads the probe found (Teagasc, Bord Bia, BIM, Dept Defence/DPER/Culture, NTA, Revenue,
HSE, Tusla, SVUH, HEA, ATU, CIB, NTPF, Marine...). Reads the sample URL for each straight
out of procurement_publishers_probe.json, so it ingests whatever the probe surfaced.

Extraction = HEADER-ANCHORED columns, a step up from the LA 2-column largest-x-gap
(probe_procurement_pdf_counties.py, whose cluster_word_rows/to_eur we reuse). These files
have 4-6 columns (PO#, supplier, description, amount, paid, date) in different orders, so
we find the header row, take each header label's x-position as a COLUMN ANCHOR, and assign
every data word to the nearest anchor. Then map columns -> roles and run the plan §6
strange-value battery (incl. the single-outlier check that caught TII's €1.2bn row).

This is a probe, not a parser: wrapped descriptions / multi-line cells fragment a bit, and
that's fine — the goal is to confirm schema + landmines per source before Phase-4 parsers.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/sample_extract_procurement_pdf.py
      ./.venv/Scripts/python.exe pipeline_sandbox/sample_extract_procurement_pdf.py --max-pages 3
Writes c:/tmp/procurement_publishers/sample_extraction_pdf_report.json
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import fitz  # PyMuPDF
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

TMP = Path("c:/tmp/procurement_publishers")
PROBE = TMP / "procurement_publishers_probe.json"
OUT = TMP / "sample_extraction_pdf_report.json"
H = {"User-Agent": "Mozilla/5.0 (dail-tracker research probe)"}

MONEY_RE = re.compile(r"(?:€|EUR)?\s?\d{1,3}(?:,\d{3})+(?:\.\d{2})?|\d+\.\d{2}")
NUM_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")

ROLE_RE = {
    "supplier": re.compile(r"supplier|payee|vendor|provider|customer|recipient|\bname\b", re.I),
    "amount": re.compile(r"amount|total|value|gross|\beuro\b|€|\bpaid\b|\bvat\b|ledger", re.I),
    "description": re.compile(r"descript|\bdesc\b|detail|categor|service|goods|nature|\bgl\b|main gl", re.I),
    "po": re.compile(r"\border\b|\bpo\b|\bpor\b|referen|\bref\b|\bnumber\b|invoice|\bdoc\b", re.I),
    "period": re.compile(r"period|quarter|\bqtr\b|\bdate\b|\byear\b|posting", re.I),
    "paid": re.compile(r"\bpaid\b|payment type|status|no\.? of payments", re.I),
}
CAVEAT_RE = re.compile(r"\bvat\b|exclud|inclus|indicativ|not (a )?payment|net of|estimate|note:|please note", re.I)
COMPANY_SUFFIX = re.compile(
    r"\b(ltd|limited|dac|plc|clg|llp|teo|teoranta|t/a|uc|inc|llc|gmbh|company|co\.|group|"
    r"services|solutions|consult|engineer|partners|associates|holdings|university|college|"
    r"council|hse|board|institute|ireland|technolog|systems|media|hotel|centre|&)\b", re.I)
CATEGORY_WORD = re.compile(r"^\s*(total|category total|sum|subtotal|grand total|all suppliers|various)\b", re.I)
MERGE_GAP = 22.0  # px: header words closer than this fuse into one column label
# LA fix (Mayo/Donegal): an adjacent PO/vendor-ID column bleeds a leading digit run into
# the supplier cell ("539106 A HORTON LTD"). Strip it so names are real + dedup works.
DIGIT_PREFIX = re.compile(r"^(?:\d{3,}\s+){1,3}")


def clean_supplier(s: str | None) -> str:
    return DIGIT_PREFIX.sub("", s or "").strip(" -:|")


def to_eur(token: str) -> float | None:
    m = NUM_RE.search(token or "")
    if not m:
        return None
    with contextlib.suppress(ValueError):
        return float(m.group().replace(",", ""))
    return None


def _curl(url: str) -> bytes | None:
    try:
        p = subprocess.run(["curl", "-sS", "-k", "-L", "--max-time", "90", "-A", H["User-Agent"], url],
                           capture_output=True, timeout=120)
        return p.stdout if p.returncode == 0 and p.stdout else None
    except Exception:
        return None


def fetch(url: str) -> bytes | None:
    # disk cache: these PDFs (HSE 159pp etc.) don't change run-to-run; stop re-downloading.
    import hashlib
    cache = TMP / "_pdf_cache" / (hashlib.md5(url.encode()).hexdigest() + ".pdf")
    if cache.exists() and cache.stat().st_size > 2000:
        return cache.read_bytes()
    b = None
    try:
        r = requests.get(url, headers=H, timeout=90, allow_redirects=True)
        if r.content[:4] == b"%PDF" or "pdf" in r.headers.get("content-type", ""):
            b = r.content
    except Exception:
        pass
    if not (b and b[:4] == b"%PDF"):
        b = _curl(url)
    if b and b[:4] == b"%PDF":
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(b)
        return b
    return None


def cluster_word_rows(page, ytol: float = 3.0) -> list[list]:
    words = page.get_text("words")
    words.sort(key=lambda w: (round(w[1] / ytol), w[0]))
    rows, cur, cur_y = [], [], None
    for w in words:
        y = w[1]
        if cur_y is None or abs(y - cur_y) <= ytol:
            cur.append(w)
            cur_y = y if cur_y is None else cur_y
        else:
            rows.append(cur)
            cur, cur_y = [w], y
    if cur:
        rows.append(cur)
    return rows


def find_header(rows: list[list]) -> list | None:
    """First row (top 18) whose words collectively hit >=2 roles incl. supplier OR amount."""
    best, best_hits = None, 1
    for r in rows[:18]:
        text = " ".join(w[4] for w in r)
        hits = sum(bool(rx.search(text)) for rx in ROLE_RE.values())
        has_anchor = ROLE_RE["supplier"].search(text) or ROLE_RE["amount"].search(text)
        if hits >= 2 and has_anchor and hits > best_hits:
            best, best_hits = r, hits
    return best


def header_columns(header: list) -> list[dict]:
    """Fuse adjacent header words into column anchors; return [{label, x0, x1, center}]."""
    ws = sorted(header, key=lambda w: w[0])
    cols: list[dict] = []
    for w in ws:
        if cols and w[0] - cols[-1]["x1"] < MERGE_GAP:
            cols[-1]["label"] += " " + w[4]
            cols[-1]["x1"] = max(cols[-1]["x1"], w[2])
        else:
            cols.append({"label": w[4], "x0": w[0], "x1": w[2]})
    for c in cols:
        c["center"] = (c["x0"] + c["x1"]) / 2
    return cols


def assign_role(cols: list[dict]) -> dict[str, int]:
    """Map column index -> role. amount prefers the RIGHTMOST amount-labelled column."""
    roles: dict[str, int] = {}
    for role, rx in ROLE_RE.items():
        cands = [i for i, c in enumerate(cols) if rx.search(c["label"])]
        if not cands:
            continue
        roles[role] = cands[-1] if role == "amount" else cands[0]
    return roles


def refine_roles(cols: list[dict], roles: dict[str, int], records: list[list[str]]) -> dict[str, int]:
    """Evidence-based fix for header-only role guesses:
      - amount   = the amount-labelled column whose cells are MOST numeric (skips a 'Paid'
                   Y/N flag column that merely contains the word 'paid' — e.g. Revenue).
      - supplier = the supplier-labelled column that is LEAST numeric (a real name column,
                   not an adjacent 'Supplier ID' number column — e.g. ATU/Bord Bia)."""
    if not records:
        return roles
    def numfrac(i: int) -> float:
        vals = [r[i] for r in records if i < len(r) and r[i]]
        return sum(to_eur(v) is not None for v in vals) / len(vals) if vals else 0.0
    amt_cands = [i for i, c in enumerate(cols) if ROLE_RE["amount"].search(c["label"])]
    if amt_cands:
        roles["amount"] = max(amt_cands, key=numfrac)
    sup_cands = [i for i, c in enumerate(cols) if ROLE_RE["supplier"].search(c["label"])]
    if sup_cands:
        roles["supplier"] = min(sup_cands, key=numfrac)
    return roles


def row_to_cols(words: list, cols: list[dict]) -> list[str]:
    """Bucket each word into the nearest column anchor by word centre."""
    bounds = [(cols[i]["center"] + cols[i + 1]["center"]) / 2 for i in range(len(cols) - 1)]
    buckets: list[list] = [[] for _ in cols]
    for w in sorted(words, key=lambda w: w[0]):
        c = (w[0] + w[2]) / 2
        idx = 0
        while idx < len(bounds) and c > bounds[idx]:
            idx += 1
        buckets[idx].append(w[4])
    return [" ".join(b).strip(" -:|") for b in buckets]


def parse_pdf(b: bytes, max_pages: int | None) -> dict:
    doc = fitz.open(stream=b, filetype="pdf")
    npages = doc.page_count
    limit = min(npages, max_pages) if max_pages else npages
    # header from the first page that has one
    cols: list[dict] = []
    header_label = ""
    page0_text = ""
    for i in range(min(npages, 3)):
        rows = cluster_word_rows(doc[i])
        if i == 0:
            page0_text = doc[i].get_text("text")
        h = find_header(rows)
        if h:
            cols = header_columns(h)
            header_label = " | ".join(c["label"] for c in cols)
            break
    # NOTE: a data-gutter column splitter (columns_from_data) was trialled here and REVERTED
    # — it didn't fix Tusla's vendor/amount bleed (no consistent gutter when the vendor sits
    # right after a right-aligned amount) and regressed HSE (trailing doc-ref into vendor).
    # HSE/Tusla/ATU need per-publisher column x-specs, which is Phase-4 parser work.
    roles = assign_role(cols) if cols else {}
    records: list[list[str]] = []
    digital_chars = 0
    for i in range(limit):
        page = doc[i]
        digital_chars += len(page.get_text("text").strip())
        if not cols:
            continue
        for wrow in cluster_word_rows(page):
            xs = [w[4] for w in wrow]
            # skip the header row itself + obvious title/note rows (no money anywhere)
            if not any(MONEY_RE.search(t) for t in xs):
                continue
            rec = row_to_cols(wrow, cols)
            records.append(rec)
    doc.close()
    roles = refine_roles(cols, roles, records) if cols else roles
    return {"pages": npages, "pages_parsed": limit, "digital": digital_chars > 200,
            "header_label": header_label, "cols": cols, "roles": roles,
            "records": records, "page0_text": page0_text}


def battery(records, roles, page0_text, header_label) -> dict:
    sup_i = roles.get("supplier")
    amt_i = roles.get("amount")
    suppliers = [clean_supplier(r[sup_i]) for r in records if sup_i is not None and sup_i < len(r)]
    amts = []
    if amt_i is not None:
        for r in records:
            amts.append(to_eur(r[amt_i]) if amt_i < len(r) else None)
    amts_ok = [a for a in amts if a is not None]
    total = sum(amts_ok)
    biggest = max(amts_ok) if amts_ok else 0
    indiv = [s for s in suppliers if s and not COMPANY_SUFFIX.search(s) and 1 <= len(s.split()) <= 3]
    strange = {
        "negative_amounts": sum(a < 0 for a in amts_ok),
        "zero_amounts": sum(a == 0 for a in amts_ok),
        "very_large_amounts_gt_10m": sum(a > 10_000_000 for a in amts_ok),
        "max_amount": biggest or None,
        "missing_supplier": sum(not s for s in suppliers),
        "missing_or_unparseable_amount": sum(a is None for a in amts) if amt_i is not None else None,
        "duplicate_full_rows": len(records) - len({tuple(r) for r in records}),
        "category_total_masquerade": sum(bool(CATEGORY_WORD.search(s or "")) for s in suppliers),
        "largest_amount_share_of_total": round(biggest / total, 3) if total else None,
        "outlier_warning": bool(total and biggest / total > 0.5),
    }
    amt_counts = Counter(round(a, 2) for a in amts_ok)
    top = amt_counts.most_common(1)[0] if amt_counts else (None, 0)
    strange["most_repeated_amount"] = {"value": top[0], "count": top[1]}
    caveat = bool(CAVEAT_RE.search(page0_text)) or bool(CAVEAT_RE.search(header_label))
    return {
        "rows_extracted": len(records),
        "supplier_column_idx": sup_i, "amount_column_idx": amt_i,
        "amount_safe_to_sum_eur": round(total, 2) if amts_ok else None,
        "personal_name_risk_frac": round(len(indiv) / len(suppliers), 3) if suppliers else None,
        "sample_suppliers": [s for s in suppliers if s][:5],
        "caveat_text_detected": caveat,
        "strange_values": strange,
    }


def confidence(info, b) -> str:
    if not info["cols"] or "amount" not in info["roles"] or "supplier" not in info["roles"]:
        return "low"
    sup_i, amt_i = info["roles"]["supplier"], info["roles"]["amount"]
    recs = info["records"]
    if not recs:
        return "low"
    good = sum(to_eur(r[amt_i]) is not None for r in recs if amt_i < len(r)) / len(recs)
    miss_sup = sum(not r[sup_i] for r in recs if sup_i < len(r)) / len(recs)
    if good > 0.85 and miss_sup < 0.1:
        return "high"
    return "medium" if good > 0.5 else "low"


def load_pdf_targets() -> list[dict]:
    d = json.loads(PROBE.read_text(encoding="utf-8"))
    out = []
    for p in d["publishers"]:
        s = p.get("sample") or ""
        if s.lower().split("?")[0].endswith(".pdf"):
            out.append({"publisher_id": p["publisher_id"], "publisher_name": p["publisher_name"],
                        "url": s, "privacy_risk": p.get("privacy_risk")})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages parsed per PDF")
    args = ap.parse_args()

    targets = load_pdf_targets()
    print(f"{'=' * 78}\nPHASE-3b PDF SAMPLE EXTRACTION — {len(targets)} digital-PDF leads"
          + (f" (first {args.max_pages}pp each)" if args.max_pages else " (all pages)") + f"\n{'=' * 78}")
    report = []
    for t in targets:
        print(f"\n[{t['publisher_id']}] {t['publisher_name']}")
        b = fetch(t["url"])
        if not b:
            print("  ERROR: download failed / not a PDF")
            report.append({**t, "error": "download failed or not a PDF"})
            continue
        info = parse_pdf(b, args.max_pages)
        if not info["digital"]:
            print(f"  SCANNED ({info['pages']}pp) -> OCR territory, deferred")
            report.append({**t, "format": "PDF_SCANNED", "pages": info["pages"],
                           "header_label": None, "rows_extracted": 0, "parser_confidence": "low"})
            continue
        if not info["cols"]:
            print(f"  DIGITAL {info['pages']}pp but NO header row detected — needs custom anchor")
            report.append({**t, "format": "PDF_DIGITAL", "pages": info["pages"],
                           "header_label": None, "rows_extracted": 0, "parser_confidence": "low",
                           "note": "header not auto-detected"})
            continue
        bat = battery(info["records"], info["roles"], info["page0_text"], info["header_label"])
        conf = confidence(info, b)
        rec = {**t, "format": "PDF_DIGITAL", "pages": info["pages"], "pages_parsed": info["pages_parsed"],
               "header_label": info["header_label"],
               "roles": {k: info["cols"][v]["label"] for k, v in info["roles"].items()},
               "parser_confidence": conf, **bat}
        report.append(rec)
        print(f"  {info['pages']}pp (parsed {info['pages_parsed']})  rows={bat['rows_extracted']}  conf={conf}")
        print(f"  header: {info['header_label']}")
        print(f"  roles:  {rec['roles']}")
        if bat["amount_safe_to_sum_eur"]:
            print(f"  sum(parsed)=€{bat['amount_safe_to_sum_eur']:,}  "
                  f"max=€{bat['strange_values']['max_amount']:,}")
        s = bat["strange_values"]
        print(f"  strange: neg={s['negative_amounts']} zero={s['zero_amounts']} >10m={s['very_large_amounts_gt_10m']} "
              f"miss_sup={s['missing_supplier']} dup={s['duplicate_full_rows']} cat_total={s['category_total_masquerade']} "
              f"repeat={s['most_repeated_amount']}")
        if s.get("outlier_warning"):
            print(f"  !! OUTLIER: top row = {s['largest_amount_share_of_total']:.0%} of parsed sum")
        print(f"  caveat_text={bat['caveat_text_detected']}  personal_name_risk={bat['personal_name_risk_frac']}")
        print(f"  sample suppliers: {bat['sample_suppliers']}")

    TMP.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    ok = [r for r in report if r.get("parser_confidence") in ("high", "medium")]
    print(f"\n{'=' * 78}\nSUMMARY: {len(ok)}/{len(report)} PDFs auto-extracted at >=medium confidence")
    print(f"  high  : {[r['publisher_id'] for r in report if r.get('parser_confidence') == 'high']}")
    print(f"  medium: {[r['publisher_id'] for r in report if r.get('parser_confidence') == 'medium']}")
    print(f"  low/needs-work: {[r['publisher_id'] for r in report if r.get('parser_confidence') == 'low']}")
    print(f"  errors: {[r['publisher_id'] for r in report if r.get('error')]}")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
