"""DCEDIY legacy purchase-orders → IPAS/asylum + Ukraine accommodation (2020-2024).

Fills the 2020-2024 gap in the Accommodation-Spend feature: when IPAS sat under the
Department of Children, Equality, Disability, Integration & Youth (DCEDIY, 2020-2024),
its accommodation purchase orders were published in DCEDIY's over-€20k register. Those
files are legacy gov.ie hash-slug PDFs that the generic public-body extractor skipped
(transient 405s trip its breaker). This bespoke extractor: retries the flaky fetch,
parses the (column-geometry) tables with fitz, detects the varying column layouts, and
keeps ONLY the accommodation rows (IP/asylum/Ukraine), so it can be UNION-ed into the
v_accommodation_spend_* views without touching the main payments fact.

Reads  : the 27 legacy DCEDIY PO PDFs on the gov.ie collection page (live fetch, retry).
Writes : data/gold/parquet/dceidy_ipas_legacy_spend.parquet
         (provider, year, amount_eur, category, stream, source_file)

NOTE sandbox/experimental. po_committed semantics. Provider/amount only — no PII
(supplier-level, public register). Default run is dry.
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import fitz
import polars as pl
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[2]
_OUT = _ROOT / "data" / "gold" / "parquet" / "dceidy_ipas_legacy_spend.parquet"
_LISTING = (
    "https://www.gov.ie/en/department-of-children-disability-and-equality/collections/"
    "department-of-children-equality-disability-integration-and-youth-purchase-orders-for-20000-or-above/"
)
_H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.gov.ie/"}

# Keep only accommodation rows. IP/asylum/Ukraine; exclude childcare/EYPP/disability/etc.
_ACC_RE = re.compile(r"ip accommodation|asylum|direct provision|international protection|"
                     r"(provision of )?accommodation|emergency accommodation", re.I)
_UKR_RE = re.compile(r"ukrain|beneficiar.* temporary protection|\bbotp\b", re.I)
# Lines that say "accommodation" but are NOT protection spend → drop.
_NOT_ACC_RE = re.compile(r"student accommodation|conference|staff accommodation|"
                         r"homeless", re.I)


def _get(url: str, tries: int = 5) -> bytes | None:
    for i in range(tries):
        try:
            r = requests.get(url, headers=_H, timeout=45)
            if r.status_code == 200 and r.content[:4] in (b"%PDF", b"PK\x03\x04"):
                return r.content
        except Exception:
            pass
        time.sleep(1.2 * (i + 1))
    return None


def list_pdf_urls() -> list[str]:
    html = requests.get(_LISTING, headers=_H, timeout=60).text
    urls = re.findall(r"https://assets\.gov\.ie/static/documents/[^\"\\ ]+\.pdf", html)
    return sorted(set(urls))


def _num(s) -> float | None:
    s = re.sub(r"[^0-9.]", "", str(s or "").replace(",", "").replace("€", ""))
    if not s or s == ".":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _year(s) -> int | None:
    m = re.search(r"(20[12][0-9])", str(s or ""))
    return int(m.group(1)) if m else None


def _detect_cols(header: list[str]) -> dict:
    """Map logical fields -> column index from a (possibly multi-line/merged) header."""
    h = [re.sub(r"\s+", " ", (c or "").replace("\n", " ")).strip().lower() for c in header]
    out: dict[str, int] = {}
    for i, c in enumerate(h):
        if "reference" in c or c == "ref" or c.startswith("po"):
            out.setdefault("ref", i)
        if "supplier" in c or c == "name" or "payee" in c or "vendor" in c:
            out.setdefault("supplier", i)
        if "amount" in c or "total paid" in c or c == "paid" or "value" in c:
            out.setdefault("amount", i)
        if "description" in c or "detail" in c or "purpose" in c or "category" in c:
            out.setdefault("desc", i)
        if "date" in c:
            out.setdefault("date", i)
    return out


def parse_pdf(content: bytes, source: str) -> list[dict]:
    doc = fitz.open(stream=content, filetype="pdf")
    rows: list[dict] = []
    cols: dict | None = None
    for page in doc:
        for tab in page.find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 2:
                continue
            c = _detect_cols(data[0])
            if "supplier" in c and "amount" in c:
                cols = c  # lock the layout from the first good header; reuse on continuation pages
            if not cols or "supplier" not in cols or "amount" not in cols:
                continue
            for r in data[1:]:
                if len(r) <= max(cols.values()):
                    continue
                supplier = re.sub(r"\s+", " ", (r[cols["supplier"]] or "").replace("\n", " ")).strip()
                amount = _num(r[cols["amount"]])
                desc = (r[cols.get("desc", -1)] or "") if "desc" in cols else ""
                date = (r[cols.get("date", -1)] or "") if "date" in cols else ""
                if not supplier or amount is None or amount <= 0:
                    continue
                ref = re.sub(r"\s+", " ", (r[cols.get("ref", -1)] or "").replace("\n", " ")).strip() if "ref" in cols else ""
                rows.append({"ref": ref, "provider_raw": supplier, "amount_eur": amount,
                             "category": re.sub(r"\s+", " ", str(desc).replace("\n", " ")).strip(),
                             "pay_date": re.sub(r"\s+", " ", str(date).replace("\n", " ")).strip(),
                             "year": _year(date), "source_file": source})
    doc.close()
    return rows


def classify(rows: list[dict]) -> pl.DataFrame:
    keep = []
    for r in rows:
        cat = r["category"]
        if _NOT_ACC_RE.search(cat):
            continue
        if _UKR_RE.search(cat):
            r["stream"] = "Ukraine"
        elif _ACC_RE.search(cat):
            r["stream"] = "International Protection"
        else:
            continue
        keep.append(r)
    if not keep:
        return pl.DataFrame()
    df = pl.DataFrame(keep)
    df = df.with_columns(pl.col("provider_raw").str.to_uppercase().alias("provider"))
    return df.select(["ref", "provider", "amount_eur", "category", "stream", "year", "pay_date", "source_file"])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--max-files", type=int, default=None)
    args = ap.parse_args()

    urls = list_pdf_urls()
    print(f"DCEDIY legacy PDFs found: {len(urls)}")
    all_rows: list[dict] = []
    fetched = failed = 0
    for u in urls[: args.max_files]:
        b = _get(u)
        if not b:
            failed += 1
            print(f"  FETCH FAIL {u.split('/')[-1][:40]}")
            continue
        fetched += 1
        try:
            all_rows.extend(parse_pdf(b, u.split("/")[-1][:40]))
        except Exception as e:
            print(f"  PARSE FAIL {u.split('/')[-1][:40]}: {e}")
    print(f"fetched {fetched}/{len(urls)} ({failed} failed) | raw rows {len(all_rows)}")

    df = classify(all_rows)
    if df.is_empty():
        print("no accommodation rows extracted"); return
    raw_n, raw_eur = df.height, df["amount_eur"].sum()
    # DEDUPE: same (ref, amount, pay_date) appearing in >1 quarterly file = the same PO
    # re-listed (overlapping/cumulative files). Drop exact repeats.
    df = df.unique(subset=["ref", "amount_eur", "pay_date"], keep="first")
    print(f"raw accommodation rows: {raw_n} (€{raw_eur:,.0f}) -> deduped {df.height} (€{df['amount_eur'].sum():,.0f})")
    print(f"providers {df['provider'].n_unique()}")
    print("by year (deduped):")
    for r in df.group_by("year").agg(pl.col("amount_eur").sum().round(0)).sort("year").to_dicts():
        print(f"   {r['year']}: €{r['amount_eur']:,.0f}")
    print("top providers:")
    for r in df.group_by("provider").agg(pl.col("amount_eur").sum().round(0).alias("eur")).sort("eur", descending=True).head(8).to_dicts():
        print(f"   {r['provider'][:42]:44s} €{r['eur']:,.0f}")
    if args.write:
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(_OUT, compression="zstd", compression_level=3, statistics=True)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")
    else:
        print("\n(dry-run — pass --write to persist)")


if __name__ == "__main__":
    main()
