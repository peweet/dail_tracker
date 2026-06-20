"""LRC Classified List enrichment — SANDBOX extractor (verdict spike, PR1 scope).

Fetches the Law Reform Commission "Classified List of In-Force Legislation"
(https://revisedacts.lawreform.ie/classlist/{1..36}), one server-rendered HTML
page per subject, caches each to bronze with a provenance sidecar, and parses it
into a flat table of statutory-instrument entries tagged with their LRC subject
taxonomy path.

This is a DISCOVERY + CLASSIFICATION enrichment, NOT a legal-status engine. An
SI appearing here means the LRC lists it in its Classified List of in-force
legislation under the given subject path, subject to LRC accuracy warnings and
our own number/year match confidence. "Not matched" never means "not in force".

Graduated to extractors/ — pipeline-invoked by iris_refresh.py (the LRC step),
paired with si_lrc_enrichment_build.py which promotes to gold. This stage itself
writes NOTHING to gold and does NOT touch statutory_instruments.parquet /
si_current_state.parquet; its raw output lands under extractors/_lrc_output/ as the
intermediate the build step reads.

Grain of the parsed table: one row per (SI entry occurrence) — an SI listed under
two subject paths yields two rows. Dedup/aggregation happens downstream.

Outputs:
  data/bronze/lrc_classlist/classlist_{n}.html         (raw cached pages)
  data/bronze/lrc_classlist/classlist_{n}.meta.json     (provenance sidecar)
  extractors/_lrc_output/si_lrc_classlist_raw.parquet

Run:  ./.venv/Scripts/python.exe extractors/si_lrc_classlist_extract.py
        ... --offline    # parse from cache only, never hit the network
        ... --refresh    # force re-fetch all 36 pages
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import contextlib  # noqa: E402

from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

BASE = "https://revisedacts.lawreform.ie/classlist"
HDRS = {"User-Agent": "dail-tracker civic-research/enrichment (planning; contact via repo)"}
CACHE_DIR = ROOT / "data/bronze/lrc_classlist"
OUT_DIR = ROOT / "extractors/_lrc_output"
OUT_PARQUET = OUT_DIR / "si_lrc_classlist_raw.parquet"

N_CATEGORIES = 36

# Citation in an entry's anchor text, e.g. "S.I. No. 193 of 2002".
SI_CITE = re.compile(r"S\.?I\.?\s*No\.?\s*(\d+)\s*of\s*(\d{4})", re.I)
# eISB ELI href, the most reliable number/year key, e.g. /eli/2002/si/193/...
ELI_SI = re.compile(r"/eli/(\d{4})/si/(\d+)", re.I)
# Numeric path baked into the section id, e.g. id="title-1-3-3" -> "1.3.3".
SECTION_ID = re.compile(r"^title-(\d+(?:-\d+)*)$")
UPDATED_TO = re.compile(r"Updated to\s+([0-9]{1,2}\s+\w+\s+\d{4})", re.I)
BULLET = re.compile(r"^[▶►‣•\s]+")  # strip leading ► ▶ ‣ • bullets


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def fetch(cat: int, *, offline: bool, refresh: bool) -> tuple[str, dict]:
    """Return (html, provenance) for one category page, caching to bronze."""
    raw_path = CACHE_DIR / f"classlist_{cat}.html"
    meta_path = CACHE_DIR / f"classlist_{cat}.meta.json"
    if raw_path.exists() and raw_path.stat().st_size > 1000 and not refresh:
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        return raw_path.read_text(encoding="utf-8", errors="ignore"), meta
    if offline:
        raise FileNotFoundError(f"--offline but no cache for category {cat} at {raw_path}")
    import requests  # lazy: keeps the parser importable in CI without network deps

    url = f"{BASE}/{cat}"
    r = requests.get(url, headers=HDRS, timeout=60)
    r.raise_for_status()
    r.encoding = "utf-8"
    txt = r.text
    meta = {
        "url": url,
        "retrieved_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "status_code": r.status_code,
        "content_type": r.headers.get("content-type", ""),
        "sha256": hashlib.sha256(txt.encode("utf-8")).hexdigest(),
        "raw_path": str(raw_path.relative_to(ROOT)),
        "bytes": len(txt),
    }
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(txt, encoding="utf-8")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    time.sleep(0.5)  # polite only on real fetch
    return txt, meta


def clean(text: str) -> str:
    return BULLET.sub("", " ".join(text.split())).strip()


def section_path_and_names(li) -> tuple[str, list[str]]:
    """Walk ancestor <section> elements to build the numeric path + name chain."""
    secs = []
    node = li
    while node is not None:
        node = node.find_parent("section")
        if node is None:
            break
        sid = node.get("id", "") or ""
        m = SECTION_ID.match(sid)
        if not m:
            continue
        hdr = node.find(["h2", "h3", "h4", "h5"], recursive=False)
        name = clean(hdr.get_text(" ", strip=True)) if hdr else ""
        # drop leading numbering "1.3.3." from the displayed name
        name = re.sub(r"^\d+(?:\.\d+)*\.?\s*", "", name)
        secs.append((m.group(1).replace("-", "."), name))
    secs.reverse()  # outermost first
    path = secs[-1][0] if secs else ""
    names = [n for _, n in secs if n]
    return path, names


def parse_category(cat: int, html: str) -> tuple[list[dict], dict]:
    soup = BeautifulSoup(html, "html.parser")
    # subject heading: the <h2> whose text begins with this category number,
    # e.g. "1. Agriculture and Food" (NOT the page-header "Classified List" h2).
    subject = ""
    subj_re = re.compile(rf"^{cat}\.\s+\D")
    for h2 in soup.find_all("h2"):
        t = clean(h2.get_text(" ", strip=True))
        if subj_re.match(t):
            subject = re.sub(r"^\d+\.?\s*", "", t)
            break
    upd = ""
    m = UPDATED_TO.search(soup.get_text(" ", strip=True))
    if m:
        upd = m.group(1)

    rows = []
    for li in soup.find_all("li", id=re.compile(r"^si\d+$")):
        a = li.find("a", href=True)
        text = clean(li.get_text(" ", strip=True))
        href = a["href"] if a else ""
        # number/year: prefer the ELI href, fall back to the citation text
        si_year = si_number = None
        me = ELI_SI.search(href)
        if me:
            si_year, si_number = int(me.group(1)), int(me.group(2))
        else:
            mc = SI_CITE.search(text)
            if mc:
                si_number, si_year = int(mc.group(1)), int(mc.group(2))
        path, names = section_path_and_names(li)
        rows.append(
            {
                "lrc_category_number": cat,
                "lrc_subject_heading": subject,
                "lrc_subheading_path_num": path,
                "lrc_subheading_path_name": " › ".join(names),
                "lrc_subheading_leaf": names[-1] if names else "",
                "lrc_entry_title": text,
                "lrc_eisb_url": href,
                "si_number": si_number,
                "si_year": si_year,
                "lrc_list_updated_to": upd,
                "lrc_entry_dom_id": li.get("id", ""),
            }
        )
    stats = {"category": cat, "subject": subject, "updated_to": upd, "si_rows": len(rows)}
    return rows, stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--offline", action="store_true", help="parse from cache only")
    ap.add_argument("--refresh", action="store_true", help="force re-fetch all pages")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    hr("LRC Classified List — fetch + parse (sandbox)")
    all_rows: list[dict] = []
    provenance: list[dict] = []
    for cat in range(1, N_CATEGORIES + 1):
        html, meta = fetch(cat, offline=args.offline, refresh=args.refresh)
        rows, stats = parse_category(cat, html)
        all_rows.extend(rows)
        provenance.append({**stats, **{k: meta.get(k) for k in ("sha256", "retrieved_at")}})
        print(f"  [{cat:2d}] {stats['subject'][:42]:42s}  SIs={stats['si_rows']:4d}  upd={stats['updated_to']}")

    df = pl.DataFrame(all_rows)
    # entries with no recognisable SI number/year are parse noise — keep but flag
    df = df.with_columns(
        pl.when(pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null())
        .then(pl.format("{}/{}", pl.col("si_number"), pl.col("si_year")))
        .otherwise(None)
        .alias("si_number_year")
    )
    save_parquet(df, OUT_PARQUET)

    hr("Summary")
    print(f"total SI entry rows (occurrences): {df.height}")
    print(f"rows with parsed number/year      : {df['si_number_year'].is_not_null().sum()}")
    print(f"distinct SIs (number/year)        : {df['si_number_year'].n_unique()}")
    print(f"distinct subject headings         : {df['lrc_subject_heading'].n_unique()}")
    print(f"distinct subheading leaves        : {df['lrc_subheading_leaf'].n_unique()}")
    print(f"\nwrote {OUT_PARQUET.relative_to(ROOT)}")
    (OUT_DIR / "classlist_provenance.json").write_text(
        json.dumps(provenance, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
