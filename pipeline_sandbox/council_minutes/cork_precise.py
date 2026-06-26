"""Precise Cork harvest (full-council minutes only, NOT LCDC committee) → fold into outputs.

Cork City: index -> /YYYY/ year pages -> minutes-council-meeting-*.pdf  (exclude lcdc/municipal/agenda)
Cork County: crawl meetings sub-pages -> full-council-meeting-minutes-*.pdf
Replaces all 'Cork*' rows in the consolidated outputs and regenerates the quality report. Sandbox only.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

import council_minutes_consolidate as C

HERE = Path(__file__).resolve().parent
H = C.HDRS
EXCL = re.compile(r"lcdc|municipal|joint policing|agenda|standing|corporate policy", re.I)
RECENT = re.compile(r"202[4-9]|2024|2025|2026|24|25|26")


def get(u, t=40):
    try:
        r = requests.get(u, headers=H, timeout=t, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:  # noqa: BLE001
        return None


def links(r, pat):
    s = BeautifulSoup(r.text, "html.parser")
    return list(dict.fromkeys(urljoin(r.url, a["href"]) for a in s.find_all("a", href=True)
                              if re.search(pat, a["href"], re.I)))


def cork_city_pdfs():
    idx = get("https://www.corkcity.ie/en/council-services/councillors-and-democracy/"
              "meetings-of-the-city-council/full-council-meetings/full-council-meetings-minutes/")
    if not idx:
        return []
    years = [u for u in links(idx, r"full-council-meetings-minutes/202[4-6]")]
    pdfs = []
    for y in years:
        r = get(y)
        if r:
            pdfs += [u for u in links(r, r"\.pdf$") if "minutes" in u.lower() and not EXCL.search(u)]
    return list(dict.fromkeys(pdfs))


def cork_county_pdfs():
    seeds = [
        "https://www.corkcoco.ie/en/council/elected-members-meetings-agendas-and-minutes/"
        "full-council-meetings-agendas-and-minutes/full-council-meetings-agendas-and-minutes",
        "https://www.corkcoco.ie/en/council/elected-members-meetings-agendas-and-minutes/"
        "meetings-agendas-and-minutes/full-council-meetings",
        "https://www.corkcoco.ie/en/council/elected-members-meetings-agendas-and-minutes/"
        "meetings-agendas-and-minutes",
    ]
    pdfs, pages = [], list(seeds)
    for seed in seeds:
        r = get(seed)
        if r:
            pages += links(r, r"full-council|meetings-agendas")
    for p in list(dict.fromkeys(pages))[:14]:
        r = get(p)
        if r:
            pdfs += [u for u in links(r, r"\.pdf$")
                     if re.search(r"full-council.*minute|minute.*full-council", u, re.I)
                     and not EXCL.search(u)]
    return list(dict.fromkeys(pdfs))


def extract(url, la):
    import fitz
    fname = url.split("/")[-1][:80]
    rec = {"url": url, "local_authority": la, "meeting": fname}
    text, votes = "", []
    r = get(url, 70)
    if not r:
        rec["status"] = "fetch_fail"
    else:
        try:
            doc = fitz.open(stream=r.content, filetype="pdf")
            native = sum(len(p.get_text().strip()) for p in doc)
            rec["n_pages"] = len(doc)
            if native >= C.TEXT_MIN * max(1, len(doc)):
                text = "\n".join(p.get_text() for p in doc)
                rec["status"] = "text"
                votes = C.votes_pdf(doc, la, fname)
            else:
                rec["status"] = "staged_offbox_scanned"
        except Exception as e:  # noqa: BLE001
            rec["status"] = f"err_{type(e).__name__}"
    dtype = C.doc_type(url, text)
    rec.update({"doc_type": dtype, "text_chars": len(text)})
    rec.update(C.parse_struct(text))
    ok, reason = C.classify(rec, text, dtype)
    rec["clean"], rec["reason"] = ok, reason
    if ok:
        d = C.CORPUS / C.slug(la)
        d.mkdir(exist_ok=True)
        (d / (C.slug(fname)[:80] + ".txt")).write_text(text, encoding="utf-8")
        rec["text_path"] = f"corpus/{C.slug(la)}/{C.slug(fname)[:80]}.txt"
    return rec, (votes if ok else [])


def main():
    city = [("Cork City", u) for u in cork_city_pdfs()]
    county = [("Cork County", u) for u in cork_county_pdfs()]
    print(f"Cork City full-council pdfs: {len(city)} | Cork County: {len(county)}")
    nc, nq, nv = [], [], []
    for la, u in city + county:
        rec, votes = extract(u, la)
        (nc if rec["clean"] else nq).append(rec)
        nv += votes
        print(f"  [{rec['status']:10}] {rec['reason']:22} {u.split('/')[-1][:44]}")

    def load(p):
        p = Path(p)
        return [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()] if p.exists() else []
    clean = [r for r in load(HERE / "meetings_clean.jsonl") if not r["local_authority"].startswith("Cork")] + nc
    quar = [r for r in load(HERE / "quarantine" / "quarantine.jsonl") if not r["local_authority"].startswith("Cork")] + nq
    votes = [v for v in load(HERE / "member_votes_all.jsonl") if not v["local_authority"].startswith("Cork")] + C.norm_members(nv)
    (HERE / "meetings_clean.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8")
    (HERE / "quarantine" / "quarantine.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in quar), encoding="utf-8")
    (HERE / "member_votes_all.jsonl").write_text("\n".join(json.dumps(v, ensure_ascii=False) for v in votes), encoding="utf-8")
    C.write_quality_report(clean, quar, votes)
    print(f"\nCork fixed. City clean={sum(r['clean'] for r in nc+nq if r['local_authority']=='Cork City')} "
          f"County clean={sum(1 for r in nc if r['local_authority']=='Cork County')} | "
          f"total clean={len(clean)} quar={len(quar)} votes={len(votes)}")


if __name__ == "__main__":
    main()
