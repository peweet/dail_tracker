"""Cork follow-up: re-extract Cork City + Cork County from the CORRECT full-council-minutes
sources (the auto-discovery had grabbed an LCDC committee page / an economic plan), then FOLD the
results into the consolidated outputs WITHOUT re-fetching every other council.

Reuses the consolidation + v2 logic. Appends to meetings_clean.jsonl / quarantine/quarantine.jsonl /
member_votes_all.jsonl (dedup by url, dropping Cork's old wrong-source rows) and regenerates
QUALITY_ASSESSMENT.md. SANDBOX ONLY — nothing written to gold.
"""
from __future__ import annotations

import json
from pathlib import Path

import requests

import council_minutes_consolidate as C
import council_minutes_v2 as V2

HERE = Path(__file__).resolve().parent
CORK = {
    "Cork City": "https://www.corkcity.ie/en/council-services/councillors-and-democracy/meetings-of-the-city-council/full-council-meetings/full-council-meetings-minutes/",
    "Cork County": "https://www.corkcoco.ie/en/council/elected-members-meetings-agendas-and-minutes/meetings-agendas-and-minutes/full-council-meetings",
}


def extract_doc(url, la):
    import fitz
    fname = url.split("/")[-1][:80]
    rec = {"url": url, "local_authority": la, "meeting": fname}
    text, votes = "", []
    try:
        r = requests.get(url, headers=C.HDRS, timeout=70)
        if r.status_code != 200:
            rec["status"] = "fetch_fail"
        elif url.lower().endswith(".html"):
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            for x in soup(["script", "style", "nav", "header", "footer"]):
                x.decompose()
            text = soup.get_text("\n", strip=True)
            rec["status"] = "html"
        else:
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
    rec["doc_type"] = dtype
    rec["text_chars"] = len(text)
    rec.update(C.parse_struct(text))
    ok, reason = C.classify(rec, text, dtype)
    rec["clean"], rec["reason"] = ok, reason
    if ok:
        cdir = C.CORPUS / C.slug(la)
        cdir.mkdir(exist_ok=True)
        (cdir / (C.slug(fname)[:80] + ".txt")).write_text(text, encoding="utf-8")
        rec["text_path"] = f"corpus/{C.slug(la)}/{C.slug(fname)[:80]}.txt"
    return rec, (votes if ok else [])


def main():
    # harvest correct Cork docs
    cork_docs = []
    for la, seed in CORK.items():
        try:
            docs = V2.collect_docs(seed)
        except Exception:  # noqa: BLE001
            docs = []
        print(f"{la}: {len(docs)} docs from corrected source")
        for url, _kind in docs:
            cork_docs.append((la, url))

    new_clean, new_quar, new_votes = [], [], []
    for la, url in cork_docs:
        rec, votes = extract_doc(url, la)
        (new_clean if rec["clean"] else new_quar).append(rec)
        new_votes += votes
        print(f"  [{rec['status']:10}] {rec['reason']:24} {url.split('/')[-1][:46]}")

    # load existing consolidated outputs, drop old Cork rows, append fresh Cork
    def load(p):
        return [json.loads(l) for l in Path(p).read_text(encoding="utf-8").splitlines() if l.strip()] \
            if Path(p).exists() else []
    clean = [r for r in load(HERE / "meetings_clean.jsonl") if not r["local_authority"].startswith("Cork")]
    quar = [r for r in load(HERE / "quarantine" / "quarantine.jsonl") if not r["local_authority"].startswith("Cork")]
    votes = [v for v in load(HERE / "member_votes_all.jsonl") if not v["local_authority"].startswith("Cork")]

    clean += new_clean
    quar += new_quar
    votes += C.norm_members(new_votes)

    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8")
    (HERE / "quarantine" / "quarantine.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in quar), encoding="utf-8")
    (HERE / "member_votes_all.jsonl").write_text(
        "\n".join(json.dumps(v, ensure_ascii=False) for v in votes), encoding="utf-8")
    C.write_quality_report(clean, quar, votes)
    print(f"\nCork folded in. clean={len(clean)} quar={len(quar)} votes={len(votes)} "
          f"(Cork new clean={len(new_clean)} quar={len(new_quar)})")


if __name__ == "__main__":
    main()
