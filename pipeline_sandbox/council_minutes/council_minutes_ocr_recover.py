"""Reprocess quarantined docs whose reason is now stale: `scanned_not_ocr` (winocr unblocks
them) and `not_minutes_standing_orders` (the doc_type() fix — a "conduct the meeting in line
with Standing Orders" boilerplate line was misclassifying real minutes as a standing-orders
regulation doc).

EXPERIMENTAL sandbox. council_minutes_consolidate.py quarantines scanned PDFs as
`staged_offbox_scanned`->`scanned_not_ocr` because its local OCR fallback (rapidocr, CPU/ONNX)
is bounded to MAX_OCR_DOCS=30 / MAX_OCR_PAGES=25 and PaddleOCR-GPU (the production engine)
crashes this box (feedback_paddleocr_crashes_local_box) — so the doc stays "staged" for an
off-box run. winocr wraps Windows.Media.Ocr, runs locally without that ban, and reconciled well
on real scanned planning PDFs (~136pp in ~3 min at 200dpi via PyMuPDF —
project_pharma_reconciliation_2026_07_31).

Each doc is re-fetched and re-classified with the CURRENT doc_type()/classify() from
council_minutes_consolidate — fitz text layer if present (no OCR cost, same path as a fresh
born-digital doc), winocr otherwise. Only docs whose stored `reason` is in TARGET_REASONS are
touched; everything else in meetings_clean.jsonl / quarantine.jsonl is left untouched. Results
merge back and QUALITY_ASSESSMENT.md regenerates via the same write_quality_report() the main
consolidation uses.

Usage:
    python council_minutes_ocr_recover.py [--limit N] [--council "Galway City"] [--reason not_minutes_standing_orders]
"""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import fitz
import requests

import council_minutes_consolidate as base

HERE = Path(__file__).resolve().parent
DPI = 200
TARGET_REASONS = {"scanned_not_ocr", "not_minutes_standing_orders"}


def _winocr_run():
    from PIL import Image
    import winocr

    def run(png: bytes) -> list[str]:
        img = Image.open(io.BytesIO(png))
        r = winocr.recognize_pil_sync(img)
        return [ln["text"] for ln in r.get("lines", [])]

    return run


def process_one(rec: dict, ocr) -> tuple[dict, list[dict]]:
    """Re-fetch + re-classify one previously-quarantined doc. Returns (record, votes)."""
    url, la, fname = rec["url"], rec["local_authority"], rec["meeting"]
    out = dict(rec)
    try:
        resp = requests.get(url, headers=base.HDRS, timeout=90)
        if resp.status_code != 200:
            out["status"] = "fetch_fail"
            out["clean"], out["reason"] = False, "extract_fetch_fail"
            return out, []
        doc = fitz.open(stream=resp.content, filetype="pdf")
        native = sum(len(p.get_text().strip()) for p in doc)
        if native >= base.TEXT_MIN * max(1, len(doc)):
            text = "\n".join(p.get_text() for p in doc)
            out["status"] = "text"
        else:
            text = "\n".join(
                "\n".join(ocr(page.get_pixmap(dpi=DPI).tobytes("png"))) for page in doc
            )
            out["status"] = "ocr_winocr"
        dvotes = base.votes_pdf(doc, la, fname)
    except Exception as e:  # noqa: BLE001
        out["status"] = f"err_{type(e).__name__}"
        out["clean"], out["reason"] = False, out["status"]
        return out, []

    dtype = base.doc_type(url, text)
    out["doc_type"] = dtype
    out["text_chars"] = len(text)
    out.update(base.parse_struct(text))
    ok, reason = base.classify(out, text, dtype)
    out["clean"], out["reason"] = ok, reason
    if ok:
        cdir = base.CORPUS / base.slug(la)
        cdir.mkdir(exist_ok=True)
        stem = base.slug(fname)[:80]
        (cdir / (stem + ".txt")).write_text(text, encoding="utf-8")
        out["text_path"] = f"corpus/{base.slug(la)}/{stem}.txt"
    return out, (dvotes if ok else [])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--council", default=None)
    ap.add_argument("--reason", action="append", default=None, help="restrict to one reason (repeatable)")
    args = ap.parse_args()
    target_reasons = set(args.reason) if args.reason else TARGET_REASONS

    quarantine = [
        json.loads(line)
        for line in (base.QDIR / "quarantine.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    to_retry = [r for r in quarantine if r.get("reason") in target_reasons]
    keep = [r for r in quarantine if r.get("reason") not in target_reasons]
    if args.council:
        held = [r for r in to_retry if r["local_authority"] != args.council]
        to_retry = [r for r in to_retry if r["local_authority"] == args.council]
        keep += held
    if args.limit:
        to_retry, deferred = to_retry[: args.limit], to_retry[args.limit :]
        keep += deferred

    print(f"reprocessing {len(to_retry)} docs ({sorted(target_reasons)}); "
          f"{len(keep)} deferred/other quarantine kept as-is")
    ocr = _winocr_run()

    clean = [
        json.loads(line)
        for line in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    votes = [
        json.loads(line)
        for line in (HERE / "member_votes_all.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ] if (HERE / "member_votes_all.jsonl").exists() else []

    recovered, still_quarantined, new_votes = 0, 0, 0
    for i, rec in enumerate(to_retry):
        result, dvotes = process_one(rec, ocr)
        if result["clean"]:
            clean.append(result)
            votes += dvotes
            new_votes += len(dvotes)
            recovered += 1
        else:
            keep.append(result)
            still_quarantined += 1
        print(
            f"[{i + 1}/{len(to_retry)}] {result['local_authority']} {result['meeting'][:50]} "
            f"({result['status']}) -> {'CLEAN' if result['clean'] else result['reason']} "
            f"(chars={result.get('text_chars', 0)})"
        )

    votes = base.norm_members(votes)
    (HERE / "meetings_clean.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in clean), encoding="utf-8"
    )
    (base.QDIR / "quarantine.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in keep), encoding="utf-8"
    )
    (HERE / "member_votes_all.jsonl").write_text(
        "\n".join(json.dumps(v, ensure_ascii=False) for v in votes), encoding="utf-8"
    )
    base.write_quality_report(clean, keep, votes)
    print(
        f"\nDONE recovered={recovered} still_quarantined={still_quarantined} "
        f"new_vote_rows={new_votes} total_clean={len(clean)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
