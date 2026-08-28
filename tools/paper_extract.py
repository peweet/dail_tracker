"""Deterministic paragraph/figure extraction for academic-paper PDFs.

Stage 0 of the paper-findings pipeline: turns a PDF into an ordered list of
text and figure blocks with real bounding-box positions, so a later
finding-extraction pass is grounded in structure that was actually measured
(not guessed from a caption, not re-derived by an agent "reading" a page
image each time). Per doc/EXTRACTION_QUALITY_CHECKLIST.md conventions:
page-level scanned/digital classification, no silent gaps, and a
Completeness/Recall summary shipped alongside the extracted blocks.

Usage:
    uv run python tools/paper_extract.py <pdf_path> [--out-dir DIR] [--render-figures]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
from scipy.optimize import linear_sum_assignment

MIN_DIGITAL_PAGE_CHARS = 20  # below this, treat the page as scanned (no usable text layer)
MIN_DRAWING_DIM_PT = 8  # filters decorative rules/borders out of cluster_drawings() output
CAPTION_MAX_DIST_PT = 150  # max center-to-center distance for a caption<->figure match
FIGURE_LINK_GAP_PT = 60  # max vertical gap between a paragraph and an "adjacent" figure
CAPTION_RE = re.compile(r"^(figure|fig\.?|table)\s*\d+", re.IGNORECASE)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _contained(inner: tuple, outer: tuple, min_overlap: float = 0.6) -> bool:
    """True if `inner` sits mostly inside `outer` (area-overlap ratio), used to
    drop in-figure text (axis labels, legends) from the paragraph stream."""
    ix0, iy0, ix1, iy1 = inner
    ox0, oy0, ox1, oy1 = outer
    overlap_w = max(0, min(ix1, ox1) - max(ix0, ox0))
    overlap_h = max(0, min(iy1, oy1) - max(iy0, oy0))
    overlap_area = overlap_w * overlap_h
    inner_area = max(1e-6, (ix1 - ix0) * (iy1 - iy0))
    return (overlap_area / inner_area) >= min_overlap


def _rect_gap(b1: tuple, b2: tuple) -> float:
    """Euclidean distance between the nearest edges of two bboxes (0 if they
    overlap/touch). Edge-to-edge, not center-to-center: a tall figure and a
    short caption sitting right on top of each other have centers far apart
    but a near-zero gap, which is what "adjacent" actually means here."""
    dx = max(b1[0] - b2[2], b2[0] - b1[2], 0)
    dy = max(b1[1] - b2[3], b2[1] - b1[3], 0)
    return (dx**2 + dy**2) ** 0.5


def _match_figures_to_captions(
    figure_bboxes: list[tuple],
    caption_blocks: list[dict],
    page_no: int,
    existing_count: int,
    is_scanned: bool,
) -> list[dict]:
    """Optimal 1:1 assignment (Hungarian algorithm) between figure candidates
    and caption text on the same page, minimizing total center-to-center
    distance — the same approach PDFFigures2 (Allen AI, 2016) uses, in place
    of a greedy first-match that breaks when two figures sit close together.
    """
    figs = []
    assigned_caption: dict[int, int] = {}
    if figure_bboxes and caption_blocks:
        cost = [[_rect_gap(fb, cb["bbox"]) for cb in caption_blocks] for fb in figure_bboxes]
        row_ind, col_ind = linear_sum_assignment(cost)
        assigned_caption = {r: c for r, c in zip(row_ind, col_ind, strict=True) if cost[r][c] <= CAPTION_MAX_DIST_PT}
    for i, bbox in enumerate(figure_bboxes):
        fig_id = f"p{page_no + 1}_fig{existing_count + len(figs) + 1}"
        caption = caption_blocks[assigned_caption[i]]["text"] if i in assigned_caption else None
        figs.append(
            {
                "id": fig_id,
                "page": page_no + 1,
                "bbox": bbox,
                "caption": caption,
                "band": "Verified" if not is_scanned else "Indicative",
            }
        )
    return figs


def _page_text_blocks(page: fitz.Page) -> list[dict]:
    raw = page.get_text("dict")["blocks"]
    blocks = []
    for b in raw:
        if b.get("type") == 0:  # text
            text = "".join(span["text"] for line in b.get("lines", []) for span in line.get("spans", [])).strip()
            if text:
                blocks.append({"bbox": b["bbox"], "text": text})
        elif b.get("type") == 1:  # image
            blocks.append({"bbox": b["bbox"], "image": True})
    return blocks


def _sentence_coherence_flag(text: str) -> bool:
    """Heuristic OCR-corruption signal: a wall of very short 'sentences' or
    a high fraction of single-char tokens usually means a mangled read
    order or bad OCR, not real prose. Extracted-band signal, not a verdict.
    """
    sentences = [s for s in SENTENCE_SPLIT_RE.split(text) if s.strip()]
    if len(sentences) < 2:
        return False
    short = sum(1 for s in sentences if len(s.split()) <= 2)
    tokens = text.split()
    single_char = sum(1 for t in tokens if len(t) == 1)
    return (short / len(sentences) > 0.5) or (tokens and single_char / len(tokens) > 0.3)


def extract(pdf_path: Path, render_figures: bool) -> dict:
    doc = fitz.open(pdf_path)
    pages_digital = 0
    pages_scanned = 0
    blocks: list[dict] = []
    figures: list[dict] = []

    for page_no in range(len(doc)):
        page = doc[page_no]
        page_blocks = _page_text_blocks(page)
        text_chars = sum(len(b["text"]) for b in page_blocks if "text" in b)
        is_scanned = text_chars < MIN_DIGITAL_PAGE_CHARS
        pages_scanned += is_scanned
        pages_digital += not is_scanned

        text_blocks = [b for b in page_blocks if "text" in b]
        image_blocks = [b for b in page_blocks if b.get("image")]

        drawing_clusters = [tuple(r) for r in page.cluster_drawings() if min(r.width, r.height) >= MIN_DRAWING_DIM_PT]
        figure_bboxes = [tuple(img["bbox"]) for img in image_blocks] + drawing_clusters
        caption_blocks = [tb for tb in text_blocks if CAPTION_RE.match(tb["text"])]

        page_figures = _match_figures_to_captions(figure_bboxes, caption_blocks, page_no, len(figures), is_scanned)
        figures.extend(page_figures)

        if is_scanned:
            blocks.append(
                {
                    "type": "page_gap",
                    "page": page_no + 1,
                    "band": "Indicative",
                    "reason": "no usable text layer on this page and no OCR pass has been run "
                    "(winocr is not wired into this script yet) — page content is missing, "
                    'not absent; do not treat blank as "no findings here"',
                }
            )
            continue

        for tb in text_blocks:
            if CAPTION_RE.match(tb["text"]):
                continue
            tx0, ty0, tx1, ty1 = tb["bbox"]
            if len(tb["text"].split()) <= 8 and any(_contained(tb["bbox"], fig["bbox"]) for fig in page_figures):
                continue  # short axis-label/legend text inside a figure — never drop a real
                # paragraph this way even if a mis-merged cluster geometrically covers it
            related_figure = None
            for fig in page_figures:
                fy0, fy1 = fig["bbox"][1], fig["bbox"][3]
                gap = min(abs(ty0 - fy1), abs(fy0 - ty1))
                if gap <= FIGURE_LINK_GAP_PT:
                    related_figure = fig["id"]
                    break
            blocks.append(
                {
                    "type": "paragraph",
                    "page": page_no + 1,
                    "bbox": tb["bbox"],
                    "text": tb["text"],
                    "band": "Verified",
                    "related_figure": related_figure,
                    "possible_ocr_corruption": _sentence_coherence_flag(tb["text"]),
                }
            )

    for i, b in enumerate(blocks):
        if b["type"] != "paragraph":
            continue
        nxt = next((x for x in blocks[i + 1 :] if x["type"] == "paragraph"), None)
        b["next_paragraph_text"] = nxt["text"][:120] if nxt else None

    if render_figures:
        out_dir = pdf_path.parent / f"{pdf_path.stem}_figures"
        out_dir.mkdir(exist_ok=True)
        for fig in figures:
            page = doc[fig["page"] - 1]
            clip = fitz.Rect(fig["bbox"])
            pix = page.get_pixmap(clip=clip, dpi=200)
            pix.save(out_dir / f"{fig['id']}.png")

    doc.close()

    return {
        "source": str(pdf_path),
        "completeness": {
            "total_pages": pages_digital + pages_scanned,
            "digital_pages": pages_digital,
            "scanned_pages_no_ocr": pages_scanned,
        },
        "recall": {
            "paragraphs_extracted": sum(1 for b in blocks if b["type"] == "paragraph"),
            "figures_found": len(figures),
            "flagged_possible_ocr_corruption": sum(
                1 for b in blocks if b["type"] == "paragraph" and b["possible_ocr_corruption"]
            ),
            "note": "figures = raster image blocks + page.cluster_drawings() vector clusters "
            "(MIN_DRAWING_DIM_PT filter drops rules/borders); captions matched by "
            "optimal 1:1 assignment (Hungarian algorithm) on center distance, not a "
            "trained layout model — a table's bordered cells can cluster as multiple "
            "unmatched figure candidates on the same page. Verify a sample before "
            "trusting at scale (fewer than ~5 checked examples stays Indicative per "
            "evidence.md).",
        },
        "figures": figures,
        "blocks": blocks,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("pdf_path", type=Path)
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--render-figures", action="store_true")
    args = ap.parse_args()

    if not args.pdf_path.exists():
        print(f"error: {args.pdf_path} does not exist", file=sys.stderr)
        return 1

    result = extract(args.pdf_path, args.render_figures)

    out_dir = args.out_dir or args.pdf_path.parent
    out_path = out_dir / f"{args.pdf_path.stem}_blocks.json"
    out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    c, r = result["completeness"], result["recall"]
    print(f"pages: {c['total_pages']} ({c['digital_pages']} digital, {c['scanned_pages_no_ocr']} scanned/no-OCR)")
    print(
        f"paragraphs: {r['paragraphs_extracted']}, figures: {r['figures_found']}, "
        f"flagged: {r['flagged_possible_ocr_corruption']}"
    )
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
