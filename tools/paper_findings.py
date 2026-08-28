"""Stage 1 of the paper-findings pipeline: assemble paragraph-level findings.

Deliberately does NOT call an LLM itself. Stating a paragraph's claim is a
semantic judgment made by whoever is reading the paragraph (an agent, or a
person) — this module's job is the deterministic, testable part: take that
claim plus the paragraph's Stage-0 structure (figure link, next-paragraph
continuity, OCR-corruption flag) and assemble a schema-correct finding
record with the right confidence band, never re-reading the source PDF.

Every finding is capped at "Extracted" band at best (a lossy inference step
over Verified paragraph text, per .claude/rules/evidence.md's min() rule),
downgraded to "Indicative" whenever Stage 0 flagged possible OCR corruption
on that paragraph.

Usage:
    uv run python tools/paper_findings.py <blocks.json> <claims.json> [--out PATH]

`claims.json` maps the paragraph's position among paragraph blocks (as a
string index, "0", "1", ...) to either a claim string or null (no finding —
boilerplate, citation list, etc).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

VALID_BANDS = {"Verified", "Reported", "Extracted", "Indicative"}


def build_finding(block: dict, claim: str | None, paper_name: str) -> dict:
    """Assemble one finding record from a Stage-0 paragraph block and a
    supplied claim. Pure function — no I/O, no LLM call."""
    if block.get("type") != "paragraph":
        raise ValueError(f"build_finding requires a paragraph block, got {block.get('type')!r}")

    band = None
    if claim is not None:
        band = "Indicative" if block.get("possible_ocr_corruption") else "Extracted"

    return {
        "paper": paper_name,
        "page": block["page"],
        "paragraph_excerpt": block["text"][:200],
        "finding": claim,
        "band": band,
        "related_figure": block.get("related_figure"),
        "next_paragraph_excerpt": block.get("next_paragraph_text"),
        "citation": f"{paper_name} p.{block['page']}",
    }


def extract_findings(blocks: dict, claims: dict[str, str | None], paper_name: str) -> list[dict]:
    """Walk the paragraph blocks in order, pairing each with its claim by
    position (index among paragraph-type blocks only)."""
    paragraphs = [b for b in blocks["blocks"] if b["type"] == "paragraph"]
    findings = []
    for i, block in enumerate(paragraphs):
        claim = claims.get(str(i))
        findings.append(build_finding(block, claim, paper_name))
    return findings


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("blocks_path", type=Path)
    ap.add_argument("claims_path", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    if not args.blocks_path.exists():
        print(f"error: {args.blocks_path} does not exist", file=sys.stderr)
        return 1
    if not args.claims_path.exists():
        print(f"error: {args.claims_path} does not exist", file=sys.stderr)
        return 1

    blocks = json.loads(args.blocks_path.read_text(encoding="utf-8"))
    claims = json.loads(args.claims_path.read_text(encoding="utf-8"))
    paper_name = Path(blocks["source"]).name

    findings = extract_findings(blocks, claims, paper_name)

    out_path = args.out or args.blocks_path.parent / f"{args.blocks_path.stem}_findings.json"
    out_path.write_text(json.dumps(findings, indent=2), encoding="utf-8")

    with_finding = sum(1 for f in findings if f["finding"] is not None)
    print(f"paragraphs: {len(findings)}, with a finding: {with_finding}")
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
