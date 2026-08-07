"""Recall probe for CE-report procurement leads: strict anchors vs a broad net.

EXPERIMENTAL sandbox. Per doc/EXTRACTION_QUALITY_CHECKLIST.md, absence of a strict-pattern
hit is not evidence a class is absent — proving it needs a BROAD search. This measures what
``procurement_leads()`` misses, at the grain it actually emits (sentences, not documents).

Method, following reference_extraction_hybrid_recipe (validated on council minutes):
  strict = extractors/council_ce_reports_contract.procurement_leads — the shipped anchors
  broad  = a deliberately loose commercial-activity net (high recall, low precision)
The GAP (broad-hit sentences the strict net did not emit) is the upper bound on missed
leads. A TF-IDF + LinearSVC model (positives = strict hits, negatives = no-broad-hit
sentences) ranks gap sentences by decision score so the likeliest TRUE misses surface first
for an independent P(True) labelling pass.

Sentences are split with the SAME splitter the extractor uses, so strict and broad counts
are measured over identical units. Comparing a document-grain broad net against a
sentence-grain strict net would inflate the gap and misstate recall.

Outputs: ce_lead_recall_gap.jsonl (ranked gap sentences) · CE_LEAD_RECALL_PROBE.md
Usage: python ce_lead_recall_probe.py [--sample-per-stratum N]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import services.runtime_env  # noqa: F401,E402  — must precede numpy/sklearn imports

import polars as pl  # noqa: E402

from extractors.council_ce_reports_contract import (  # noqa: E402
    _lead_spans,
    amount_mentions,
    procurement_leads,
)

HERE = Path(__file__).resolve().parent
CORPUS = HERE / "ce_reports_corpus"
MANIFEST = HERE / "ce_reports_manifest.jsonl"
PUBLISHED_CORPUS = ROOT / "data" / "gold" / "parquet" / "council_ce_reports_corpus.parquet"
OUT_GAP = HERE / "ce_lead_recall_gap.jsonl"
OUT_REPORT = HERE / "CE_LEAD_RECALL_PROBE.md"

# Deliberately loose: commercial-activity vocabulary the strict anchors do NOT require.
# The strict net needs "tender" or "contract" present; these phrasings can occur without
# either, which is precisely the blind spot being measured.
BROAD = re.compile(
    r"\b(?:procure(?:d|ment|ing)?|purchase(?:d|s|ing)?|framework|quotation|quote[sd]?|"
    r"consultan(?:t|ts|cy)|supplier|vendor|contractor|works?\s+(?:package|programme)|"
    r"appoint(?:ed|ment|ments|ing)?|engag(?:ed|ement|ing)|commission(?:ed|ing)?|"
    r"award(?:ed|ing|s)?|letter\s+of\s+acceptance|tender|contract|"
    r"design\s+team|service\s+provider|scheme\s+cost|capital\s+programme)\b",
    re.I,
)


def split_sentences(text: str) -> list[tuple[int, int, str]]:
    """Split page text into (start, end, quote) using the extractor's OWN span logic.

    This calls ``_lead_spans``, the same helper ``procurement_leads`` uses, rather than
    reimplementing the split. A simpler reimplementation misses the long-sentence path
    (spans over 800 chars are re-split on newlines), which silently reclassified 49 real
    leads as non-leads and fed them to the model as negatives.
    """
    value = str(text or "")
    out: list[tuple[int, int, str]] = []
    for span_start, span_end in _lead_spans(value):
        raw = value[span_start:span_end]
        left = len(raw) - len(raw.lstrip())
        right = len(raw.rstrip())
        quote = raw[left:right]
        if quote:
            out.append((span_start + left, span_start + right, quote))
    return out


def load_pages() -> list[dict]:
    """Read every PUBLISHED page with its council and document identity.

    Only documents the builder actually publishes are in scope. Two councils hold page
    files that the build excludes (unresolved report month, non-CE-report classification);
    scanning those would measure recall over documents no consumer can see.
    """
    published = {
        row["document_id"]
        for row in pl.read_parquet(PUBLISHED_CORPUS).select("document_id").unique().to_dicts()
    }
    records = {}
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            if row.get("pages_path") and str(row.get("document_id") or "") in published:
                records[row["pages_path"]] = row
    pages: list[dict] = []
    for pages_path, record in sorted(records.items()):
        path = HERE / pages_path
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            page_row = json.loads(line)
            pages.append(
                {
                    "council": str(record.get("council") or ""),
                    "document_id": str(record.get("document_id") or ""),
                    "page": int(page_row.get("page") or 0),
                    "text": str(page_row.get("text") or ""),
                }
            )
    return pages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-per-stratum", type=int, default=15)
    args = parser.parse_args()

    pages = load_pages()
    strict_rows: list[dict] = []
    gap_rows: list[dict] = []
    negative_texts: list[str] = []
    total_sentences = 0

    for page in pages:
        strict_spans = {
            (lead["source_span_start"], lead["source_span_end"])
            for lead in procurement_leads(page["text"])
        }
        for span_start, span_end, quote in split_sentences(page["text"]):
            total_sentences += 1
            is_strict = (span_start, span_end) in strict_spans
            is_broad = bool(BROAD.search(quote))
            if is_strict:
                strict_rows.append({**page, "quote": quote})
            elif is_broad:
                gap_rows.append(
                    {
                        "council": page["council"],
                        "document_id": page["document_id"],
                        "page": page["page"],
                        "source_span_start": span_start,
                        "source_span_end": span_end,
                        "quote": quote,
                        "amount_mentions": amount_mentions(quote),
                    }
                )
            else:
                negative_texts.append(quote)

    print(
        f"sentences: {total_sentences} | strict: {len(strict_rows)} | "
        f"gap (broad not strict): {len(gap_rows)} | neither: {len(negative_texts)}",
        flush=True,
    )

    # Rank the gap by similarity to confirmed strict hits so likely true misses sort first.
    ranked = gap_rows
    if strict_rows and gap_rows and negative_texts:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.svm import LinearSVC

        positives = [row["quote"] for row in strict_rows]
        negatives = negative_texts
        vec = TfidfVectorizer(ngram_range=(1, 2), max_features=40000, sublinear_tf=True)
        matrix = vec.fit_transform(positives + negatives)
        labels = [1] * len(positives) + [0] * len(negatives)
        model = LinearSVC(C=0.5, max_iter=5000).fit(matrix, labels)
        scores = model.decision_function(vec.transform([row["quote"] for row in gap_rows]))
        for row, score in zip(gap_rows, scores, strict=True):
            row["rank_score"] = round(float(score), 4)
        ranked = sorted(gap_rows, key=lambda row: row["rank_score"], reverse=True)
    else:
        for row in ranked:
            row["rank_score"] = 0.0

    # Stratify the labelling queue by council so one verbose council cannot carry the sample.
    queue: list[dict] = []
    per_council: Counter = Counter()
    for row in ranked:
        if per_council[row["council"]] < args.sample_per_stratum:
            queue.append({**row, "label_p_true": None, "labeller": "", "note": ""})
            per_council[row["council"]] += 1

    OUT_GAP.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in queue) + "\n",
        encoding="utf-8",
    )

    by_council = Counter(row["council"] for row in gap_rows)
    strict_by_council = Counter(row["council"] for row in strict_rows)
    lines = [
        "# CE-report lead recall probe",
        "",
        "Strict net = the shipped `procurement_leads()` anchors. Broad net = a loose",
        "commercial-activity net. The gap bounds what the strict net can miss; it is an",
        "UPPER bound, since the broad net is low-precision by design.",
        "",
        f"- sentences scanned: {total_sentences}",
        f"- strict-net leads: {len(strict_rows)}",
        f"- gap sentences (broad hit, strict miss): {len(gap_rows)}",
        f"- labelling queue written: {len(queue)} (max {args.sample_per_stratum}/council)",
        "",
        "| Council | strict | gap | gap share |",
        "|---|---:|---:|---:|",
    ]
    for council in sorted(set(by_council) | set(strict_by_council)):
        strict_n = strict_by_council[council]
        gap_n = by_council[council]
        denom = strict_n + gap_n
        share = f"{gap_n / denom:.0%}" if denom else "—"
        lines.append(f"| {council} | {strict_n} | {gap_n} | {share} |")
    lines += [
        "",
        "## Labelling protocol",
        "",
        "`ce_lead_recall_gap.jsonl` is a QUEUE, not a result. Recall stays unmeasured until the",
        "queue is labelled P(True) by readers who have not seen the strict net's output, per",
        "doc/EXTRACTION_QUALITY_CHECKLIST.md. Recall = strict / (strict + true misses), where",
        "true misses are extrapolated from the labelled sample's positive rate.",
        "",
        "Measured results for the current corpus live in CE_REPORTS_QUALITY.md; re-run this",
        "probe and re-label after ANY change to the anchors in council_ce_reports_contract.py.",
    ]
    OUT_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {OUT_GAP.name} and {OUT_REPORT.name}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
