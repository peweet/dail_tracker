"""Materialise vetted council minutes into a searchable gold parquet.

The harvest/OCR pipeline leaves extracted text and a manifest under
``pipeline_sandbox/council_minutes``. This build is the publication boundary: it
reclassifies document type defensively, excludes agendas and other non-minutes,
keeps committee and municipal scope distinct, hard-bounds search chunks, and
writes an atomic coverage/provenance sidecar.

Run: ``python -m extractors.council_minutes_corpus_build``
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from extractors.council_minutes_contract import (
    PUBLISHED_MINUTE_TYPES,
    chunk_text,
    classify_document,
    meeting_scope,
)
from services.coverage_io import save_coverage
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SANDBOX = ROOT / "pipeline_sandbox" / "council_minutes"
OUT = ROOT / "data" / "gold" / "parquet" / "council_minutes_corpus.parquet"
OUT_COVERAGE = ROOT / "data" / "_meta" / "council_minutes_corpus_coverage.json"


def build_corpus(
    manifest: Path = SANDBOX / "meetings_clean.jsonl",
    sandbox: Path = SANDBOX,
) -> tuple[pl.DataFrame, dict]:
    """Build the publication frame and its completeness/provenance sidecar.

    Missing text, non-minute documents and exact duplicate text are explicit
    exclusions. They are never silently counted as searchable minutes.
    """
    rows: list[dict] = []
    documents: list[dict] = []
    missing: list[dict] = []
    excluded = Counter()
    seen_content: dict[str, str] = {}
    duplicate_content: list[dict] = []
    input_records = 0

    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        input_records += 1
        record = json.loads(line)
        text_path = str(record.get("text_path") or "")
        path = sandbox / text_path if text_path else None
        if path is None or not path.exists():
            missing.append(
                {
                    "local_authority": record.get("local_authority", ""),
                    "meeting": record.get("meeting", ""),
                    "text_path": text_path,
                    "source_url": record.get("url", "") or "",
                }
            )
            continue

        text = path.read_text(encoding="utf-8", errors="replace").strip()
        doc_type = classify_document(
            meeting=str(record.get("meeting") or ""),
            source_url=str(record.get("url") or ""),
            text=text,
            upstream_doc_type=str(record.get("doc_type") or ""),
        )
        if doc_type not in PUBLISHED_MINUTE_TYPES:
            excluded[doc_type] += 1
            continue

        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        if content_hash in seen_content:
            duplicate_content.append(
                {
                    "meeting": record.get("meeting", ""),
                    "duplicate_of": seen_content[content_hash],
                    "content_sha256": content_hash,
                }
            )
            continue
        seen_content[content_hash] = text_path

        council = "Galway County" if record["local_authority"] == "Galway" else record["local_authority"]
        source_url = str(record.get("url") or "")
        document_id = hashlib.sha256(f"{council}|{source_url or text_path}".encode()).hexdigest()[:24]
        chunks = chunk_text(text)
        base = {
            "document_id": document_id,
            "entity_type": "local_authority",
            "council": council,
            "meeting": str(record.get("meeting") or "")[:160],
            "meeting_date": str(record.get("meeting_date") or ""),
            "doc_type": doc_type,
            "meeting_scope": meeting_scope(doc_type),
            "source_status": str(record.get("status") or ""),
            "source_url": source_url,
        }
        documents.append({**base, "text_chars": len(text), "chunks": len(chunks)})
        rows.extend({**base, "chunk": index, "body": chunk} for index, chunk in enumerate(chunks))

    if not rows:
        raise ValueError("council minutes build produced no publishable text")

    frame = pl.DataFrame(rows)
    councils = sorted({str(document["council"]) for document in documents})
    coverage = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "layer": "gold",
        "source_manifest": str(manifest.relative_to(ROOT) if manifest.is_relative_to(ROOT) else manifest),
        "input_records": input_records,
        "published_documents": len(documents),
        "published_chunks": frame.height,
        "published_councils": len(councils),
        "expected_statutory_councils": 31,
        "councils": councils,
        "documents_by_type": dict(sorted(Counter(d["doc_type"] for d in documents).items())),
        "documents_by_source_status": dict(sorted(Counter(d["source_status"] for d in documents).items())),
        "documents_with_date": sum(bool(d["meeting_date"]) for d in documents),
        "documents_with_source_url": sum(bool(d["source_url"]) for d in documents),
        "excluded_non_minutes": dict(sorted(excluded.items())),
        "missing_text_files": missing,
        "exact_duplicate_documents": duplicate_content,
        "completeness": (
            "Council coverage is measured against Ireland's 31 statutory local authorities. "
            "Document-level completeness still requires a verified meeting-calendar denominator per council."
        ),
        "recall": (
            "Document-type recall is unmeasured. Publication uses deterministic filename/header rules; "
            "agenda filenames are excluded even when their text quotes prior minutes."
        ),
        "caveat": (
            "All text is machine extracted. source_status=ocr_winocr carries OCR risk; committee and "
            "municipal-district minutes remain distinct from plenary council minutes."
        ),
    }
    return frame, coverage


def main() -> int:
    setup_standalone_logging("council_minutes_corpus_build")
    frame, coverage = build_corpus()
    save_parquet(frame, OUT)
    save_coverage(coverage, OUT_COVERAGE)
    log.info(
        "council_minutes_corpus: %d chunks from %d docs / %d councils -> %s",
        len(frame),
        coverage["published_documents"],
        coverage["published_councils"],
        OUT,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
