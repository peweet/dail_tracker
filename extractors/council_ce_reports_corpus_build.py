"""Build a separate CE-report corpus and source-linked commercial follow-up queue.

CE reports are not minutes. This builder reads only ``ce_reports_manifest.jsonl`` and never
changes the council-minutes gold corpus or its publication types.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from extractors.council_ce_reports_contract import CE_REPORT_TYPE, amount_mentions, procurement_leads
from extractors.council_minutes_contract import chunk_text, classify_document
from services.coverage_io import save_coverage
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
SANDBOX = ROOT / "pipeline_sandbox" / "council_minutes"
MANIFEST = SANDBOX / "ce_reports_manifest.jsonl"
SEED_REGISTRY = SANDBOX / "ce_report_seeds.csv"
REVIEW_LEDGER = SANDBOX / "ce_report_lead_reviews.csv"
CENSUS = SANDBOX / "ce_report_census.json"
OUT_CORPUS = ROOT / "data" / "gold" / "parquet" / "council_ce_reports_corpus.parquet"
OUT_LEADS = ROOT / "data" / "gold" / "parquet" / "council_ce_report_leads.parquet"
OUT_COVERAGE = ROOT / "data" / "_meta" / "council_ce_reports_corpus_coverage.json"

_CORPUS_SCHEMA = {
    "document_id": pl.String,
    "doc_type": pl.String,
    "council": pl.String,
    "report_title": pl.String,
    "report_month": pl.String,
    "date_parse_status": pl.String,
    "source_status": pl.String,
    "source_landing_url": pl.String,
    "source_url": pl.String,
    "source_file_hash": pl.String,
    "source_page": pl.Int64,
    "chunk": pl.Int64,
    "source_locator": pl.String,
    "body": pl.String,
    "amount_mentions": pl.List(pl.String),
    "procurement_sentences": pl.List(pl.String),
    "evidence_band": pl.String,
}
_REVIEWED_RELEVANT = "REVIEWED_RELEVANT"
_RESOLVED_SITE_STATES = {"MATCHED", "NOT_MATCHED"}
_REVIEWED_STAGES = {
    "pre_tender_approval",
    "procurement_planned",
    "tender_preparation",
    "framework_mini_competition",
}

_LEAD_SCHEMA = {
    "lead_id": pl.String,
    "document_id": pl.String,
    "doc_type": pl.String,
    "council": pl.String,
    "report_title": pl.String,
    "report_month": pl.String,
    "date_parse_status": pl.String,
    "source_status": pl.String,
    "source_landing_url": pl.String,
    "source_url": pl.String,
    "source_file_hash": pl.String,
    "source_page": pl.Int64,
    "source_span_start": pl.Int64,
    "source_span_end": pl.Int64,
    "source_locator": pl.String,
    "quote": pl.String,
    "lead_types": pl.List(pl.String),
    "amount_mentions": pl.List(pl.String),
    "evidence_band": pl.String,
    "reviewer_state": pl.String,
    "relevance_status": pl.String,
    "site_relationship": pl.String,
    "reviewed_project_name": pl.String,
    "reviewed_stage": pl.String,
    "reviewed_by": pl.String,
    "reviewed_utc": pl.String,
    "review_note": pl.String,
    "publication_status": pl.String,
    "promotion_permitted": pl.Boolean,
}


def _frame(rows: list[dict], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)


def _load_review_ledger(path: Path) -> dict[str, dict]:
    """Read reviewer decisions keyed by lead_id; absent ledger means nothing is reviewed."""
    if not path.exists():
        return {}
    ledger: dict[str, dict] = {}
    for row in csv.DictReader(path.open(encoding="utf-8")):
        lead_id = str(row.get("lead_id") or "").strip()
        if lead_id:
            ledger[lead_id] = row
    return ledger


def _review_state(ledger_row: dict | None) -> dict:
    """Derive publication state from a reviewer decision.

    Promotion is a data question answered by review, never a code default: a lead is
    promotable only when a human marked it relevant AND resolved its site relationship.
    An absent or partial decision stays in the review queue.
    """
    if not ledger_row:
        return {
            "reviewer_state": "NOT_REVIEWED",
            "relevance_status": "NOT_REVIEWED",
            "site_relationship": "UNRESOLVED",
            "reviewed_project_name": None,
            "reviewed_stage": None,
            "reviewed_by": None,
            "reviewed_utc": None,
            "review_note": None,
            "publication_status": "review_queue_only",
            "promotion_permitted": False,
        }
    relevance = str(ledger_row.get("relevance_status") or "NOT_REVIEWED").strip().upper()
    site = str(ledger_row.get("site_relationship") or "UNRESOLVED").strip().upper()
    reviewer = str(ledger_row.get("reviewer_state") or "REVIEWED").strip().upper()
    project_name = str(ledger_row.get("reviewed_project_name") or "").strip() or None
    stage = str(ledger_row.get("reviewed_stage") or "").strip().lower() or None
    reviewed_by = str(ledger_row.get("reviewer") or "").strip() or None
    reviewed_utc = str(ledger_row.get("reviewed_utc") or "").strip() or None
    review_note = str(ledger_row.get("note") or "").strip() or None
    promotable = (
        relevance == _REVIEWED_RELEVANT
        and site in _RESOLVED_SITE_STATES
        and reviewer in {"REVIEWED", "REVIEWED_ASSISTED"}
        and bool(project_name)
        and stage in _REVIEWED_STAGES
    )
    return {
        "reviewer_state": reviewer,
        "relevance_status": relevance,
        "site_relationship": site,
        "reviewed_project_name": project_name,
        "reviewed_stage": stage,
        "reviewed_by": reviewed_by,
        "reviewed_utc": reviewed_utc,
        "review_note": review_note,
        "publication_status": "promotable" if promotable else "review_queue_only",
        "promotion_permitted": promotable,
    }


def build_ce_report_corpus(
    manifest: Path = MANIFEST,
    sandbox: Path = SANDBOX,
    seed_registry: Path = SEED_REGISTRY,
    review_ledger: Path = REVIEW_LEDGER,
    census: Path = CENSUS,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """Build page-aware search chunks and lead sentences with exact locators.

    Lead publication state is derived from ``review_ledger`` — see ``_review_state``.
    """
    manifest_rows = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines() if line.strip()]
    seed_rows = list(csv.DictReader(seed_registry.open(encoding="utf-8"))) if seed_registry.exists() else []
    ledger = _load_review_ledger(review_ledger)
    census_payload = json.loads(census.read_text(encoding="utf-8")) if census.exists() else None
    corpus_rows: list[dict] = []
    lead_rows: list[dict] = []
    documents: list[dict] = []
    exclusions = Counter()
    exclusions_by_council: dict[str, Counter] = {}

    def _exclude(record: dict, reason: str) -> None:
        """Count an exclusion at both corpus and council grain.

        Reason-only totals hide whole councils: a council whose every record is excluded
        disappears from documents_by_council with nothing saying why.
        """
        exclusions[reason] += 1
        council = str(record.get("council") or "").strip() or "(unattributed)"
        exclusions_by_council.setdefault(council, Counter())[reason] += 1

    seen_hashes: dict[str, str] = {}
    duplicates: list[dict] = []

    for record in manifest_rows:
        if record.get("doc_type") != CE_REPORT_TYPE:
            _exclude(record, "wrong_doc_type")
            continue
        if record.get("extraction_status") not in {"text", "ocr_winocr"}:
            _exclude(record, str(record.get("extraction_status") or "unknown_status"))
            continue
        source_url = str(record.get("source_url") or "")
        if not source_url:
            _exclude(record, "missing_source_url")
            continue
        pages_path = str(record.get("pages_path") or "")
        path = sandbox / pages_path if pages_path else None
        if path is None or not path.exists():
            _exclude(record, "missing_pages_file")
            continue
        report_month = str(record.get("report_month") or "")
        if not report_month:
            _exclude(record, "unresolved_report_month")
            continue
        page_records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        classified_doc_type = classify_document(
            meeting=str(record.get("report_title") or ""),
            source_url=source_url,
            text="\n".join(str(row.get("text") or "") for row in page_records[:2]),
        )
        if classified_doc_type != CE_REPORT_TYPE:
            _exclude(record, f"classified_{classified_doc_type}")
            continue
        source_hash = str(record.get("source_file_hash") or "")
        if source_hash and source_hash in seen_hashes:
            duplicates.append(
                {
                    "document_id": record.get("document_id", ""),
                    "duplicate_of": seen_hashes[source_hash],
                    "source_file_hash": source_hash,
                }
            )
            continue
        if source_hash:
            seen_hashes[source_hash] = str(record.get("document_id") or "")

        document_id = str(record.get("document_id") or "")
        common = {
            "document_id": document_id,
            "doc_type": CE_REPORT_TYPE,
            "council": str(record.get("council") or ""),
            "report_title": str(record.get("report_title") or ""),
            "report_month": report_month,
            "date_parse_status": str(record.get("date_parse_status") or "unresolved"),
            "source_status": str(record.get("extraction_status") or ""),
            "source_landing_url": str(record.get("source_landing_url") or ""),
            "source_url": source_url,
            "source_file_hash": source_hash,
        }
        page_count = 0
        document_leads = 0
        document_amounts = 0
        for page_row in page_records:
            page = int(page_row["page"])
            page_text = str(page_row.get("text") or "")
            page_count += 1
            document_amounts += len(amount_mentions(page_text))
            for chunk_index, body in enumerate(chunk_text(page_text), start=1):
                corpus_rows.append(
                    {
                        **common,
                        "source_page": page,
                        "chunk": chunk_index,
                        "source_locator": f"page:{page}#chunk:{chunk_index}",
                        "body": body,
                        "amount_mentions": amount_mentions(body),
                        "procurement_sentences": [lead["quote"] for lead in procurement_leads(body)],
                        "evidence_band": "Extracted",
                    }
                )
            for lead in procurement_leads(page_text):
                lead_id = hashlib.sha256(
                    f"{document_id}|{page}|{lead['source_span_start']}|{lead['quote']}".encode()
                ).hexdigest()[:24]
                lead_rows.append(
                    {
                        **common,
                        "lead_id": lead_id,
                        "source_page": page,
                        "source_span_start": lead["source_span_start"],
                        "source_span_end": lead["source_span_end"],
                        "source_locator": f"page:{page}#chars:{lead['source_span_start']}-{lead['source_span_end']}",
                        "quote": lead["quote"],
                        "lead_types": lead["lead_types"],
                        "amount_mentions": lead["amount_mentions"],
                        "evidence_band": "Extracted",
                        **_review_state(ledger.get(lead_id)),
                    }
                )
                document_leads += 1
        documents.append(
            {
                **common,
                "pages": page_count,
                "lead_sentences": document_leads,
                "amount_mentions": document_amounts,
            }
        )

    corpus = _frame(corpus_rows, _CORPUS_SCHEMA)
    leads = _frame(lead_rows, _LEAD_SCHEMA)
    published_councils = sorted({row["council"] for row in documents})
    harvested_councils = sorted(
        str(row.get("council") or "").strip() for row in manifest_rows if str(row.get("council") or "").strip()
    )
    harvested_without_published = sorted(set(harvested_councils) - set(published_councils))
    manifest_councils = sorted({str(row.get("council") or "") for row in manifest_rows if row.get("council")})
    confirmed_councils = sorted(row["local_authority"] for row in seed_rows if row.get("status") == "confirmed")
    coverage = {
        "schema": "council-ce-reports-corpus/1",
        "generated_utc": datetime.now(UTC).isoformat(),
        "source_manifest": str(manifest.relative_to(ROOT) if manifest.is_relative_to(ROOT) else manifest),
        "input_records": len(manifest_rows),
        "published_documents": len(documents),
        "published_chunks": corpus.height,
        "review_queue_leads": leads.height,
        "published_councils": len(published_councils),
        "published_council_names": published_councils,
        "source_registry_councils": len(seed_rows),
        "source_registry_by_status": dict(sorted(Counter(row.get("status", "") for row in seed_rows).items())),
        "manifest_record_councils": len(manifest_councils),
        "manifest_council_names": manifest_councils,
        "confirmed_councils_without_manifest_record": sorted(set(confirmed_councils) - set(manifest_councils)),
        "registry_absent_councils": sorted(
            row["local_authority"] for row in seed_rows if row.get("status") == "absent"
        ),
        "registry_engineering_councils": sorted(
            row["local_authority"] for row in seed_rows if row.get("status") == "engineering"
        ),
        "source_census": (
            {
                "schema": census_payload.get("schema"),
                "generated_utc": census_payload.get("generated_utc"),
                "run_state": census_payload.get("run_state"),
                "selection": census_payload.get("selection"),
                "complete_council_accounting": census_payload.get("complete_council_accounting"),
                "outcome_counts": census_payload.get("outcome_counts", {}),
                "recall": census_payload.get("recall"),
            }
            if census_payload
            else {"status": "not_run", "recall": "Unmeasured; no source-census artifact is present."}
        ),
        "documents_by_council": dict(sorted(Counter(row["council"] for row in documents).items())),
        "documents_by_source_status": dict(sorted(Counter(row["source_status"] for row in documents).items())),
        "documents_with_parsed_month": sum(bool(row["report_month"]) for row in documents),
        "documents_with_amount_mentions": sum(bool(row["amount_mentions"]) for row in documents),
        "documents_with_procurement_leads": sum(bool(row["lead_sentences"]) for row in documents),
        "excluded_records": dict(sorted(exclusions.items())),
        "excluded_records_by_council": {
            council: dict(sorted(reasons.items())) for council, reasons in sorted(exclusions_by_council.items())
        },
        "harvested_councils_without_published_documents": harvested_without_published,
        "review_ledger_decisions": len(ledger),
        "leads_promotable": int(leads["promotion_permitted"].sum()) if leads.height else 0,
        "leads_in_review_queue": (int((~leads["promotion_permitted"]).sum()) if leads.height else 0),
        "exact_duplicate_documents": duplicates,
        "publication_boundary": (
            "CE reports are officer reports, not council minutes. Lead sentences are Extracted-band. "
            "promotion_permitted is derived per lead from ce_report_lead_reviews.csv and is true only "
            "where a reviewer set relevance_status=REVIEWED_RELEVANT, resolved site_relationship, "
            "and supplied a source-verified project name and allowed procurement stage; "
            "unreviewed leads stay review_queue_only."
        ),
        "recall": "Unmeasured; this is a source-registry pilot, not a complete meeting-calendar census.",
    }
    return corpus, leads, coverage


def main() -> int:
    setup_standalone_logging("council_ce_reports_corpus_build")
    corpus, leads, coverage = build_ce_report_corpus()
    if corpus.is_empty():
        raise ValueError("CE-report build produced no publishable page text")
    for council in coverage["harvested_councils_without_published_documents"]:
        log.warning(
            "council_ce_reports: %s harvested but published 0 documents (%s)",
            council,
            coverage["excluded_records_by_council"].get(council, {}),
        )
    save_parquet(corpus, OUT_CORPUS)
    save_parquet(leads, OUT_LEADS)
    save_coverage(coverage, OUT_COVERAGE)
    log.info(
        "council_ce_reports: %d chunks / %d leads from %d docs / %d councils",
        corpus.height,
        leads.height,
        coverage["published_documents"],
        coverage["published_councils"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
