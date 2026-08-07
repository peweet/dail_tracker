from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

from extractors.council_ce_reports_contract import amount_mentions, procurement_leads
from extractors.council_ce_reports_corpus_build import build_ce_report_corpus
from pipeline_sandbox.council_minutes.ce_report_harvest import (
    _census_payload,
    discover_report_links,
    report_month,
    report_month_from_source,
)

ROOT = Path(__file__).resolve().parents[2]


def test_seed_registry_accounts_for_all_statutory_councils() -> None:
    path = ROOT / "pipeline_sandbox/council_minutes/ce_report_seeds.csv"
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    assert len(rows) == 31
    assert len({row["local_authority"] for row in rows}) == 31
    assert Counter(row["status"] for row in rows) == {
        "confirmed": 24,
        "absent": 4,
        "engineering": 3,
    }


def test_report_discovery_requires_source_label_and_parses_month() -> None:
    html = """
    <a href="/files/Chief-Executive-Monthly-Report-March-2026.pdf">March report</a>
    <a href="/files/generic-budget.pdf">Budget</a>
    """
    assert discover_report_links("https://council.example/reports/", html) == [
        (
            "https://council.example/files/Chief-Executive-Monthly-Report-March-2026.pdf",
            "March report",
        )
    ]
    assert report_month("Chief-Executive-Monthly-Report-March-2026.pdf") == (
        "2026-03",
        "parsed_month",
    )
    assert report_month("management-report.pdf") == ("", "unresolved")
    assert report_month("26-06 Chief Executive Report.pdf") == ("2026-06", "parsed_month")
    assert report_month_from_source(
        "",
        "https://council.example/2026-01/chief-executive-report-december-2025.pdf",
    ) == ("2025-12", "parsed_month")


def test_literal_amounts_and_procurement_leads_keep_exact_spans() -> None:
    text = (
        "Funding of EUR 572,334 was approved. "
        "Invitations to tender have been sent to 7 contractors. "
        "The contract was awarded at €23.8m."
    )
    assert amount_mentions(text) == ["EUR 572,334", "€23.8m"]
    assert amount_mentions("€2,737,384 \nMarket Square") == ["€2,737,384"]
    leads = procurement_leads(text)
    assert len(leads) == 2
    assert leads[0]["lead_types"] == ["tender", "contract"]
    assert leads[1]["lead_types"] == ["contract", "award"]
    for lead in leads:
        assert text[lead["source_span_start"] : lead["source_span_end"]] == lead["quote"]


def test_builder_keeps_ce_reports_separate_and_leads_unpromoted(tmp_path: Path) -> None:
    pages = tmp_path / "ce_reports_corpus/council/doc.pages.jsonl"
    pages.parent.mkdir(parents=True)
    page_text = (
        "Chief Executive Monthly Management Report.\n"
        "Funding of EUR 572,334 was approved.\n"
        "Invitations to tender have been sent to 7 contractors for two bridges.\n"
        "The contract was awarded at €23.8m."
    )
    pages.write_text(json.dumps({"page": 3, "text": page_text}) + "\n", encoding="utf-8")
    records = [
        {
            "document_id": "doc-1",
            "doc_type": "ce_report",
            "council": "Council",
            "report_title": "March 2026 report",
            "report_month": "2026-03",
            "date_parse_status": "parsed_month",
            "source_landing_url": "https://council.example/reports",
            "source_url": "https://council.example/report.pdf",
            "source_file_hash": "hash-1",
            "source_pages": 1,
            "extraction_status": "text",
            "pages_path": "ce_reports_corpus/council/doc.pages.jsonl",
        },
        {
            "document_id": "doc-2",
            "doc_type": "ce_report",
            "council": "Council",
            "source_url": "https://council.example/scanned.pdf",
            "extraction_status": "needs_ocr",
        },
    ]
    manifest = tmp_path / "ce_reports_manifest.jsonl"
    manifest.write_text("\n".join(json.dumps(row) for row in records), encoding="utf-8")

    corpus, leads, coverage = build_ce_report_corpus(manifest, tmp_path)

    assert corpus["document_id"].unique().to_list() == ["doc-1"]
    assert corpus["source_page"].unique().to_list() == [3]
    assert corpus["evidence_band"].unique().to_list() == ["Extracted"]
    assert leads.height == 2
    assert leads["reviewer_state"].unique().to_list() == ["NOT_REVIEWED"]
    assert leads["site_relationship"].unique().to_list() == ["UNRESOLVED"]
    assert not leads["promotion_permitted"].any()
    assert not any("score" in column or "probability" in column for column in leads.columns)
    assert coverage["published_documents"] == 1
    assert coverage["excluded_records"] == {"needs_ocr": 1}
    assert coverage["excluded_records_by_council"] == {"Council": {"needs_ocr": 1}}
    assert coverage["leads_promotable"] == 0
    assert coverage["leads_in_review_queue"] == 2


def _single_report_fixture(tmp_path: Path) -> Path:
    """Write one publishable CE-report manifest whose page text yields two leads."""
    pages = tmp_path / "ce_reports_corpus/council/doc.pages.jsonl"
    pages.parent.mkdir(parents=True)
    pages.write_text(
        json.dumps(
            {
                "page": 3,
                "text": (
                    "Chief Executive Monthly Management Report.\n"
                    "Invitations to tender have been sent to 7 contractors for two bridges.\n"
                    "The contract was awarded at €23.8m."
                ),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "ce_reports_manifest.jsonl"
    manifest.write_text(
        json.dumps(
            {
                "document_id": "doc-1",
                "doc_type": "ce_report",
                "council": "Council",
                "report_title": "March 2026 report",
                "report_month": "2026-03",
                "date_parse_status": "parsed_month",
                "source_landing_url": "https://council.example/reports",
                "source_url": "https://council.example/report.pdf",
                "source_file_hash": "hash-1",
                "source_pages": 1,
                "extraction_status": "text",
                "pages_path": "ce_reports_corpus/council/doc.pages.jsonl",
            }
        ),
        encoding="utf-8",
    )
    return manifest


def _write_ledger(tmp_path: Path, rows: list[dict]) -> Path:
    ledger = tmp_path / "ce_report_lead_reviews.csv"
    fields = [
        "lead_id",
        "reviewer_state",
        "relevance_status",
        "site_relationship",
        "reviewed_project_name",
        "reviewed_stage",
    ]
    with ledger.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    return ledger


def test_absent_review_ledger_keeps_every_lead_in_the_queue(tmp_path: Path) -> None:
    manifest = _single_report_fixture(tmp_path)

    _, leads, coverage = build_ce_report_corpus(manifest, tmp_path, review_ledger=tmp_path / "missing.csv")

    assert leads.height == 2
    assert not leads["promotion_permitted"].any()
    assert leads["publication_status"].unique().to_list() == ["review_queue_only"]
    assert coverage["review_ledger_decisions"] == 0


def test_promotion_requires_relevance_resolved_context_project_and_stage(tmp_path: Path) -> None:
    manifest = _single_report_fixture(tmp_path)
    _, leads, _ = build_ce_report_corpus(manifest, tmp_path, review_ledger=tmp_path / "missing.csv")
    promoted_id, partial_id = leads["lead_id"].to_list()

    ledger = _write_ledger(
        tmp_path,
        [
            {
                "lead_id": promoted_id,
                "reviewer_state": "REVIEWED_ASSISTED",
                "relevance_status": "REVIEWED_RELEVANT",
                "site_relationship": "MATCHED",
                "reviewed_project_name": "Named civic works",
                "reviewed_stage": "tender_preparation",
            },
            # Reviewed as relevant, but the site relationship is still open — not promotable.
            {
                "lead_id": partial_id,
                "reviewer_state": "REVIEWED",
                "relevance_status": "REVIEWED_RELEVANT",
                "site_relationship": "UNRESOLVED",
                "reviewed_project_name": "Second civic works",
                "reviewed_stage": "tender_preparation",
            },
        ],
    )

    _, leads, coverage = build_ce_report_corpus(manifest, tmp_path, review_ledger=ledger)

    by_id = {row["lead_id"]: row for row in leads.to_dicts()}
    assert by_id[promoted_id]["promotion_permitted"] is True
    assert by_id[promoted_id]["publication_status"] == "promotable"
    assert by_id[promoted_id]["reviewed_project_name"] == "Named civic works"
    assert by_id[promoted_id]["reviewed_stage"] == "tender_preparation"
    assert by_id[partial_id]["promotion_permitted"] is False
    assert by_id[partial_id]["publication_status"] == "review_queue_only"
    assert coverage["leads_promotable"] == 1
    assert coverage["leads_in_review_queue"] == 1
    assert coverage["review_ledger_decisions"] == 2


def test_promotion_rejects_missing_project_name_or_unknown_stage(tmp_path: Path) -> None:
    manifest = _single_report_fixture(tmp_path)
    _, leads, _ = build_ce_report_corpus(manifest, tmp_path, review_ledger=tmp_path / "missing.csv")
    missing_name_id, invalid_stage_id = leads["lead_id"].to_list()
    ledger = _write_ledger(
        tmp_path,
        [
            {
                "lead_id": missing_name_id,
                "reviewer_state": "REVIEWED_ASSISTED",
                "relevance_status": "REVIEWED_RELEVANT",
                "site_relationship": "MATCHED",
                "reviewed_project_name": "",
                "reviewed_stage": "tender_preparation",
            },
            {
                "lead_id": invalid_stage_id,
                "reviewer_state": "REVIEWED_ASSISTED",
                "relevance_status": "REVIEWED_RELEVANT",
                "site_relationship": "MATCHED",
                "reviewed_project_name": "Named works",
                "reviewed_stage": "unknown_stage",
            },
        ],
    )

    _, reviewed, coverage = build_ce_report_corpus(manifest, tmp_path, review_ledger=ledger)

    assert not reviewed["promotion_permitted"].any()
    assert coverage["leads_promotable"] == 0


def test_reviewed_irrelevant_lead_is_never_promoted(tmp_path: Path) -> None:
    manifest = _single_report_fixture(tmp_path)
    _, leads, _ = build_ce_report_corpus(manifest, tmp_path, review_ledger=tmp_path / "missing.csv")
    lead_id = leads["lead_id"].to_list()[0]
    ledger = _write_ledger(
        tmp_path,
        [
            {
                "lead_id": lead_id,
                "reviewer_state": "REVIEWED",
                "relevance_status": "REVIEWED_IRRELEVANT",
                "site_relationship": "MATCHED",
            }
        ],
    )

    _, leads, coverage = build_ce_report_corpus(manifest, tmp_path, review_ledger=ledger)

    assert not leads["promotion_permitted"].any()
    assert coverage["leads_promotable"] == 0


def test_council_with_every_record_excluded_is_named_not_silently_dropped(tmp_path: Path) -> None:
    """A council can vanish from documents_by_council; it must not vanish from coverage."""
    manifest = _single_report_fixture(tmp_path)
    rows = [json.loads(manifest.read_text(encoding="utf-8"))]
    rows.append(
        {
            "document_id": "doc-2",
            "doc_type": "ce_report",
            "council": "Silent County",
            "report_title": "Undated report",
            "report_month": "",
            "source_url": "https://silent.example/report.pdf",
            "extraction_status": "text",
            "pages_path": "ce_reports_corpus/council/doc.pages.jsonl",
        }
    )
    manifest.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    _, _, coverage = build_ce_report_corpus(manifest, tmp_path, review_ledger=tmp_path / "missing.csv")

    assert "Silent County" not in coverage["documents_by_council"]
    assert coverage["harvested_councils_without_published_documents"] == ["Silent County"]
    assert coverage["excluded_records_by_council"]["Silent County"] == {"unresolved_report_month": 1}


def test_source_census_accounts_for_every_registry_council_without_claiming_recall() -> None:
    seeds = [
        {"local_authority": "Available", "status": "confirmed"},
        {"local_authority": "Absent", "status": "absent"},
    ]
    outcomes = {
        "Available": {"council": "Available", "registry_status": "confirmed", "outcome": "links_selected"},
        "Absent": {"council": "Absent", "registry_status": "absent", "outcome": "source_absent"},
    }

    census = _census_payload(seeds, outcomes, run_state="complete", latest_per_council=1)

    assert census["complete_council_accounting"] is True
    assert census["outcome_counts"] == {"links_selected": 1, "source_absent": 1}
    assert "calendar census" in census["recall"]


def test_builder_surfaces_source_census_without_conflating_it_with_recall(tmp_path: Path) -> None:
    manifest = _single_report_fixture(tmp_path)
    census_path = tmp_path / "ce_report_census.json"
    census_path.write_text(
        json.dumps(
            {
                "schema": "council-ce-report-source-census/1",
                "generated_utc": "2026-08-07T00:00:00+00:00",
                "run_state": "complete",
                "selection": {"latest_per_council": 1},
                "complete_council_accounting": True,
                "outcome_counts": {"links_selected": 1},
                "recall": "Unmeasured: bounded discovery only.",
            }
        ),
        encoding="utf-8",
    )

    _, _, coverage = build_ce_report_corpus(
        manifest,
        tmp_path,
        review_ledger=tmp_path / "missing.csv",
        census=census_path,
    )

    assert coverage["source_census"]["complete_council_accounting"] is True
    assert coverage["source_census"]["recall"] == "Unmeasured: bounded discovery only."
