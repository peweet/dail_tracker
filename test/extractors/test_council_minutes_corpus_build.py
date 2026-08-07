from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from extractors.council_minutes_contract import (
    chunk_text,
    classify_document,
    extract_participation_signals,
)
from extractors.council_minutes_corpus_build import build_corpus
from pipeline_sandbox.council_minutes.council_minutes_consolidate import doc_type as sandbox_doc_type
from pipeline_sandbox.council_minutes.recover_misclassified_minutes import (
    is_recovery_candidate,
)


def test_agenda_filename_wins_over_quoted_previous_minutes() -> None:
    assert (
        classify_document(
            meeting="260525 Monthly Agenda Full Council.pdf",
            source_url="https://example.ie/Agendas/260525%20Monthly%20Agenda%20Full%20Council.pdf",
            text="Confirmation of the minutes of the previous meeting\nHousing report",
        )
        == "agenda"
    )

    assert (
        sandbox_doc_type(
            "https://www.offaly.ie/meetings/Minutes/January_Agenda.pdf",
            "Agenda\nMinutes of the previous meeting\nChief Executive Management Report",
        )
        == "agenda"
    )


@pytest.mark.parametrize(
    "source_url",
    [
        "https://kilkennycoco.ie/meetings/minutes-of-special-meeting-06-05-2025.pdf",
        "https://monaghan.ie/Draft-Council-Mtg-Minutes-05-January-2026.pdf",
        "https://files.galwaycity.ie/council_meetings/050721_01_Minutes.pdf",
        "https://clarecoco.ie/minutes-may-2025-monthly-meeting-clare-county-council",
        "https://waterfordcouncil.ie/Plenary-Special-Meeting-Minutes.pdf",
        "http://westmeathcoco.ie/en/media/April%202021%20JPC%20Minutes.pdf",
    ],
)
def test_real_minute_names_are_not_hijacked_by_management_report_item(source_url: str) -> None:
    text = "Minutes of the meeting\nItem 6 Chief Executive Management Report\nMembers present"
    assert sandbox_doc_type(source_url, text) in {
        "plenary_minutes",
        "md_minutes",
    }
    assert classify_document(
        meeting=source_url.rsplit("/", 1)[-1],
        source_url=source_url,
        text=text,
    ) in {"plenary_minutes", "md_minutes", "committee_minutes"}


def test_quarantine_recovery_is_bounded_to_known_classifier_defect() -> None:
    explicit_minute = {
        "reason": "not_minutes_report_or_plan",
        "local_authority": "Kilkenny",
        "url": "https://example.ie/minutes-of-special-meeting-06-05-2025.pdf",
        "meeting": "minutes-of-special-meeting-06-05-2025.pdf",
    }
    kerry_agenda = {
        "reason": "not_minutes_report_or_plan",
        "local_authority": "Kerry",
        "url": "https://example.ie/agenda/12345.pdf",
        "meeting": "12345.pdf",
    }
    unrelated = {**explicit_minute, "reason": "not_minutes_agenda"}

    assert is_recovery_candidate(explicit_minute)
    assert not is_recovery_candidate(kerry_agenda)
    assert not is_recovery_candidate(unrelated)


def test_ce_reports_have_a_distinct_non_minutes_type() -> None:
    source_url = "https://leitrim.ie/Chief-Executive-Monthly-Management-Report-March-2026.pdf"
    text = "Chief Executive Monthly Management Report\nHousing and roads programme"
    assert sandbox_doc_type(source_url, text) == "ce_report"
    assert (
        classify_document(
            meeting="Chief Executive Monthly Management Report March 2026.pdf",
            source_url=source_url,
            text=text,
            upstream_doc_type="ce_report",
        )
        == "ce_report"
    )
    assert (
        classify_document(
            meeting="June 2026 Chief Executives Report.pdf",
            source_url="https://council.example/media/1891/download?inline",
            text="Monthly activity",
        )
        == "ce_report"
    )
    assert (
        sandbox_doc_type(
            "https://leitrim.ie/Chief-Executive-Monthly-Reports/Ce-Monthly-Report-November-2025.pdf",
            "",
        )
        == "ce_report"
    )


def test_minutes_keep_committee_and_municipal_scope() -> None:
    assert (
        classify_document(
            meeting="LCDC Minutes.pdf",
            source_url="https://example.ie/LCDC-Minutes.pdf",
            text="Minutes of the Local Community Development Committee",
        )
        == "committee_minutes"
    )
    assert (
        classify_document(
            meeting="minutes.pdf",
            source_url="https://example.ie/minutes.pdf",
            text="Minutes of the Muinebheag Municipal District meeting",
        )
        == "md_minutes"
    )


def test_chunk_text_hard_bounds_unstructured_ocr() -> None:
    chunks = chunk_text("word " * 2_000, max_chars=500)
    assert len(chunks) > 1
    assert all(0 < len(chunk) <= 500 for chunk in chunks)
    assert "".join(chunks).replace(" ", "") == ("word " * 2_000).replace(" ", "")


def test_public_signal_labels_are_passage_scoped_and_conservative() -> None:
    signals = extract_participation_signals(
        "Ballyboggan Residents Association raised road access and wastewater capacity "
        "issues for Planning Ref 2460125 and ABP-319198-24."
    )

    assert signals == {
        "participant_categories": ["residents_association"],
        "issue_themes": ["planning_housing", "traffic_access", "services_infrastructure"],
        "planning_references": ["2460125"],
        "board_references": ["ABP-319198-24"],
        "collective_organisation_names": ["Ballyboggan Residents Association"],
    }


def test_build_corpus_reports_every_exclusion(tmp_path: Path) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "minutes.txt").write_text("Minutes of the meeting\n" + "decision " * 200, encoding="utf-8")
    (corpus / "agenda.txt").write_text(
        "Agenda\nConfirmation of the minutes of the previous meeting\n" + "item " * 200,
        encoding="utf-8",
    )
    records = [
        {
            "local_authority": "Carlow",
            "meeting": "Minutes March 2026.pdf",
            "meeting_date": "2026-03-01",
            "doc_type": "plenary_minutes",
            "status": "text",
            "url": "https://example.ie/minutes-march-2026.pdf",
            "text_path": "corpus/minutes.txt",
        },
        {
            "local_authority": "Carlow",
            "meeting": "Agenda March 2026.pdf",
            "meeting_date": "2026-03-01",
            "doc_type": "plenary_minutes",
            "status": "text",
            "url": "https://example.ie/agenda-march-2026.pdf",
            "text_path": "corpus/agenda.txt",
        },
        {
            "local_authority": "Carlow",
            "meeting": "Missing.pdf",
            "status": "text",
            "url": "https://example.ie/missing.pdf",
            "text_path": "corpus/missing.txt",
        },
    ]
    manifest = tmp_path / "meetings_clean.jsonl"
    manifest.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")

    frame, coverage = build_corpus(manifest, tmp_path)

    assert frame["document_id"].n_unique() == 1
    assert frame["meeting_scope"].unique().to_list() == ["plenary"]
    assert {"issue_themes", "planning_references", "board_references"} <= set(frame.columns)
    assert frame.schema["planning_references"] == pl.List(pl.String)
    assert coverage["input_records"] == 3
    assert coverage["published_documents"] == 1
    assert coverage["excluded_non_minutes"] == {"agenda": 1}
    assert len(coverage["missing_text_files"]) == 1
