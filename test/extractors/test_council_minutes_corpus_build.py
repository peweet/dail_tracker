from __future__ import annotations

import json
from pathlib import Path

from extractors.council_minutes_contract import chunk_text, classify_document
from extractors.council_minutes_corpus_build import build_corpus


def test_agenda_filename_wins_over_quoted_previous_minutes() -> None:
    assert (
        classify_document(
            meeting="260525 Monthly Agenda Full Council.pdf",
            source_url="https://example.ie/Agendas/260525%20Monthly%20Agenda%20Full%20Council.pdf",
            text="Confirmation of the minutes of the previous meeting\nHousing report",
        )
        == "agenda"
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
    assert coverage["input_records"] == 3
    assert coverage["published_documents"] == 1
    assert coverage["excluded_non_minutes"] == {"agenda": 1}
    assert len(coverage["missing_text_files"]) == 1
