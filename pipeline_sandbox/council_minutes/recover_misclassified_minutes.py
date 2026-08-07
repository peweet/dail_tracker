"""Re-fetch the bounded set of minutes hidden by the report classifier defect.

This does not reclassify quarantine metadata on filename evidence alone. Each
candidate is fetched again and is promoted only when the extracted full text
passes the current minutes classifier. Without ``--apply`` the command is a
read-only candidate listing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from pipeline_sandbox.council_minutes import council_minutes_consolidate as base
from pipeline_sandbox.council_minutes import night_harvest as harvest

HERE = Path(__file__).resolve().parent
QUARANTINE = HERE / "quarantine" / "quarantine.jsonl"
CLEAN = HERE / "meetings_clean.jsonl"
VOTES = HERE / "member_votes_all.jsonl"
MINUTE_TYPES = {"plenary_minutes", "md_minutes"}


def is_recovery_candidate(record: dict) -> bool:
    """Return true only for records implicated by this specific defect."""
    if record.get("reason") != "not_minutes_report_or_plan":
        return False
    if base.doc_type(str(record.get("url") or ""), "") in MINUTE_TYPES:
        return True

    council = str(record.get("local_authority") or "")
    meeting = str(record.get("meeting") or "").lower()
    return (council == "Waterford" and "plenary-special-meeting" in meeting) or (
        council == "Wicklow" and meeting.startswith("transcript ")
    )


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _atomic_jsonl(path: Path, rows: list[dict]) -> None:
    temporary = path.with_suffix(path.suffix + ".recovery-tmp")
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def recover(*, apply: bool) -> tuple[int, int]:
    clean = _read_jsonl(CLEAN)
    quarantine = _read_jsonl(QUARANTINE)
    votes = _read_jsonl(VOTES)
    candidates = [record for record in quarantine if is_recovery_candidate(record)]
    print(f"bounded recovery candidates: {len(candidates)}")
    for record in candidates:
        print(f"  {record.get('local_authority')}: {record.get('meeting')}")
    if not apply:
        return len(candidates), 0

    ocr = harvest._winocr_run()
    recovered_by_url: dict[str, dict] = {}
    recovered_votes: list[dict] = []
    for old in candidates:
        url = str(old.get("url") or "")
        record, document_votes, text = harvest.process_doc(
            url,
            str(old.get("local_authority") or ""),
            ocr,
            print,
        )
        if not record["clean"] or record["doc_type"] not in MINUTE_TYPES:
            print(f"  retained in quarantine: {url} ({record['reason']})")
            continue

        council_dir = base.CORPUS / base.slug(record["local_authority"])
        council_dir.mkdir(parents=True, exist_ok=True)
        suffix = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
        stem = f"{base.slug(record['meeting'])[:68]}_{suffix}"
        text_path = council_dir / f"{stem}.txt"
        text_path.write_text(text, encoding="utf-8")
        record["text_path"] = text_path.relative_to(HERE).as_posix()
        recovered_by_url[url] = record
        recovered_votes.extend(document_votes)
        print(f"  recovered: {url}")

    if recovered_by_url:
        existing_clean_urls = {str(row.get("url") or "") for row in clean}
        clean.extend(record for url, record in recovered_by_url.items() if url not in existing_clean_urls)
        quarantine = [row for row in quarantine if str(row.get("url") or "") not in recovered_by_url]
        votes = base.norm_members(votes + recovered_votes)
        _atomic_jsonl(CLEAN, clean)
        _atomic_jsonl(QUARANTINE, quarantine)
        _atomic_jsonl(VOTES, votes)
        base.write_quality_report(clean, quarantine, votes)

    print(f"recovered {len(recovered_by_url)} of {len(candidates)} candidates")
    return len(candidates), len(recovered_by_url)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    recover(apply=args.apply)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
