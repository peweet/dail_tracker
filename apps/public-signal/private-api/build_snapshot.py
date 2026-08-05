"""Materialise the reviewed PublicSignal procurement snapshot from Dail Tracker.

This is intentionally a one-way copy: the source data and query logic remain in
Dail Tracker. The private product receives only its bounded opportunity feed and
evidence briefs, and a refresh is a new image build.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from dail_tracker_core.connections import api_conn
from dail_tracker_core.queries.procurement.opportunities import opportunity_brief, opportunity_feed


def build_snapshot(output: Path) -> dict:
    """Write a compact, provenance-carrying snapshot for the private API."""
    conn = api_conn()
    try:
        feed = opportunity_feed(conn, limit=200, within_days=365)
        briefs = {
            item["id"]: brief
            for item in feed["opportunities"]
            if (brief := opportunity_brief(conn, item["id"])) is not None
        }
    finally:
        conn.close()

    payload = {
        "schema": "publicsignal-procurement-snapshot/1",
        "built_at": datetime.now(UTC).isoformat(),
        "feed": feed,
        "briefs": briefs,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return {"opportunities": len(feed["opportunities"]), "briefs": len(briefs), "output": str(output)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the private PublicSignal procurement snapshot")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).with_name("snapshot.json"),
        help="private snapshot path (default: alongside this script)",
    )
    args = parser.parse_args()
    print(json.dumps(build_snapshot(args.output)))


if __name__ == "__main__":
    main()
