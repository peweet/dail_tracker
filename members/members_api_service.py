"""
Members API Service

Fetches member data for all TDs from the Oireachtas API and saves to JSON.
This is a separate single-call service (no concurrency needed) that provides
the foundation for other scripts like flatten_members_json_to_csv.py

Concurrency is not needed here as the payload is only one API call for all members,
not one call per member. We can parallelize the next step of fetching legislation/questions
for each TD, which is where we have one API call per TD and the payload is larger.

Documentation:
- https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
"""

import logging
from pathlib import Path

import orjson
import requests

from config import API_BASE, MEMBERS_DIR

# Module logger only — the pipeline orchestrator configures root logging
# (per-run dir or stream-only under orchestration); standalone callers get
# the root logger's default stderr handler.
logger = logging.getLogger(__name__)

# Reusable session — keeps connections alive across requests (TCP reuse / connection pooling)
session = requests.Session()


def fetch_members(
    house: str,
    house_no: int | None = None,
    date_start: str = "2024-01-01",
    date_end: str = "2099-01-01",
) -> dict:
    """Load member data for a house from the Oireachtas API.

    Defaults reproduce the original behaviour exactly (current term: 34th Dáil /
    27th Seanad, from 2024-01-01). Pass ``house_no`` + a wider ``date_start`` to
    scope the pull to a historic term — the lever the historic-members backfill
    needs (see members/historic_members_build.py).

    Args:
        house: The house identifier ("dail" or "seanad")
        house_no: House number to scope to (e.g. 33 for the 33rd Dáil). Defaults
            to the current term when omitted.
        date_start: Membership window start (inclusive).
        date_end: Membership window end.

    Returns:
        dict: API response containing the matching members
    """
    if house.lower() == "dail":
        chamber_id = f"%2Fie%2Foireachtas%2Fhouse%2Fdail%2F{house_no or 34}"
    elif house.lower() == "seanad":
        chamber_id = f"%2Fie%2Foireachtas%2Fhouse%2Fseanad%2F{house_no or 27}"
    else:
        raise ValueError("Invalid house specified. Use 'dail' or 'seanad'.")
    url = f"{API_BASE}/members?chamber_id={chamber_id}&date_start={date_start}&date_end={date_end}&limit=600"
    logger.info(f"Fetching members from: {url}")
    response = session.get(url, timeout=60)
    response.raise_for_status()  # Raise on 4xx/5xx
    data = response.json()

    member_count = len(data.get("results", []))
    logger.info(f"Loaded member data for {member_count} members from the API.")

    return data


def save_members_json(data: dict, output_path: Path) -> None:
    """Save members API response to JSON file.

    Args:
        data: API response data containing members information
        output_path: Path where JSON file will be saved
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode("utf-8"))
    logger.info(f"Members JSON saved to: {output_path}")


if __name__ == "__main__":
    logger.info("Starting Members API fetch...")

    # Fetch and save member data
    members_data = fetch_members("dail")
    members_output = MEMBERS_DIR / "members.json"
    save_members_json(members_data, members_output)

    logger.info("Members API fetch complete.")
