import logging

import polars as pl

from services.dail_config import API_BASE
from services.http_engine import fetch_json
from services.storage import load_json, members_file_path, output_exists, save_json

logger = logging.getLogger(__name__)


def fetch_members_payload() -> dict:
    """Fetch full members payload from API."""
    chamber_id = "%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34"
    url = (
        f"{API_BASE}/members"
        f"?chamber_id={chamber_id}"
        f"&date_start=2024-01-01"
        f"&date_end=2099-01-01"
        f"&limit=200"
    )

    payload, raw_bytes = fetch_json(url)
    logger.info(
        f"Fetched members payload | rows={len(payload.get('results', []))} | bytes={raw_bytes:,}"
    )
    return payload


def load_members_payload() -> dict:
    """Load saved members payload, supporting both old and new file shapes."""
    path = members_file_path()
    data = load_json(path)

    # Backward compatibility with old saved format: [payload_dict]
    if (
        isinstance(data, list)
        and len(data) == 1
        and isinstance(data[0], dict)
        and "results" in data[0]
    ):
        data = data[0]

    return data


def get_or_create_members_payload(overwrite: bool = False) -> dict:
    """Use saved members payload when available, otherwise fetch and save."""
    path = members_file_path()

    if output_exists(path, overwrite=overwrite):
        logger.info("Using existing members.json")
        return load_members_payload()

    payload = fetch_members_payload()
    save_json(payload, path)
    return payload


def _extract_member_uri(row: dict) -> str | None:
    """Extract one usable member URI from a row."""
    member = row.get("member", row)

    member_uri = (
        member.get("uri")
        or member.get("member_id")
        or row.get("uri")
        or row.get("member_id")
    )

    if not member_uri:
        return None

    member_uri = str(member_uri)

    # Normalize absolute URI -> relative URI
    if member_uri.startswith("https://data.oireachtas.ie"):
        member_uri = member_uri.replace("https://data.oireachtas.ie", "")

    return member_uri


def members_payload_to_df(payload: dict) -> pl.DataFrame:
    """Convert raw members payload into a simple dataframe of unique member URIs."""
    rows = payload.get("results", [])

    cleaned_rows = []
    for row in rows:
        member_uri = _extract_member_uri(row)
        if member_uri:
            cleaned_rows.append({"member_uri": member_uri})

    if not cleaned_rows:
        logger.warning("No usable member URIs were found in members payload.")
        return pl.DataFrame({"member_uri": []}, schema={"member_uri": pl.Utf8})

    df = (
        pl.DataFrame(cleaned_rows)
        .filter(pl.col("member_uri").is_not_null())
        .unique()
        .sort("member_uri")
    )

    logger.info(f"Prepared member dataframe with {df.height} unique member URIs")
    return df


def get_or_create_member_df(overwrite_members: bool = False) -> pl.DataFrame:
    payload = get_or_create_members_payload(overwrite=overwrite_members)
    return members_payload_to_df(payload)