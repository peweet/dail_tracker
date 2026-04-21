import requests
import json
import polars as pl
import concurrent.futures
import logging
from pathlib import Path
from config import API_BASE, DATA_DIR, MEMBERS_DIR, LEGISLATION_DIR, VOTES_DIR, BRONZE_DIR

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
file_handler = logging.FileHandler("pipeline.log")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

# Reusable session — keeps connections alive across requests (TCP reuse / connection pooling)
session = requests.Session()

# Reference URL used during development (single TD: Noel Grealish)
# https://api.oireachtas.ie/v1/legislation?&bill_source=Government,Private%20Member&date_start=1900-01-01&date_end=2099-01-01&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en


def fetch_members() -> dict:
    """Load member data for all TDs from the Oireachtas API.
    
    This is the critical raw source data for the entire pipeline.
    Returns member records for the current Dáil term.
    
    Returns:
        dict: API response containing all TD member data
    """
    chamber_id = "%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34"
    url = (
        f"{API_BASE}/members"
        f"?chamber_id={chamber_id}"
        f"&date_start=2024-01-01&date_end=2099-01-01&limit=200"
    )
    logger.info(f"Fetching members from: {url}")
    response = session.get(url, timeout=60)
    response.raise_for_status()  # Raise on 4xx/5xx
    data = response.json()
    
    member_count = len(data.get("results", []))
    logger.info(f"Loaded member data for {member_count} members from the API.")
    
    return data


def save_members_json(data: dict, path_override : Path =None, scenario : str = None) -> Path:
    """Save members API response to JSON file.
    
    Args:
        data: API response data containing members information
        
    Returns:
        Path: Location where JSON file was saved
    """
    members_dir = MEMBERS_DIR
    members_dir.mkdir(parents=True, exist_ok=True)

    output_path = members_dir / f"{scenario}_members.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([data], f, indent=2, ensure_ascii=False)  # Wrap in list for consistency
    logger.info(f"Members JSON saved to: {output_path}")
    
    return output_path


def construct_urls_for_api(api_scenario: str) -> list[str]:
    """Build one URL per TD for the given API scenario (legislation or questions)."""
    enriched_csv_path = DATA_DIR / "gold" / "enriched_td_attendance.csv"
    df = pl.read_csv(enriched_csv_path).select(
        pl.col("unique_member_code")
    ).filter(
        pl.col("unique_member_code").is_not_null()
    ).unique()

    urls = []
    for row in df.rows():
        code = row[0]
        if code is None:
            continue  # Skip null codes

        if api_scenario == "legislation":
            urls.append(
                f"{API_BASE}/legislation"
                f"?date_start=2014-01&date_end=2099-01-01&limit=1000"
                f"&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2F{code}"
                f"&chamber_id=&lang=en"
            )
        elif api_scenario == "questions":
            urls.append(
                f"{API_BASE}/questions"
                f"?skip=0&limit=1000&qtype=oral,written"
                f"&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2F{code}"
            )
        else:
            logger.warning(f"Unrecognised API scenario: '{api_scenario}' — skipping.")
            continue

    logger.info(f"Constructed {len(urls)} URLs for API scenario '{api_scenario}'.")
    return urls


def load_url(url: str, timeout: int = 60) -> dict:
    """Fetch a single URL using requests.
    
    requests handles unicode/encoding automatically via response.json(),
    which avoids the raw-bytes decoding issues urllib has.
    """
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()  # already decoded — no unicode issues

def fetch_votes():
    #votes bulk queries 2026-2015, 1000 entries each, unpaginated
    votes_query1 = f"{API_BASE}/votes?chamber_type=house&chamber_id=&chamber=dail&date_start=2020-01-01&limit=10000&outcome="
    votes_query2 = f"{API_BASE}/votes?chamber_type=house&chamber_id=&chamber=dail&date_start=2020-01-01"
    votes_query3 =f"{API_BASE}/votes?chamber_type=house&chamber_id=&chamber=dail&date_end=2019-12-31&limit=10000&outcome="
    votes_url = [votes_query1, votes_query2, votes_query3]
    votes = []
    for url in votes_url:
        response = session.get(url, timeout=60)
        response.raise_for_status()  # Raise on 4xx/5xx
        vote_json = response.json()
        votes.append(vote_json)
    return votes
   
def fetch_all(urls: list[str], max_workers: int = 5) -> list[dict]:
    """Fetch all URLs concurrently using a thread pool."""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(load_url, url): url for url in urls}

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                results.append(data)
                logger.info(f"Fetched {url[:80]}...")
            except Exception as exc:
                logger.error(f"API call failed for {url}: {exc}")

    return results


def save_results(results: list[dict], scenario: str = None, path_override : Path = None) -> None:
    """Save results to a JSON file named after the scenario."""
    if scenario == "legislation":
        output_file = LEGISLATION_DIR / f"{scenario}_results.json"
    elif scenario == "votes":
        output_file = VOTES_DIR / f"{scenario}_results.json"
    elif scenario == "questions":
        output_file = LEGISLATION_DIR / f"{scenario}_results.json"
    else:
        output_file = BRONZE_DIR / f"{scenario}_results.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved {len(results)} results to {output_file}")


if __name__ == "__main__":
    logger.info("Starting Oireachtas API pipeline...")
    
    # Step 1: Fetch and save members data (critical raw source)
    logger.info("=" * 70)
    logger.info("STEP 1: Fetching members data (critical raw source)")
    logger.info("=" * 70)
    try:
        members_data = fetch_members()
        members_path = save_members_json(members_data)
        logger.info(f"✓ Members data successfully saved to {members_path}")
    except Exception as exc:
        logger.error(f"✗ Failed to fetch members data: {exc}")
        logger.error("Pipeline cannot continue without members data. Exiting.")
        exit(1)
    
    # Step 2: Fetch legislation and questions for each TD
    logger.info("=" * 70)
    logger.info("STEP 2: Fetching legislation and questions for each TD")
    logger.info("=" * 70)
    for scenario in ["legislation", "questions"]:
        logger.info(f"Starting {scenario} pipeline")
        urls = construct_urls_for_api(scenario)
        logger.info(f"Built {len(urls)} URLs for {scenario}")
        results = fetch_all(urls)
        save_results(results, scenario)
        logger.info(f"Finished {scenario} pipeline")
    logger.info("=" * 70)
    logger.info("STEP 3: Fetching voting history per TD")
    votes = fetch_votes()
    save_results(votes, scenario="votes")
    logger.info(f"Loaded member data for {votes} members from the API.")
    logger.info("=" * 70)
    logger.info("✓ Oireachtas API pipeline complete.")
    logger.info("Raw data saved to:")
    logger.info(f"  - data/bronze/members/members.json")
    logger.info(f"  - data/bronze/legislation/legislation_results.json")
    logger.info(f"  - data/bronze/legislation/questions_results.json")
    logger.info("=" * 70)
    logger.info("=" * 70)
  
  
  