import requests              # HTTP client for calling the Oireachtas API      
import json        
import polars as pl
import concurrent.futures
import urllib.request       
import logging

# The below code is directly copied  from the Python docs for concurrent.futures, 
# with some adjustments to fit our use case of loading multiple URLs in parallel. 
# The `construct_urls_for_api` function builds the list of URLs to fetch based on the unique_member_code values *
#  from the enriched TD attendance CSV. The `load_url` function is a helper that loads a single URL with a timeout. 
# The main block uses a ThreadPoolExecutor to load all URLs concurrently and collects the results, which are then saved to a JSON file.
# https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
# https://stackoverflow.com/questions/57284126/how-can-i-use-threading-with-requests

# Reference URL used during development (single TD: Noel Grealish)
#https://api.oireachtas.ie/v1/legislation?&bill_source=Government,Private%20Member&date_start=1900-01-01&date_end=2099-01-01&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
#TODO adapt Claude code to improve the api call  logic (commented out below) and error handling, and to add logging at each step of the process (e.g. when loading members JSON, when constructing URLs, when starting and finishing API calls, when saving results, etc.)

#LOGGING SETUP
file_handler = logging.FileHandler("pipeline.log")
file_handler.setLevel(logging.INFO)
# Set a formatter for the file handler to include timestamps and log levels
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
logger = logging.getLogger(__name__)
# Add the file handler to the logger
logger.addHandler(file_handler)

def member_api_request():
    # Load member data for all TDs from the Oireachtas API and save to JSON
    # concurrency not needed here as the payload is only one API call for all members, not one call per member. We can parallelize the next step of fetching legislation/questions for each TD, which is where we have one API call per TD and the payload is larger.
    chamber_id = "%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34"
    URL = f"https://api.oireachtas.ie/v1/members?chamber_id={chamber_id}&date_start=2024-01-01&date_end=2099-01-01&limit=200"
    all_members = []  # Accumulates one API response dict per party
    response = requests.get(URL)
    data = response.json()    # Parse the JSON response
    all_members.append(data)  # Add this party's member data to the list
    # Flatten the list of API responses into a single list of member records
    strip_head = [member for group in all_members for member in group.get("results", [])]
    logger.info(f"Loaded member data for {len(strip_head)} members from the API.")
    with open("members/members.json", "w") as f:
        json.dump(all_members, f, indent=2)
logger.info("Member data starting to load from API...")    
member_api_request()
logger.info("Finished loading members JSON data from the API. This will serve as the basis for fetching legislation and questions for each TD and master reference for all current members.")
# # --- Loop over every TD URI and fetch their associated legislation ---
bills = {}
def construct_urls_for_api(api_scenario: str = None) -> list:
    URLS = []
    df = pl.read_csv('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv')
    df = df.select(
        pl.col('unique_member_code')
        ).filter(
        pl.col('unique_member_code'
               ).is_not_null()
        )
    df = df.unique()
    for uri in df.rows():
        if uri[0] is not None and api_scenario == "legislation":
            URLS.append(f"https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=2099-01-01&limit=1000&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2F{uri[0]}&chamber_id=&lang=en")  # Construct the URL for this TD and add it to the list
        elif uri[0] is not None and api_scenario == "questions":
            URLS.append(f"https://api.oireachtas.ie/v1/questions?skip=0&limit=1000&qtype=oral,written&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2F{uri[0]}")  # Construct the URL for this TD and add it to the list
        else:
            logger.warning(f"Skipping URI {uri[0]} due to null value or unrecognized API scenario.")
            return 
    logging.info(f"Constructed {len(URLS)} URLs for API scenario '{api_scenario}'.")
    return URLS
construct_urls_for_api(api_scenario="legislation")
def load_url(url, timeout):
    # Load a single URL with a timeout and return the response data
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()
    
def fetch_all(urls):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(load_url, url, 60): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                results.append(json.loads(data))
            except Exception as exc:
                logging.error("API call failed for %s: %s", url, exc)
            else:
                logging.info('%r page is %d bytes' % (url, len(data)))
    return results
logging.info("loading json...")

construct_urls_for_api(api_scenario="questions")

def save_results(results, scenario):
    # Save the results to a JSON file named after the scenario (e.g. legislation_results.json, questions_results.json)
    file_name = f"bills/{scenario}_results.json"
    with open(file_name, 'w', encoding='utf-8') as f:
        logger.info(f"loading {scenario} json...")
        json.dump(results, f, indent=2)
    logger.info(f"Saved {len(results)} results to {file_name}")
member_api_request()

for scenario in ["legislation", "questions"]:
    logger.info(f"Starting {scenario} pipeline")
    urls = construct_urls_for_api(scenario)
    logger.info(f"Built {len(urls)} URLs for {scenario}")
    results = fetch_all(urls)
    save_results(results, scenario)
    logger.info(f"Finished {scenario} pipeline")
# keep for future testing in Polars
# # write_dict_to_json = pl.from_dict({"TD": combined}, strict=False)
# # write_dict_to_json.write_json('bills/all_bills_by_td.json')

# import requests
# import json
# import polars as pl
# import concurrent.futures
# import logging

# # Logging setup
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# )
# file_handler = logging.FileHandler("pipeline.log")
# file_handler.setLevel(logging.INFO)
# file_handler.setFormatter(
#     logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# )
# logger = logging.getLogger(__name__)
# logger.addHandler(file_handler)

# # Reusable session — keeps connections alive across requests (TCP reuse / connection pooling)
# session = requests.Session()
# def member_api_request():
#     """Load member data for all TDs from the Oireachtas API and save to JSON."""
#     chamber_id = "%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34"
#     url = (
#         f"https://api.oireachtas.ie/v1/members"
#         f"?chamber_id={chamber_id}"
#         f"&date_start=2024-01-01&date_end=2099-01-01&limit=200"
#     )
#     response = session.get(url, timeout=60)
#     response.raise_for_status()  # Raise on 4xx/5xx
#     data = response.json()

#     strip_head = data.get("results", [])
#     logger.info(f"Loaded member data for {len(strip_head)} members from the API.")

#     with open("members/members.json", "w", encoding="utf-8") as f:
#         json.dump(data, f, indent=2, ensure_ascii=False)


# def construct_urls_for_api(api_scenario: str) -> list[str]:
#     """Build one URL per TD for the given API scenario (legislation or questions)."""
#     df = pl.read_csv(
#             r"C:\Users\pglyn\PycharmProjects\dail_extractor\members\enriched_td_attendance.csv"
#         ).select(pl.col("unique_member_code")).filter(pl.col("unique_member_code").is_not_null()).unique()

#     urls = []
#     for (code,) in df.rows():
#         if code is None:
#             continue  # was `return` before — that killed the whole function

#         if api_scenario == "legislation":
#             urls.append(
#                 f"https://api.oireachtas.ie/v1/legislation"
#                 f"?date_start=1900-01-01&date_end=2099-01-01&limit=1000"
#                 f"&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2F{code}"
#                 f"&chamber_id=&lang=en"
#             )
#         elif api_scenario == "questions":
#             urls.append(
#                 f"https://api.oireachtas.ie/v1/questions"
#                 f"?skip=0&limit=1000&qtype=oral,written"
#                 f"&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2F{code}"
#             )
#         else:
#             logger.warning(f"Unrecognised API scenario: '{api_scenario}' — skipping.")
#             continue

#     logger.info(f"Constructed {len(urls)} URLs for API scenario '{api_scenario}'.")
#     return urls


# def load_url(url: str, timeout: int = 60) -> dict:
#     """Fetch a single URL using requests (replaces urllib.request.urlopen).

#     requests handles unicode/encoding automatically via response.json(),
#     which avoids the raw-bytes decoding issues urllib has.
#     """
#     response = session.get(url, timeout=timeout)
#     response.raise_for_status()
#     return response.json()  # already decoded — no unicode issues


# def fetch_all(urls: list[str], max_workers: int = 5) -> list[dict]:
#     """Fetch all URLs concurrently using a thread pool."""
#     results = []
#     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#         future_to_url = {executor.submit(load_url, url): url for url in urls}

#         for future in concurrent.futures.as_completed(future_to_url):
#             url = future_to_url[future]
#             try:
#                 data = future.result()
#                 results.append(data)
#                 logger.info(f"Fetched {url[:80]}...")
#             except Exception as exc:
#                 logger.error(f"API call failed for {url}: {exc}")

#     return results


# def save_results(results: list[dict], scenario: str):
#     """Save results to a JSON file named after the scenario."""
#     file_name = f"bills/{scenario}_results.json"
#     with open(file_name, "w", encoding="utf-8") as f:
#         json.dump(results, f, indent=2, ensure_ascii=False)
#     logger.info(f"Saved {len(results)} results to {file_name}")


# ── Main pipeline ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1. Fetch member data (single API call)
    logger.info("Loading member data from API...")
    member_api_request()
    logger.info("Finished loading members JSON.")
    # 2. For each scenario, build URLs → fetch concurrently → save
    for scenario in ["legislation", "questions"]:
        logger.info(f"Starting {scenario} pipeline")
        urls = construct_urls_for_api(scenario)
        logger.info(f"Built {len(urls)} URLs for {scenario}")
        results = fetch_all(urls)
        save_results(results, scenario)
        logger.info(f"Finished {scenario} pipeline")