import requests              # HTTP client for calling the Oireachtas API
import requests           
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

member_api_request()
logger.info("Finished loading members JSON data from the API. This will serve as the basis for fetching legislation and questions for each TD and master reference for all current members.")

# # --- Loop over every TD URI and fetch their associated legislation ---
bills = {}
def construct_urls_for_api(api_scenario: str = None) -> list:
    URLS = []
    df = pl.read_csv('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv')
    df = df.select(pl.col('unique_member_code')).filter(pl.col('unique_member_code').is_not_null())
    df = df.unique()
    for uri in df.rows():
        if uri[0] is not None and api_scenario == "legislation":
            URLS.append(f"https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=2099-01-01&limit=1000&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2F{uri[0]}&chamber_id=&lang=en")  # Construct the URL for this TD and add it to the list
        elif uri[0] is not None and api_scenario == "questions":
            URLS.append(f"https://api.oireachtas.ie/v1/questions?skip=0&limit=1000&qtype=oral,written&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2F{uri[0]}")  # Construct the URL for this TD and add it to the list
        else:
            logger.warning(f"Skipping URI {uri[0]} due to null value or unrecognized API scenario.")
            return 
    return URLS
construct_urls_for_api(api_scenario="legislation")


def load_url(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()
# We can use a with statement to ensure threads are cleaned up promptly

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
                logger.error("API call failed for %s: %s", url, exc)
            else:
                logger.info('%r page is %d bytes' % (url, len(data)))
    return results
logger.info("loading json...")


construct_urls_for_api(api_scenario="questions")

#todo fix api scenario logic below, it's currently broken and only 
# loads legislation URLs, not questions. 
# We should refactor the URL construction logic to be more
#  flexible and accommodate different API scenarios 
# without hardcoding the URL patterns for each scenario. 
# This would allow us to easily add new scenarios in the 
# future without having to modify the core logic of the service.
def save_results(results, scenario):
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




# import json        # JSON parsing
# import requests    # HTTP client for calling the Oireachtas API
# # --- Load the members JSON produced by member_api_request.py ---
# with open("members/members.json", "r") as f:
#     members_data = json.load(f)

# # TODO: We should probably move the JSON loading and flattening logic into a separate module (e.g. `data_loader.py`) that can be reused across different services (e.g. questions, bills, etc.). This would help avoid code duplication and make it easier to maintain the data loading logic in one place.
# # logic is redundant and can be improved by creating a reusable function that takes the file path as an argument and returns the flattened list of member records. This would allow us to easily load and flatten different JSON files (e.g. questions, bills) without duplicating code.
# # --- Extract the unique URI for each TD from the nested JSON structure ---
# # Each element in `questions` is one party's API response: { "results": [...] }
# results = [value['results'] for value in members_data]

# # Flatten the list-of-lisnto a single list of member records
# #same as nested for loop [leats if for leaf in tree for tree in forest]
# flat_members = [member for group in results for member in group]

# # Drill into each record to get the 'member' dict
# members= [member['member'] for member in flat_members]


# # --- Commented-out: Fetch parliamentary questions for each TD ---
# # This would call /v1/questions for every TD URI and save them to JSON.
# # Has the same try/except/else bug as bills_by_td.py (the `else` runs
# # on success, not failure).
# counter = 0
# combined = []
# for uri in list_of_td_uris:
#    params = {
#        "date_start": "1900-01-01",
#        "date_end":"2099-01-01",
#        "type": "oral,written",
#        "member_id":uri

# }
#    response = requests.get("https://api.oireachtas.ie/v1/questions?", params=params)
#    counter+=1
#    if response.status_code == 200:
#        try:
#            print(f"procssesing api...TD: {counter}")
#            questions = response.json()
#            combined.append(questions)
#        except json.JSONDecodeError:
#                print(f"Invalid JSON at call {uri}")
#        else:
#            print(f"Failed request {uri}: {response.status_code}")

# with open('questions/questions_all_current_tds.json', 'w', encoding='utf-8') as f:
#    print("loading json...")
#    json.dump(combined, f, ensure_ascii=False,  indent=2)
#    print("finished dumping TD info")
   