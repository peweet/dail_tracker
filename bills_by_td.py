
import requests     # HTTP client for calling the Oireachtas REST API
import json         # JSON encoding / decoding
import polars as pl
import concurrent.futures
import urllib.request



# The below code is directly copied  from the Python docs for concurrent.futures, 
# with some adjustments to fit our use case of loading multiple URLs in parallel. 
# The `construct_urls_for_api` function builds the list of URLs to fetch based on the unique_member_code values *
#  from the enriched TD attendance CSV. The `load_url` function is a helper that loads a single URL with a timeout. 
# The main block uses a ThreadPoolExecutor to load all URLs concurrently and collects the results, which are then saved to a JSON file.
# https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor-example
# https://stackoverflow.com/questions/57284126/how-can-i-use-threading-with-requests

# Reference URL used during development (single TD: Noel Grealish)
#https://api.oireachtas.ie/v1/legislation?&bill_source=Government,Private%20Member&date_start=1900-01-01&date_end=2099-01-01&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en


URLS = []

# Another working test URL for a single TD
working = "https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"


# # --- Loop over every TD URI and fetch their associated legislation ---
bills = {}
def construct_urls_for_api():
    df = pl.read_csv('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\enriched_td_attendance.csv')
    df = df.select(pl.col('unique_member_code')).filter(pl.col('unique_member_code').is_not_null())
    df = df.unique()
    for uri in df.rows():
        if uri[0] is not None:
            URLS.append(f"https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=2099-01-01&limit=1000&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2F{uri[0]}&chamber_id=&lang=en")  # Construct the URL for this TD and add it to the list
    return URLS
construct_urls_for_api()
print(f"test loading of: {URLS}")


def load_url(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()
# We can use a with statement to ensure threads are cleaned up promptly
results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
            results.append(json.loads(data))
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            print('%r page is %d bytes' % (url, len(data)))
print("loading json...")
value = load_url(URLS[0], 60)

with open('bills/all_bills_by_td.json', 'w', encoding='utf-8') as f:
    print("loading json...")
    json.dump(results, f, indent=2)
print("Finished dumping Dail bill info")
# # write_dict_to_json = pl.from_dict({"TD": combined}, strict=False)
# # write_dict_to_json.write_json('bills/all_bills_by_td.json')
