
import requests     # HTTP client for calling the Oireachtas REST API
import json         # JSON encoding / decoding
import polars as pl
# Reference URL used during development (single TD: Noel Grealish)
#https://api.oireachtas.ie/v1/legislation?&bill_source=Government,Private%20Member&date_start=1900-01-01&date_end=2099-01-01&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en

# Another working test URL for a single TD
working = "https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"

# --- Load the list of TD member URIs from the pickle file ---
# These URIs were extracted from the Oireachtas members API in question_api.py


df = pl.read_csv('/members/unique_member_code.csv')
df = df.select(pl.col('member_uri').is_unique()
counter = 0       # Tracks how many successful API responses we've collected
combined = []     # Accumulates all bill JSON responses

# --- Loop over every TD URI and fetch their associated legislation ---
for uri in df.iter_rows():
    params = {
        "bill_status": "Enacted,Rejected,Defeated,Lapsed",
        "bill_source": "Government,Private Member",
        "date_start": "1900-01-01",
        "date_end": "2099-01-01",
        "limit": "1000",     # Request up to 1000 bills per TD
        "member_id": uri,    # The unique Oireachtas URI identifying this TD
        "chamber_id": "",
        "lang": "en"
    }
    # Call the Oireachtas legislation endpoint
    response = requests.get("https://api.oireachtas.ie/v1/legislation?", params=params)
    print(f"response passed: {response.status_code}")


    if response.status_code == 200:
        try:
            bills = response.json()
            combined.append(bills)
            counter += 1
            print(f"Success. Processing bill request...TD: {counter}, url: {response.url}")
        except json.JSONDecodeError:
            print(f"Invalid JSON at call {uri}")
            continue
    else:
        print(f"Failed request {uri}: {response.status_code}")

# --- Write the combined results to a single JSON file ---
with open('bills/all_bills_by_td.json', 'w', encoding='utf-8') as f:
    print("loading json...")
    json.dump(combined, f, ensure_ascii=False, indent=2)
    print("finished dumping various bill info")