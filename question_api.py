
import json        # JSON parsing
import requests    # HTTP client for calling the Oireachtas API
# --- Load the members JSON produced by member_api_request.py ---
with open("members/members.json", "r") as f:
    members_data = json.load(f)

# TODO: We should probably move the JSON loading and flattening logic into a separate module (e.g. `data_loader.py`) that can be reused across different services (e.g. questions, bills, etc.). This would help avoid code duplication and make it easier to maintain the data loading logic in one place.
# logic is redundant and can be improved by creating a reusable function that takes the file path as an argument and returns the flattened list of member records. This would allow us to easily load and flatten different JSON files (e.g. questions, bills) without duplicating code.
# --- Extract the unique URI for each TD from the nested JSON structure ---
# Each element in `questions` is one party's API response: { "results": [...] }
results = [value['results'] for value in members_data]

# Flatten the list-of-lisnto a single list of member records
#same as nested for loop [leats if for leaf in tree for tree in forest]
flat_members = [member for group in results for member in group]

# Drill into each record to get the 'member' dict
members= [member['member'] for member in flat_members]


# --- Commented-out: Fetch parliamentary questions for each TD ---
# This would call /v1/questions for every TD URI and save them to JSON.
# Has the same try/except/else bug as bills_by_td.py (the `else` runs
# on success, not failure).
counter = 0
combined = []
for uri in list_of_td_uris:
   params = {
       "date_start": "1900-01-01",
       "date_end":"2099-01-01",
       "type": "oral,written",
       "member_id":uri

}
   response = requests.get("https://api.oireachtas.ie/v1/questions?", params=params)
   counter+=1
   if response.status_code == 200:
       try:
           print(f"procssesing api...TD: {counter}")
           questions = response.json()
           combined.append(questions)
       except json.JSONDecodeError:
               print(f"Invalid JSON at call {uri}")
       else:
           print(f"Failed request {uri}: {response.status_code}")

with open('questions/questions_all_current_tds.json', 'w', encoding='utf-8') as f:
   print("loading json...")
   json.dump(combined, f, ensure_ascii=False,  indent=2)
   print("finished dumping TD info")
   