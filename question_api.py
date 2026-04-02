
import json        # JSON parsing
import requests    # HTTP client for calling the Oireachtas API
# --- Load the members JSON produced by member_api_request.py ---
with open("members/members.json", "r") as f:
    members_data = json.load(f)

# --- Extract the unique URI for each TD from the nested JSON structure ---
# Each element in `questions` is one party's API response: { "results": [...] }
results = [value['results'] for value in members_data]

# Flatten the list-of-lisnto a single list of member records
#same as nested for loop [leats if for leaf in tree for tree in forest]
flat_members = [member for group in results for member in group]

# Drill into each record to get the 'member' dict
members= [member['member'] for member in flat_members]

# Extract just the 'uri' string from each member dict
list_of_td_uris = [uri['uri'] for uri in members]

# --- Persist the URI list as a JSON file ---
# bills_by_td.py loads this to know which TDs to query
with open("key_data/key_data.json", "w") as f:
    json.dump(list_of_td_uris, f, indent=2)


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
   