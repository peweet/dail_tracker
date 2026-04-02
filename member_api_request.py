# =============================================================================
# member_api_request.py — Fetches all current Dáil members from the
# Oireachtas Members API, one party at a time, and saves the raw JSON
# to members/members.json.
#
# This is Step 1 of the data collection pipeline:
#   member_api_request.py → question_api.py → bills_by_td.py
#
# Output: members/members.json  (appended, not overwritten — see BUG note)
# =============================================================================

import requests              # HTTP client for calling the Oireachtas API
import json                  # JSON encoding

# Reference URL for the parties endpoint (used during development)
link="https://api.oireachtas.ie/v1/parties?chamber_id=&chamber=dail&house_no=33&limit=60"



# List of party codes to iterate over
# NOTE: '100%_RDR' and 'Independent_Ireland' were added for the 34th Dáil;
#       'Independents_4_Change' from the string above is missing here.
political_parties = ['Social_Democrats','100%_RDR','Independent_Ireland','Sinn_Féin','Fianna_Fáil','Fine_Gael','People_Before_Profit_Solidarity','Independent','Green_Party','Labour_Party','Aontú']

all_members = []  # Accumulates one API response dict per party

# --- Loop over each party and fetch their members ---
for party in political_parties:
    params = {
        "chamber_id": "",
        "chamber": "dail",
        "date_start": "2024-01-01",
        "date_end": "2099-01-01",
        "limit": "200",
        "party_code": party
    }

    response = requests.get("https://api.oireachtas.ie/v1/members", params=params)
    print(response.url)       # Debug: print the full request URL
    data = response.json()    # Parse the JSON response
    all_members.append(data)  # Add this party's member data to the list

# Flatten the list of API responses into a single list of member records
strip_head = [member for group in all_members for member in group.get("results", [])]

with open("members/members.json", "w") as f:
    json.dump(all_members, f, indent=2)


