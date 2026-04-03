import json
from flatten_json import flatten
import pandas as pd
import os
from utility.select_cols_drop_cols import members_drop_cols,members_rename

members_json_path = "members/members.json"
json_data = json.load(open(members_json_path, "r"))
members = json_data

all_members = []
for member in members:
    all_members.extend(member["results"])

json.dump(all_members, open("members/filtered_members.json", "w"), indent=2)

list_of_names = [member["member"]["fullName"] for member in all_members]
print(f"Test for Total members (should be 175 - minus Ceann Comhairle): {len(list_of_names)}")


filtered_members_path = os.path.join("members", "filtered_members.json")
with open(filtered_members_path) as f:
    data = json.load(f)
    flattened_data = [flatten(member) for member in data]
    # Save to CSV and replace NaN with empty strings
    df = pd.DataFrame(flattened_data).fillna('Null')
    df = df.rename(
        members_rename, 
        axis=1
        )
    df = df.drop(columns=members_drop_cols, errors='ignore')
    df.to_csv('members/flattened_members.csv', index=False
    )  # Drop the original fullName column after splitting
    print("CSV file created successfully.")

#delete no longer needed data
if os.path.exists('members/filtered_members.json' or os.path.exists('members/flattened_members.json') or os.path.exists('members/members.json')):
    os.remove('members/filtered_members.json')
    os.remove('members/members.json')
    os.remove('members/flattened_members.json')
    print('Filtered and flattened JSON files deleted successfully.')
    