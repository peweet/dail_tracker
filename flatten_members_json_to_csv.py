import json
from flatten_json import flatten
import pandas as pd
import os
from utility.select_drop_rename_cols_mappings import members_drop_cols,members_rename
docstring="""
This script processes the members.json file, which contains metadata about TDs (members of the Irish
parliament). It performs the following steps:
1. Loads the members.json file and extracts individual member records.
2. Flattens the nested JSON structure of each member record into a flat dictionary format.
3. Converts the list of flattened member records into a Pandas DataFrame.
4. Cleans the DataFrame by renaming columns according to a predefined mapping and dropping unnecessary
columns.
5. Saves the cleaned DataFrame to a CSV file named 'flattened_members.csv'.
6. Deletes the intermediate JSON files that are no longer needed after creating the CSV.
This script is part of the data processing pipeline for enriching TD attendance records with member
metadata. The resulting 'flattened_members.csv' file will be used in subsequent steps to join with
attendance data and create an enriched dataset for analysis.
"""
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
#TODO: put this type of logic at the end of the pipeline, and make it more robust (e.g. check if files exist before trying to delete them, and only delete files that are no longer needed for any future steps in the pipeline)
if os.path.exists('members/filtered_members.json' or os.path.exists('members/flattened_members.json') or os.path.exists('members/members.json')):
    os.remove('members/filtered_members.json')
    os.remove('members/members.json')
    os.remove('members/flattened_members.json')
    print('Filtered and flattened JSON files deleted successfully.')
    