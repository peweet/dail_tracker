import json
from flatten_json import flatten
import pandas as pd
import os
from utility.select_drop_rename_cols_mappings import bill_cols_to_drop, bill_rename

with open("bills/all_bills_by_td.json", "r") as f:
    data = json.load(f)
# Extract individual bill records from each TD's response
bills = []
for td_response in data:
    for result in td_response.get("results", []):
        bills.append(result)
# Flatten the nested JSON structure
flattened_data = [flatten(record) for record in bills]
# Convert the list of flattened records into a DataFrame
df = pd.DataFrame(flattened_data)
# Save the DataFrame to a CSV file
output_path = "bills/new_flattened_bills.csv"
df.to_csv(output_path) 
df1 = pd.read_csv('bills/flattened_bills.csv')
df1 = df1.drop(
    bill_cols_to_drop, axis=1, errors='ignore'
    ).rename(
        columns=bill_rename
    ).to_csv('bills/drop_cols_flattened_bills.csv')
# Adjust infer_schema_length as needed for better type inference
# df = pd.read_csv('bills/new_flattened_bills.csv')