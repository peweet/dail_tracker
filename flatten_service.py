import json
from turtle import pd
from flatten_json import flatten
import pandas as pd
import os
from drop_cols import bill_cols_to_drop

with open("bills/all_bills_by_td.json", "r") as f:
    data = json.load(f)
# Flatten the nested JSON structure
flattened_data = [flatten(record) for record in data]
# Convert the list of flattened records into a DataFrame

df = pd.DataFrame(flattened_data)
df = df.drop(bill_cols_to_drop)
# Save the DataFrame to a CSV file
output_path = "bills/new_flattened_bills.csv"

df.to_csv(output_path, index=False)
df = pd.read_csv('bills/new_flattened_bills.csv')