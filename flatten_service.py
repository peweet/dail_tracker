import json
from flatten_json import flatten
import pandas as pd
from config import DATA_DIR
from utility.select_drop_rename_cols_mappings import bill_cols_to_drop, bill_rename

with open(DATA_DIR / "all_bills_by_td.json", "r") as f:
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
output_path = DATA_DIR / "silver"/"new_flattened_bills.csv"
df.to_csv(output_path) 
df1 = pd.read_csv(DATA_DIR / "silver"/ "flattened_bills.csv")
df1 = df1.drop(
    bill_cols_to_drop, axis=1, errors='ignore'
    ).rename(
        columns=bill_rename
    ).to_csv(DATA_DIR / "silver"/ "drop_cols_flattened_bills.csv")


if __name__ == "__main__":
    print("Bills JSON flattening complete. Output saved to new_flattened_bills.csv and drop_cols_flattened_bills.csv.")