import json

import pandas as pd
from flatten_json import flatten

from config import LEGISLATION_DIR, SILVER_DIR
from utility.select_drop_rename_cols_mappings import bill_cols_to_drop, bill_rename

with open(LEGISLATION_DIR / "all_bills_by_td.json") as f:
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
output_path = SILVER_DIR / "new_flattened_bills.csv"
df.to_csv(output_path)
df1 = pd.read_csv(SILVER_DIR / "new_flattened_bills.csv")
df1 = (
    df1.drop(bill_cols_to_drop, axis=1, errors="ignore")
    .rename(columns=bill_rename)
)
df1.to_csv(SILVER_DIR / "drop_cols_flattened_bills.csv")
df1.to_parquet(SILVER_DIR / "parquet" / "drop_cols_flattened_bills.parquet", index=False)

if __name__ == "__main__":
    print("Bills JSON flattening complete. Output saved to new_flattened_bills.csv and drop_cols_flattened_bills.csv.")
