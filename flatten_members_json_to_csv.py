import json

from duckdb import df
import pandas as pd
from flatten_json import flatten

from config import DATA_DIR, MEMBERS_DIR
from members_api_service import fetch_members, save_members_json
from utility.select_drop_rename_cols_mappings import members_drop_cols, members_rename


def flatten_members_to_csv(house: str = "dail"):
    """
    Fetch members from API, flatten JSON structure, and save to CSV.

    Args:
        house: "dail" or "seanad"
    """
    csv_name = "flattened_members.csv" if house == "dail" else "flattened_seanad_members.csv"

    members_data = fetch_members(house)
    save_members_json(members_data, MEMBERS_DIR / f"members_{house}.json")

    all_members = [result for result in members_data.get("results", [])]

    filtered_path = MEMBERS_DIR / f"filtered_members_{house}.json"
    with open(filtered_path, "w", encoding="utf-8") as f:
        json.dump(all_members, f, indent=2, ensure_ascii=False)

    list_of_names = [member["member"]["fullName"] for member in all_members]
    print(f"Total {house} members loaded: {len(list_of_names)}")

    with open(filtered_path, encoding="utf-8") as f:
        data = json.load(f)
        flattened_data = [flatten(member) for member in data]
        df = pd.DataFrame(flattened_data)
    df = df.rename(members_rename, axis=1)
    df = df.drop(columns=members_drop_cols, errors="ignore")
    minister_bool_mask = df['office_1_name'].notna() & df['office_1_name'].str.contains("Minister", case=False, na=False)
    df['ministerial_office'] = minister_bool_mask.astype(str).replace({"True": "true", "False": "false"})
    df['year_elected'] = df["unique_member_code"].str.extract(r"(\b\d{4}\b)", expand=False)
    # df['year_elected'] = df['unique_member_code'].str.extract(r"(\b\d{4}\b)", expand=False)  # Extract year from unique_member_code
    csv_path = DATA_DIR / "silver" / csv_name
    df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_parquet(DATA_DIR / "silver" / "parquet" / csv_name.replace(".csv", ".parquet"), index=False)
    print(f"Flattened {house} members saved to {csv_path}")
if __name__ == "__main__":
    print("Starting member flattening service...")
    flatten_members_to_csv("dail")
    flatten_members_to_csv("seanad")
    print("Member flattening complete.")
