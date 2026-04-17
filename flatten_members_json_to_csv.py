import json
from flatten_json import flatten
import pandas as pd
import os
from pathlib import Path
from utility.select_drop_rename_cols_mappings import members_rename, members_drop_cols
from config import DATA_DIR, MEMBERS_DIR
from members_api_service import fetch_members, save_members_json


def flatten_members_to_csv():
    """
    Fetch members from API, flatten JSON structure, and save to CSV.
    
    Steps:
    1. Fetch members from API
    2. Save raw members JSON
    3. Flatten nested structure
    4. Clean and rename columns
    5. Export to CSV
    """
    # Fetch and save raw members data
    members_data = fetch_members()
    save_members_json(members_data, MEMBERS_DIR / 'members.json')
    
    # Extract results from API response
    all_members = []
    for result in members_data.get("results", []):
        all_members.append(result)
    
    # Save filtered members to intermediate JSON
    filtered_path = MEMBERS_DIR / "filtered_members.json"
    with open(filtered_path, "w", encoding="utf-8") as f:
        json.dump(all_members, f, indent=2, ensure_ascii=False)
    
    # Count members
    list_of_names = [member["member"]["fullName"] for member in all_members]
    print(f"Total members loaded: {len(list_of_names)}")
    
    # Flatten and process
    with open(filtered_path, encoding="utf-8") as f:
        data = json.load(f)
        flattened_data = [flatten(member) for member in data]
    
        # Create DataFrame
        df = pd.DataFrame(flattened_data).fillna('Null')
        
        df = df.rename(members_rename, axis=1)
        df = df.drop(columns=members_drop_cols, errors='ignore')
        # Save to CSV
        csv_path = DATA_DIR /'silver' / 'flattened_members.csv'
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"Flattened members saved to {csv_path}")
        # Coalesce and save consolidated version

    # Clean up intermediate files
    # if filtered_path.exists():
    #     filtered_path.unlink()
    #     print("Cleaned up intermediate files")

if __name__ == "__main__":
    print("Starting member flattening service...")
    flatten_members_to_csv()
    print("Member flattening complete.")

