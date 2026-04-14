import csv
import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
import os
from config import LOBBY_DIR
# Parse each non-empty line using the parse_line function
# Read the raw input file line by line
# Function to parse a single line from the input CSV, the lobbyist data is very messy and 
# the csv dialect changes which necessistates manual intervention. Pandas will not work effectively with this data even with different dialect settings, so we have to do it manually and then read the cleaned data with polars for further processing. The main issues with the raw data are inconsistent use of quotes, embedded commas in fields, and inconsistent line breaks, which makes it difficult to parse with standard CSV parsers without manual cleaning.
# To extract the faw data go to https://www.lobbying.ie/app/Organisation/Search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&lobbyingActivities=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&includeClients=false
# and click the CSV export option to export all the registered lobby organizations in the Republic of Ireland

# def main():
#     def main():
#     lobbying_df = load_raw_lobbying_csvs(LOBBY_DIR)
#     lobby_org   = load_lobby_orgs(LOBBY_DIR)
#     politicians_df = explode_dpo_lobbied(lobbying_df)
#     activities_df  = explode_activities(politicians_df)
#     most_lobbied   = most_lobbied_politicians(politicians_df)
#     most_prolific  = most_prolific_lobbyists(lobbying_df, lobby_org)
#     save_outputs(activities_df, most_lobbied, most_prolific, LOBBY_DIR)

def parse_line(line) -> list:
    # print(f"Parsing line: {line.strip()}")
    # Replace triple double-quotes with double double-quotes
    line =line.replace('\"\'', '\'')  # Replace triple double-quotes with double double-quotes
    stripped = line.strip().split('","') # Split the line on '","' to separate fields, which handles quoted fields with embedded commas
    stripped[0] = stripped[0].lstrip('"')  # Remove the leading quote from the first field
    stripped[-1] = stripped[-1].rstrip('"')  # Remove the trailing quote
    # print(f"Test: {stripped}")  # Remove the first two quotes from the first field
    return stripped
with open(os.path.join(LOBBY_DIR / 'raw', 'Lobbying_ie_organisation_results.csv'), 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()

def lobby_org_csv_sanitizer()-> None:
    rows = []
    for line in raw_lines:
        rows.append(parse_line(line))
    with open(LOBBY_DIR / 'raw' / 'cleaned.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(rows)
    #manually assign col names as the csv has no header row, and then persist the cleaned data to a new csv file for further processing with polars
    #additioanlly the rows are ragged and uneven which can cause further issues down the line, so we need to make sure all rows have the same number of columns before writing to the cleaned csv file, and if not, we can log the issue and skip those rows to avoid errors in the downstream processing. This is a common issue with messy CSV data where some rows may have missing or extra fields, which can cause parsing errors if not handled properly.
    column_names = ["lobby_issue_uri",
                    "name",
                    "address",
                    "county",
                    "country",
                    "phone_number",
                    "website",
                    "main_activities_of_organisation",
                    "person_responsible_name",
                    "person_responsible_email",
                    "person_responsible_telephone",
                    "email",
                    "company_registration_number",
                    "company_registered_name",
                    "company_registered_address",
                    "charity_regulation_number",
                    "chy_number"
                ]
    df = pl.read_csv(LOBBY_DIR / 'raw' / 'cleaned.csv',  has_header=False,infer_schema=True, skip_lines=1)
    df.columns = column_names
    df.write_csv(LOBBY_DIR / 'raw' / 'cleaned_output.csv')
    # os.remove(LOBBY_DIR / 'raw' / 'cleaned.csv')
    print("Lobbying organization CSV sanitized and cleaned successfully. Cleaned data saved to cleaned_output.csv")
lobby_org_csv_sanitizer()
#HOW TO EXTRACT THE LOBBYING DATA:
# eg: https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=11&returnDateFrom=01-02-2026&returnDateTo=08-04-2026&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId=
#Do it in batches of 1000 and download it manually, the max date range is 12 months ie 2025-01 to 2026-01, and then stack the csvs together and do the cleaning and processing with polars. The lobbying data is not available via an API, so we have to download it manually in batches of 1000 records, which is the maximum allowed by the website for a single download. We can filter the data by job titles (e.g. TDs, Senators, Ministers, etc.) and date range to get more relevant data for our analysis. Once we have downloaded the raw CSV files, we can use the csv sanitizer to clean the data and persist it to a new CSV file for further processing with polars. This will allow us to extract meaningful insights from the lobbying data and analyze the lobbying activities and their impact on the politicians in Ireland.
#drop it in the raw folder in the lobbying directory, and then run the csv sanitizer to clean the data and persist it to a new csv file for further processing with polars. The csv sanitizer will handle the messy and inconsistent formatting of the raw data, and ensure that we have a clean and consistent dataset to work with for our analysis of the lobbying activities and their impact on the politicians. This is a crucial step in the data processing pipeline, as it allows us to extract meaningful insights from the lobbying data, and identify any potential patterns or trends in the lobbying activities that may be relevant for our analysis of the political landscape and potential conflicts of interest.

#TODO: def csv_stacker()
csvs_to_stack = []
for file in os.listdir(LOBBY_DIR / 'raw'):
    if file.endswith(".csv") and not file.startswith("Lobbying_ie_organisation_results") and not file.startswith("cleaned_output") and not file.startswith("cleaned"):  # Ensure we only process the raw CSV files and not the cleaned output
        print(f"Processing file: {file}")
        df = pl.read_csv(LOBBY_DIR / 'raw' / file)
        df= df.rename(lobbying_rename)
        print(f"Number of rows in {file}: {df.height}")
        csvs_to_stack.append(df)
        
lobbying_df = pl.concat(csvs_to_stack, how="diagonal")
# lobbying_df.write_csv(LOBBY_DIR / 'raw' / 'combined_lobbying_data.csv')  # Save the combined data to a new CSV file for further processing
print(f"Total number of rows in combined lobbying data: {lobbying_df.height}")
lobby_org = pl.read_csv(LOBBY_DIR / 'raw' / 'cleaned_output.csv', infer_schema=4000)

#TODO: try breakup transformation below into functions and more modular steps to make it more readable and maintainable, 
# as the current code is quite dense and does a lot of transformations in a single block, 
# which can make it difficult to understand and debug. 
# By breaking it up into smaller functions with clear responsibilities, 
# we can improve the readability and maintainability of the code, and make it easier to test and debug individual 
# components of the transformation process. This will also allow us to reuse some of the transformation logic if 
# needed, and make it easier to modify or extend the transformation process in the future if we need to add additional 
# steps or handle new edge cases in the data.


#TODO: def lobby_org_transformations(lobby_org: pl.DataFrame) -> pl.DataFrame:
    # Select only the relevant columns for the lobby organizations, which are the columns that contain the key information about the lobbying organizations such as their name, website, main activities, and registration details. This will allow us to focus on the most important information about the lobbying organizations for our analysis, and avoid unnecessary clutter from irrelevant columns that may not be useful for our analysis of the lobbying activities and their impact on the politicians in Ireland.
lobby_org = lobby_org.select("lobby_issue_uri",
                             "name",
                             "website",
                             "main_activities_of_organisation",
                             "company_registration_number",
                             "company_registered_name"
                             )
#construct hyperlinks for the lobby orgs
lobby_org = lobby_org.with_columns(pl.col('name')
                                        .str.to_lowercase()
                                        .str.replace(" ", "-")
                                        .str.replace(" ", "-")
                                        .alias("name_for_link")
                                    )
lobby_org = lobby_org.with_columns(
    #create hyper link
    #https://www.lobbying.ie/organisation
    pl.format("https://www.lobbying.ie/organisation/{}/{}", 
    pl.col("lobby_issue_uri"), 
    pl.col('name_for_link')).alias("lobby_org_link")
    )
lobby_org = lobby_org.drop("name_for_link"
                           ).select(
                               "lobby_issue_uri", 
                               "name", 
                               "main_activities_of_organisation", 
                               "website", 
                               "company_registration_number", 
                               "company_registered_name", 
                               "lobby_org_link"
                            )
#TODO: def lobby_activity_transformations(lobbying_df: pl.DataFrame) -> pl.DataFrame:

# split_df = df.with_columns(
split_df = lobbying_df.with_columns(
        pl.col("dpo_lobbied").str.split("::").alias("lobbyists")
        ).explode("lobbyists").with_columns(
        pl.col("lobbyists")
        .str.split("|").alias("parts")
    )
split_df = split_df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    ).drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")
split_df = split_df.with_columns(
        pl.col("lobbying_activities"
        ).str.split("::").alias("activities_list")
        ).explode("activities_list"
                  ).with_columns(
        pl.col("activities_list").str.split("|").alias("activities_parts")
    )

split_df = split_df.with_columns(
        pl.col("activities_parts").list.get(0).alias("action"),
        pl.col("activities_parts").list.get(1).alias("delivery"),
        pl.col("activities_parts").list.get(2).alias("members_targeted"),
        pl.col("date_published_timestamp").str.to_datetime(format="%d/%m/%Y %H:%M").alias("date_published_timestamp_dt")
    ).drop("activities_list", "activities_parts", "lobbying_activities", "date_published_timestamp")

split_df = split_df.with_columns(
        pl.col("clients").str.split("|").alias("clients_list"))
        # ).explode("clients_list")

split_df = split_df.with_columns(
        pl.col("clients_list").list.get(0).alias("client_name"),
        pl.col("clients_list").list.get(1).alias("client_address"),
        pl.col("clients_list").list.get(2).alias("email"),
        pl.col("clients_list").list.get(3).alias("telephone")
    ).drop("clients_list", "clients")

split_df = split_df.with_columns(
    pl.col("current_or_former_dpos").str.split("|")
    .alias("current_or_former_dpos_list"
    ))
split_df = split_df.with_columns(
    pl.col("current_or_former_dpos_list").list.get(0).alias("current_or_former_dpos"),
    pl.col("current_or_former_dpos_list").list.get(1).alias("current_or_former_dpos_position"),
    pl.col("current_or_former_dpos_list").list.get(2).alias("current_or_former_dpos_chamber")
).drop("current_or_former_dpos_list").rename({"current_or_former_dpos": "dpos_or_former_dpos_who_carried_out_lobbying_name"})
df = split_df.select("full_name","position", "chamber")

#TODO: def most_lobbied_politicians(df: pl.DataFrame) -> pl.DataFrame:
segmented = df.group_by(["full_name", "chamber"]
    ).agg(pl.count().alias("lobby_requests_in_relation_to_position"))

# Total count by full_name
total = df.group_by("full_name"
        ).agg(pl.count().alias("total_count"))

most_lobbied_politician = segmented.join(total, on="full_name").sort("total_count", descending=True)
#TODO: find a way to export the lobby requests url associated to each politician
#big bug on counts (look at test.py)
most_lobbied_politician.write_csv(LOBBY_DIR / 'output' / 'most_lobbied_politicians.csv')
if not os.path.exists(LOBBY_DIR / 'output' / 'lobby_break_down_by_politician.csv'):
    split_df.write_csv(LOBBY_DIR / 'output' / 'lobby_break_down_by_politician.csv')
else: 
    print("total lobbying activities count by politician already exists, skipping creation of lobby_break_down_by_politician.csv to avoid overwriting existing file. If you want to update the file, please delete the existing file and run the code again.")
    most_prolific_lobbyist = lobbying_df.select(
            pl.col('primary_key'),
            pl.col("lobby_enterprise_uri"),
            pl.col("lobbyist_name"),
            pl.col("dpo_lobbied").str.split("::").list.len().alias("politicians_involved_count")
        )
    counts = most_prolific_lobbyist.group_by(
                        pl.col("lobbyist_name")
                        ).agg(pl.len().alias("lobby_requests_count"
            )).join(
                lobby_org.select(
                    pl.col("main_activities_of_organisation"),
                    pl.col("company_registration_number"),
                    pl.col("company_registered_name"),
                    pl.col("website"),
                    pl.col("name"), 
                    pl.col("lobby_org_link")), left_on="lobbyist_name", right_on="name", how="inner")
    most_prolific_lobbyist = most_prolific_lobbyist.join(
                    counts, on="lobbyist_name"
                    ).sort(["politicians_involved_count", "lobby_requests_count"],  descending=True)
    most_prolific_lobbyist.write_csv(LOBBY_DIR / 'output' / 'lobby_count_details.csv')
   
# if __name__ == "__main__":
#     main()

# import csv
# import polars as pl
# from utility.select_drop_rename_cols_mappings import lobbying_rename
# import os
# import logging
# from config import LOBBY_DIR

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger(__name__)


# # ---------------------------------------------------------------------------
# # INGEST
# # ---------------------------------------------------------------------------

# def load_raw_lobbying_csvs(lobby_dir) -> pl.DataFrame:
#     """Stack all raw lobbying activity CSVs into one DataFrame."""
#     EXCLUDE = {"cleaned_output.csv", "cleaned.csv", "Lobbying_ie_organisation_results.csv"}
#     frames = []
#     for file in os.listdir(lobby_dir / "raw"):
#         if file.endswith(".csv") and file not in EXCLUDE:
#             logger.info(f"Loading lobbying file: {file}")
#             df = pl.read_csv(lobby_dir / "raw" / file)
#             frames.append(df.rename(lobbying_rename))
#             logger.info(f"Rows in {file}: {df.height}")
#     if not frames:
#         raise FileNotFoundError(f"No raw lobbying CSVs found in {lobby_dir / 'raw'}")
#     combined = pl.concat(frames, how="diagonal")
#     logger.info(f"Total rows after stacking: {combined.height}")
#     return combined


# def parse_line(line: str) -> list:
#     """Manually parse one raw line from the messy lobbying.ie org CSV export."""
#     line = line.replace('\"\'', '\'')
#     stripped = line.strip().split('","')
#     stripped[0] = stripped[0].lstrip('"')
#     stripped[-1] = stripped[-1].rstrip('"')
#     return stripped


# def sanitize_lobby_org_csv(lobby_dir) -> None:
#     """Read the raw org CSV, manually parse it, write a cleaned version."""
#     raw_path = lobby_dir / "raw" / "Lobbying_ie_organisation_results.csv"
#     cleaned_path = lobby_dir / "raw" / "cleaned.csv"

#     with open(raw_path, "r", encoding="utf-8") as f:
#         raw_lines = f.readlines()

#     rows = [parse_line(line) for line in raw_lines]

#     with open(cleaned_path, "w", newline="", encoding="utf-8") as f:
#         csv.writer(f, quoting=csv.QUOTE_ALL).writerows(rows)

#     logger.info("Raw lobby org CSV sanitized.")


# def load_lobby_orgs(lobby_dir) -> pl.DataFrame:
#     """Load, sanitize, and return a clean lobby organisations reference table."""
#     sanitize_lobby_org_csv(lobby_dir)

#     column_names = [
#         "lobby_issue_uri", "name", "address", "county", "country",
#         "phone_number", "website", "main_activities_of_organisation",
#         "person_responsible_name", "person_responsible_email",
#         "person_responsible_telephone", "email", "company_registration_number",
#         "company_registered_name", "company_registered_address",
#         "charity_regulation_number", "chy_number",
#     ]

#     cleaned_path = lobby_dir / "raw" / "cleaned.csv"
#     df = pl.read_csv(cleaned_path, has_header=False, infer_schema=True, skip_lines=1)
#     df.columns = column_names
#     df.write_csv(lobby_dir / "raw" / "cleaned_output.csv")
#     logger.info("Lobby org reference table created.")
#     return df


# # ---------------------------------------------------------------------------
# # TRANSFORM
# # ---------------------------------------------------------------------------

# def explode_dpo_lobbied(lobbying_df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Explode the dpo_lobbied column (politicians targeted) from '::' separated
#     pipe-delimited strings into one row per politician per lobbying activity.
#     """
#     return (
#         lobbying_df
#         .with_columns(pl.col("dpo_lobbied").str.split("::").alias("lobbyists"))
#         .explode("lobbyists")
#         .with_columns(pl.col("lobbyists").str.split("|").alias("parts"))
#         .with_columns(
#             pl.col("parts").list.get(0).alias("full_name"),
#             pl.col("parts").list.get(1).alias("position"),
#             pl.col("parts").list.get(2).alias("chamber"),
#         )
#         .drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")
#     )


# def explode_activities(df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Explode the lobbying_activities column from '::' separated pipe-delimited
#     strings into one row per activity.
#     """
#     return (
#         df
#         .with_columns(pl.col("lobbying_activities").str.split("::").alias("activities_list"))
#         .explode("activities_list")
#         .with_columns(pl.col("activities_list").str.split("|").alias("activities_parts"))
#         .with_columns(
#             pl.col("activities_parts").list.get(0).alias("action"),
#             pl.col("activities_parts").list.get(1).alias("delivery"),
#             pl.col("activities_parts").list.get(2).alias("members_targeted"),
#         )
#         .drop("activities_list", "activities_parts", "lobbying_activities")
#     )


# def build_lobby_org_links(lobby_org: pl.DataFrame) -> pl.DataFrame:
#     """Add a hyperlink column to the lobby organisations table."""
#     return (
#         lobby_org
#         .select(
#             "lobby_issue_uri", "name", "main_activities_of_organisation",
#             "website", "company_registration_number", "company_registered_name",
#         )
#         .with_columns(
#             pl.col("name")
#             .str.to_lowercase()
#             .str.replace_all(" ", "-")   # fixed: was .str.replace (only first match)
#             .alias("name_for_link")
#         )
#         .with_columns(
#             pl.format(
#                 "https://www.lobbying.ie/organisation/{}/{}",
#                 pl.col("lobby_issue_uri"),
#                 pl.col("name_for_link"),
#             ).alias("lobby_org_link")
#         )
#         .drop("name_for_link")
#     )


# # ---------------------------------------------------------------------------
# # ANALYSE
# # ---------------------------------------------------------------------------

# def most_lobbied_politicians(politicians_df: pl.DataFrame) -> pl.DataFrame:
#     """
#     Return a ranked table of politicians by how many times they were targeted,
#     broken down by chamber.

#     Fixed: the original joined 'total' twice, inflating all counts.
#     """
#     segmented = (
#         politicians_df
#         .group_by(["full_name", "chamber"])
#         .agg(pl.len().alias("appearances_by_chamber"))
#     )
#     total = (
#         politicians_df
#         .group_by("full_name")
#         .agg(pl.len().alias("total_appearances"))
#     )
#     return (
#         segmented
#         .join(total, on="full_name")
#         .sort("total_appearances", descending=True)
#     )


# def most_prolific_lobbyists(lobbying_df: pl.DataFrame, lobby_org: pl.DataFrame) -> pl.DataFrame:
#     """
#     Return a ranked table of lobbying organisations by number of lobby requests,
#     joined with org metadata and politician-count per request.
#     """
#     # Count how many politicians each individual lobbying return targeted
#     with_politician_count = lobbying_df.select(
#         pl.col("primary_key"),
#         pl.col("lobby_enterprise_uri"),
#         pl.col("lobbyist_name"),
#         pl.col("dpo_lobbied")
#         .str.split("::")
#         .list.len()
#         .alias("politicians_involved_count"),
#     )

#     # Count total lobby requests per lobbyist name
#     request_counts = (
#         with_politician_count
#         .group_by("lobbyist_name")
#         .agg(pl.len().alias("lobby_requests_count"))
#     )

#     # Join org metadata onto request counts
#     request_counts = request_counts.join(
#         lobby_org.select(
#             "name", "main_activities_of_organisation",
#             "company_registration_number", "company_registered_name",
#             "website", "lobby_org_link",
#         ),
#         left_on="lobbyist_name",
#         right_on="name",
#         how="inner",
#     )

#     # Join back to get per-record politician counts and sort
#     return (
#         with_politician_count
#         .join(request_counts, on="lobbyist_name")
#         .sort(["politicians_involved_count", "lobby_requests_count"], descending=True)
#     )


# # ---------------------------------------------------------------------------
# # SAVE
# # ---------------------------------------------------------------------------

# def save_outputs(
#     split_df: pl.DataFrame,
#     most_lobbied: pl.DataFrame,
#     most_prolific: pl.DataFrame,
#     lobby_dir,
# ) -> None:
#     """Write all output CSVs to the output directory."""
#     out = lobby_dir / "output"
#     out.mkdir(parents=True, exist_ok=True)

#     most_lobbied.write_csv(out / "most_lobbied_politicians.csv")
#     logger.info("Saved most_lobbied_politicians.csv")

#     breakdown_path = out / "lobby_break_down_by_politician.csv"
#     if not breakdown_path.exists():
#         split_df.write_csv(breakdown_path)
#         logger.info("Saved lobby_break_down_by_politician.csv")
#     else:
#         logger.info("lobby_break_down_by_politician.csv already exists, skipping.")

#     most_prolific.write_csv(out / "lobby_count_details.csv")
#     logger.info("Saved lobby_count_details.csv")


# # ---------------------------------------------------------------------------
# # MAIN
# # ---------------------------------------------------------------------------

# def main() -> None:
#     logger.info("=== Lobbying pipeline starting ===")

#     lobbying_df  = load_raw_lobbying_csvs(LOBBY_DIR)
#     lobby_org    = load_lobby_orgs(LOBBY_DIR)
#     lobby_org    = build_lobby_org_links(lobby_org)

#     politicians_df = explode_dpo_lobbied(lobbying_df)
#     activities_df  = explode_activities(politicians_df)

#     most_lobbied  = most_lobbied_politicians(politicians_df)
#     most_prolific = most_prolific_lobbyists(lobbying_df, lobby_org)

#     save_outputs(activities_df, most_lobbied, most_prolific, LOBBY_DIR)

#     logger.info("=== Lobbying pipeline complete ===")


# if __name__ == "__main__":
#     main()