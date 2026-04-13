import csv
import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
import os
#HOW TO EXTRACT THE LOBBYING DATA:
# eg: https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=11&returnDateFrom=01-02-2026&returnDateTo=08-04-2026&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId=
#TODO make read csv more agnostic and read any pdf from the lobbying folder, and then persist the cleaned and filtered data to a dedicated folder in the processed data directory, instead of hardcoding the file paths in the code. This way we can easily update the data by just updating the files in the data directory without having to change the code in multiple places.
#TODO the join logic is very confusing and some columns are being dropped and not added to the final output, so need to review the join logic and make sure all the relevant columns are included in the final output, and also make sure the column names are consistent across the different dataframes before joining, to avoid any confusion and errors in the join process. Also need to review the filtering logic and make sure it's correctly filtering the data based on the relevant criteria (e.g. date range, job titles, etc.) before joining, to ensure we're only including relevant lobbying activities in the final output.
df = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/Lobbying_ie_returns_results_1.csv")
df1=pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/Lobbying_ie_returns_results_01_02_2024_to_01_02_2025.csv")
df2=pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/Lobbying_ie_returns_results_01_02_2025_to_1_02_2026.csv")
lobby_org = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned_output.csv", infer_schema=4000)
lobby_org = lobby_org.select("lobby_issue_uri",
                             "name",
                             "website",
                             "main_activities_of_organisation",
                             "company_registration_number",
                             "company_registered_name"
                             )
#construct hyperlinks for the lobby orgs
lobby_org = lobby_org.with_columns(pl.col('name'
                                          ).str.to_lowercase(
                                          ).str.replace(" ", "-").str.replace(" ", "-").alias("name_for_link"))
lobby_org = lobby_org.with_columns(
    #https://www.lobbying.ie/organisation
    pl.format("https://www.lobbying.ie/organisation/{}/{}", 
    pl.col("lobby_issue_uri"), pl.col('name_for_link')).alias("lobby_org_link")
    )
lobby_org = lobby_org.drop("name_for_link")
lobby_org = lobby_org.select("lobby_issue_uri", "name", "main_activities_of_organisation", "website", "company_registration_number", "company_registered_name", "lobby_org_link")
df = df.vstack(df1).vstack(df2)
df = df.rename(lobbying_rename)
split_df = df.with_columns(pl.col("dpo_lobbied").str.split("::").alias("lobbyists")
        ).explode("lobbyists").with_columns(
        pl.col("lobbyists").str.split("|").alias("parts")
    )
split_df = split_df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    ).drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")

split_df = split_df.with_columns(
        pl.col("lobbying_activities"
        ).str.split("::").alias("activities_list")
        ).explode("activities_list").with_columns(
        pl.col("activities_list").str.split("|").alias("activities_parts")
    )
split_df = split_df.with_columns(
        pl.col("activities_parts").list.get(0).alias("action"),
        pl.col("activities_parts").list.get(1).alias("delivery"),
        pl.col("activities_parts").list.get(2).alias("members_targeted"),
    ).drop("activities_list", "activities_parts", "lobbying_activities")
most_lobbied_politician = split_df.select("full_name","position", "chamber")
most_lobbied_politician = most_lobbied_politician.with_columns(
    pl.col("full_name")).group_by(
        "full_name"
        ).agg(pl.len().alias("total_lobbying_activities_count"))
most_lobbied_politician = most_lobbied_politician.join(
    split_df.select("full_name", "position", "chamber").unique(),
    on="full_name", how="left"
)
#todo do breakdown by chamber and position as well, to see if there are any patterns in terms of which politicians are being lobbied the most (e.g. ministers vs backbenchers, government vs opposition, etc.)
#as well as ID the most prolific lobbyists and the organisations they represent, to see if there are any patterns in terms of which lobbyists and organisations are the most active in lobbying the politicians, 
# and which politicians they are targeting the most. 
# This can help to identify any potential conflicts of interest
#  or undue influence in the lobbying activities, and also provide insights into the lobbying 
# landscape in terms of which issues and sectors are being targeted the most by the lobbyists.
most_lobbied_politician = most_lobbied_politician.sort("total_lobbying_activities_count", descending=True)
most_lobbied_politician.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/most_lobbied_politicians.csv')
if not os.path.exists('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv'):
    split_df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv')
else: 
    print("total lobbying activities count by politician already exists, skipping creation of lobby_break_down_by_politician.csv to avoid overwriting existing file. If you want to update the file, please delete the existing file and run the code again.")
    most_prolific_lobbyist = df.select(
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
most_prolific_lobbyist.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_count_details.csv')

# Function to parse a single line from the input CSV, the lobbyist data is very messy and 
#the csv dialect changes which necessistates manual intervention. Pandas will not work effectively with this data even with different dialect settings, so we have to do it manually and then read the cleaned data with polars for further processing. The main issues with the raw data are inconsistent use of quotes, embedded commas in fields, and inconsistent line breaks, which makes it difficult to parse with standard CSV parsers without manual cleaning.
def parse_line(line):
    # print(f"Parsing line: {line.strip()}")
     # Replace triple double-quotes with double double-quotes
    line =line.replace('\"\'', '\'')  # Replace triple double-quotes with double double-quotes
    stripped = line.strip().split('","') # Split the line on '","' to separate fields, which handles quoted fields with embedded commas
    stripped[0] = stripped[0].lstrip('"')  # Remove the leading quote from the first field
    stripped[-1] = stripped[-1].rstrip('"')  # Remove the trailing quote
    # print(f"Test: {stripped}")  # Remove the first two quotes from the first field
    return stripped
# Read the raw input file line by line
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv', 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()

# Parse each non-empty line using the parse_line function
def lobby_org_csv_sanitizer():
    rows = []
    for line in raw_lines:
        rows.append(parse_line(line))
    with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerows(rows)
    #manually assign col names as the csv has no header row, and then persist the cleaned data to a new csv file for further processing with polars
    #additioanlly the rows are ragged and uneven which can cause further issues down the line, so we need to make sure all rows have the same number of columns before writing to the cleaned csv file, and if not, we can log the issue and skip those rows to avoid errors in the downstream processing. This is a common issue with messy CSV data where some rows may have missing or extra fields, which can cause parsing errors if not handled properly.
    column_names = ["lobby_issue_uri","name","address","county","country","phone_number","website","main_activities_of_organisation","person_responsible_name","person_responsible_email","person_responsible_telephone","email","company_registration_number","company_registered_name","company_registered_address","charity_regulation_number","chy_number"]
    df = pl.read_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv',  has_header=False,infer_schema=True, skip_lines=1)
    df.columns = column_names
    df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv')
lobby_org_csv_sanitizer()