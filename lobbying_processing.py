import csv
import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
import os
# Parse each non-empty line using the parse_line function
# Read the raw input file line by line
# Function to parse a single line from the input CSV, the lobbyist data is very messy and 
# the csv dialect changes which necessistates manual intervention. Pandas will not work effectively with this data even with different dialect settings, so we have to do it manually and then read the cleaned data with polars for further processing. The main issues with the raw data are inconsistent use of quotes, embedded commas in fields, and inconsistent line breaks, which makes it difficult to parse with standard CSV parsers without manual cleaning.
# To extract the faw data go to https://www.lobbying.ie/app/Organisation/Search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&lobbyingActivities=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&includeClients=false
# and click the CSV export option to export all the registered lobby organizations in the Republic of Ireland
def parse_line(line):
    # print(f"Parsing line: {line.strip()}")
     # Replace triple double-quotes with double double-quotes
    line =line.replace('\"\'', '\'')  # Replace triple double-quotes with double double-quotes
    stripped = line.strip().split('","') # Split the line on '","' to separate fields, which handles quoted fields with embedded commas
    stripped[0] = stripped[0].lstrip('"')  # Remove the leading quote from the first field
    stripped[-1] = stripped[-1].rstrip('"')  # Remove the trailing quote
    # print(f"Test: {stripped}")  # Remove the first two quotes from the first field
    return stripped
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/Lobbying_ie_organisation_results.csv', 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()

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
    os.remove('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned.csv')
    print("Lobbying organization CSV sanitized and cleaned successfully. Cleaned data saved to cleaned_output.csv")
lobby_org_csv_sanitizer()

#HOW TO EXTRACT THE LOBBYING DATA:
# eg: https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=11&returnDateFrom=01-02-2026&returnDateTo=08-04-2026&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId=
#Do it in batches of 1000 and download it manually, the max date range is 12 months ie 2025-01 to 2026-01, and then stack the csvs together and do the cleaning and processing with polars. The lobbying data is not available via an API, so we have to download it manually in batches of 1000 records, which is the maximum allowed by the website for a single download. We can filter the data by job titles (e.g. TDs, Senators, Ministers, etc.) and date range to get more relevant data for our analysis. Once we have downloaded the raw CSV files, we can use the csv sanitizer to clean the data and persist it to a new CSV file for further processing with polars. This will allow us to extract meaningful insights from the lobbying data and analyze the lobbying activities and their impact on the politicians in Ireland.
#drop it in the raw folder in the lobbying directory, and then run the csv sanitizer to clean the data and persist it to a new csv file for further processing with polars. The csv sanitizer will handle the messy and inconsistent formatting of the raw data, and ensure that we have a clean and consistent dataset to work with for our analysis of the lobbying activities and their impact on the politicians. This is a crucial step in the data processing pipeline, as it allows us to extract meaningful insights from the lobbying data, and identify any potential patterns or trends in the lobbying activities that may be relevant for our analysis of the political landscape and potential conflicts of interest.
csvs_to_stack = []
for file in os.listdir("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw"):
    if file.endswith(".csv") and not file.startswith("Lobbying_ie_organisation_results"):  # Ensure we only process the raw CSV files and not the cleaned output
        print(f"Processing file: {file}")
        df = pl.read_csv(f"C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/raw/{file}")
        df= df.rename(lobbying_rename)
        print(f"Number of rows in {file}: {df.height}")
        csvs_to_stack.append(df)
        
lobbying_df = pl.concat(csvs_to_stack, how="diagonal")
lobbying_df.write_csv('c:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/combined_lobbying_data.csv')  # Save the combined data to a new CSV file for further processing
print(f"Total number of rows in combined lobbying data: {lobbying_df.height}")
lobby_org = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv", infer_schema=4000)
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
    #create hyper link
    #https://www.lobbying.ie/organisation
    pl.format("https://www.lobbying.ie/organisation/{}/{}", 
    pl.col("lobby_issue_uri"), pl.col('name_for_link')).alias("lobby_org_link")
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
df = split_df.select("full_name","position", "chamber")

segmented = df.group_by(["full_name", "chamber"]
    ).agg(pl.count().alias("segmented_count"))

# Total count by full_name
total = df.group_by("full_name"
        ).agg(pl.count().alias("total_count"))

combined = segmented.join(total, on="full_name")

# Join total count onto segmented count
most_lobbied_politician = combined.join(total, on="full_name")

#big bug on counts (look at test.py)
most_lobbied_politician.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/most_lobbied_politicians.csv')
if not os.path.exists('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv'):
    split_df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_break_down_by_politician.csv')
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
most_prolific_lobbyist.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_count_details.csv')
os.remove('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/cleaned_output.csv')




