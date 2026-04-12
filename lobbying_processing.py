import polars as pl
from utility.select_drop_rename_cols_mappings import lobbying_rename
#HOW TO EXTRACT THE LOBBYING DATA:
# eg: https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=11&returnDateFrom=01-02-2026&returnDateTo=08-04-2026&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId=
#TODO make read csv more agnostic and read any pdf from the lobbying folder, and then persist the cleaned and filtered data to a dedicated folder in the processed data directory, instead of hardcoding the file paths in the code. This way we can easily update the data by just updating the files in the data directory without having to change the code in multiple places.
df = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_returns_results_1.csv")
df1=pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_returns_results_01_02_2024_to_01_02_2025.csv")
df2=pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_returns_results_01_02_2025_to_1_02_2026.csv")

# lobby_org = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv")
# lobby_org = pl.read_csv("C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv", infer_schema=4000)
# lobby_org = pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\lobbyist\\Lobbying_ie_organisation_results.csv", ,infer_schema=False, quote_char='"', separator=',', missing_utf8_is_empty_string=True)
lobby_org = lobby_org.with_columns(
    pl.col('CompanyRegistrationNumber').str.replace_all(r'"{2}', '\"' )
    )
lobby_org = lobby_org.drop("Main activities of the organisation", "CompanyRegistrationNumber", "CompanyRegisteredName").rename({"Id": "lobby_enterprise_uri", "Name": "lobbyist_name"})

lobby_org = lobby_org.select("Id","Name","Main activities of the organisation", "CompanyRegistrationNumber", "CompanyRegisteredName")
#construct hyperlinks for the lobby orgs
lobby_org = lobby_org.with_columns(pl.col('Name').lower_case().replace(" ", "-").alias("name_for_link"))
lobby_org = lobby_org.with_columns(
    #https://www.lobbying.ie/organisation
    pl.format("https://www.lobbying.ie/organisation/{}/{}", pl.col("Id"), pl.col('name_for_link')).alias("lobby_org_link")
    )
lobby_org = lobby_org.drop("name_for_link")
lobby_org = lobby_org.rename({"Id": "lobby_enterprise_uri", "Name": "lobbyist_name", "Main activities of the organisation": "lobby_activities", "CompanyRegistrationNumber": "company_registration_number", "CompanyRegisteredName": "company_registered_name"})    
lobby_org = lobby_org.selet("lobby_enterprise_uri", "lobbyist_name", "lobby_activities", "company_registration_number", "company_registered_name", "lobby_org_link")
lobby_org.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_orgs.csv')
# lobby_org.with_columns(pl.col(''))
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
    ).drop("lobbyists", "parts", "dpo_lobbied")
split_df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/filtered_lobby_1.csv')
most_prolific_lobbyist = df.select(
        pl.col('primary_key'),
        pl.col("lobby_enterprise_uri"),
        pl.col("lobbyist_name"),
        pl.col("dpo_lobbied").str.split("::").list.len().alias("politicians_involved_count")
    )
counts = most_prolific_lobbyist.group_by(pl.col("lobbyist_name")).agg(pl.len().alias("lobby_requests_count"
        ))
most_prolific_lobbyist = most_prolific_lobbyist.join(counts, on="lobbyist_name")
most_prolific_lobbyist= most_prolific_lobbyist.sort("politicians_involved_count",  descending=True)
most_prolific_lobbyist.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/lobby_count_1.csv')
c