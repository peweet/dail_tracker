

# Standard library imports for CSV parsing and in-memory string handling
import csv
import polars as pl
import pandas as pd
import io
# Function to parse a single line from the input CSV
def parse_line(line):
    print(f"Parsing line: {line.strip()}")
     # Replace triple double-quotes with double double-quotes
    line =line.replace('\"\'', '\'')  # Replace triple double-quotes with double double-quotes
    stripped = line.strip().split('","') # Split the line on '","' to separate fields, which handles quoted fields with embedded commas
    stripped[0] = stripped[0].lstrip('"')  # Remove the leading quote from the first field
    stripped[-1] = stripped[-1].rstrip('"')  # Remove the trailing quote
    print(f"Test: {stripped}")  # Remove the first two quotes from the first field
    return stripped
# Read the raw input file line by line
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv', 'r', encoding='utf-8') as f:
    raw_lines = f.readlines()


# Parse each non-empty line using the parse_line function
rows = []
for line in raw_lines:
    rows.append(parse_line(line))
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
    writer.writerows(rows)
column_names = ["ID","name","address","county","country","phone_number","website","main_activities_of_organisation","PersonResponsibleName","person_responsible_email","person_responsible_telephone","email","company_registration_number","company_registered_name","company_registered_address","charity_regulation_number","chy_number"]
df = pl.read_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv',  has_header=False,infer_schema=True, skip_lines=1)
df.columns = column_names
df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned_output.csv')