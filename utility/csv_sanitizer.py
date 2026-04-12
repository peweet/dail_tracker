
import polars as pl
import csv
import pandas as pd
# rows = clevercsv.read_table('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv')
# # clevercsv.write_table(rows, 'C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', encoding='utf-8')
# cleaned_rows = []
# cleaned_value = ''
# for row in rows:
#     for value in row:
#         value = value.strip('"')  # remove leading and trailing double quotes
#         value = value.strip()
#         value = value.replace('""', '"') 
#         print(f"Original value: {value}")
#     cleaned_rows.append(value)

# working
import clevercsv
import csv
rows = clevercsv.read_table('C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv')
# clevercsv.write_table(rows, 'C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', encoding='utf-8')
cleaned_rows = []

for row in rows:
    cleaned_row = []  # ← new row container
    
    for value in row:
        value = value.strip('"')  # remove leading/trailing quotes
        value = value.strip()
        value = value.replace('""', '"')  # fix escaped quotes
        
        print(f"Original value: {value}")
        
        cleaned_row.append(value)  # ← append to row
    
    cleaned_rows.append(cleaned_row)  # ← append full row

     # remove single quotes from the data, which are causing issues with parsing the CSV in Polars later on
with open('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', 'w', newline='', encoding='utf-8', ) as f:
     # remove single quotes from the data, which are causing issues with parsing the CSV in Polars later on
    print(rows)
    csv.writer(f, skipinitialspace=True, delimiter=",", quoting=csv.QUOTE_STRINGS).writerows(cleaned_rows)
    print("CSV file cleaned and saved successfully.")
# df = pl.read_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', infer_schema_length=100000)
# df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned_final.csv')
# import clevercsv

# input_path = 'C:/Users/pglyn/PycharmProjects/dail_extractor/lobbyist/Lobbying_ie_organisation_results.csv'
# temp_path = 'C:/Users/pglyn/PycharmProjects/dail_extractor/utility/temp_cleaned_input.csv'
# # Step 1: Read raw file
# with open(input_path, encoding='utf-8') as f:
#     lines = f.readlines()

# cleaned_lines = []
# for line in lines:
#     line = line.replace('""', '"')
#     # line = line.replace("'", '"')
#     # line = line.replace('"s', "'s")
#     cleaned_lines.append(line)

# # Step 3: Write to temp file
# with open(temp_path, 'w', encoding='utf-8') as f:
#     csv.writer(f,  delimiter=",",quoting=csv.QUOTE_MINIMAL).writerows(cleaned_lines)
#     # f.writelines()
# with open('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv', 'w', newline='', encoding='utf-8', ) as f:    
#     #,  quotechar=',',escapechar='"' skipinitialspace=True,
#     csv.writer(f,  delimiter=",").writerows(cleaned_rows)
#     print("CSV file cleaned and saved successfully.")
# Step 4: Parse with clevercsv
# rows = clevercsv.read_table(temp_path)

# # Step 5: Write final cleaned CSV
# clevercsv.write_table(rows, output_path, encoding='utf-8')

# print("CSV file cleaned and saved successfully.")
# df = pl.read_csv(temp_path, infer_schema_length=100000)
# df.write_csv('C:/Users/pglyn/PycharmProjects/dail_extractor/utility/cleaned.csv')