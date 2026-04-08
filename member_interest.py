
import fitz  # PyMuPDF
import pathlib
import polars as pl
import json
import re
member_interest = pathlib.Path(r"C:\Users\pglyn\PycharmProjects\dail_extractor\pdf_member_interest\2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf")

text_boxes = []
clean_text= []

print('Starting to process member interest PDF...')
doc = fitz.open(member_interest)  # Open the PDF document using PyMuPDF
print(f"Processing scanned file: {member_interest} with {doc.page_count} pages...")

for page in doc:
    text = page.get_text(option="text")
    # print(text)
    text = text.strip()
    text = text.splitlines(False) # split the text into lines and remove empty lines
    text_boxes.append(text)
flat = []
for sublist in text_boxes:
    for item in sublist:
        flat.append(item)

result = list(filter(str.strip, flat))
result = result[8:-5]
print(result)
df = pl.DataFrame(result, schema=['text'], orient='row') 
df.write_json('C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\member_interest_raw_text.json')
# Claude code test out:
# import fitz  # PyMuPDF
# import pathlib
# import json
# import re

# member_interest = pathlib.Path(
#     r"C:\Users\pglyn\PycharmProjects\dail_extractor\pdf_member_interest\2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
# )

# print("Starting to process member interest PDF...")
# doc = fitz.open(member_interest)
# print(f"Processing file: {member_interest} with {doc.page_count} pages...")

# # Extract and flatten all text lines
# text_boxes = []
# for page in doc:
#     text = page.get_text(option="text")
#     text = text.strip()
#     lines = text.splitlines(False)
#     text_boxes.append(lines)

# flat = []
# for sublist in text_boxes:
#     for item in sublist:
#         flat.append(item)

# # Remove empty/whitespace-only lines and trim header/footer
# result = list(filter(str.strip, flat))
# result = result[8:-5]

# # Define boundary patterns
# categories = re.compile(r"^\d+\.\s")       # "1. ", "2. " etc.
# member_name = re.compile(r"^[A-Z]{2,},\s")  # "ARDAGH, Catherine"

# # Group fragmented lines together
# grouped = []
# current = ""

# for line in result:
#     if categories.match(line) or member_name.match(line):
#         if current.strip():
#             grouped.append(current.strip())
#         current = line
#     else:
#         current = current + " " + line

# if current.strip():
#     grouped.append(current.strip())

# # Structure into members with their interests
# members = []
# current_member = None

# for line in grouped:
#     if member_name.match(line):
#         if current_member:
#             members.append(current_member)
#         current_member = {"name": line, "interests": []}
#     elif current_member is not None:
#         current_member["interests"].append(line)

# if current_member:
#     members.append(current_member)

# # Save output
# output_path = r"C:\Users\pglyn\PycharmProjects\dail_extractor\members\member_interests_grouped.json"
# with open(output_path, "w", encoding="utf-8") as f:
#     json.dump(members, f, indent=4, ensure_ascii=False)

# print(f"Processed {len(members)} members")
# print(f"Output saved to {output_path}")