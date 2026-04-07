#  Hardcoded API links and constants used across multiple services (e.g. bills_by_td.py, question_api.py, etc.) can be centralized here to avoid duplication and make it easier to maintain. For example, we can define the base API URL, common query parameters, and any other constants that are shared across services in this file. This way, if we need to update the API endpoint or parameters in the future, we can do it in one place instead of having to search through multiple files.
#  This also helps improve code readability and organization by keeping all constants in a dedicated module.
#  TODO: create tests and validation


# API link: https://api.oireachtas.ie/

#SOURCE https://www.oireachtas.ie/en/publications/?q=&author%5B%5D=dail-eireann&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026&topic%5B%5D=record-of-attendance
working_api_question_request="https://api.oireachtas.ie/v1/questions?skip=0&limit=50&qtype=oral,written&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2FBrendan-Howlin.S.1983-02-23"

test_working_legislation_single_td = "https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"
# Another working test URL for a single TD
legislation_working = "https://api.oireachtas.ie/v1/legislation?date_start=1900-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"

#ATTENDANCE PDF LINKS
pdf_2023="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2024/2024-02-01_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2023-to-31-december-2023_en.pdf"
pdf_2024="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2025/2025-02-17_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2024-to-08-november-2024_en.pdf"
pdf_2024_gap="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2025/2025-02-28_deputies-verification-of-attendance-for-the-payment-of-taa-29-november-2024-to-31-december-2024_en.pdf"
pdf_2025_gap="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2025/2025-04-09_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-31-january-2025_en.pdf"
pdf_2025="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2026/2026-02-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-february-2025-to-30-december-2025_en.pdf"
pdf_2026="https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa/2026/2026-04-02_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-28-february-2026_en.pdf"

# Reference URL for the parties endpoint (used during development)
link="https://api.oireachtas.ie/v1/parties?chamber_id=&chamber=dail&house_no=33&limit=60"


#Payment pdf reference URL
payment_feb_td_2026="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"
payment_jan_td_2026="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2026/2026-03-06_parliamentary-standard-allowance-payments-to-deputies-for-january-2026_en.pdf"
payment_dec_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2026/2026-02-16_parliamentary-standard-allowance-payments-to-deputies-for-december-2025_en.pdf"
payment_nov_td_2025="https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul/2026/2026-01-16_parliamentary-standard-allowance-payments-to-deputies-for-november-2025_en.pdf"

payment_september_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-11-18_parliamentary-standard-allowance-payments-to-deputies-for-september-2025_en.pdf"
payment_august_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-10-06_parliamentary-standard-allowance-payments-to-deputies-for-august-2025_en.pdf"
payment_july_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-09-03_parliamentary-standard-allowance-payments-to-deputies-for-july-2025_en.pdf"
payment_june_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-08-15_parliamentary-standard-allowance-payments-to-deputies-for-june-2025_en.pdf"
payment_may_td_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-07-03_parliamentary-standard-allowance-payments-to-deputies-for-may-2025_en.pdf"
payment_april_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-06-10_parliamentary-standard-allowance-payments-to-deputies-for-april-2025_en.pdf"

payment_feb_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-04-22_parliamentary-standard-allowance-payments-to-deputies-for-february-2025_en.pdf"
payment_jan_2025="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-04-09_parliamentary-standard-allowance-payments-to-deputies-for-january-2025_en.pdf"



WIKI_url = "https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician/Ireland/data/Dail/Current_Members"


lobbying_source_github = "https://github.com/robmcelhinney/lobbyieng/blob/main/parser.py"

lobbying_url = "https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId="
lobbying_url_2 = "https://www.lobbying.ie/"