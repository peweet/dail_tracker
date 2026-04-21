#  Hardcoded API links and constants used across multiple services (e.g. bills_by_td.py, question_api.py, etc.) can be centralized here to avoid duplication and make it easier to maintain. For example, we can define the base API URL, common query parameters, and any other constants that are shared across services in this file. This way, if we need to update the API endpoint or parameters in the future, we can do it in one place instead of having to search through multiple files.
#  This also helps improve code readability and organization by keeping all constants in a dedicated module.
#  TODO: create tests and validation


# API link: https://api.oireachtas.ie/

#SOURCE https://www.oireachtas.ie/en/publications/?q=&author%5B%5D=dail-eireann&date=&term=%2Fie%2Foireachtas%2Fhouse%2Fdail%2F34&fromDate=03%2F04%2F2026&toDate=03%2F04%2F2026&topic%5B%5D=record-of-attendance
working_api_question_request="https://api.oireachtas.ie/v1/questions?skip=0&limit=50&qtype=oral,written&member_id=%2Fie%2Foireachtas%2Fmember%2Fid%2FBrendan-Howlin.S.1983-02-23"

test_working_legislation_single_td = "https://api.oireachtas.ie/v1/legislation?date_start=2014-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"
# Another working test URL for a single TD
legislation_working = "https://api.oireachtas.ie/v1/legislation?date_start2014-01-01&date_end=&limit=50&member_id=https%3A%2F%2Fdata.oireachtas.ie%2Fie%2Foireachtas%2Fmember%2Fid%2FNoel-Grealish.D.2002-06-06&chamber_id=&lang=en"


interesting_github="https://github.com/robmcelhinney/OireachtasVote"
interesting_github_2="https://github.com/robmcelhinney/OireachtasVote/blob/master/python/OireachtasVotingHistory.py"
interesting_github_3="https://github.com/robmcelhinney/OireachtasVote/blob/master/python/OireachtasVoting.py"


#ATTENDANCE PDF LINKS

#TODO make api calls to persist this into dedicated folders and files in the data directory, and then read from those files in the relevant services (e.g. payments.py, attendance.py, etc.) instead of hardcoding the URLs in those services. This way we can easily update the data by just updating the files in the data directory without having to change the code in multiple places.
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
payment_dec_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-1-31-december-2024_en.pdf"
payment_29_30_nov_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-29-30-november-2024_en.pdf"
payments_1_8_nov_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2025/2025-02-17_parliamentary-standard-allowance-payments-to-deputies-for-1-8-november-2024_en.pdf"
payments_oct_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-12-16_parliamentary-standard-allowance-payments-to-deputies-for-october-2024_en.pdf"
payments_sep_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-11-01_parliamentary-standard-allowance-payments-to-deputies-for-september-2024_en.pdf"
payments_aug_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-10-11_parliamentary-standard-allowance-payments-to-deputies-for-august-2024_en.pdf"
payments_july_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-09-04_parliamentary-standard-allowance-payments-to-deputies-for-july-2024_en.pdf"
payments_june_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
payments_may_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
payments_april_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-06-02_parliamentary-standard-allowance-payments-to-deputies-for-april-2024_en.pdf"
payments_march_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-05-02_parliamentary-standard-allowance-payments-to-deputies-for-march-2024_en.pdf"
paymenents_feb_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2024_en.pdf"
payments_jan_2024="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2024_en.pdf"

payments_dec_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-december-2023_en.pdf"
payments_nov_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-november-2023_en.pdf"
payments_october_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-12-01_parliamentary-standard-allowance-payments-to-deputies-for-october-2023_en.pdf"
payments_septeber_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-10-27_parliamentary-standard-allowance-payments-to-deputies-for-september-2023_en.pdf"
payments_august_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2023_en.pdf"
payments_july_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-09-01_parliamentary-standard-allowance-payments-to-deputies-for-july-2023_en.pdf"
payments_june_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-08-01_parliamentary-standard-allowance-payments-to-deputies-for-june-2023_en.pdf"
payments_may_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2023/2023-07-01_parliamentary-standard-allowance-payments-to-deputies-for-may-2023_en.pdf"
payments_april_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-06-01_parliamentary-standard-allowance-payments-to-deputies-for-april-2023_en.pdf"
payments_march_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-05-01_parliamentary-standard-allowance-payments-to-deputies-for-march-2023_en.pdf"
payments_feb_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2023_en.pdf"
payments_jan_2023="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2023_en.pdf"
payments_dec_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-03-13_parliamentary-standard-allowance-payments-to-deputies-for-december-2022_en.pdf"

payments_nov_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-01-03_parliamentary-standard-allowance-payments-to-deputies-for-november-2022_en.pdf"
payments_oct_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-12-06_parliamentary-standard-allowance-payments-to-deputies-for-october-2022_en.pdf"
payments_sep_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-11-14_parliamentary-standard-allowance-payments-to-deputies-for-september-2022_en.pdf"
payments_aug_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2022_en.pdf"
payments_aug_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-09-20_parliamentary-standard-allowance-payments-to-deputies-for-july-2022_en.pdf"
payments_july_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-09-20_parliamentary-standard-allowance-payments-to-deputies-for-july-2022_en.pdf"
payments_june_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-08-02_parliamentary-standard-allowance-payments-to-deputies-for-june-2022_en.pdf"
payments_may_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-07-11_parliamentary-standard-allowance-payments-to-deputies-for-may-2022_en.pdf"
payments_april_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-06-07_parliamentary-standard-allowance-payments-to-deputies-for-april-2022_en.pdf"
payments_march_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-05-25_parliamentary-standard-allowance-payments-to-deputies-for-march-2022_en.pdf"
payments_feb_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-04-19_parliamentary-standard-allowance-payments-to-deputies-for-february-2022_en.pdf"
payments_jan_2022="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-03-16_parliamentary-standard-allowance-payments-to-deputies-for-january-2022_en.pdf"
payments_dec_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-03-10_parliamentary-standard-allowance-payments-to-deputies-for-december-2021_en.pdf"
payments_nov_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2022/2022-01-13_parliamentary-standard-allowance-payments-to-deputies-for-november-2021_en.pdf"
payments_oct_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-12-22_parliamentary-standard-allowance-payments-to-deputies-for-october-2021_en.pdf"
payments_sep_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-12-10_parliamentary-standard-allowance-payments-to-deputies-for-september-2021_en.pdf"
payments_aug_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-10-12_parliamentary-standard-allowance-payments-to-deputies-for-august-2021_en.pdf"
payments_july_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-july-2021_en.pdf"
payments_june_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-june-2021_en.pdf"
payments_may_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-07-12_parliamentary-standard-allowance-payments-to-deputies-for-may-2021_en.pdf"
payments_april_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-06-30_parliamentary-standard-allowance-payments-to-deputies-for-april-2021_en.pdf"
payments_march_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-march-2021_en.pdf"
payments_feb_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-february-2021_en.pdf"
payments_jan_2021="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-january-2021_en.pdf"
payments_dec_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-december-2020_en.pdf"
payments_nov_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2021/2021-02-12_parliamentary-standard-allowance-payments-to-deputies-for-november-2020_en.pdf"
payments_oct_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-october-2020_en.pdf"
payments_sep_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-september-2020_en.pdf"
payments_aug_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-august-2020_en.pdf"
payments_july_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-july-2020_en.pdf"
payments_june_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-08-06_parliamentary-standard-allowance-payments-to-deputies-for-june-2020_en.pdf"
payments_may_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-may-2020_en.pdf"
payments_april_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-april-2020_en.pdf"
payment_march_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.pdf"
payments_feb_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2020_en.pdf"
payments_jan_2020="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa/2020/2020-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2020_en.pdf"

WIKI_url = "https://www.wikidata.org/wiki/Wikidata:WikiProject_every_politician/Ireland/data/Dail/Current_Members"
#https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2015/2015-03-11_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2016/2016-03-01_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2019/2019-02-13_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2018/2018-02-14_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2017/2017-03-10_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2016/2016-03-01_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2020/2020-03-03_register-of-members-interests-dail-eireann_en.pdf
dail_member_interests_2020="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2021/2021-02-25_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2021="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2022/2022-02-16_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2022="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2023/2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf"
dail_member_interests_2023="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2024/2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf"
dail_memeber_interests_2024="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2025/2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
dail_member_interests_2025 = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"


#meta data rules
members_interests_rules="https://data.oireachtas.ie/ie/oireachtas/committee/dail/34/committee_on_members_interests_of_dail_eireann/termsOfReference/2025/2025-12-18_guidelines-for-members-of-dail-eireann-who-are-not-office-holders-concerning-the-steps-to-be-taken-by-them-to-ensure-compliance-with-the-provisions-of-the-ethics-in-public-office-acts-1995-and-2001_en.pdf"

rules_payments="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other/2023/2023-04-25_information-on-the-parliamentary-standard-allowance-and-end-of-year-statements-2022_en.pdf"

#Lobby manual sources
lobbying_url = "https://www.lobbying.ie/app/home/search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&publicBodys=&jobTitles=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&responsible=&lobbyist=&lobbyistId="
lobbying_url_2 = "https://www.lobbying.ie/"
lobbying_org ="https://www.lobbying.ie/app/Organisation/Search?currentPage=0&pageSize=20&queryText=&subjectMatters=&subjectMatterAreas=&lobbyingActivities=&returnDateFrom=&returnDateTo=&period=&dpo=&client=&includeClients=false"
member_interests_2025="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
#Inspiration
parrse="https://github.com/mysociety/parlparse"
they_work_for_you="github.com/mysociety/theyworkforyou"
lobbying_source_github = "https://github.com/robmcelhinney/lobbyieng/blob/main/parser.py"
kildare_street_code="https://github.com/shoveyourgaggingorderupyourarse/oireachtasdata/blob/master/bin/search"


#Potential enrichments
cro_api = "https://services.cro.ie/overview.aspx"
cro_data_dict="https://services.cro.ie/datadict.aspx"