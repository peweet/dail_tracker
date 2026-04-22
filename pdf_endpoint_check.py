import requests
import logging
session = requests.Session()
# This script checks the accessibility of a list of PDF URLs related to TD attendance and payments data. It makes HTTP requests to each URL and verifies that the response status code is 200 (OK). If any URL is not accessible or returns an error, it prints an appropriate message. If all URLs are accessible and working correctly, it confirms that the endpoint check is complete.
#TODO make api calls to persist this into dedicated folders and files in the data directory, and then read from those files in the relevant services (e.g. payments.py, attendance.py, etc.) instead of hardcoding the URLs in those services. This way we can easily update the data by just updating the files in the data directory without having to change the code in multiple places.

# Attendance PDFs
payment_url  = "https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa"
pdf_2023     = f"{payment_url}/2024/2024-02-01_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2023-to-31-december-2023_en.pdf"
pdf_2024     = f"{payment_url}/2025/2025-02-17_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2024-to-08-november-2024_en.pdf"
pdf_2024_gap = f"{payment_url}/2025/2025-02-28_deputies-verification-of-attendance-for-the-payment-of-taa-29-november-2024-to-31-december-2024_en.pdf"
pdf_2025_gap = f"{payment_url}/2025/2025-04-09_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-31-january-2025_en.pdf"
pdf_2025     = f"{payment_url}/2026/2026-02-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-february-2025-to-30-december-2025_en.pdf"
pdf_2026     = f"{payment_url}/2026/2026-04-02_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-28-february-2026_en.pdf"

# Payment PDFs
payment_url ="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"

payment_feb_td_2026         = f"{payment_url}/2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"
payment_jan_td_2026         = f"{payment_url}/2026/2026-03-06_parliamentary-standard-allowance-payments-to-deputies-for-january-2026_en.pdf"
payment_dec_td_2025         = f"{payment_url}/2026/2026-02-16_parliamentary-standard-allowance-payments-to-deputies-for-december-2025_en.pdf"
payment_nov_td_2025         = "https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul/2026/2026-01-16_parliamentary-standard-allowance-payments-to-deputies-for-november-2025_en.pdf"
payment_september_td_2025   = f"{payment_url}/2025/2025-11-18_parliamentary-standard-allowance-payments-to-deputies-for-september-2025_en.pdf"
payment_august_td_2025      = f"{payment_url}/2025/2025-10-06_parliamentary-standard-allowance-payments-to-deputies-for-august-2025_en.pdf"
payment_july_td_2025        = f"{payment_url}/2025/2025-09-03_parliamentary-standard-allowance-payments-to-deputies-for-july-2025_en.pdf"
payment_june_td_2025        = f"{payment_url}/2025/2025-08-15_parliamentary-standard-allowance-payments-to-deputies-for-june-2025_en.pdf"
payment_may_td_2025         = f"{payment_url}/2025/2025-07-03_parliamentary-standard-allowance-payments-to-deputies-for-may-2025_en.pdf"
payment_april_2025          = f"{payment_url}/2025/2025-06-10_parliamentary-standard-allowance-payments-to-deputies-for-april-2025_en.pdf"
payment_feb_2025            = f"{payment_url}/2025/2025-04-22_parliamentary-standard-allowance-payments-to-deputies-for-february-2025_en.pdf"
payment_jan_2025            = f"{payment_url}/2025/2025-04-09_parliamentary-standard-allowance-payments-to-deputies-for-january-2025_en.pdf"
payment_dec_2024            = f"{payment_url}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-1-31-december-2024_en.pdf"
payment_29_30_nov_2024      = f"{payment_url}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-29-30-november-2024_en.pdf"
payments_1_8_nov_2024       = f"{payment_url}/2025/2025-02-17_parliamentary-standard-allowance-payments-to-deputies-for-1-8-november-2024_en.pdf"
payments_oct_2024           = f"{payment_url}/2024/2024-12-16_parliamentary-standard-allowance-payments-to-deputies-for-october-2024_en.pdf"
payments_sep_2024           = f"{payment_url}/2024/2024-11-01_parliamentary-standard-allowance-payments-to-deputies-for-september-2024_en.pdf"
payments_aug_2024           = f"{payment_url}/2024/2024-10-11_parliamentary-standard-allowance-payments-to-deputies-for-august-2024_en.pdf"
payments_july_2024          = f"{payment_url}/2024/2024-09-04_parliamentary-standard-allowance-payments-to-deputies-for-july-2024_en.pdf"
payments_june_2024          = f"{payment_url}/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
payments_may_2024           = f"{payment_url}/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
payments_april_2024         = f"{payment_url}/2024/2024-06-02_parliamentary-standard-allowance-payments-to-deputies-for-april-2024_en.pdf"
payments_march_2024         = f"{payment_url}/2024/2024-05-02_parliamentary-standard-allowance-payments-to-deputies-for-march-2024_en.pdf"
payments_feb_2024           = f"{payment_url}/2024/2024-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2024_en.pdf"
payments_jan_2024           = f"{payment_url}/2024/2024-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2024_en.pdf"
payments_dec_2023           = f"{payment_url}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-december-2023_en.pdf"
payments_nov_2023           = f"{payment_url}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-november-2023_en.pdf"
payments_oct_2023           = f"{payment_url}/2023/2023-12-01_parliamentary-standard-allowance-payments-to-deputies-for-october-2023_en.pdf"
payments_sep_2023           = f"{payment_url}/2023/2023-10-27_parliamentary-standard-allowance-payments-to-deputies-for-september-2023_en.pdf"
payments_aug_2023           = f"{payment_url}/2023/2023-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2023_en.pdf"
payments_july_2023          = f"{payment_url}/2023/2023-09-01_parliamentary-standard-allowance-payments-to-deputies-for-july-2023_en.pdf"
payments_june_2023          = f"{payment_url}/2023/2023-08-01_parliamentary-standard-allowance-payments-to-deputies-for-june-2023_en.pdf"
payments_may_2023           = f"{payment_url}/2023/2023-07-01_parliamentary-standard-allowance-payments-to-deputies-for-may-2023_en.pdf"

other_url = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other"
payments_april_2023         = f"{other_url}/2023/2023-06-01_parliamentary-standard-allowance-payments-to-deputies-for-april-2023_en.pdf"
payments_march_2023         = f"{other_url}/2023/2023-05-01_parliamentary-standard-allowance-payments-to-deputies-for-march-2023_en.pdf"
payments_feb_2023           = f"{other_url}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2023_en.pdf"
payments_jan_2023           = f"{other_url}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2023_en.pdf"
payments_dec_2022           = f"{other_url}/2023/2023-03-13_parliamentary-standard-allowance-payments-to-deputies-for-december-2022_en.pdf"
payments_nov_2022           = f"{other_url}/2023/2023-01-03_parliamentary-standard-allowance-payments-to-deputies-for-november-2022_en.pdf"
payments_oct_2022           = f"{payment_url}/2022/2022-12-06_parliamentary-standard-allowance-payments-to-deputies-for-october-2022_en.pdf"
payments_sep_2022           = f"{payment_url}/2022/2022-11-14_parliamentary-standard-allowance-payments-to-deputies-for-september-2022_en.pdf"
payments_aug_2022           = f"{payment_url}/2022/2022-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2022_en.pdf"
payments_july_2022          = f"{payment_url}/2022/2022-09-20_parliamentary-standard-allowance-payments-to-deputies-for-july-2022_en.pdf"
payments_june_2022          = f"{payment_url}/2022/2022-08-02_parliamentary-standard-allowance-payments-to-deputies-for-june-2022_en.pdf"
payments_may_2022           = f"{payment_url}/2022/2022-07-11_parliamentary-standard-allowance-payments-to-deputies-for-may-2022_en.pdf"
payments_april_2022         = f"{payment_url}/2022/2022-06-07_parliamentary-standard-allowance-payments-to-deputies-for-april-2022_en.pdf"
payments_march_2022         = f"{payment_url}/2022/2022-05-25_parliamentary-standard-allowance-payments-to-deputies-for-march-2022_en.pdf"
payments_feb_2022           = f"{payment_url}/2022/2022-04-19_parliamentary-standard-allowance-payments-to-deputies-for-february-2022_en.pdf"
payments_jan_2022           = f"{payment_url}/2022/2022-03-16_parliamentary-standard-allowance-payments-to-deputies-for-january-2022_en.pdf"
payments_dec_2021           = f"{payment_url}/2022/2022-03-10_parliamentary-standard-allowance-payments-to-deputies-for-december-2021_en.pdf"
payments_nov_2021           = f"{payment_url}/2022/2022-01-13_parliamentary-standard-allowance-payments-to-deputies-for-november-2021_en.pdf"
payments_oct_2021           = f"{payment_url}/2021/2021-12-22_parliamentary-standard-allowance-payments-to-deputies-for-october-2021_en.pdf"
payments_sep_2021           = f"{payment_url}/2021/2021-12-10_parliamentary-standard-allowance-payments-to-deputies-for-september-2021_en.pdf"
payments_aug_2021           = f"{payment_url}/2021/2021-10-12_parliamentary-standard-allowance-payments-to-deputies-for-august-2021_en.pdf"
payments_july_2021          = f"{payment_url}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-july-2021_en.pdf"
payments_june_2021          = f"{payment_url}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-june-2021_en.pdf"
payments_may_2021           = f"{payment_url}/2021/2021-07-12_parliamentary-standard-allowance-payments-to-deputies-for-may-2021_en.pdf"
payments_april_2021         = f"{payment_url}/2021/2021-06-30_parliamentary-standard-allowance-payments-to-deputies-for-april-2021_en.pdf"
payments_march_2021         = f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-march-2021_en.pdf"
payments_feb_2021           = f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-february-2021_en.pdf"
payments_jan_2021           = f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-january-2021_en.pdf"
payments_dec_2020           = f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-december-2020_en.pdf"
payments_nov_2020           = f"{payment_url}/2021/2021-02-12_parliamentary-standard-allowance-payments-to-deputies-for-november-2020_en.pdf"
payments_oct_2020           = f"{payment_url}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-october-2020_en.pdf"
payments_sep_2020           = f"{payment_url}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-september-2020_en.pdf"
payments_aug_2020           = f"{payment_url}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-august-2020_en.pdf"
payments_july_2020          = f"{payment_url}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-july-2020_en.pdf"
payments_june_2020          = f"{payment_url}/2020/2020-08-06_parliamentary-standard-allowance-payments-to-deputies-for-june-2020_en.pdf"
payments_may_2020           = f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-may-2020_en.pdf"
payments_april_2020         = f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-april-2020_en.pdf"
payments_march_2020         = f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.pdf"
payments_feb_2020           = f"{payment_url}/2020/2020-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2020_en.pdf"
payments_jan_2020           = f"{payment_url}/2020/2020-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2020_en.pdf"

# Dail member interests PDFs
interests_url = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests"
dail_member_interests_2020  = f"{interests_url}/dail/2021/2021-02-25_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2021  = f"{interests_url}/dail/2022/2022-02-16_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2022  = f"{interests_url}/dail/2023/2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf"
dail_member_interests_2023  = f"{interests_url}/dail/2024/2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf"
dail_member_interests_2024  = f"{interests_url}/dail/2025/2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
dail_member_interests_2025  = f"{interests_url}/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"

# Seanad member interests PDFs
seanad_member_interests_2020 = f"{interests_url}/seanad/2021/2021-03-16_register-of-members-interests-seanad-eireann_en.pdf"
seanad_member_interests_2021 = f"{interests_url}/seanad/2022/2022-02-25_register-of-members-interests-seanad-eireann_en.pdf"
seanad_member_interests_2022 = f"{interests_url}/seanad/2023/2023-02-24_register-of-members-interests-seanad-eireann_en.pdf"
seanad_member_interests_2023 = f"{interests_url}/seanad/2024/2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf"
seanad_member_interests_2024 = f"{interests_url}/seanad/2025/2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf"
seanad_member_interests_2025 = f"{interests_url}/seanad/2026/2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf"

urls = [
    # Attendance
    pdf_2023, pdf_2024, pdf_2024_gap, pdf_2025_gap, pdf_2025, pdf_2026,
    # Payments 2026
    payment_feb_td_2026, payment_jan_td_2026,
    # Payments 2025
    payment_dec_td_2025, payment_nov_td_2025, payment_september_td_2025,
    payment_august_td_2025, payment_july_td_2025, payment_june_td_2025,
    payment_may_td_2025, payment_april_2025, payment_feb_2025, payment_jan_2025,
    # Payments 2024
    payment_dec_2024, payment_29_30_nov_2024, payments_1_8_nov_2024,
    payments_oct_2024, payments_sep_2024, payments_aug_2024, payments_july_2024,
    payments_june_2024, payments_may_2024, payments_april_2024, payments_march_2024,
    payments_feb_2024, payments_jan_2024,
    # Payments 2023
    payments_dec_2023, payments_nov_2023, payments_oct_2023, payments_sep_2023,
    payments_aug_2023, payments_july_2023, payments_june_2023, payments_may_2023,
    payments_april_2023, payments_march_2023, payments_feb_2023, payments_jan_2023,
    # Payments 2022
    payments_dec_2022, payments_nov_2022, payments_oct_2022, payments_sep_2022,
    payments_aug_2022, payments_july_2022, payments_june_2022, payments_may_2022,
    payments_april_2022, payments_march_2022, payments_feb_2022, payments_jan_2022,
    # Payments 2021
    payments_dec_2021, payments_nov_2021, payments_oct_2021, payments_sep_2021,
    payments_aug_2021, payments_july_2021, payments_june_2021, payments_may_2021,
    payments_april_2021, payments_march_2021, payments_feb_2021, payments_jan_2021,
    # Payments 2020
    payments_dec_2020, payments_nov_2020, payments_oct_2020, payments_sep_2020,
    payments_aug_2020, payments_july_2020, payments_june_2020, payments_may_2020,
    payments_april_2020, payments_march_2020, payments_feb_2020, payments_jan_2020,
    # Dail interests
    dail_member_interests_2020, dail_member_interests_2021, dail_member_interests_2022,
    dail_member_interests_2023, dail_member_interests_2024, dail_member_interests_2025,
    # Seanad interests
    seanad_member_interests_2020, seanad_member_interests_2021, seanad_member_interests_2022,
    seanad_member_interests_2023, seanad_member_interests_2024, seanad_member_interests_2025,
]

manual_endpoints = ['https://www.oireachtas.ie/en/foi/frequently-requested-information/', 'https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance', 'https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=parliamentary-allowances']

broken_urls = []
def endpoint_checker(urls, session=session):
    for url in urls:
        try:
            response =  session.head(url, timeout=10)
            if response.status_code == 200:
                print(f"{response.url} has content")
                print("Success - API is accessible.")
                print(f"{response.url} has content")
            else:
                print(f"Failure - API is accessible but PDF url is no longer working: {response.status_code}")
                print(f"Response content: {response.content}")
                print(f"The PDF URL {response.url} is no longer working. Please check the URL and try again.")
                broken_urls.append(url)
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            print(f"Failure - Unable to establish connection: {e}.")
            broken_urls.append(url)
        except Exception as e:
            print(f"Failure - Unknown error occurred: {e}. Unfortunately, this data is only available via manual PDF extraction.")
            [print(f"Manual endpoints are here: {endpoint}") for endpoint in manual_endpoints]
            broken_urls.append(url)
            logging.error(f"Error checking URLs {broken_urls}: {e}")
    return len(broken_urls) == 0

def return_endpoints(urls):
    return urls

if __name__ == "__main__":
    is_complete = endpoint_checker(urls, session=session)
    returned_urls = return_endpoints(urls)
    print(f"Endpoint check complete. All URLs are accessible and working correctly." if is_complete else f"Endpoint check complete. {len(broken_urls)} broken URL(s): {broken_urls}")
