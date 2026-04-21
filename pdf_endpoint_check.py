import requests
import logging
# This script checks the accessibility of a list of PDF URLs related to TD attendance and payments data. It makes HTTP requests to each URL and verifies that the response status code is 200 (OK). If any URL is not accessible or returns an error, it prints an appropriate message. If all URLs are accessible and working correctly, it confirms that the endpoint check is complete.
#TODO make api calls to persist this into dedicated folders and files in the data directory, and then read from those files in the relevant services (e.g. payments.py, attendance.py, etc.) instead of hardcoding the URLs in those services. This way we can easily update the data by just updating the files in the data directory without having to change the code in multiple places.
payment_url  = "https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa"

pdf_2023     = f"{payment_url}/2024/2024-02-01_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2023-to-31-december-2023_en.pdf"
pdf_2024     = f"{payment_url}/2025/2025-02-17_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2024-to-08-november-2024_en.pdf"
pdf_2024_gap = f"{payment_url}/2025/2025-02-28_deputies-verification-of-attendance-for-the-payment-of-taa-29-november-2024-to-31-december-2024_en.pdf"
pdf_2025_gap = f"{payment_url}/2025/2025-04-09_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-31-january-2025_en.pdf"
pdf_2025     = f"{payment_url}/2026/2026-02-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-february-2025-to-30-december-2025_en.pdf"
pdf_2026     = f"{payment_url}/2026/2026-04-02_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-28-february-2026_en.pdf"

#Payment pdf reference URL
payment_url ="https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"

payment_feb_td_2026         = f"{payment_url}/2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"
payment_jan_td_2026         = f"{payment_url}/2026/2026-03-06_parliamentary-standard-allowance-payments-to-deputies-for-january-2026_en.pdf"
payment_dec_td_2025         = f"{payment_url}/2026/2026-02-16_parliamentary-standard-allowance-payments-to-deputies-for-december-2025_en.pdf"
payment_nov_td_2025         = f"https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul/2026/2026-01-16_parliamentary-standard-allowance-payments-to-deputies-for-november-2025_en.pdf"
payment_september_td_2025   = f"{payment_url}/2025/2025-11-18_parliamentary-standard-allowance-payments-to-deputies-for-september-2025_en.pdf"
payment_august_td_2025      = f"{payment_url}/2025/2025-10-06_parliamentary-standard-allowance-payments-to-deputies-for-august-2025_en.pdf"
payment_july_td_2025        = f"{payment_url}/2025/2025-09-03_parliamentary-standard-allowance-payments-to-deputies-for-july-2025_en.pdf"
payment_june_td_2025        = f"{payment_url}/2025/2025-08-15_parliamentary-standard-allowance-payments-to-deputies-for-june-2025_en.pdf"

#payment_may_td_2025         = f"{payment_url}/2025/2025-07-03_parliamentary-standard-allowance-payments-to-deputies-for-may-2025_en.pdf"
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

member_interests_2025 = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
#  payment_may_td_2025,

# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2016/2016-03-01_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2019/2019-02-13_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2018/2018-02-14_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2017/2017-03-10_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2016/2016-03-01_register-of-members-interests-dail-eireann_en.pdf
# https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2020/2020-03-03_register-of-members-interests-dail-eireann_en.pdf
member_interests_2022="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2023/2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf"
member_interests_2023="https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2024/2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf"
member_interests_2025 = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"

urls = [member_interests_2025,pdf_2023, pdf_2024, pdf_2024_gap, pdf_2025_gap, pdf_2025, pdf_2026, payment_feb_td_2026, payment_jan_td_2026, payment_dec_td_2025, payment_nov_td_2025, payment_september_td_2025, payment_august_td_2025, payment_july_td_2025, payment_june_td_2025, payment_april_2025, payment_feb_2025, payment_jan_2025, payment_dec_2024, payment_29_30_nov_2024, payments_1_8_nov_2024, payments_oct_2024, payments_sep_2024, payments_aug_2024, payments_july_2024, payments_june_2024, payments_may_2024, payments_april_2024, payments_march_2024, payments_feb_2024, payments_jan_2024]
manual_endpoints=['https://www.oireachtas.ie/en/foi/frequently-requested-information/', 'https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance', 'https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=parliamentary-allowances']

broken_urls = []
def endpoint_checker(urls : list) -> bool:
    for url in urls:
        try:
            response = requests.head(url, timeout=10)
            if response.status_code == 200:
                print(f"{response.url} has content")
                print("Success - API is accessible.")
                print(f"{response.url} has content")
                return True
            else:
                print(f"Failure - API is accessible but PDF url is no longer working: {response.status_code}")
                print(f"Response content: {response.content}")
                print(f"The PDF URL {response.url} is no longer working. Please check the URL and try again.")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            print(f"Failure - Unable to establish connection: {e}.")
            broken_urls.append(url)
        except Exception as e:
            print(f"Failure - Unknown error occurred: {e}. Unfortunately, this data is only available via manual PDF extraction.")
            [print(f"Manual endpoints are here: {endpoint}") for endpoint in manual_endpoints]
            broken_urls.append(url)
            logging.error(f"Error checking URLs {broken_urls}: {e}")
        return False
    
def return_endpoints(urls) -> list:
    return urls

# returned_urls = return_endpoints(urls)
if __name__ == "__main__":
    is_complete = endpoint_checker(urls)
    returned_urls = return_endpoints(urls)
    print(f"Endpoint check complete. All URLs {returned_urls} are accessible and working correctly." if is_complete else f"Endpoint check complete. Some URLs {broken_urls} are not accessible or not working correctly. Please review the error messages above for details.")