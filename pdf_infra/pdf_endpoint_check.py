import logging
import sys
from datetime import date

import requests

session = requests.Session()


# Attendance PDFs
payment_url = "https://data.oireachtas.ie/ie/oireachtas/members/recordAttendanceForTaa"
pdf_2023 = f"{payment_url}/2024/2024-02-01_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2023-to-31-december-2023_en.pdf"
pdf_2024 = f"{payment_url}/2025/2025-02-17_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2024-to-08-november-2024_en.pdf"
pdf_2024_gap = f"{payment_url}/2025/2025-02-28_deputies-verification-of-attendance-for-the-payment-of-taa-29-november-2024-to-31-december-2024_en.pdf"
pdf_2025_gap = f"{payment_url}/2025/2025-04-09_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2025-to-31-january-2025_en.pdf"
pdf_2025 = f"{payment_url}/2026/2026-02-16_deputies-verification-of-attendance-for-the-payment-of-taa-01-february-2025-to-30-december-2025_en.pdf"
pdf_2026 = f"{payment_url}/2026/2026-04-02_deputies-verification-of-attendance-for-the-payment-of-taa-01-january-2026-to-28-february-2026_en.pdf"

# Payment PDFs
payment_url = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"

payment_feb_td_2026 = (
    f"{payment_url}/2026/2026-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2026_en.pdf"
)
payment_jan_td_2026 = (
    f"{payment_url}/2026/2026-03-06_parliamentary-standard-allowance-payments-to-deputies-for-january-2026_en.pdf"
)
payment_dec_td_2025 = (
    f"{payment_url}/2026/2026-02-16_parliamentary-standard-allowance-payments-to-deputies-for-december-2025_en.pdf"
)
payment_nov_td_2025 = "https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul/2026/2026-01-16_parliamentary-standard-allowance-payments-to-deputies-for-november-2025_en.pdf"
payment_september_td_2025 = (
    f"{payment_url}/2025/2025-11-18_parliamentary-standard-allowance-payments-to-deputies-for-september-2025_en.pdf"
)
payment_august_td_2025 = (
    f"{payment_url}/2025/2025-10-06_parliamentary-standard-allowance-payments-to-deputies-for-august-2025_en.pdf"
)
payment_july_td_2025 = (
    f"{payment_url}/2025/2025-09-03_parliamentary-standard-allowance-payments-to-deputies-for-july-2025_en.pdf"
)
payment_june_td_2025 = (
    f"{payment_url}/2025/2025-08-15_parliamentary-standard-allowance-payments-to-deputies-for-june-2025_en.pdf"
)
payment_may_td_2025 = (
    f"{payment_url}/2025/2025-07-03_parliamentary-standard-allowance-payments-to-deputies-for-may-2025_en.pdf"
)
payment_april_2025 = (
    f"{payment_url}/2025/2025-06-10_parliamentary-standard-allowance-payments-to-deputies-for-april-2025_en.pdf"
)
payment_feb_2025 = (
    f"{payment_url}/2025/2025-04-22_parliamentary-standard-allowance-payments-to-deputies-for-february-2025_en.pdf"
)
payment_jan_2025 = (
    f"{payment_url}/2025/2025-04-09_parliamentary-standard-allowance-payments-to-deputies-for-january-2025_en.pdf"
)
payment_dec_2024 = (
    f"{payment_url}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-1-31-december-2024_en.pdf"
)
payment_29_30_nov_2024 = f"{payment_url}/2025/2025-02-28_parliamentary-standard-allowance-payments-to-deputies-for-29-30-november-2024_en.pdf"
payments_1_8_nov_2024 = (
    f"{payment_url}/2025/2025-02-17_parliamentary-standard-allowance-payments-to-deputies-for-1-8-november-2024_en.pdf"
)
payments_oct_2024 = (
    f"{payment_url}/2024/2024-12-16_parliamentary-standard-allowance-payments-to-deputies-for-october-2024_en.pdf"
)
payments_sep_2024 = (
    f"{payment_url}/2024/2024-11-01_parliamentary-standard-allowance-payments-to-deputies-for-september-2024_en.pdf"
)
payments_aug_2024 = (
    f"{payment_url}/2024/2024-10-11_parliamentary-standard-allowance-payments-to-deputies-for-august-2024_en.pdf"
)
payments_july_2024 = (
    f"{payment_url}/2024/2024-09-04_parliamentary-standard-allowance-payments-to-deputies-for-july-2024_en.pdf"
)
payments_june_2024 = (
    f"{payment_url}/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
)
payments_may_2024 = (
    f"{payment_url}/2024/2024-07-29_parliamentary-standard-allowance-payments-to-deputies-for-june-2024_en.pdf"
)
payments_april_2024 = (
    f"{payment_url}/2024/2024-06-02_parliamentary-standard-allowance-payments-to-deputies-for-april-2024_en.pdf"
)
payments_march_2024 = (
    f"{payment_url}/2024/2024-05-02_parliamentary-standard-allowance-payments-to-deputies-for-march-2024_en.pdf"
)
payments_feb_2024 = (
    f"{payment_url}/2024/2024-04-02_parliamentary-standard-allowance-payments-to-deputies-for-february-2024_en.pdf"
)
payments_jan_2024 = (
    f"{payment_url}/2024/2024-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2024_en.pdf"
)
payments_dec_2023 = (
    f"{payment_url}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-december-2023_en.pdf"
)
payments_nov_2023 = (
    f"{payment_url}/2024/2024-02-01_parliamentary-standard-allowance-payments-to-deputies-for-november-2023_en.pdf"
)
payments_oct_2023 = (
    f"{payment_url}/2023/2023-12-01_parliamentary-standard-allowance-payments-to-deputies-for-october-2023_en.pdf"
)
payments_sep_2023 = (
    f"{payment_url}/2023/2023-10-27_parliamentary-standard-allowance-payments-to-deputies-for-september-2023_en.pdf"
)
payments_aug_2023 = (
    f"{payment_url}/2023/2023-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2023_en.pdf"
)
payments_july_2023 = (
    f"{payment_url}/2023/2023-09-01_parliamentary-standard-allowance-payments-to-deputies-for-july-2023_en.pdf"
)
payments_june_2023 = (
    f"{payment_url}/2023/2023-08-01_parliamentary-standard-allowance-payments-to-deputies-for-june-2023_en.pdf"
)
payments_may_2023 = (
    f"{payment_url}/2023/2023-07-01_parliamentary-standard-allowance-payments-to-deputies-for-may-2023_en.pdf"
)

other_url = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/other"
payments_april_2023 = (
    f"{other_url}/2023/2023-06-01_parliamentary-standard-allowance-payments-to-deputies-for-april-2023_en.pdf"
)
payments_march_2023 = (
    f"{other_url}/2023/2023-05-01_parliamentary-standard-allowance-payments-to-deputies-for-march-2023_en.pdf"
)
payments_feb_2023 = (
    f"{other_url}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2023_en.pdf"
)
payments_jan_2023 = (
    f"{other_url}/2023/2023-04-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2023_en.pdf"
)
payments_dec_2022 = (
    f"{other_url}/2023/2023-03-13_parliamentary-standard-allowance-payments-to-deputies-for-december-2022_en.pdf"
)
payments_nov_2022 = (
    f"{other_url}/2023/2023-01-03_parliamentary-standard-allowance-payments-to-deputies-for-november-2022_en.pdf"
)
payments_oct_2022 = (
    f"{payment_url}/2022/2022-12-06_parliamentary-standard-allowance-payments-to-deputies-for-october-2022_en.pdf"
)
payments_sep_2022 = (
    f"{payment_url}/2022/2022-11-14_parliamentary-standard-allowance-payments-to-deputies-for-september-2022_en.pdf"
)
payments_aug_2022 = (
    f"{payment_url}/2022/2022-10-01_parliamentary-standard-allowance-payments-to-deputies-for-august-2022_en.pdf"
)
payments_july_2022 = (
    f"{payment_url}/2022/2022-09-20_parliamentary-standard-allowance-payments-to-deputies-for-july-2022_en.pdf"
)
payments_june_2022 = (
    f"{payment_url}/2022/2022-08-02_parliamentary-standard-allowance-payments-to-deputies-for-june-2022_en.pdf"
)
payments_may_2022 = (
    f"{payment_url}/2022/2022-07-11_parliamentary-standard-allowance-payments-to-deputies-for-may-2022_en.pdf"
)
payments_april_2022 = (
    f"{payment_url}/2022/2022-06-07_parliamentary-standard-allowance-payments-to-deputies-for-april-2022_en.pdf"
)
payments_march_2022 = (
    f"{payment_url}/2022/2022-05-25_parliamentary-standard-allowance-payments-to-deputies-for-march-2022_en.pdf"
)
payments_feb_2022 = (
    f"{payment_url}/2022/2022-04-19_parliamentary-standard-allowance-payments-to-deputies-for-february-2022_en.pdf"
)
payments_jan_2022 = (
    f"{payment_url}/2022/2022-03-16_parliamentary-standard-allowance-payments-to-deputies-for-january-2022_en.pdf"
)
payments_dec_2021 = (
    f"{payment_url}/2022/2022-03-10_parliamentary-standard-allowance-payments-to-deputies-for-december-2021_en.pdf"
)
payments_nov_2021 = (
    f"{payment_url}/2022/2022-01-13_parliamentary-standard-allowance-payments-to-deputies-for-november-2021_en.pdf"
)
payments_oct_2021 = (
    f"{payment_url}/2021/2021-12-22_parliamentary-standard-allowance-payments-to-deputies-for-october-2021_en.pdf"
)
payments_sep_2021 = (
    f"{payment_url}/2021/2021-12-10_parliamentary-standard-allowance-payments-to-deputies-for-september-2021_en.pdf"
)
payments_aug_2021 = (
    f"{payment_url}/2021/2021-10-12_parliamentary-standard-allowance-payments-to-deputies-for-august-2021_en.pdf"
)
payments_july_2021 = (
    f"{payment_url}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-july-2021_en.pdf"
)
payments_june_2021 = (
    f"{payment_url}/2021/2021-09-09_parliamentary-standard-allowance-payments-to-deputies-for-june-2021_en.pdf"
)
payments_may_2021 = (
    f"{payment_url}/2021/2021-07-12_parliamentary-standard-allowance-payments-to-deputies-for-may-2021_en.pdf"
)
payments_april_2021 = (
    f"{payment_url}/2021/2021-06-30_parliamentary-standard-allowance-payments-to-deputies-for-april-2021_en.pdf"
)
payments_march_2021 = (
    f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-march-2021_en.pdf"
)
payments_feb_2021 = (
    f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-february-2021_en.pdf"
)
payments_jan_2021 = (
    f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-january-2021_en.pdf"
)
payments_dec_2020 = (
    f"{payment_url}/2021/2021-05-27_parliamentary-standard-allowance-payments-to-deputies-for-december-2020_en.pdf"
)
payments_nov_2020 = (
    f"{payment_url}/2021/2021-02-12_parliamentary-standard-allowance-payments-to-deputies-for-november-2020_en.pdf"
)
payments_oct_2020 = (
    f"{payment_url}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-october-2020_en.pdf"
)
payments_sep_2020 = (
    f"{payment_url}/2020/2020-12-03_parliamentary-standard-allowance-payments-to-deputies-for-september-2020_en.pdf"
)
payments_aug_2020 = (
    f"{payment_url}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-august-2020_en.pdf"
)
payments_july_2020 = (
    f"{payment_url}/2020/2020-10-21_parliamentary-standard-allowance-payments-to-deputies-for-july-2020_en.pdf"
)
payments_june_2020 = (
    f"{payment_url}/2020/2020-08-06_parliamentary-standard-allowance-payments-to-deputies-for-june-2020_en.pdf"
)
payments_may_2020 = (
    f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-may-2020_en.pdf"
)
payments_april_2020 = (
    f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-april-2020_en.pdf"
)
payments_march_2020 = (
    f"{payment_url}/2020/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.pdf"
)
payments_feb_2020 = (
    f"{payment_url}/2020/2020-04-01_parliamentary-standard-allowance-payments-to-deputies-for-february-2020_en.pdf"
)
payments_jan_2020 = (
    f"{payment_url}/2020/2020-03-01_parliamentary-standard-allowance-payments-to-deputies-for-january-2020_en.pdf"
)

# Dail member interests PDFs
interests_url = "https://data.oireachtas.ie/ie/oireachtas/members/registerOfMembersInterests"
dail_member_interests_2020 = f"{interests_url}/dail/2021/2021-02-25_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2021 = f"{interests_url}/dail/2022/2022-02-16_register-of-members-interests-dail-eireann_en.pdf"
dail_member_interests_2022 = (
    f"{interests_url}/dail/2023/2023-02-22_register-of-member-s-interests-dail-eireann-2022_en.pdf"
)
dail_member_interests_2023 = (
    f"{interests_url}/dail/2024/2024-02-21_register-of-member-s-interests-dail-eireann-2023_en.pdf"
)
dail_member_interests_2024 = (
    f"{interests_url}/dail/2025/2025-02-27_register-of-member-s-interests-dail-eireann-2024_en.pdf"
)
dail_member_interests_2025 = (
    f"{interests_url}/dail/2026/2026-02-25_register-of-member-s-interests-dail-eireann-2025_en.pdf"
)

# Seanad member interests PDFs
seanad_member_interests_2020 = (
    f"{interests_url}/seanad/2021/2021-03-16_register-of-members-interests-seanad-eireann_en.pdf"
)
seanad_member_interests_2021 = (
    f"{interests_url}/seanad/2022/2022-02-25_register-of-members-interests-seanad-eireann_en.pdf"
)
seanad_member_interests_2022 = (
    f"{interests_url}/seanad/2023/2023-02-24_register-of-members-interests-seanad-eireann_en.pdf"
)
seanad_member_interests_2023 = (
    f"{interests_url}/seanad/2024/2024-02-27_register-of-members-interests-seanad-eireann-2023_en.pdf"
)
seanad_member_interests_2024 = (
    f"{interests_url}/seanad/2025/2025-02-27_register-of-member-s-interests-seanad-eireann-2024_en.pdf"
)
seanad_member_interests_2025 = (
    f"{interests_url}/seanad/2026/2026-03-10_register-of-member-s-interests-seanad-eireann-2025_en.pdf"
)

urls = [
    # Attendance
    pdf_2023,
    pdf_2024,
    pdf_2024_gap,
    pdf_2025_gap,
    pdf_2025,
    pdf_2026,
    # Payments 2026
    payment_feb_td_2026,
    payment_jan_td_2026,
    # Payments 2025
    payment_dec_td_2025,
    payment_nov_td_2025,
    payment_september_td_2025,
    payment_august_td_2025,
    payment_july_td_2025,
    payment_june_td_2025,
    payment_may_td_2025,
    payment_april_2025,
    payment_feb_2025,
    payment_jan_2025,
    # Payments 2024
    payment_dec_2024,
    payment_29_30_nov_2024,
    payments_1_8_nov_2024,
    payments_oct_2024,
    payments_sep_2024,
    payments_aug_2024,
    payments_july_2024,
    payments_june_2024,
    payments_may_2024,
    payments_april_2024,
    payments_march_2024,
    payments_feb_2024,
    payments_jan_2024,
    # Payments 2023
    payments_dec_2023,
    payments_nov_2023,
    payments_oct_2023,
    payments_sep_2023,
    payments_aug_2023,
    payments_july_2023,
    payments_june_2023,
    payments_may_2023,
    payments_april_2023,
    payments_march_2023,
    payments_feb_2023,
    payments_jan_2023,
    # Payments 2022
    payments_dec_2022,
    payments_nov_2022,
    payments_oct_2022,
    payments_sep_2022,
    payments_aug_2022,
    payments_july_2022,
    payments_june_2022,
    payments_may_2022,
    payments_april_2022,
    payments_march_2022,
    payments_feb_2022,
    payments_jan_2022,
    # Payments 2021
    payments_dec_2021,
    payments_nov_2021,
    payments_oct_2021,
    payments_sep_2021,
    payments_aug_2021,
    payments_july_2021,
    payments_june_2021,
    payments_may_2021,
    payments_april_2021,
    payments_march_2021,
    payments_feb_2021,
    payments_jan_2021,
    # Payments 2020
    payments_dec_2020,
    payments_nov_2020,
    payments_oct_2020,
    payments_sep_2020,
    payments_aug_2020,
    payments_july_2020,
    payments_june_2020,
    payments_may_2020,
    payments_april_2020,
    payments_march_2020,
    payments_feb_2020,
    payments_jan_2020,
    # Dail interests
    dail_member_interests_2020,
    dail_member_interests_2021,
    dail_member_interests_2022,
    dail_member_interests_2023,
    dail_member_interests_2024,
    dail_member_interests_2025,
    # Seanad interests
    seanad_member_interests_2020,
    seanad_member_interests_2021,
    seanad_member_interests_2022,
    seanad_member_interests_2023,
    seanad_member_interests_2024,
    seanad_member_interests_2025,
]

manual_endpoints = [
    "https://www.oireachtas.ie/en/foi/frequently-requested-information/",
    "https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=record-of-attendance",
    "https://www.oireachtas.ie/en/publications/?q=&topic%5B%5D=parliamentary-allowances",
]


def endpoint_checker(manual_urls=manual_endpoints, urls=urls, session=session, timeout=10):

    broken = []
    validated_endpoints = False
    for url in urls:
        try:
            head = session.head(url, timeout=timeout, allow_redirects=True)
            head.raise_for_status()

            content_type = (head.headers.get("Content-Type") or "").lower()
            content_length = head.headers.get("Content-Length", "unknown")
            last_modified = head.headers.get("Last-Modified", "unknown")
            final_url = head.url

            print(f"[OK] {url}")
            print(f"     Final URL: {final_url}")
            print(f"     Status: {head.status_code}")
            print(f"     Content-Type: {content_type or 'unknown'}")
            print(f"     Content-Length: {content_length}")
            print(f"     Last-Modified: {last_modified}")

            logging.info(
                "OK | requested=%s | final=%s | status=%s | content_type=%s | content_length=%s | last_modified=%s",
                url,
                final_url,
                head.status_code,
                content_type or "unknown",
                content_length,
                last_modified,
            )

        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", "unknown")
            print(f"[BROKEN] {url}")
            print(f"         HTTP error: {status}")
            print("         Action: go to the source portal, find the new PDF URL, and update constants.py")
            broken.append(url)
            logging.error("HTTPError | url=%s | status=%s | error=%s", url, status, e)

        except requests.exceptions.ConnectionError as e:
            print(f"[BROKEN] {url}")
            print(f"         Connection error: {e}")
            print("         Action: check connectivity first, then verify the portal manually")
            broken.append(url)
            logging.error("ConnectionError | url=%s | error=%s", url, e)

        except requests.exceptions.Timeout as e:
            print(f"[BROKEN] {url}")
            print(f"         Timeout: {e}")
            print("         Action: retry later; if repeated, verify the source portal manually")
            broken.append(url)
            logging.error("Timeout | url=%s | error=%s", url, e)

        except requests.exceptions.RequestException as e:
            print(f"[BROKEN] {url}")
            print(f"         Request failed: {e}")
            print("         Action: verify the source portal and update constants.py if the PDF moved")
            broken.append(url)
            logging.error("RequestException | url=%s | error=%s", url, e)

        except Exception as e:
            print(f"[BROKEN] {url}")
            print(f"         Unknown error: {e}")
            print("         Action: manual check required")
            broken.append(url)
            logging.exception("Unknown error while checking url=%s", url)
    if not broken:
        validated_endpoints = True
    return broken, validated_endpoints


# ── API endpoint canaries ─────────────────────────────────────────────────────
# The PDF checks above HEAD-validate fixed file URLs. The feed/API sources can't
# be HEAD-checked (POST-only / query-parameterised), so each gets a *canary*: a
# minimal VALID request that proves the endpoint is up AND returns the expected
# shape. Kept self-contained here (endpoints inline, like the PDF URLs above) so
# the nightly endpoint-health job validates every source from one place. Each
# canary returns {ok, detail, http_status, rows}; pass a session for offline tests.
_CANARY_UA = "dail-tracker-bot/0.1 (endpoint-canary; +https://github.com/peweet/dail_tracker)"
CANARY_TIMEOUT = 60


def _canary(ok: bool, detail: str, status=None, rows=None) -> dict:
    return {"ok": ok, "detail": detail, "http_status": status, "rows": rows}


def canary_oireachtas_api(session=None) -> dict:
    """Oireachtas API: 1-row legislation GET -> JSON with `head` + `results` list."""
    s = session or requests
    try:
        r = s.get(
            "https://api.oireachtas.ie/v1/legislation",
            params={"limit": 1},
            headers={"Accept": "application/json"},
            timeout=CANARY_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 — a transport error IS the health signal
        return _canary(False, f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return _canary(False, f"HTTP {r.status_code}", r.status_code)
    body = r.json()
    results = body.get("results")
    if "head" not in body or not isinstance(results, list):
        return _canary(False, "missing head/results", 200)
    return _canary(True, "API OK", 200, len(results))


def canary_lobbying(session=None) -> dict:
    """lobbying.ie ExportReturns CSV: 1-row, 1-day GET (the full filter-param
    schema is required or the API 404s) -> CSV whose header carries the columns."""
    s = session or requests
    stamp = date.today().strftime("%d-%m-%Y")
    params = {
        "currentPage": 0,
        "pageSize": 1,
        "queryText": "",
        "subjectMatters": "",
        "subjectMatterAreas": "",
        "publicBodys": "",
        "jobTitles": "",
        "returnDateFrom": stamp,
        "returnDateTo": stamp,
        "period": "",
        "dpo": "",
        "client": "",
        "responsible": "",
        "lobbyist": "",
        "lobbyistId": "",
    }
    try:
        r = s.get(
            "https://api.lobbying.ie/api/ExportReturns/Csv",
            headers={"User-Agent": _CANARY_UA},
            params=params,
            timeout=CANARY_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 — a transport error IS the health signal
        return _canary(False, f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return _canary(False, f"HTTP {r.status_code}", r.status_code)
    text = r.content.lstrip(b"\xef\xbb\xbf").decode("utf-8", errors="replace")
    lines = text.splitlines()
    header = {c.strip() for c in lines[0].split(",")} if lines else set()
    needed = {"Id", "Lobbyist Name", "Date Published", "Period", "Relevant Matter"}
    if needed - header:
        return _canary(False, f"CSV missing columns: {sorted(needed - header)}", 200)
    return _canary(True, "CSV header valid", 200, max(0, len(lines) - 1))


def canary_ted(session=None) -> dict:
    """TED search API: POST a 1-row Ireland-CAN query -> JSON with a `notices` list."""
    s = session or requests
    body = {
        "query": "buyer-country=IRL AND notice-type=can-standard AND publication-date>=20240101",
        "fields": ["publication-number"],
        "limit": 1,
        "page": 1,
        "paginationMode": "PAGE_NUMBER",
    }
    try:
        r = s.post(
            "https://api.ted.europa.eu/v3/notices/search",
            json=body,
            headers={"User-Agent": _CANARY_UA, "Accept": "application/json"},
            timeout=CANARY_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 — a transport error IS the health signal
        return _canary(False, f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return _canary(False, f"HTTP {r.status_code}: {r.text[:120]}", r.status_code)
    notices = r.json().get("notices")
    if not isinstance(notices, list):
        return _canary(False, "no 'notices' list", 200)
    return _canary(True, "search OK", 200, len(notices))


def canary_etenders(session=None) -> dict:
    """eTenders open data: data.gov.ie CKAN `package_show` -> non-empty resources
    list (also surfaces resource-URL drift of the direct CSV download)."""
    s = session or requests
    try:
        r = s.get(
            "https://data.gov.ie/api/3/action/package_show",
            params={"id": "contract-notices-published-on-etenders"},
            headers={"User-Agent": _CANARY_UA},
            timeout=CANARY_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 — a transport error IS the health signal
        return _canary(False, f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return _canary(False, f"HTTP {r.status_code}", r.status_code)
    body = r.json()
    resources = (body.get("result") or {}).get("resources") or []
    if not body.get("success") or not resources:
        return _canary(False, "CKAN package_show: no resources", 200, 0)
    return _canary(True, f"{len(resources)} CKAN resources", 200, len(resources))


def canary_wikidata(session=None) -> dict:
    """Wikidata SPARQL: trivial LIMIT 1 query -> JSON with a `results.bindings` list."""
    s = session or requests
    params = {"query": "SELECT ?x WHERE { ?x wdt:P31 wd:Q5 } LIMIT 1", "format": "json"}
    try:
        r = s.get(
            "https://query.wikidata.org/sparql",
            params=params,
            headers={"User-Agent": _CANARY_UA, "Accept": "application/sparql-results+json"},
            timeout=CANARY_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001 — a transport error IS the health signal
        return _canary(False, f"{type(e).__name__}: {e}")
    if r.status_code != 200:
        return _canary(False, f"HTTP {r.status_code}", r.status_code)
    bindings = (r.json().get("results") or {}).get("bindings")
    if not isinstance(bindings, list):
        return _canary(False, "no results.bindings", 200)
    return _canary(True, "SPARQL OK", 200, len(bindings))


# Each feed/API source -> its canary. The endpoint URLs are intentionally inline
# (same convention as the PDF URLs above); keep them in sync with the owning
# extractor's config (lobbying_poller.ENDPOINT, ted_ireland_extract.URL, etc.).
API_CANARIES = [
    ("oireachtas_api", canary_oireachtas_api),
    ("lobbying", canary_lobbying),
    ("ted", canary_ted),
    ("etenders", canary_etenders),
    ("wikidata", canary_wikidata),
]


def run_api_canaries(session=None) -> list[tuple[str, dict]]:
    """Run every API canary, print one line each, and return [(name, result)]."""
    results: list[tuple[str, dict]] = []
    for name, fn in API_CANARIES:
        result = fn(session)
        rows = result.get("rows")
        suffix = f" (rows={rows})" if rows is not None else ""
        print(f"[{'OK' if result['ok'] else 'BROKEN'}] api:{name} — {result['detail']}{suffix}")
        logging.info("canary %s: ok=%s %s", name, result["ok"], result["detail"])
        results.append((name, result))
    return results


if __name__ == "__main__":
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("endpoint_check")

    session = requests.Session()
    broken, _ = endpoint_checker(urls=urls, session=session)

    print("\n=== API endpoint canaries ===")
    canary_results = run_api_canaries(session)
    failed_canaries = [name for name, result in canary_results if not result["ok"]]

    if not broken and not failed_canaries:
        print("\nEndpoint check complete. All PDF URLs and API canaries are healthy.")
    else:
        print("\nEndpoint check complete.")
        if broken:
            print(f"Broken PDF URLs: {len(broken)}")
            for url in broken:
                print(f" - {url}")
        if failed_canaries:
            print(f"Failed API canaries: {', '.join(failed_canaries)}")
        print("See logs/standalone/endpoint_check.log for full details.")

    # Non-zero exit so CI (nightly endpoint-health workflow) can detect link rot
    # OR a feed/API endpoint going down / changing shape.
    sys.exit(1 if (broken or failed_canaries) else 0)
