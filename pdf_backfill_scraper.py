import calendar
import sys
import requests

from pipeline_sandbox.payment_pdf_url_probe import construct_candidates


#TODO add real user agent and contact info when deploying application
USER_AGENT = (
    "dail-tracker-bot/0.1 (+https://github.com/<owner>/dail-extractor; "
    "mailto:<contact>)"
)


DEFAULT_TIMEOUT = (10, 30)  # connect, read


MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december",
}

PSA_BASE = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"
# Observed alternate folder seen in `payment_nov_td_2025`. Used as fallback.
ALT_BASE = "https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul"

# Topic-filtered listing page used as discovery fallback.
PUBLICATIONS_INDEX = (
    "https://www.oireachtas.ie/en/publications/"
    "?topic[]=parliamentary-allowances&resultsPerPage=50"
)

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def head_check(session: requests.Session, url: str) -> int:
    """Return HTTP status code for a HEAD request. Conditional-GET headers omitted
    here because we only care about existence, not content-changed semantics.
    Production version (pipeline/sources/_http.py) should add If-Modified-Since.
    """
    try:
        resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        return resp.status_code
    except requests.RequestException:
        return False



def try_construction(
    data_year: int,
    data_month: int,
    session: requests.Session,
    max_attempts: int = 80,
) -> str | None:
    """First strategy: construct candidate URLs and HEAD-check each.

    `max_attempts` caps HEAD requests so a fully-missing PDF doesn't generate
    50+ upstream calls. Tier 1 alone is 10 candidates × 2 variants = 20.
    """
    attempts = 0
    for candidate in construct_candidates(data_year, data_month):
        if attempts >= max_attempts:
            break
        status = head_check(session, candidate.url)
        attempts += 1
        if status == 200:
            return candidate.url
        # Useful diagnostic; production should structured-log this.
        print(f"  [{status}] {candidate.url}")
    return None



# LAG_MIN_DAYS = 25
# LAG_MAX_DAYS = 60


if __name__ == "__main__":
    print('Starting payment PDF URL scraper...')