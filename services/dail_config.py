from pathlib import Path

from requests.help import main

# Project root = folder containing config.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

LEGISLATION_DIR = BRONZE_DIR / "legislation"
QUESTIONS_DIR = BRONZE_DIR / "questions"
VOTES_DIR = BRONZE_DIR / "votes"
MEMBERS_DIR = BRONZE_DIR / "members"
DEBATES_DIR = BRONZE_DIR / "debates"
DEBATES_LISTINGS_DIR = DEBATES_DIR / "listings"
# Reserved for Stage 2 (AKN XML pool). Declared now so Stage 2 doesn't reshuffle config.
AKN_DIR = DEBATES_DIR / "akn"

LOG_DIR = PROJECT_ROOT / "logs"

API_BASE = "https://api.oireachtas.ie/v1"
VOTES_DATE_START = "2016-01-01"  # cutoff for paginated vote fetch

# Ensure directories exist
for path in [
    DATA_DIR,
    BRONZE_DIR,
    LEGISLATION_DIR,
    QUESTIONS_DIR,
    VOTES_DIR,
    MEMBERS_DIR,
    DEBATES_DIR,
    DEBATES_LISTINGS_DIR,
    AKN_DIR,
    LOG_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    print("Directories created:")
    print(f"  - {DATA_DIR}")
    print(f"  - {BRONZE_DIR}")
    print(f"  - {LEGISLATION_DIR}")
    print(f"  - {QUESTIONS_DIR}")
    print(f"  - {VOTES_DIR}")
    print(f"  - {MEMBERS_DIR}")
    print(f"  - {LOG_DIR}")