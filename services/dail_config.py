from pathlib import Path

from requests.help import main

# Project root = folder containing config.py
PROJECT_ROOT = Path(__file__).resolve().parent

DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"

LEGISLATION_DIR = BRONZE_DIR / "legislation"
QUESTIONS_DIR = BRONZE_DIR / "questions"
VOTES_DIR = BRONZE_DIR / "votes"
MEMBERS_DIR = BRONZE_DIR / "members"

LOG_DIR = PROJECT_ROOT / "logs"

API_BASE = "https://api.oireachtas.ie/v1"

# Ensure directories exist
for path in [
    DATA_DIR,
    BRONZE_DIR,
    LEGISLATION_DIR,
    QUESTIONS_DIR,
    VOTES_DIR,
    MEMBERS_DIR,
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