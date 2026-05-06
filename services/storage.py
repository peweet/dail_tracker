import json
import logging
from pathlib import Path

from services.dail_config import (
    BRONZE_DIR,
    DEBATES_LISTINGS_DIR,
    LEGISLATION_DIR,
    MEMBERS_DIR,
    QUESTIONS_DIR,
    VOTES_DIR,
)

logger = logging.getLogger(__name__)


def members_file_path() -> Path:
    return MEMBERS_DIR / "members.json"


def result_file_path(scenario: str) -> Path:
    if scenario == "legislation":
        return LEGISLATION_DIR / "legislation_results.json"
    if scenario == "questions":
        return QUESTIONS_DIR / "questions_results.json"
    if scenario == "votes":
        return VOTES_DIR / "votes_results.json"
    if scenario == "debates_listings":
        return DEBATES_LISTINGS_DIR / "debates_listings_results.json"
    if scenario == "legislation_unscoped":
       return LEGISLATION_DIR / "legislation_results_unscoped.json"
    return BRONZE_DIR / f"{scenario}_results.json"


def output_exists(path: Path, overwrite: bool = False) -> bool:
    if path.exists() and not overwrite:
        logger.info(f"Output already exists, skipping: {path}")
        return True
    return False


def save_json(data, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved JSON to: {path}")
    return path


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
if __name__ == "__main__":
    # Test saving and loading JSON
    test_data = {"test": "This is a test."}
    test_path = BRONZE_DIR / "test.json"
    save_json(test_data, test_path)
    loaded_data = load_json(test_path)
    assert loaded_data == test_data
    logger.info("JSON save/load test passed.")