# config.py
from pathlib import Path

PROJECT_ROOT = Path(__file__)
print(f"Project root directory: {PROJECT_ROOT}")
DATA_DIR = PROJECT_ROOT / "data"
API_BASE = "https://api.oireachtas.ie/v1"
# PARTY_CODES = ["Social_Democrats", "Sinn_Féin", ...]
DATE_RANGE = ("2024-01-01", "2099-01-01")