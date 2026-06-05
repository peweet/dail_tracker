"""probe_review_jud_privacy.py — READ-ONLY audit of the judiciary gold cases layer.

Checks, WITHOUT writing any case/party data anywhere, whether the published
gold cases parquet (judicial_legal_diary_cases.parquet):
  - carries only the contracted columns (no raw_case / no party text column);
  - actually anonymised natural persons (heuristic: any 'v' row whose two sides
    are NOT org-keyword bearing should be initials-only, e.g. 'A.B. v C.D.');
  - dropped statutory in-camera categories (no protected leakage);
  - whether the SANDBOX audit parquet (raw, un-anonymised) is git-ignored.

Prints only AGGREGATE statistics + a SMALL number of *already-anonymised*
sample strings so a reviewer can eyeball anonymisation quality. It deliberately
does NOT print anything from the raw sandbox audit parquet.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold" / "parquet" / "judicial_legal_diary_cases.parquet"
AUDIT = ROOT / "data" / "sandbox" / "parquet" / "judicial_legal_diary_audit.parquet"

CONTRACT_COLS = {"diary_date", "court", "judge", "list_type", "status", "category",
                 "case_anonymised", "source", "source_url", "source_sha256"}
FORBIDDEN_COLS = {"raw_case", "raw", "party", "parties", "solicitor"}

# org tokens copied from the extractor (kept-in-clear set)
ORG_KEYS = ["limited", " ltd", "d.a.c", " dac", " plc", "company", "bank", "insurance",
            "minister", "ireland", "attorney general", "commissioner", "council", "authority",
            "agency", "board", "revenue", " hse", "an garda", "designated activity",
            "university", "college", "credit union", "society", "fund", "holdings",
            "dpp", "director of public prosecutions", "people at the suit", "state"]
PROTECTED_KEYS = ["minor", "tusla", "care order", "ward of court", "wards of court",
                  "special care", "family law", "in camera", "guardian", "adoption",
                  "childcare", "asylum", "immigration", "citizenship"]

# a "name-looking" token = TitleCase word >=4 chars that is not an org keyword.
NAME_TOKEN = re.compile(r"\b[A-Z][a-z]{3,}\b")


def _is_org(side: str) -> bool:
    return any(k in side.lower() for k in ORG_KEYS)


def main() -> int:
    if not GOLD.exists():
        print(f"GOLD MISSING: {GOLD}")
        return 1
    df = pl.read_parquet(GOLD)
    cols = set(df.columns)
    print(f"rows={df.height}  cols={sorted(cols)}")
    print(f"contract_ok={cols == CONTRACT_COLS}  forbidden_present={sorted(cols & FORBIDDEN_COLS)}")

    # protected leakage in the published anonymised text
    blob = (df["case_anonymised"].fill_null("") + " " + df["list_type"].fill_null("")).str.to_lowercase()
    prot_hits = {k: int(blob.str.contains(re.escape(k), literal=False).sum()) for k in PROTECTED_KEYS}
    prot_hits = {k: v for k, v in prot_hits.items() if v}
    print(f"protected_keyword_hits_in_gold={prot_hits or 'NONE'}")

    # anonymisation quality: for each 'X v Y', check sides that are NOT org -> must look like initials.
    leak_examples = 0
    name_leak_rows = 0
    checked = 0
    for val in df["case_anonymised"].to_list():
        if not val or " v " not in val:
            continue
        checked += 1
        for side in val.split(" v ", 1):
            side = side.strip()
            if _is_org(side):
                continue
            # a non-org side should be initials (e.g. 'A.B.' or 'A.B. & Ors'), NOT a full name token
            if NAME_TOKEN.search(side):
                name_leak_rows += 1
                if leak_examples < 12:
                    # print only the side flagged — helps reviewer judge; still anonymised-layer text
                    print(f"  POSSIBLE NAME LEAK side=<{side!r}>  full=<{val!r}>")
                    leak_examples += 1
                break
    print(f"v_rows_checked={checked}  rows_with_nonorg_titlecase_token={name_leak_rows}")

    # category distribution
    print("category_counts=" + str(df["category"].value_counts().sort("count", descending=True).to_dicts()))

    # sandbox audit (raw) gitignore check
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8", errors="ignore")
    audit_ignored = "sandbox" in gi and ("data/sandbox" in gi or "sandbox/" in gi)
    print(f"audit_parquet_exists={AUDIT.exists()}  data_sandbox_in_gitignore={audit_ignored}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
