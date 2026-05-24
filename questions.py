"""
questions.py — flatten bronze parliamentary questions to silver.

Reads the per-member questions JSON written by services/oireachtas_api_main.py
and produces a flat per-question silver table.

One row per (question, asker). Joint-asker questions appear once per asker;
downstream views may dedupe on `question_ref` if a single-row-per-question
shape is needed.
"""

import pandas as pd

from config import BRONZE_DIR, SILVER_DIR

QUESTIONS_DIR = BRONZE_DIR / "questions"

# Walk pages -> per-question items (mirrors legislation.py's bills walk)
items = []
for results in pd.read_json(QUESTIONS_DIR / "questions_results.json")["results"]:
    items.extend(results)

questions_df = pd.json_normalize(items)

questions_rename = {
    "contextDate": "context_date",
    "question.by.memberCode": "unique_member_code",
    "question.by.showAs": "td_name",
    "question.by.uri": "member_uri",
    "question.to.showAs": "ministry",
    "question.to.roleCode": "ministry_role_code",
    "question.debateSection.showAs": "topic",
    "question.debateSection.debateSectionId": "debate_section_id",
    "question.debateSection.uri": "debate_section_uri",
    "question.questionType": "question_type",
    "question.questionNumber": "question_number",
    "question.date": "question_date",
    "question.uri": "uri",
    "question.showAs": "question_text",
    "question.house.showAs": "house",
    "question.house.houseNo": "house_no",
}

questions_df = questions_df.rename(columns=questions_rename)

# Reference number like [31202/26] from the body of question_text
questions_df["question_ref"] = questions_df["question_text"].str.extract(r"\[(\d{1,6}/\d{2,4})\]")

# Whitespace tidy on free-text columns (matches legislation.py:132)
questions_df = questions_df.replace(r"[\r\n]+", " ", regex=True).replace(r"\s{2,}", " ", regex=True)

questions_df["question_date"] = pd.to_datetime(questions_df["question_date"], errors="coerce")
questions_df["year"] = questions_df["question_date"].dt.year

questions_df = questions_df.sort_values(
    ["question_date", "question_number", "unique_member_code"], na_position="last", ascending=[False, False, True]
)

# Drop reconstructable URIs and verified-empty columns before write.
# Every URI here is an internal data.oireachtas.ie API endpoint, fully
# rebuildable from kept IDs (context_date, debate_section_id, question_number,
# unique_member_code, house). The four nullable columns are 100% empty in
# current data. Public per-question URLs are built by v_member_debate_sections.
QUESTIONS_DROP_COLS = [
    "debate_section_uri",
    "uri",
    "member_uri",
    "question.debateSection.formats.xml.uri",
    "question.house.uri",
    "question.debateSection.formats.pdf",
    "question.to.uri",
    "question.to.roleType",
    "ministry_role_code",
]
questions_df = questions_df.drop(columns=[c for c in QUESTIONS_DROP_COLS if c in questions_df.columns])

questions_df.to_csv(SILVER_DIR / "questions.csv", index=False)
questions_df.to_parquet(
    SILVER_DIR / "parquet" / "questions.parquet",
    index=False,
    compression="zstd",
    compression_level=3,
)
print("Questions dataset created successfully.")
