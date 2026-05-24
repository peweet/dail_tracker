"""Diagnose: does name_join_key() actually equal unique_member_code for
NOTABLE_TDS? Phase 1 of the consolidation promised this bridge worked.
The Mary Lou screenshot says otherwise."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

from config import NOTABLE_TDS  # noqa: E402
from ui.entity_links import name_join_key  # noqa: E402
from data_access.member_overview_data import get_member_overview_conn  # noqa: E402

conn = get_member_overview_conn()
if conn is None:
    print("!! no conn")
    sys.exit(1)

# What codes exist in the registry?
reg = conn.execute(
    "SELECT unique_member_code, member_name FROM v_member_registry ORDER BY member_name"
).df()
print(f"v_member_registry: {len(reg)} rows")

# What's in the attendance year summary (the one _identity hits first)?
att = conn.execute(
    "SELECT DISTINCT unique_member_code, member_name FROM v_attendance_member_year_summary"
).df()
print(f"v_attendance_member_year_summary: {len(att)} unique codes\n")

print(f"{'NAME':<28} {'name_join_key()':<24} {'IN REGISTRY':<13} {'IN ATTENDANCE':<13} actual code(s)")
print("-" * 110)
for nm in NOTABLE_TDS:
    derived = name_join_key(nm)
    reg_hit = reg[reg["member_name"] == nm]
    att_hit = att[att["member_name"] == nm]
    reg_codes = reg_hit["unique_member_code"].tolist() if not reg_hit.empty else []
    att_codes = att_hit["unique_member_code"].tolist() if not att_hit.empty else []
    actual = reg_codes or att_codes or ["(not found)"]
    matches_derived = derived in actual
    flag = "OK" if matches_derived else "MISMATCH"
    print(
        f"{nm:<28} {derived:<24} {'yes' if reg_codes else 'no':<13} "
        f"{'yes' if att_codes else 'no':<13} {actual[0]} [{flag}]"
    )

# Show me a working code so I can re-run captures.
print("\n3 valid TDs with attendance data (for screenshots):")
sample = att.merge(reg, on=["unique_member_code", "member_name"], how="inner").head(10)
for _, row in sample.iterrows():
    print(f"  {row['member_name']:<30} {row['unique_member_code']}")
