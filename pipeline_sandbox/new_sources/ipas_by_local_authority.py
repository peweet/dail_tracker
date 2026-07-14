"""Parse the IPAS weekly-stats PDF into a clean per-LOCAL-AUTHORITY IP-applicant
count — the real, groundable source for a clickable council choropleth (the C&AG
Fig 10.2 per-capita map is derived from this + LA population). SANDBOX ONLY.

Snapshot: 29 December 2024 (filename date). Value = IP applicants in State-provided
accommodation by contracting local-authority area.
"""
import re
import fitz
import polars as pl
from _common import BRONZE, SILVER, now_iso

PDF = BRONZE / "ipas_weekly" / "29122024-ipas-stats-weekly-report.pdf"
SNAPSHOT = "2024-12-29"
SRC_URL = "https://assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf"

# a line is an LA if it carries a county/city token (tolerant of all suffix forms:
# "Kerry County", "Carlow County Council", "Cork City", "Limerick City & County",
# "South Dublin County", "Dun Laoghaire-Rathdown", "Fingal County Council")
LA_CORE = ("carlow|cavan|clare|cork|donegal|dublin|laoghaire|fingal|galway|kerry|"
           "kildare|kilkenny|laois|leitrim|limerick|longford|louth|mayo|meath|"
           "monaghan|offaly|roscommon|sligo|tipperary|waterford|westmeath|wexford|wicklow")
LA_RX = re.compile(rf"(?i)^(?=.*\b(?:{LA_CORE})\b)(?=.*\b(?:county|city|laoghaire|fingal)\b)"
                   r"[A-Za-zÀ-ÿ&\-.' ]{4,45}$")
NUM_RX = re.compile(r"^[\d,]+$")

doc = fitz.open(PDF)
lines = [l.strip() for pg in doc for l in pg.get_text("text").splitlines() if l.strip()]

# pair each LA-name line with the next pure-integer line
rows, i = [], 0
grand_total = None
while i < len(lines):
    ln = lines[i]
    if NUM_RX.fullmatch(ln) and i > 0 and "grand total" in lines[i-1].lower():
        grand_total = int(ln.replace(",", ""))
    if LA_RX.fullmatch(ln) and "grand total" not in ln.lower():
        for j in range(i + 1, min(i + 3, len(lines))):
            if NUM_RX.fullmatch(lines[j]):
                rows.append({"local_authority_raw": ln,
                             "ip_applicants": int(lines[j].replace(",", ""))})
                i = j
                break
    i += 1

df = (pl.DataFrame(rows)
        .group_by("local_authority_raw").agg(pl.col("ip_applicants").max())  # dedupe
        .sort("ip_applicants", descending=True)
        .with_columns([
            pl.lit(SNAPSHOT).alias("snapshot_date"),
            pl.lit("IP applicants in State-provided accommodation, by contracting LA area").alias("metric"),
            pl.lit(SRC_URL).alias("source_url"),
            pl.lit("IPAS weekly accommodation & arrivals statistics").alias("source_name"),
            pl.lit(now_iso()).alias("fetched_at"),
            pl.lit("fitz_text_table_parse").alias("extraction_method"),
            pl.lit("public_aggregates").alias("privacy_tier"),
            pl.lit(False).alias("value_safe_to_sum"),  # a snapshot headcount, not a money/flow sum
        ]))

out = SILVER / "ipas_by_local_authority.parquet"
df.write_parquet(out, compression="zstd", statistics=True)
(SILVER / "_eyeball").mkdir(exist_ok=True)
df.write_csv(SILVER / "_eyeball" / "ipas_by_local_authority.csv")

total = df["ip_applicants"].sum()
print(f"wrote {out} - {df.height} LAs, total {total:,}"
      + (f" vs Grand Total {grand_total:,} ({'MATCH' if grand_total==total else 'DIFF '+str(grand_total-total)})"
         if grand_total else " (no grand total found)"))
with pl.Config(tbl_rows=40):
    print(df.select("local_authority_raw", "ip_applicants"))
