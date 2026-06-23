import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

SRC = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "pipeline_sandbox/disclosed_po_spend/cross_body_suppliers.csv"

df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})

# --- clean entity (strip "Agency : " / "Section 38 : " prefixes) ---
def clean_entity(e):
    e = re.sub(r"^\s*(Agency|Section 38)\s*:\s*", "", e or "").strip()
    return e

# --- rough-normalise supplier names ---
LEGAL = [
    r"\bunlimited company\b", r"\bdesignated activity company\b", r"\bd\.?a\.?c\.?\b",
    r"\blimited\b", r"\bltd\b", r"\bteoranta\b", r"\bteo\b", r"\bplc\b",
    r"\bllp\b", r"\bclg\b", r"\bcompany\b", r"\bco\b", r"\binc\b", r"\bgmbh\b",
    r"\bb\.?v\.?\b", r"\bs\.?a\.?\b", r"\bag\b", r"\bnv\b", r"\bpvt\b",
    r"\b\(ireland\)\b", r"\bireland\b", r"\b\(irl\)\b",
    r"\bt/?a\b", r"\btrading as\b", r"\band company\b", r"\b& co\b",
]

def norm_supplier(s):
    if s is None:
        return ""
    x = s.lower().strip()
    # strip accents crudely
    x = (x.replace("é", "e").replace("á", "a").replace("í", "i")
          .replace("ó", "o").replace("ú", "u").replace("ç", "c"))
    x = x.replace("&", " and ")
    x = re.sub(r"[.,/()]", " ", x)
    x = re.sub(r"['\"`]", "", x)
    for pat in LEGAL:
        x = re.sub(pat, " ", x)
    x = re.sub(r"[^a-z0-9 ]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

df = df.with_columns(
    pl.col("entity").map_elements(clean_entity, return_dtype=pl.Utf8).alias("entity_clean"),
    pl.col("Supplier").map_elements(norm_supplier, return_dtype=pl.Utf8).alias("supplier_norm"),
)

df = df.filter(pl.col("supplier_norm").str.len_chars() > 1)

# --- per-body PO-blank fingerprint (semantics flag) ---
body_fp = (
    df.with_columns((pl.col("PO").fill_null("").str.strip_chars() == "").alias("po_blank"))
      .group_by("entity_clean")
      .agg(pl.col("po_blank").mean().alias("frac_blank_po"))
)
# >0.5 blank => PAYMENT list; else PURCHASE-ORDER commitments
body_fp = body_fp.with_columns(
    pl.when(pl.col("frac_blank_po") > 0.5).then(pl.lit("payment"))
      .otherwise(pl.lit("purchase_order")).alias("body_semantics")
)
df = df.join(body_fp.select(["entity_clean", "body_semantics"]), on="entity_clean", how="left")

# --- aggregate per normalised supplier ---
agg = (
    df.group_by("supplier_norm")
      .agg(
          pl.col("entity_clean").n_unique().alias("n_bodies"),
          pl.len().alias("n_rows"),
          pl.col("Total").sum().alias("gross_eur"),
          pl.col("entity_clean").unique().alias("bodies_list"),
          # which raw supplier strings collapsed into this norm (transparency)
          pl.col("Supplier").unique().alias("raw_variants"),
          pl.col("body_semantics").n_unique().alias("n_semantics_mix"),
      )
      .sort(["n_bodies", "gross_eur"], descending=[True, True])
)

# example bodies: take up to 6, sorted for stability
agg = agg.with_columns(
    pl.col("bodies_list").list.sort().list.head(6).list.join("; ").alias("example_bodies"),
    pl.col("bodies_list").list.len().alias("bodies_check"),
    pl.col("raw_variants").list.len().alias("n_raw_variants"),
)

# --- crude category tagging: utility/ubiquitous vs concentrated-commercial ---
UTILITY_PATTERNS = {
    "electricity/energy": r"\b(esb|electric ireland|bord gais|bord gas|gas networks|airtricity|sse|energia|flogas|prepaypower|pinergy)\b",
    "telecoms": r"\b(eir|eircom|vodafone|three|3 ?ireland|bt communications|virgin media|imagine|magnet|colt|verizon)\b",
    "post/courier": r"\b(an post|dpd|fastway|nightline|gls|fedex|dhl|ups )\b",
    "fuel/motor": r"\b(circle k|topaz|maxol|certa|emo oil|tedcastle|nrg|texaco|applegreen)\b",
    "bank/payments": r"\b(bank of ireland|aib|allied irish|ulster bank|permanent tsb|ptsb|revenue commissioners|elavon|worldpay|stripe|realex)\b",
    "water": r"\b(irish water|uisce eireann)\b",
    "office/facilities": r"\b(codex|lyreco|grosvenor|noonan|mitie|ocs |iss |sodexo|aramark|veolia)\b",
}
COMMERCIAL_PATTERNS = {
    "consultancy/advisory": r"\b(deloitte|kpmg|pwc|pricewaterhouse|ernst|\bey\b|grant thornton|mazars|mckinsey|accenture|bearingpoint|crowe|rsm|bdo)\b",
    "IT/tech": r"\b(microsoft|oracle|\bsap\b|dell|hewlett|\bhp\b|ibm|fujitsu|capgemini|version 1|ergo|datapac|micromail|pfh|sord data|softcat|dxc|wipro|infosys|tcs|cognizant|sungard|trilogy|codec|triangle|asystec|storm technology|auxilion|enovation|singlepoint|word perfect|fujitsu)\b",
    "legal": r"\b(arthur cox|mccann|matheson|mason hayes|byrne wallace|a and l goodbody|al goodbody|philip lee|eversheds|william fry|beauchamps|dac beachcroft|hayes solicitors|lk shields)\b",
    "recruitment/agency_staff": r"\b(cpl|sigmar|hays|grafton recruit|matrix recruit|cromwell|servisource|threeq|red recruit|recruitment|cordant|approach people)\b",
    "construction/engineering": r"\b(john sisk|bam |jons civil|roadbridge|wills bros|coffey|jons|ward and burke|murphy|sorensen|mott macdonald|arup|jacobs|rps|ayesa|atkins|aecom|tobin|byrne looby)\b",
    "facilities_mgmt/cleaning": r"\b(noonan|mitie|ocs|iss facility|sodexo|aramark|emerald|momentum|derrycourt)\b",
    "advertising/media": r"\b(mediavest|core media|javelin|publicis|mindshare|ebiquity|spark foundry)\b",
    "motor/vehicle_dealer": r"\b(henry ford|joe duffy|spirit ford|frank keane|windsor motors|kearys|johnson and perrott)\b",
}

# Over-generic / over-collapsed norm tokens that pool unrelated firms into one
# bogus cross-body node (e.g. bare "electric" merges electric ireland + electric
# skyline + dozens of "*electrical ltd" sole-trader contractors). Flag, don't trust.
BOGUS_NORM = {
    "electric", "electrical", "services", "consulting", "consultants", "solutions",
    "group", "systems", "technology", "technologies", "media", "supplies",
    "engineering", "construction", "communications", "associates", "partners",
}

def category(sn):
    for cat, pat in UTILITY_PATTERNS.items():
        if re.search(pat, sn):
            return ("utility/ubiquitous", cat)
    for cat, pat in COMMERCIAL_PATTERNS.items():
        if re.search(pat, sn):
            return ("concentrated_commercial", cat)
    return ("other/unclassified", "")

cats = [category(s) for s in agg["supplier_norm"].to_list()]
agg = agg.with_columns(
    pl.Series("supplier_class", [c[0] for c in cats]),
    pl.Series("category", [c[1] for c in cats]),
)
# flag over-collapsed/generic norm nodes (false cross-body links)
agg = agg.with_columns(
    pl.col("supplier_norm").is_in(list(BOGUS_NORM)).alias("over_collapsed_flag")
)

# semantics flag: does this supplier's gross mix payment + PO bodies?
agg = agg.with_columns(
    pl.when(pl.col("n_semantics_mix") > 1).then(pl.lit("MIXED_do_not_treat_as_spend"))
      .otherwise(pl.lit("single_regime")).alias("gross_caveat")
)

out = agg.select([
    "supplier_norm", "supplier_class", "category", "over_collapsed_flag",
    "n_bodies", "n_rows", "gross_eur", "gross_caveat", "n_raw_variants",
    "example_bodies",
])

out.write_csv(OUT)
print("wrote", OUT, "rows:", out.height)

# trustworthy ranking excludes over-collapsed bogus nodes
clean = out.filter(~pl.col("over_collapsed_flag"))

print("\n=== TOP 25 by n_bodies (over-collapsed nodes excluded) ===")
with pl.Config(tbl_rows=30, tbl_cols=-1, fmt_str_lengths=70, tbl_width_chars=240):
    print(clean.head(25))

print("\n=== TOP 15 CONCENTRATED-COMMERCIAL by n_bodies ===")
with pl.Config(tbl_rows=20, tbl_cols=-1, fmt_str_lengths=70, tbl_width_chars=240):
    print(clean.filter(pl.col("supplier_class") == "concentrated_commercial").head(15))

print("\n=== TOP 10 UTILITY by n_bodies ===")
with pl.Config(tbl_rows=12, tbl_cols=-1, fmt_str_lengths=60, tbl_width_chars=220):
    print(clean.filter(pl.col("supplier_class") == "utility/ubiquitous").head(10))

print("\n=== still unclassified in top 25 (manual review) ===")
with pl.Config(tbl_rows=15, fmt_str_lengths=50):
    print(clean.filter(pl.col("supplier_class") == "other/unclassified").head(12)
              .select(["supplier_norm","n_bodies","gross_eur","example_bodies"]))

# sanity: how many distinct bodies total
print("\nTotal distinct bodies:", df["entity_clean"].n_unique())
print("Total distinct normalised suppliers:", agg.height)
print("Body semantics split:", body_fp.group_by("body_semantics").len().to_dicts())
