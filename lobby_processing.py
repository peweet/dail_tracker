import csv
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

from config import GOLD_CSV_DIR, GOLD_DIR, GOLD_PARQUET_DIR, LOBBY_OUTPUT_DIR, LOBBY_PARQUET_DIR, LOBBYING_RAW_DIR, SILVER_PARQUET_DIR
from pipeline_sandbox.quarantine import quarantine
from utility.select_drop_rename_cols_mappings import lobbying_rename


SOURCE = "lobbying"

RULE_DUPLICATE_PRIMARY_KEY = "lobbying_duplicate_primary_key"
RULE_NIL_RETURN = "lobbying_nil_return"
RULE_COLLECTIVE_DPO = "lobbying_collective_dpo_filter"
RULE_EMPTY_DPO_NAME = "lobbying_empty_dpo_name"


def _make_run_id() -> str:
    """ISO timestamp + short uuid; matches pipeline_sandbox/quarantine.py shape."""
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    return f"{ts}-{uuid.uuid4().hex[:8]}"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOBBY_ORG_COLUMNS = [
    "lobby_issue_uri",
    "name",
    "address",
    "county",
    "country",
    "phone_number",
    "website",
    "main_activities_of_organisation",
    "person_responsible_name",
    "person_responsible_email",
    "person_responsible_telephone",
    "email",
    "company_registration_number",
    "company_registered_name",
    "company_registered_address",
    "charity_regulation_number",
    "chy_number",
]

# File-name prefixes in LOBBY_DIR/raw that are NOT raw lobbying-activity CSVs
ACTIVITY_CSV_EXCLUDES = (
    "Lobbying_ie_organisation_results",
    "cleaned_output",
    "cleaned",
)

# --- URL handling --------------------------------------------------------
# If you have a CSV with columns (primary_key, lobby_url), drop it here and
# it will be joined onto every return. If the file is absent, the template
# below is used instead.
URL_LOOKUP_PATH = LOBBYING_RAW_DIR / "lobby_urls.csv"

# Fallback URL template — used for any primary_key not found in the lookup,
# or for every row when the lookup CSV is absent. Adjust to whatever
# lobbying.ie's actual return URL pattern is if this doesn't match.
RETURN_URL_TEMPLATE = "https://www.lobbying.ie/return/{}"


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


def parse_line(line: str) -> list:
    """Manually parse one raw line from the messy lobbying.ie org CSV export.

    The raw CSV uses inconsistent quoting and has embedded commas, so standard
    CSV parsers misinterpret fields. This collapses the quote noise and splits
    on the field boundary '","'.
    """
    line = line.replace("\"'", "'")
    parts = line.strip().split('","')
    parts[0] = parts[0].lstrip('"')
    parts[-1] = parts[-1].rstrip('"')
    return parts


def sanitize_lobby_org_csv() -> None:
    """Read the raw lobby-org CSV, manually parse each line, write a cleaned CSV."""
    # pdf_files = glob(str(LOBBYING_RAW_DIR / "*.pdf"))
    raw_path = LOBBYING_RAW_DIR / "Lobbying_ie_organisation_results.csv"
    cleaned_path = LOBBYING_RAW_DIR / "cleaned.csv"

    with open(raw_path, encoding="utf-8") as f:
        raw_lines = f.readlines()

    rows = [parse_line(line) for line in raw_lines]

    with open(cleaned_path, "w", newline="", encoding="utf-8") as f:
        csv.writer(f, quoting=csv.QUOTE_ALL).writerows(rows)

    print("Lobby org CSV sanitized.")


def load_lobby_orgs() -> pl.DataFrame:
    """Sanitize then load the lobby organisations reference table."""
    sanitize_lobby_org_csv()
    lobby_org = pl.read_csv(
        LOBBYING_RAW_DIR / "cleaned.csv",
        has_header=False,
        infer_schema=True,
        skip_lines=1,
    )
    lobby_org.columns = LOBBY_ORG_COLUMNS
    lobby_org.write_csv(LOBBYING_RAW_DIR / "cleaned_output.csv")
    print("Lobby org reference table loaded.")
    return lobby_org


def stack_lobbying_csvs() -> pl.DataFrame:
    """Stack every raw lobbying-activity CSV in LOBBY_DIR/raw into one DataFrame."""
    frames = []
    for file in os.listdir(LOBBYING_RAW_DIR):
        if not file.endswith(".csv"):
            continue
        if any(file.startswith(prefix) for prefix in ACTIVITY_CSV_EXCLUDES):
            continue
        print(f"Processing file: {file}")
        df = pl.read_csv(LOBBYING_RAW_DIR / file)
        df = df.rename(lobbying_rename)
        print(f"  rows: {df.height}")
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No raw lobbying CSVs found in {LOBBYING_RAW_DIR}")

    lobbying_df = pl.concat(frames, how="diagonal")
    print(f"Total rows after stacking: {lobbying_df.height}")
    return lobbying_df


# ---------------------------------------------------------------------------
# URL attachment
# ---------------------------------------------------------------------------


def attach_lobby_urls(df: pl.DataFrame) -> pl.DataFrame:
    """Add a lobby_url column to any DataFrame that has primary_key.

    Prefers joining against URL_LOOKUP_PATH if the file exists; falls back to
    RETURN_URL_TEMPLATE for any primary_keys not matched by the lookup (or for
    every row if the lookup is absent). Called once on lobbying_df so the URL
    propagates through the whole explode chain.
    """
    template_url = pl.format(RETURN_URL_TEMPLATE, pl.col("primary_key"))

    if URL_LOOKUP_PATH.exists():
        lookup = pl.read_csv(URL_LOOKUP_PATH).select(["primary_key", "lobby_url"])
        df = df.join(lookup, on="primary_key", how="left")
        df = df.with_columns(
            pl.when(pl.col("lobby_url").is_null()).then(template_url).otherwise(pl.col("lobby_url")).alias("lobby_url")
        )
        return df

    df = df.with_columns(template_url.alias("lobby_url"))
    return df


# ---------------------------------------------------------------------------
# Transform: lobby orgs
# ---------------------------------------------------------------------------


def transform_lobby_orgs(lobby_org: pl.DataFrame) -> pl.DataFrame:
    """Select relevant columns and build the lobbying.ie hyperlink per org."""
    lobby_org = lobby_org.select(
        "lobby_issue_uri",
        "name",
        "website",
        "main_activities_of_organisation",
        "company_registration_number",
        "company_registered_name",
    )
    # Bug fix: original called .str.replace(" ", "-") twice, which only handles
    # the first two spaces. replace_all handles any number of spaces.
    lobby_org = lobby_org.with_columns(
        pl.col("name").str.to_lowercase().str.replace_all(" ", "-").alias("name_for_link")
    )
    lobby_org = lobby_org.with_columns(
        pl.format(
            "https://www.lobbying.ie/organisation/{}/{}",
            pl.col("lobby_issue_uri"),
            pl.col("name_for_link"),
        ).alias("lobby_org_link")
    )
    lobby_org = lobby_org.drop("name_for_link")
    lobby_org = lobby_org.select(
        "lobby_issue_uri",
        "name",
        "main_activities_of_organisation",
        "website",
        "company_registration_number",
        "company_registered_name",
        "lobby_org_link",
    )
    return lobby_org


# ---------------------------------------------------------------------------
# Transform: lobbying activity chain
# ---------------------------------------------------------------------------


def parse_lobbying_period(df: pl.DataFrame) -> pl.DataFrame:
    """Split the 'DD MMM, YYYY to DD MMM, YYYY' period string into typed start/end datetimes."""
    df = df.with_columns(pl.col("lobbying_period").str.split(" to ").alias("lobbying_period_dates"))
    df = df.with_columns(
        pl.col("lobbying_period_dates")
        .list.to_struct(fields=["lobbying_period_start_date", "lobbying_period_end_date"])
        .alias("lobbying_period_struct")
    )
    df = df.unnest("lobbying_period_struct")
    df = df.with_columns(
        pl.col("lobbying_period_start_date").str.to_date("%e %b, %Y").cast(pl.Datetime),
        pl.col("lobbying_period_end_date").str.to_date("%e %b, %Y").cast(pl.Datetime),
    )
    df = df.drop("lobbying_period", "lobbying_period_dates")
    return df


COLLECTIVE_DPO_NAMES = {
    "Dáil Éireann (all TDs)",
    "Seanad Éireann (all Senators)",
    "All Oireachtas members",
    "All TDs",
    "All Senators",
    "Members of Government",
}

TITLE_PREFIXES = (
    "Minister ",
    "An Taoiseach ",
    "Tánaiste ",
    "Senator ",
    "TD ",
    "Deputy ",
    "Dr ",
    "Dr. ",
    "Mr ",
    "Mr. ",
    "Ms ",
    "Ms. ",
    "Mrs ",
    "Mrs. ",
    "Prof ",
    "Prof. ",
    "Dep ",
)

TITLE_SUFFIXES = (
    ", TD",
    " TD",
    ", Senator",
)

# Known spelling/encoding variants that appear in source data mapped to canonical form
NAME_CANONICAL = {
    "michael martin": "Micheál Martin",
}


def clean_dpo_name(name: str) -> str:
    """Strip title prefixes/suffixes, whitespace/punctuation dirt, and canonicalise known variants."""
    name = name.strip().rstrip(",").strip()
    for prefix in TITLE_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix) :].strip()
            break
    for suffix in TITLE_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    canonical = NAME_CANONICAL.get(name.lower())
    if canonical:
        return canonical
    return name


def explode_politicians(df: pl.DataFrame, run_id: str | None = None) -> pl.DataFrame:
    """Explode dpo_lobbied ('::' separated, '|' delimited) into one row per politician.

    After explosion, cleans each name: strips whitespace/punctuation dirt, removes
    title prefixes (Minister, Senator, etc.), and drops collective non-person entries
    like 'Dáil Éireann (all TDs)'. Dropped rows are quarantined when run_id is given.
    """
    df = df.with_columns(pl.col("dpo_lobbied").str.split("::").alias("lobbyists"))
    df = df.explode("lobbyists")
    df = df.with_columns(pl.col("lobbyists").str.split("|").alias("parts"))
    df = df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    )
    df = df.drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")

    # Clean names
    df = df.with_columns(pl.col("full_name").map_elements(clean_dpo_name, return_dtype=pl.String))

    # Capture + drop collective/non-person entries
    collective_mask = pl.col("full_name").is_in(list(COLLECTIVE_DPO_NAMES))
    if run_id is not None:
        collective = df.filter(collective_mask)
        if not collective.is_empty():
            quarantine(
                collective, source=SOURCE, rule=RULE_COLLECTIVE_DPO,
                reason="full_name matched a collective sentinel (e.g. 'Dáil Éireann (all TDs)')",
                run_id=f"{run_id}_{RULE_COLLECTIVE_DPO}",
            )
            print(f"  Quarantined collective DPO rows: {collective.height}")
    df = df.filter(~collective_mask)

    # Capture + drop empty-name rows
    empty_mask = pl.col("full_name").str.len_chars() == 0
    if run_id is not None:
        empty = df.filter(empty_mask)
        if not empty.is_empty():
            quarantine(
                empty, source=SOURCE, rule=RULE_EMPTY_DPO_NAME,
                reason="full_name was empty after stripping titles/whitespace",
                run_id=f"{run_id}_{RULE_EMPTY_DPO_NAME}",
            )
            print(f"  Quarantined empty DPO-name rows: {empty.height}")
    df = df.filter(~empty_mask)

    return df


def explode_activities(df: pl.DataFrame) -> pl.DataFrame:
    """Explode lobbying_activities ('::' separated, '|' delimited) into one row per activity."""
    df = df.with_columns(pl.col("lobbying_activities").str.split("::").alias("activities_list"))
    df = df.explode("activities_list")
    df = df.with_columns(pl.col("activities_list").str.split("|").alias("activities_parts"))
    df = df.with_columns(
        pl.col("activities_parts").list.get(0).alias("action"),
        pl.col("activities_parts").list.get(1).alias("delivery"),
        pl.col("activities_parts").list.get(2).alias("members_targeted"),
        pl.col("date_published_timestamp")
        .str.to_datetime(format="%d/%m/%Y %H:%M")
        .alias("date_published_timestamp_dt"),
    )
    df = df.drop(
        "activities_list",
        "activities_parts",
        "lobbying_activities",
        "date_published_timestamp",
    )
    return df


def parse_clients(df: pl.DataFrame) -> pl.DataFrame:
    """Split the pipe-delimited clients field into named columns."""
    # If the clients column is entirely null, skip parsing to avoid creating a huge exploded table of nulls.
    if df.select(pl.col("clients").is_null().all()).item():
        return df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("client_name"),
            pl.lit(None).cast(pl.Utf8).alias("client_address"),
            pl.lit(None).cast(pl.Utf8).alias("email"),
            pl.lit(None).cast(pl.Utf8).alias("telephone"),
        )
    df = df.with_columns(pl.col("clients").str.split("|").alias("clients_list"))
    df = df.with_columns(
        pl.col("clients_list").list.get(0).alias("client_name"),
        pl.col("clients_list").list.get(1).alias("client_address"),
        pl.col("clients_list").list.get(2).alias("email"),
        pl.col("clients_list").list.get(3).alias("telephone"),
    )
    df = df.drop("clients_list", "clients")
    return df


def parse_current_or_former_dpos(df: pl.DataFrame) -> pl.DataFrame:
    """Split the pipe-delimited current_or_former_dpos field into named columns."""
    df = df.with_columns(pl.col("current_or_former_dpos").str.split("|").alias("current_or_former_dpos_list"))
    df = df.with_columns(
        pl.col("current_or_former_dpos_list").list.get(0).alias("current_or_former_dpos"),
        pl.col("current_or_former_dpos_list").list.get(1).alias("current_or_former_dpos_position"),
        pl.col("current_or_former_dpos_list").list.get(2).alias("current_or_former_dpos_chamber"),
    )
    df = df.drop("current_or_former_dpos_list")
    df = df.rename({"current_or_former_dpos": "dpos_or_former_dpos_who_carried_out_lobbying_name"})
    return df


def get_clients(df: pl.DataFrame) -> pl.DataFrame:
    # BROKEN — df.col() is not a valid Polars method (AttributeError at runtime).
    # This function is also a half-finished coordinator: it tries to decide whether
    # to call parse_current_or_former_dpos and parse_clients based on column values,
    # but the column guards are wrong and inconsistent with how the rest of the pipeline
    # calls those functions unconditionally in main().
    # REFACTOR TARGET: either delete this function and keep the explicit calls in main(),
    # or fix it as a proper coordinator using _require_col() guards.
    # See: doc/lobby_processing_refactor.md
    if df.col("dpos_or_former_dpos_who_carried_out_lobbying_name").is_not_null().any():
        df = parse_current_or_former_dpos(df)
    if df.col("was_this_lobbying_done_on_behalf_of_a_client") == "Yes":
        get_clients = df.select("clients").drop_nulls()
        if get_clients.height > 0:
            df = parse_clients(df)
    return df
def split_lobbyists(
    lobbying_df: pl.DataFrame,
    lobby_org: pl.DataFrame,
) -> pl.DataFrame:
    """Rank lobbyists by total lobby requests, enriched with org metadata."""
    per_return = lobbying_df.select(
        pl.col("primary_key"),
        pl.col("lobby_enterprise_uri"),
        pl.col("lobbyist_name"),
        pl.col("dpo_lobbied").str.split("::").list.len().alias("politicians_involved_count"),
    )
    request_counts = per_return.group_by("lobbyist_name").agg(pl.len().alias("lobby_requests_count"))
    org_meta = lobby_org.select(
        "name",
        "main_activities_of_organisation",
        "company_registration_number",
        "company_registered_name",
        "website",
        "lobby_org_link",
    )
    request_counts = request_counts.join(
        org_meta,
        left_on="lobbyist_name",
        right_on="name",
        how="inner",
    )
    # REMOVE: .sort() here is redundant — gold/top_lobbyist_organisations.parquet handles ranking
    # Keep: org metadata join produces lobby_count_details needed by _load_orgs() for sector/website/CRN
    return request_counts

def experimental_compute_members_targeted_reach(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: parse the 'members_targeted' band strings into a numeric reach midpoint.

    The field contains strings like '1-5', '11-20', '51-100', '100+'.  This converts
    each band to its approximate midpoint so returns can be ranked or averaged by reach.
    Results: per lobbyist and per policy_area, total and average estimated reach.
    """
    band_midpoints = {
        "1": 1,
        "1-5": 3,
        "6-10": 8,
        "11-20": 15,
        "21-50": 35,
        "51-100": 75,
        "100+": 125,
    }
    df = activities_df.with_columns(pl.col("members_targeted").str.strip_chars().alias("members_targeted_clean"))
    df = df.with_columns(
        pl.col("members_targeted_clean")
        .replace(band_midpoints, default=None)
        .cast(pl.Int32, strict=False)
        .alias("reach_estimate")
    )
    per_lobbyist = (
        df.filter(pl.col("reach_estimate").is_not_null())
        .group_by("lobbyist_name")
        .agg(
            pl.col("reach_estimate").sum().alias("total_reach_estimate"),
            pl.col("reach_estimate").mean().alias("avg_reach_per_return"),
            pl.col("primary_key").n_unique().alias("return_count"),
        )
        .sort("total_reach_estimate", descending=True)
    )
    return per_lobbyist


def experimental_compute_time_to_publish(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: days between lobbying_period_end_date and date_published_timestamp_dt.

    Late filers (large gap) may signal minimal-compliance behaviour. Only rows where
    both dates are present are included.  Grouped by lobbyist with median and max gap.
    """
    df = lobbying_df.select(
        "lobbyist_name",
        "primary_key",
        "lobbying_period_end_date",
        "date_published_timestamp",
    ).drop_nulls(subset=["lobbying_period_end_date", "date_published_timestamp"])

    df = df.with_columns(
        pl.col("date_published_timestamp")
        .str.to_datetime(format="%d/%m/%Y %H:%M", strict=False)
        .alias("date_published_dt")
    ).drop_nulls(subset=["date_published_dt"])

    df = df.with_columns(
        (pl.col("date_published_dt") - pl.col("lobbying_period_end_date")).dt.total_days().alias("days_to_publish")
    )
    df = df.filter(pl.col("days_to_publish") >= 0)

    summary = df.group_by("lobbyist_name").agg(
        pl.col("days_to_publish").median().alias("median_days_to_publish"),
        pl.col("days_to_publish").max().alias("max_days_to_publish"),
        pl.col("primary_key").n_unique().alias("returns_filed"),
    )
    summary = summary.sort("median_days_to_publish", descending=True)
    return summary


def experimental_compute_return_description_length(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: character length of specific_details and intended_results as a transparency proxy.

    Very short entries (< ~30 chars) signal low-effort or evasive returns.
    Returns per-return lengths plus a per-lobbyist average.
    """
    df = (
        lobbying_df.select(
            "lobbyist_name",
            "primary_key",
            "lobby_url",
            "specific_details",
            "intended_results",
            "lobbying_period_start_date",
        )
        .with_columns(
            pl.col("specific_details").fill_null("").str.len_chars().alias("specific_details_len"),
            pl.col("intended_results").fill_null("").str.len_chars().alias("intended_results_len"),
        )
        .with_columns((pl.col("specific_details_len") + pl.col("intended_results_len")).alias("total_desc_len"))
    )

    per_return = df.select(
        "lobbyist_name",
        "primary_key",
        "lobby_url",
        "lobbying_period_start_date",
        "specific_details_len",
        "intended_results_len",
        "total_desc_len",
    ).sort("total_desc_len")

    return per_return

def compute_policy_area_quarterly_trend(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: quarterly return volume broken down by public_policy_area.

    The aggregate quarterly trend exists; this adds the per-sector dimension so
    accelerating or decelerating lobbying campaigns become visible.
    """
    df = lobbying_df.with_columns(
        pl.col("lobbying_period_start_date").dt.year().alias("year"),
        pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
    ).with_columns(pl.format("{}-Q{}", pl.col("year"), pl.col("quarter")).alias("year_quarter"))
    df = df.group_by(["year_quarter", "public_policy_area"]).agg(
        pl.len().alias("return_count"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
    )
    df = df.sort(["public_policy_area", "year_quarter"])
    return df


# ---------------------------------------------------------------------------
# Fact tables
#
# Each summary in the section above tells you who ranks where; the tables here
# tell you *which specific lobby returns* are behind those numbers, with URLs
# to each one. In Streamlit you load the summary and the matching detail
# table, use the summary for a selectbox, and filter the detail for display.
# ---------------------------------------------------------------------------


def build_returns_master_fact_table(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """One row per lobby return with its URL and headline fields. Acts as the
    lookup anyone can join back to by primary_key.
    """
    df = lobbying_df.select(
        "primary_key",
        "lobby_url",
        "lobbyist_name",
        "relevant_matter",
        "public_policy_area",
        "specific_details",
        "intended_results",
        "person_primarily_responsible",
        "was_this_a_grassroots_campaign",
        "was_this_lobbying_done_on_behalf_of_a_client",
        "lobbying_period_start_date",
        "lobbying_period_end_date",
    )
    df = df.unique(subset=["primary_key"])
    df = df.sort("lobbying_period_start_date", descending=True)
    return df


def build_politician_returns_fact_table(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for most_lobbied_politicians: one row per (politician, return)."""
    df = activities_df.select(
        "full_name",
        "chamber",
        "position",
        "primary_key",
        "lobby_url",
        "lobbyist_name",
        "public_policy_area",
        "lobbying_period_start_date",
    )
    df = df.unique(subset=["full_name", "primary_key"])
    df = df.sort(
        ["full_name", "lobbying_period_start_date"],
        descending=[False, True],
    )
    return df


def build_lobbyist_returns_fact_table(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for most_prolific_lobbyists: every return each lobbyist filed."""
    df = lobbying_df.select(
        "lobbyist_name",
        "primary_key",
        "lobby_url",
        "public_policy_area",
        "relevant_matter",
        "lobbying_period_start_date",
    )
    df = df.unique(subset=["lobbyist_name", "primary_key"])
    df = df.sort(
        ["lobbyist_name", "lobbying_period_start_date"],
        descending=[False, True],
    )
    return df


def build_client_company_returns_fact_table(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for top_client_companies: which returns were filed on their behalf.

    Includes the lobbying firm that represented them (lobbyist_name), the policy
    areas they targeted (aggregated per return), and the politicians they targeted.
    """
    df = activities_df.filter(pl.col("client_name").is_not_null() & (pl.col("client_name") != ""))

    # Per (client, return): aggregate politicians and policy areas touched
    per_return = df.group_by(["client_name", "primary_key"]).agg(
        pl.col("lobbyist_name").first().alias("lobbying_firm"),
        pl.col("lobby_url").first().alias("lobby_url"),
        pl.col("lobbying_period_start_date").first().alias("lobbying_period_start_date"),
        pl.col("public_policy_area").first().alias("public_policy_area"),
        pl.col("full_name").unique().sort().str.join(", ").alias("politicians_targeted"),
        pl.col("full_name").n_unique().alias("politicians_count"),
    )

    # Per (client, return): all distinct policy areas as a joined string
    policy_per_return = (
        df.unique(subset=["client_name", "primary_key", "public_policy_area"])
        .group_by(["client_name", "primary_key"])
        .agg(
            pl.col("public_policy_area").unique().sort().str.join(" · ").alias("policy_areas"),
        )
    )

    per_return = per_return.join(policy_per_return, on=["client_name", "primary_key"], how="left")
    per_return = per_return.select(
        "client_name",
        "primary_key",
        "lobby_url",
        "lobbying_firm",
        "policy_areas",
        "politicians_targeted",
        "politicians_count",
        "lobbying_period_start_date",
    )
    per_return = per_return.sort(
        ["client_name", "lobbying_period_start_date"],
        descending=[False, True],
    )
    return per_return

def build_bilateral_returns_fact_table(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for bilateral_relationships: every return underlying each persistent org-politician pair.
    Only includes (lobbyist, politician) pairs that appear across more than one return.
    """
    pair_counts = (
        activities_df.unique(subset=["primary_key", "lobbyist_name", "full_name"])
        .group_by(["lobbyist_name", "full_name"])
        .agg(pl.col("primary_key").n_unique().alias("returns_in_relationship"))
        .filter(pl.col("returns_in_relationship") > 1)
    )
    df = activities_df.unique(subset=["primary_key", "lobbyist_name", "full_name"])
    df = df.join(
        pair_counts.select(["lobbyist_name", "full_name", "returns_in_relationship"]),
        on=["lobbyist_name", "full_name"],
        how="inner",
    )
    df = df.select(
        "lobbyist_name",
        "full_name",
        "chamber",
        "primary_key",
        "lobby_url",
        "public_policy_area",
        "lobbying_period_start_date",
        "returns_in_relationship",
    )
    df = df.sort(
        ["returns_in_relationship", "lobbyist_name", "lobbying_period_start_date"],
        descending=[True, False, True],
    )
    return df

def build_revolving_door_returns_fact_table(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for revolving_door_dpos: each return a named ex-DPO carried out lobbying on."""
    name_col = "dpos_or_former_dpos_who_carried_out_lobbying_name"
    df = activities_df.filter(pl.col(name_col).is_not_null() & (pl.col(name_col) != ""))
    # REFACTOR NOTE: this is the only build_* function that guards on column presence.
    # The others assume their columns exist. A _require_col() guard applied consistently
    # across all build_* functions would make the contracts visible and failures explicit.
    has_clients = "client_name" in df.columns and df.select(pl.col("client_name")).drop_nulls().height > 0
    if has_clients:
        df = df.with_columns(
            [
                pl.when(pl.col("client_name").is_not_null() & (pl.col("client_name") != ""))
                .then(pl.col("client_name"))
                .otherwise(pl.col("lobbyist_name"))
                .alias("display_client_name"),
                pl.when(pl.col("client_address").is_not_null() & (pl.col("client_address") != ""))
                .then(pl.col("client_address"))
                .otherwise(pl.lit(""))
                .alias("display_client_address"),
            ]
        )
    else:
        df = df.with_columns(
            [
                pl.col("lobbyist_name").alias("display_client_name"),
                pl.lit("").alias("display_client_address"),
            ]
        )
    df = df.select(
        name_col,
        "current_or_former_dpos_position",
        "current_or_former_dpos_chamber",
        "primary_key",
        "lobby_url",
        "lobbyist_name",
        "display_client_name",
        "display_client_address",
        "public_policy_area",
        "lobbying_period_start_date",
    )
    df = df.unique(subset=[name_col, "primary_key"])
    df = df.sort(
        [name_col, "lobbying_period_start_date"],
        descending=[False, True],
    )
    return df


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------


def save_output(df: pl.DataFrame, filename: str, overwrite: bool = True) -> None:
    """Write a DataFrame to silver/lobbying/ (CSV) and silver/lobbying/parquet/ (parquet)."""
    LOBBY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOBBY_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    path = LOBBY_OUTPUT_DIR / filename
    if not overwrite and path.exists():
        print(f"{filename} already exists, skipping.")
        return
    df.write_csv(path)
    df.write_parquet(LOBBY_PARQUET_DIR / path.with_suffix(".parquet").name)
    print(f"Saved {filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def save_gold_outputs(activities_df: pl.DataFrame, lobbying_df: pl.DataFrame) -> None:
    """Run sql_queries/*.sql against in-memory DuckDB and write results to gold/parquet/ and gold/csv/.

    Tables registered inside each SQL file:
      activities  — one row per return × politician × activity (post-explode, in-memory)
      returns     — one row per lobby return (deduped, nil-filtered, in-memory)
      + all *.parquet files found under silver/parquet/ and silver/lobbying/parquet/
      + all *.csv files found under data/gold/ (vote history, committee assignments, etc.)
    """
    con = duckdb.connect()

    # In-memory tables from this run take precedence — register first
    con.register("activities", activities_df.to_arrow())
    con.register("returns", lobbying_df.to_arrow())
    registered = {"activities", "returns"}

    # Auto-register silver Parquet files (attendance, payments, members, interests …)
    for p in sorted(SILVER_PARQUET_DIR.glob("*.parquet")):
        name = p.stem
        if name not in registered:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{p.as_posix()}')")
            registered.add(name)

    # Silver lobbying Parquet outputs written earlier this run
    for p in sorted(LOBBY_PARQUET_DIR.glob("*.parquet")):
        name = p.stem
        if name not in registered:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{p.as_posix()}')")
            registered.add(name)

    # Gold CSVs: vote history, committee assignments — not yet converted to Parquet
    for p in sorted(GOLD_DIR.glob("*.csv")):
        name = p.stem
        if name not in registered:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_csv_auto('{p.as_posix()}')")
            registered.add(name)

    GOLD_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    GOLD_CSV_DIR.mkdir(parents=True, exist_ok=True)

    sql_dir = Path(__file__).parent / "sql_queries"
    for sql_file in sorted(sql_dir.glob("*.sql")):
        name = sql_file.stem
        query = sql_file.read_text(encoding="utf-8")
        try:
            result = pl.from_arrow(con.execute(query).arrow())
            result.write_parquet(GOLD_PARQUET_DIR / f"{name}.parquet")
            result.write_csv(GOLD_CSV_DIR / f"{name}.csv")
            print(f"  Gold: {name} ({result.height} rows)")
        except Exception as e:
            print(f"  Gold: {name} FAILED — {e}")

    con.close()


def filter_nil_returns(df: pl.DataFrame, run_id: str | None = None) -> pl.DataFrame:
    """Drop Nil Return declarations — these record that no lobbying occurred.

    Registrants are required to file a Nil Return for each period when they have
    no lobbying activity. These are administrative obligations, not lobbying events,
    and should be excluded from any analysis of lobbying behaviour.

    Two patterns exist in the source data:

    1. Proper system nil returns: relevant_matter is null (lobbying.ie leaves the
       activity fields blank when a registrant correctly submits a nil).

    2. Workaround nil returns: registrants who encountered a system bug typed
       'NIL', 'NIL Return', or 'NIL RETURN' into specific_details manually.
       Matched via regex '^nil(\\s+return)?$' on specific_details.
    """
    nil_text_pattern = r"(?i)^nil(\s+return)?$"
    is_system_nil = pl.col("relevant_matter").is_null()
    is_text_nil = pl.col("specific_details").str.strip_chars().str.contains(nil_text_pattern)
    nil_mask = is_system_nil | is_text_nil
    before = df.height
    if run_id is not None:
        nil_rows = df.filter(nil_mask)
        if not nil_rows.is_empty():
            quarantine(
                nil_rows, source=SOURCE, rule=RULE_NIL_RETURN,
                reason="Nil Return declaration (relevant_matter null OR specific_details ~ '^nil( return)?$')",
                run_id=f"{run_id}_{RULE_NIL_RETURN}",
            )
    df = df.filter(~nil_mask)
    dropped = before - df.height
    if dropped:
        print(f"  Nil returns removed: {dropped} ({before}: {df.height})")
    return df


def main() -> None:
    run_id = _make_run_id()
    print(f"=== Lobbying pipeline starting === (run_id={run_id})")

    # 1. Ingest
    lobby_org_raw = load_lobby_orgs()
    lobbying_df = stack_lobbying_csvs()

    # 1b. Deduplicate returns — source CSVs from lobbying.ie often have overlapping
    #     date ranges, so the same primary_key can appear in multiple raw files.
    #     Drop duplicates on primary_key before any explosion so counts are not inflated.
    #     The non-first occurrences are quarantined so the overlap is visible.
    indexed = lobbying_df.with_row_index("__rn")
    keep_rn = indexed.group_by("primary_key").agg(pl.col("__rn").min().alias("__keep_rn"))
    indexed = indexed.join(keep_rn, on="primary_key", how="left")
    duplicates = indexed.filter(pl.col("__rn") != pl.col("__keep_rn")).drop(["__rn", "__keep_rn"])
    deduped = indexed.filter(pl.col("__rn") == pl.col("__keep_rn")).drop(["__rn", "__keep_rn"])
    if not duplicates.is_empty():
        quarantine(
            duplicates, source=SOURCE, rule=RULE_DUPLICATE_PRIMARY_KEY,
            reason="primary_key duplicated across stacked CSVs (overlapping export windows)",
            run_id=f"{run_id}_{RULE_DUPLICATE_PRIMARY_KEY}",
        )
        print(
            f"  Deduplication: removed {duplicates.height} duplicate returns "
            f"({lobbying_df.height} -> {deduped.height})"
        )
    lobbying_df = deduped

    # 1c. Drop Nil Return declarations (no lobbying took place — administrative filings only)
    lobbying_df = filter_nil_returns(lobbying_df, run_id=run_id)

    # 2. Parse return-level dates once, up front (both the activity chain and)
    lobbying_df = parse_lobbying_period(lobbying_df)

    # 3. Attach lobby_url to every return (propagates through every explode)
    lobbying_df = attach_lobby_urls(lobbying_df)

    # 4. Transform the lobby-org reference table
    lobby_org = transform_lobby_orgs(lobby_org_raw)

    # 5. Explode the activity chain: return → politician → activity → client → former-DPO
    activities_df = explode_politicians(lobbying_df, run_id=run_id)
    activities_df = explode_activities(activities_df)
    activities_df = parse_clients(activities_df)
    activities_df = parse_current_or_former_dpos(activities_df)

    # 6. Silver — persist the two base tables that SQL queries run against
    save_output(lobbying_df, "returns.csv")
    save_output(activities_df, "lobby_break_down_by_politician.csv", overwrite=False)

    # 7. Polars metrics — string parsing / transformation that belongs in Python, not SQL
    split_lobbyists_df = split_lobbyists(lobbying_df, lobby_org)
    # 8. Drill-down fact tables (Streamlit-friendly; grain is return-level, not summary)
    returns_master = build_returns_master_fact_table(lobbying_df)
    politician_returns = build_politician_returns_fact_table(activities_df)
    lobbyist_returns = build_lobbyist_returns_fact_table(lobbying_df)
    client_returns = build_client_company_returns_fact_table(activities_df)
    revolving_door_returns = build_revolving_door_returns_fact_table(activities_df)
    bilateral_returns = build_bilateral_returns_fact_table(activities_df)

    # 9. Save — Polars outputs (silver Parquet for SQL; CSV for human inspection/export only)
    save_output(split_lobbyists_df, "split_lobbyists.csv")
    # 9b. Save — drill-down fact tables (silver Parquet for SQL; CSV for human inspection/export only)
    save_output(returns_master, "returns_master.csv")
    save_output(politician_returns, "politician_returns_detail.csv")
    save_output(lobbyist_returns, "lobbyist_returns_detail.csv")
    save_output(client_returns, "client_company_returns_detail.csv")
    save_output(revolving_door_returns, "revolving_door_returns_detail.csv")
    save_output(bilateral_returns, "bilateral_returns_detail.csv")

    # 10. Gold layer — run sql_queries/*.sql against activities + returns, write to gold/
    print("=== Gold layer ===")
    save_gold_outputs(activities_df, lobbying_df)

    print("=== Lobbying pipeline complete ===")


if __name__ == "__main__":
    main()
