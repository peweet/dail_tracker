import csv
import os
import polars as pl
from config import LOBBY_DIR
from utility.select_drop_rename_cols_mappings import lobbying_rename


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOBBY_ORG_COLUMNS = [
    "lobby_issue_uri", "name", "address", "county", "country",
    "phone_number", "website", "main_activities_of_organisation",
    "person_responsible_name", "person_responsible_email",
    "person_responsible_telephone", "email", "company_registration_number",
    "company_registered_name", "company_registered_address",
    "charity_regulation_number", "chy_number",
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
URL_LOOKUP_PATH = LOBBY_DIR / 'raw' / 'lobby_urls.csv'

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
    line = line.replace('\"\'', '\'')
    parts = line.strip().split('","')
    parts[0] = parts[0].lstrip('"')
    parts[-1] = parts[-1].rstrip('"')
    return parts


def sanitize_lobby_org_csv() -> None:
    """Read the raw lobby-org CSV, manually parse each line, write a cleaned CSV."""
    raw_path = LOBBY_DIR / 'raw' / 'Lobbying_ie_organisation_results.csv'
    cleaned_path = LOBBY_DIR / 'raw' / 'cleaned.csv'

    with open(raw_path, 'r', encoding='utf-8') as f:
        raw_lines = f.readlines()

    rows = [parse_line(line) for line in raw_lines]

    with open(cleaned_path, 'w', newline='', encoding='utf-8') as f:
        csv.writer(f, quoting=csv.QUOTE_ALL).writerows(rows)

    print("Lobby org CSV sanitized.")


def load_lobby_orgs() -> pl.DataFrame:
    """Sanitize then load the lobby organisations reference table."""
    sanitize_lobby_org_csv()
    lobby_org = pl.read_csv(
        LOBBY_DIR / 'raw' / 'cleaned.csv',
        has_header=False,
        infer_schema=True,
        skip_lines=1,
    )
    lobby_org.columns = LOBBY_ORG_COLUMNS
    lobby_org.write_csv(LOBBY_DIR / 'raw' / 'cleaned_output.csv')
    print("Lobby org reference table loaded.")
    return lobby_org


def stack_lobbying_csvs() -> pl.DataFrame:
    """Stack every raw lobbying-activity CSV in LOBBY_DIR/raw into one DataFrame."""
    frames = []
    for file in os.listdir(LOBBY_DIR / 'raw'):
        if not file.endswith('.csv'):
            continue
        if any(file.startswith(prefix) for prefix in ACTIVITY_CSV_EXCLUDES):
            continue
        print(f"Processing file: {file}")
        df = pl.read_csv(LOBBY_DIR / 'raw' / file)
        df = df.rename(lobbying_rename)
        print(f"  rows: {df.height}")
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No raw lobbying CSVs found in {LOBBY_DIR / 'raw'}")

    lobbying_df = pl.concat(frames, how='diagonal')
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
            pl.when(pl.col("lobby_url").is_null())
            .then(template_url)
            .otherwise(pl.col("lobby_url"))
            .alias("lobby_url")
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
        pl.col("name")
        .str.to_lowercase()
        .str.replace_all(" ", "-")
        .alias("name_for_link")
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
    df = df.with_columns(
        pl.col("lobbying_period").str.split(" to ").alias("lobbying_period_dates")
    )
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


_COLLECTIVE_DPO_NAMES = {
    "Dáil Éireann (all TDs)",
    "Seanad Éireann (all Senators)",
    "All Oireachtas members",
    "All TDs",
    "All Senators",
    "Members of Government",
}

_TITLE_PREFIXES = (
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

_TITLE_SUFFIXES = (
    ", TD",
    " TD",
    ", Senator",
)

# Known spelling/encoding variants that appear in source data mapped to canonical form
_NAME_CANONICAL = {
    "michael martin": "Micheál Martin",
}


def _clean_dpo_name(name: str) -> str:
    """Strip title prefixes/suffixes, whitespace/punctuation dirt, and canonicalise known variants."""
    name = name.strip().rstrip(",").strip()
    for prefix in _TITLE_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
            break
    for suffix in _TITLE_SUFFIXES:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
            break
    canonical = _NAME_CANONICAL.get(name.lower())
    if canonical:
        return canonical
    return name


def explode_politicians(df: pl.DataFrame) -> pl.DataFrame:
    """Explode dpo_lobbied ('::' separated, '|' delimited) into one row per politician.

    After explosion, cleans each name: strips whitespace/punctuation dirt, removes
    title prefixes (Minister, Senator, etc.), and drops collective non-person entries
    like 'Dáil Éireann (all TDs)'.
    """
    df = df.with_columns(
        pl.col("dpo_lobbied").str.split("::").alias("lobbyists")
    )
    df = df.explode("lobbyists")
    df = df.with_columns(
        pl.col("lobbyists").str.split("|").alias("parts")
    )
    df = df.with_columns(
        pl.col("parts").list.get(0).alias("full_name"),
        pl.col("parts").list.get(1).alias("position"),
        pl.col("parts").list.get(2).alias("chamber"),
    )
    df = df.drop("lobbyists", "parts", "dpo_lobbied", "lobby_enterprise_uri")

    # Clean names
    df = df.with_columns(
        pl.col("full_name").map_elements(_clean_dpo_name, return_dtype=pl.String)
    )

    # Drop collective/non-person entries
    df = df.filter(~pl.col("full_name").is_in(list(_COLLECTIVE_DPO_NAMES)))
    df = df.filter(pl.col("full_name").str.len_chars() > 0)

    return df


def explode_activities(df: pl.DataFrame) -> pl.DataFrame:
    """Explode lobbying_activities ('::' separated, '|' delimited) into one row per activity."""
    df = df.with_columns(
        pl.col("lobbying_activities").str.split("::").alias("activities_list")
    )
    df = df.explode("activities_list")
    df = df.with_columns(
        pl.col("activities_list").str.split("|").alias("activities_parts")
    )
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
        return df
    df = df.with_columns(
        pl.col("clients").str.split("|").alias("clients_list")
    )
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
    df = df.with_columns(
        pl.col("current_or_former_dpos").str.split("|").alias("current_or_former_dpos_list")
    )
    df = df.with_columns(
        pl.col("current_or_former_dpos_list").list.get(0).alias("current_or_former_dpos"),
        pl.col("current_or_former_dpos_list").list.get(1).alias("current_or_former_dpos_position"),
        pl.col("current_or_former_dpos_list").list.get(2).alias("current_or_former_dpos_chamber"),
    )
    df = df.drop("current_or_former_dpos_list")
    df = df.rename(
        {"current_or_former_dpos": "dpos_or_former_dpos_who_carried_out_lobbying_name"}
    )
    return df

def get_clients(df: pl.DataFrame) -> pl.DataFrame:
    # Only attempt to parse clients if the column is present and has non-null values
    if df.col('dpos_or_former_dpos_who_carried_out_lobbying_name').is_not_null().any():
        df = parse_current_or_former_dpos(df)
    if df.col('was_this_lobbying_done_on_behalf_of_a_client') == "Yes":
        get_clients = df.select('clients').drop_nulls()
        if get_clients.height > 0:
            df = parse_clients(df)
    return df


# ---------------------------------------------------------------------------
# Analyse: core metrics
# ---------------------------------------------------------------------------

def compute_most_lobbied_politicians(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Rank politicians by distinct returns targeting them, broken down by chamber.

    Deduplicates on (primary_key, full_name) first so multiple activities within
    the same return count as one contact, not N.
    """
    df = activities_df.unique(subset=["primary_key", "full_name"])
    segmented = df.group_by(["full_name", "chamber"]).agg(
        pl.col("primary_key").n_unique().alias("lobby_returns_targeting"),
        pl.col("lobbyist_name").n_unique().alias("distinct_orgs"),
    )
    total = df.group_by("full_name").agg(
        pl.col("primary_key").n_unique().alias("total_returns")
    )
    most_lobbied = segmented.join(total, on="full_name").sort("total_returns", descending=True)
    return most_lobbied


def compute_most_prolific_lobbyists(
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
    request_counts = per_return.group_by("lobbyist_name").agg(
        pl.len().alias("lobby_requests_count")
    )
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
    most_prolific = per_return.join(request_counts, on="lobbyist_name").sort(
        ["politicians_involved_count", "lobby_requests_count"],
        descending=True,
    )
    return most_prolific


# ---------------------------------------------------------------------------
# Analyse: experimental metrics — verify before relying on them
# ---------------------------------------------------------------------------

def compute_policy_area_breakdown(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: lobby-return volume per public_policy_area with lobbyist diversity."""
    df = lobbying_df.group_by("public_policy_area").agg(
        pl.len().alias("return_count"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
    )
    df = df.sort("return_count", descending=True)
    return df


def compute_delivery_method_mix(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: breakdown of delivery channels (Meeting / Email / Phone call) per lobbyist."""
    df = activities_df.group_by(["lobbyist_name", "delivery"]).agg(
        pl.len().alias("delivery_count")
    )
    df = df.sort(["lobbyist_name", "delivery_count"], descending=[False, True])
    return df


def compute_politician_policy_exposure(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: per politician × policy_area — distinct returns targeting them and distinct lobbyists involved.

    Deduplicates on primary_key first so the activity-explosion does not inflate counts.
    """
    df = activities_df.unique(
        subset=["primary_key", "full_name", "chamber", "public_policy_area"]
    )
    df = df.group_by(["full_name", "chamber", "public_policy_area"]).agg(
        pl.len().alias("returns_targeting"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
    )
    df = df.sort(["full_name", "returns_targeting"], descending=[False, True])
    return df


def compute_grassroots_campaigns(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: lobbyists ranked by count of returns flagged as grassroots campaigns."""
    df = lobbying_df.filter(pl.col("was_this_a_grassroots_campaign") == "Yes")
    df = df.group_by("lobbyist_name").agg(
        pl.len().alias("grassroots_returns_count")
    )
    df = df.sort("grassroots_returns_count", descending=True)
    return df


def compute_quarterly_trend(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: return volume per calendar quarter (based on lobbying_period_start_date)."""
    df = lobbying_df.with_columns(
        pl.col("lobbying_period_start_date").dt.year().alias("year"),
        pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
    )
    df = df.with_columns(
        pl.format("{}-Q{}", pl.col("year"), pl.col("quarter")).alias("year_quarter")
    )
    df = df.group_by("year_quarter").agg(
        pl.len().alias("return_count"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
    )
    df = df.sort("year_quarter")
    return df


def compute_top_client_companies(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: companies most often appearing as the ultimate client behind
    lobbying (i.e. the ones paying third-party lobbying firms).

    Influence signals captured:
      - return_count:               how many distinct lobby returns name them as client
      - distinct_lobbyist_firms:    how many different lobbying firms they hire
      - distinct_politicians_targeted: reach across named DPOs
      - distinct_policy_areas:      breadth of policy exposure they're pushing on
      - distinct_chambers:          Dáil vs Seanad vs councils etc.
    """
    df = activities_df.filter(
        pl.col("client_name").is_not_null() & (pl.col("client_name") != "")
    )
    df = df.group_by("client_name").agg(
        pl.col("primary_key").n_unique().alias("return_count"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyist_firms"),
        pl.col("full_name").n_unique().alias("distinct_politicians_targeted"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
        pl.col("chamber").n_unique().alias("distinct_chambers"),
    )
    df = df.sort(
        ["return_count", "distinct_politicians_targeted"],
        descending=True,
    )
    return df


def compute_revolving_door_dpos(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: current or former Designated Public Officials who personally
    carried out lobbying (classic revolving-door signal — ex-government people
    now working on behalf of a lobbying org).

    Grouped by the individual and their prior position/chamber, with reach signals:
      - returns_involved_in:        distinct lobby returns they appear on
      - distinct_lobbyist_firms:    how many orgs they've lobbied for
      - distinct_policy_areas:      breadth of topics
      - distinct_politicians_targeted: reach across named DPOs
    """
    name_col = "dpos_or_former_dpos_who_carried_out_lobbying_name"
    df = activities_df.filter(
        pl.col(name_col).is_not_null() & (pl.col(name_col) != "")
    )
    df = df.group_by(
        [name_col, "current_or_former_dpos_position", "current_or_former_dpos_chamber"]
    ).agg(
        pl.col("primary_key").n_unique().alias("returns_involved_in"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyist_firms"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
        pl.col("full_name").n_unique().alias("distinct_politicians_targeted"),
    )
    df = df.sort("returns_involved_in", descending=True)
    return df


# ---------------------------------------------------------------------------
# Analyse: experimental metrics — new additions
# ---------------------------------------------------------------------------

def compute_distinct_orgs_per_politician(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: per-politician count of distinct lobbying organisations, not just return volume.

    A TD targeted 40 times by 3 orgs is a different signal from 40 times by 30 orgs.
    Deduplicates on primary_key first so activity-explosion does not inflate org counts.
    """
    df = activities_df.unique(subset=["primary_key", "full_name", "lobbyist_name"])
    df = df.group_by(["full_name", "chamber", "position"]).agg(
        pl.col("lobbyist_name").n_unique().alias("distinct_orgs"),
        pl.col("primary_key").n_unique().alias("distinct_returns"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
    )
    df = df.sort(["distinct_orgs", "distinct_returns"], descending=True)
    return df


def compute_members_targeted_reach(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: parse the 'members_targeted' band strings into a numeric reach midpoint.

    The field contains strings like '1-5', '11-20', '51-100', '100+'.  This converts
    each band to its approximate midpoint so returns can be ranked or averaged by reach.
    Results: per lobbyist and per policy_area, total and average estimated reach.
    """
    band_midpoints = {
        "1": 1, "1-5": 3, "6-10": 8, "11-20": 15,
        "21-50": 35, "51-100": 75, "100+": 125,
    }
    df = activities_df.with_columns(
        pl.col("members_targeted").str.strip_chars().alias("members_targeted_clean")
    )
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


def compute_time_to_publish(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: days between lobbying_period_end_date and date_published_timestamp_dt.

    Late filers (large gap) may signal minimal-compliance behaviour. Only rows where
    both dates are present are included.  Grouped by lobbyist with median and max gap.
    """
    df = lobbying_df.select(
        "lobbyist_name", "primary_key",
        "lobbying_period_end_date", "date_published_timestamp",
    ).drop_nulls(subset=["lobbying_period_end_date", "date_published_timestamp"])

    df = df.with_columns(
        pl.col("date_published_timestamp")
        .str.to_datetime(format="%d/%m/%Y %H:%M", strict=False)
        .alias("date_published_dt")
    ).drop_nulls(subset=["date_published_dt"])

    df = df.with_columns(
        (
            (pl.col("date_published_dt") - pl.col("lobbying_period_end_date"))
            .dt.total_days()
            .alias("days_to_publish")
        )
    )
    df = df.filter(pl.col("days_to_publish") >= 0)

    summary = df.group_by("lobbyist_name").agg(
        pl.col("days_to_publish").median().alias("median_days_to_publish"),
        pl.col("days_to_publish").max().alias("max_days_to_publish"),
        pl.col("primary_key").n_unique().alias("returns_filed"),
    )
    summary = summary.sort("median_days_to_publish", descending=True)
    return summary


def compute_return_description_length(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: character length of specific_details and intended_results as a transparency proxy.

    Very short entries (< ~30 chars) signal low-effort or evasive returns.
    Returns per-return lengths plus a per-lobbyist average.
    """
    df = lobbying_df.select(
        "lobbyist_name", "primary_key", "lobby_url",
        "specific_details", "intended_results",
        "lobbying_period_start_date",
    ).with_columns(
        pl.col("specific_details").fill_null("").str.len_chars().alias("specific_details_len"),
        pl.col("intended_results").fill_null("").str.len_chars().alias("intended_results_len"),
    ).with_columns(
        (pl.col("specific_details_len") + pl.col("intended_results_len")).alias("total_desc_len")
    )

    per_return = df.select(
        "lobbyist_name", "primary_key", "lobby_url", "lobbying_period_start_date",
        "specific_details_len", "intended_results_len", "total_desc_len",
    ).sort("total_desc_len")

    return per_return


def compute_lobbyist_persistence(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: filing history per lobbyist — active span, distinct periods filed, cadence.

    Orgs that file every single period vs one-off appearances carry different weight.
    """
    df = lobbying_df.group_by("lobbyist_name").agg(
        pl.col("lobbying_period_start_date").min().alias("first_return_date"),
        pl.col("lobbying_period_start_date").max().alias("last_return_date"),
        pl.col("primary_key").n_unique().alias("total_returns"),
        pl.struct(
            pl.col("lobbying_period_start_date").dt.year().alias("year"),
            pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
        ).n_unique().alias("distinct_periods_filed"),
    )
    df = df.with_columns(
        (
            (pl.col("last_return_date") - pl.col("first_return_date"))
            .dt.total_days()
            .alias("active_span_days")
        )
    )
    df = df.sort(["total_returns", "distinct_periods_filed"], descending=True)
    return df


def compute_bilateral_relationships(activities_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: persistent lobbying relationships — same org targeting same TD across multiple periods.

    A single contact is noise; repeated targeting across periods is an ongoing relationship.
    """
    df = activities_df.unique(subset=["primary_key", "lobbyist_name", "full_name"])
    df = df.group_by(["lobbyist_name", "full_name", "chamber"]).agg(
        pl.col("primary_key").n_unique().alias("returns_in_relationship"),
        pl.col("public_policy_area").n_unique().alias("distinct_policy_areas"),
        pl.struct(
            pl.col("lobbying_period_start_date").dt.year().alias("year"),
            pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
        ).n_unique().alias("distinct_periods"),
        pl.col("lobbying_period_start_date").min().alias("relationship_start"),
        pl.col("lobbying_period_start_date").max().alias("relationship_last_seen"),
    )
    df = df.filter(pl.col("returns_in_relationship") > 1)
    df = df.sort(["returns_in_relationship", "distinct_periods"], descending=True)
    return df


def compute_policy_area_quarterly_trend(lobbying_df: pl.DataFrame) -> pl.DataFrame:
    """experimental: quarterly return volume broken down by public_policy_area.

    The aggregate quarterly trend exists; this adds the per-sector dimension so
    accelerating or decelerating lobbying campaigns become visible.
    """
    df = lobbying_df.with_columns(
        pl.col("lobbying_period_start_date").dt.year().alias("year"),
        pl.col("lobbying_period_start_date").dt.quarter().alias("quarter"),
    ).with_columns(
        pl.format("{}-Q{}", pl.col("year"), pl.col("quarter")).alias("year_quarter")
    )
    df = df.group_by(["year_quarter", "public_policy_area"]).agg(
        pl.len().alias("return_count"),
        pl.col("lobbyist_name").n_unique().alias("distinct_lobbyists"),
    )
    df = df.sort(["public_policy_area", "year_quarter"])
    return df


# ---------------------------------------------------------------------------
# Drill-down tables (URL exports, Streamlit-friendly)
#
# Each summary in the section above tells you who ranks where; the tables here
# tell you *which specific lobby returns* are behind those numbers, with URLs
# to each one. In Streamlit you load the summary and the matching detail
# table, use the summary for a selectbox, and filter the detail for display.
# ---------------------------------------------------------------------------

def build_returns_master(lobbying_df: pl.DataFrame) -> pl.DataFrame:
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


def build_politician_returns_detail(activities_df: pl.DataFrame) -> pl.DataFrame:
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


def build_lobbyist_returns_detail(lobbying_df: pl.DataFrame) -> pl.DataFrame:
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


def build_client_company_returns_detail(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for top_client_companies: which returns were filed on their behalf.

    Includes the lobbying firm that represented them (lobbyist_name), the policy
    areas they targeted (aggregated per return), and the politicians they targeted.
    """
    df = activities_df.filter(
        pl.col("client_name").is_not_null() & (pl.col("client_name") != "")
    )

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


def build_bilateral_returns_detail(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for bilateral_relationships: every return underlying each persistent org-politician pair.

    Only includes (lobbyist, politician) pairs that appear across more than one return.
    """
    pair_counts = (
        activities_df
        .unique(subset=["primary_key", "lobbyist_name", "full_name"])
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


def build_revolving_door_returns_detail(activities_df: pl.DataFrame) -> pl.DataFrame:
    """Drill-down for revolving_door_dpos: each return a named ex-DPO carried out lobbying on."""
    name_col = "dpos_or_former_dpos_who_carried_out_lobbying_name"
    df = activities_df.filter(
        pl.col(name_col).is_not_null() & (pl.col(name_col) != "")
    )
    # Add display_client_name and display_client_address: use client fields if filled, else fallback to lobbyist_name/address
    has_clients = "client_name" in df.columns and df.select(pl.col("client_name")).drop_nulls().height > 0
    if has_clients:
        df = df.with_columns([
            pl.when(pl.col("client_name").is_not_null() & (pl.col("client_name") != ""))
            .then(pl.col("client_name"))
            .otherwise(pl.col("lobbyist_name"))
            .alias("display_client_name"),
            pl.when(pl.col("client_address").is_not_null() & (pl.col("client_address") != ""))
            .then(pl.col("client_address"))
            .otherwise(pl.lit("")).alias("display_client_address"),
        ])
    else:
        df = df.with_columns([
            pl.col("lobbyist_name").alias("display_client_name"),
            pl.lit("").alias("display_client_address"),
        ])
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
    """Write a DataFrame to LOBBY_DIR/output/. If overwrite=False, skip when the file already exists."""
    out_dir = LOBBY_DIR / 'output'
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    if not overwrite and path.exists():
        print(f"{filename} already exists, skipping.")
        return
    df.write_csv(path)
    print(f"Saved {filename}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def filter_nil_returns(df: pl.DataFrame) -> pl.DataFrame:
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
    before = df.height
    df = df.filter(~(is_system_nil | is_text_nil))
    dropped = before - df.height
    if dropped:
        print(f"  Nil returns removed: {dropped} ({before}: {df.height})")
    return df


def main() -> None:
    print("=== Lobbying pipeline starting ===")

    # 1. Ingest
    lobby_org_raw = load_lobby_orgs()
    lobbying_df = stack_lobbying_csvs()

    # 1b. Deduplicate returns — source CSVs from lobbying.ie often have overlapping
    #     date ranges, so the same primary_key can appear in multiple raw files.
    #     Drop duplicates on primary_key before any explosion so counts are not inflated.
    before = lobbying_df.height
    lobbying_df = lobbying_df.unique(subset=["primary_key"], keep="first")
    dropped = before - lobbying_df.height
    if dropped:
        print(f"  Deduplication: removed {dropped} duplicate returns ({before} → {lobbying_df.height})")

    # 1c. Drop Nil Return declarations (no lobbying took place — administrative filings only)
    lobbying_df = filter_nil_returns(lobbying_df)

    # 2. Parse return-level dates once, up front (both the activity chain and
    #    the experimental return-level metrics need them)
    lobbying_df = parse_lobbying_period(lobbying_df)

    # 3. Attach lobby_url to every return (propagates through every explode)
    lobbying_df = attach_lobby_urls(lobbying_df)

    # 4. Transform the lobby-org reference table
    lobby_org = transform_lobby_orgs(lobby_org_raw)

    # 5. Explode the activity chain: return → politician → activity → client → former-DPO
    activities_df = explode_politicians(lobbying_df)
    activities_df = explode_activities(activities_df)
    activities_df = parse_clients(activities_df)
    activities_df = parse_current_or_former_dpos(activities_df)

    # 6. Core metrics
    most_lobbied = compute_most_lobbied_politicians(activities_df)
    most_prolific = compute_most_prolific_lobbyists(lobbying_df, lobby_org)

    # 7. Experimental metrics — please review before trusting
    policy_breakdown = compute_policy_area_breakdown(lobbying_df)        # experimental
    delivery_mix = compute_delivery_method_mix(activities_df)            # experimental
    policy_exposure = compute_politician_policy_exposure(activities_df)  # experimental
    grassroots = compute_grassroots_campaigns(lobbying_df)               # experimental
    quarterly_trend = compute_quarterly_trend(lobbying_df)               # experimental
    top_clients = compute_top_client_companies(activities_df)            # experimental
    revolving_door = compute_revolving_door_dpos(activities_df)          # experimental

    # 7b. Experimental metrics — new
    distinct_orgs_per_politician = compute_distinct_orgs_per_politician(activities_df)  # experimental
    reach_by_lobbyist = compute_members_targeted_reach(activities_df)                   # experimental
    time_to_publish = compute_time_to_publish(lobbying_df)                              # experimental
    description_lengths = compute_return_description_length(lobbying_df)                # experimental
    lobbyist_persistence = compute_lobbyist_persistence(lobbying_df)                    # experimental
    bilateral_relationships = compute_bilateral_relationships(activities_df)            # experimental
    policy_area_quarterly_trend = compute_policy_area_quarterly_trend(lobbying_df)     # experimental

    # 8. Drill-down / URL export tables (streamlit-friendly)
    returns_master = build_returns_master(lobbying_df)
    politician_returns = build_politician_returns_detail(activities_df)
    lobbyist_returns = build_lobbyist_returns_detail(lobbying_df)
    client_returns = build_client_company_returns_detail(activities_df)
    revolving_door_returns = build_revolving_door_returns_detail(activities_df)
    bilateral_returns = build_bilateral_returns_detail(activities_df)

    # 9. Save — core outputs
    save_output(most_lobbied, 'most_lobbied_politicians.csv')
    save_output(activities_df, 'lobby_break_down_by_politician.csv', overwrite=False)
    save_output(most_prolific, 'lobby_count_details.csv')

    # 9b. Save — experimental outputs
    save_output(policy_breakdown, 'experimental_policy_area_breakdown.csv')
    save_output(delivery_mix, 'experimental_delivery_method_mix.csv')
    save_output(policy_exposure, 'experimental_politician_policy_exposure.csv')
    save_output(grassroots, 'experimental_grassroots_campaigns.csv')
    save_output(quarterly_trend, 'experimental_quarterly_trend.csv')
    save_output(top_clients, 'experimental_top_client_companies.csv')
    save_output(revolving_door, 'experimental_revolving_door_dpos.csv')
    save_output(distinct_orgs_per_politician, 'experimental_distinct_orgs_per_politician.csv')
    save_output(reach_by_lobbyist, 'experimental_reach_by_lobbyist.csv')
    save_output(time_to_publish, 'experimental_time_to_publish.csv')
    save_output(description_lengths, 'experimental_return_description_lengths.csv')
    save_output(lobbyist_persistence, 'experimental_lobbyist_persistence.csv')
    save_output(bilateral_relationships, 'experimental_bilateral_relationships.csv')
    save_output(policy_area_quarterly_trend, 'experimental_policy_area_quarterly_trend.csv')

    # 9c. Save — drill-down tables
    save_output(returns_master, 'returns_master.csv')
    save_output(politician_returns, 'politician_returns_detail.csv')
    save_output(lobbyist_returns, 'lobbyist_returns_detail.csv')
    save_output(client_returns, 'client_company_returns_detail.csv')
    save_output(revolving_door_returns, 'revolving_door_returns_detail.csv')
    save_output(bilateral_returns, 'bilateral_returns_detail.csv')

    print("=== Lobbying pipeline complete ===")


if __name__ == "__main__":
    main()
