"""Build a private shadow crosswalk between award notices and disclosed payment/PO lines.

This module deliberately does *not* claim that a payment was made under a tender. It creates a
review queue at payment-line grain using only reviewed identity links and source-stated text:

``explicit_reference``
    Exactly one matching eTenders tender ID is written in the payment line's PO/description text.
``review_candidate``
    No tender ID is stated, but exactly one award for the exact buyer and exact CRO company is
    compatible with the published quarter/duration and has literal title/description containment.
``relationship_only``
    The exact buyer paid/ordered from the exact company, but no unique tender candidate survives.

Every row remains ``shadow_only`` and ``contract_attribution_permitted=False``. A reviewer or a
source carrying an explicit contract reference must make any later publication decision. There is
no score, probability, award/payment ratio, or blended money total.

Grain: one row per public-display payment/PO line that shares an exact curated buyer identity and
an exact-unique CRO company with at least one eTenders award.

Outputs are experimental and are not registered in the live pipeline:

* ``data/sandbox/parquet/procurement_award_payment_candidates.parquet``
* ``data/sandbox/_meta/procurement_award_payment_candidates_summary.json``
"""

from __future__ import annotations

import argparse
import contextlib
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from services import runtime_env as _runtime_env  # noqa: E402,F401
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

AWARDS = ROOT / "data/gold/parquet/procurement_awards.parquet"
PAYMENTS = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
SUPPLIER_XREF = ROOT / "data/gold/parquet/procurement_supplier_cro_match.parquet"
BUYER_XREF = ROOT / "data/_meta/procurement_publishers/buyer_xref.csv"
OUT = ROOT / "data/sandbox/parquet/procurement_award_payment_candidates.parquet"
OUT_SUMMARY = ROOT / "data/sandbox/_meta/procurement_award_payment_candidates_summary.json"

LINK_STATES = {"explicit_reference", "review_candidate", "relationship_only"}
NEVER_SUM_WITH = "awarded|budget|ted"


def _require_columns(frame: pl.DataFrame, columns: set[str], *, label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")


def load_awards(path: Path = AWARDS) -> pl.DataFrame:
    """Load the minimum award fields and parse the source-stated award date/duration."""
    raw = pl.read_parquet(path).select(
        "Tender ID",
        "Contracting Authority",
        "supplier_norm",
        "Notice Published Date/Contract Created Date",
        "Contract Duration (Months)",
        "Tender/Contract Name",
    )
    return raw.select(
        pl.col("Tender ID").cast(pl.String).str.strip_chars().replace("", None).alias("tender_id"),
        pl.col("Contracting Authority").cast(pl.String).alias("contracting_authority"),
        pl.col("supplier_norm").cast(pl.String),
        pl.col("Notice Published Date/Contract Created Date")
        .cast(pl.String)
        .str.strptime(pl.Date, "%d/%m/%Y", strict=False)
        .alias("award_date"),
        pl.col("Contract Duration (Months)").cast(pl.Int64, strict=False).alias("contract_duration_months"),
        pl.col("Tender/Contract Name").cast(pl.String).alias("tender_title"),
    )


def load_payments(path: Path = PAYMENTS) -> pl.DataFrame:
    """Load public extracted payment lines, retaining provenance and lifecycle fields."""
    raw = pl.read_parquet(path).with_row_index("_fact_row_number")
    return raw.filter(pl.col("public_display") & (pl.col("extraction_status") == "extracted")).select(
        "_fact_row_number",
        "publisher_id",
        "publisher_name",
        pl.col("supplier_raw").alias("payment_supplier"),
        "supplier_normalised",
        pl.col("cro_company_num").cast(pl.Int64, strict=False),
        "period",
        pl.col("year").cast(pl.Int64, strict=False),
        pl.col("quarter").cast(pl.Int64, strict=False),
        "amount_eur",
        "value_kind",
        "realisation_tier",
        "value_safe_to_sum",
        "vat_status",
        "po_number",
        "description",
        "source_landing_url",
        "source_file_url",
        "source_file_hash",
        "source_row_number",
    )


def load_supplier_xref(path: Path = SUPPLIER_XREF) -> pl.DataFrame:
    """Load exact-unique supplier-to-CRO links only; ambiguous names fail closed."""
    return pl.read_parquet(path).select(
        "supplier_norm",
        pl.col("company_num").cast(pl.Int64, strict=False),
        pl.col("n_cro").cast(pl.Int64, strict=False),
        "match_method",
        "match_confidence",
    )


def load_buyer_xref(path: Path = BUYER_XREF) -> pl.DataFrame:
    """Load the curated exact award-name to payment-publisher mapping."""
    return pl.read_csv(path).select("etenders_name", "payments_publisher_name", "match_tier")


def _normalised_text(column: str) -> pl.Expr:
    return pl.col(column).fill_null("").cast(pl.String).str.to_lowercase().str.replace_all(r"[^a-z0-9]", "")


def _contains_dynamic(haystack: str, needle: str, *, minimum_needle_length: int) -> pl.Expr:
    """Literal row-wise containment; no fuzzy matching or learned similarity."""
    return pl.struct(haystack, needle).map_elements(
        lambda row: bool(row[needle]) and len(row[needle]) >= minimum_needle_length and row[needle] in row[haystack],
        return_dtype=pl.Boolean,
    )


def _has_dynamic_token(tokens: str, needle: str, *, minimum_needle_length: int) -> pl.Expr:
    """Require an exact alphanumeric token, avoiding tender-ID substring collisions."""
    return pl.struct(tokens, needle).map_elements(
        lambda row: bool(row[needle]) and len(row[needle]) >= minimum_needle_length and row[needle] in row[tokens],
        return_dtype=pl.Boolean,
    )


def _exact_supplier_links(supplier_xref: pl.DataFrame) -> pl.DataFrame:
    """Return only supplier names that resolve to one high-confidence CRO company."""
    return (
        supplier_xref.filter(
            (pl.col("match_method") == "exact_unique")
            & (pl.col("n_cro") == 1)
            & (pl.col("match_confidence") >= 0.9)
            & pl.col("company_num").is_not_null()
        )
        .select("supplier_norm", "company_num")
        .unique()
    )


def _prepare_award_relationships(
    awards: pl.DataFrame,
    supplier_xref: pl.DataFrame,
    buyer_xref: pl.DataFrame,
) -> pl.DataFrame:
    _require_columns(
        awards,
        {
            "tender_id",
            "contracting_authority",
            "supplier_norm",
            "award_date",
            "contract_duration_months",
            "tender_title",
        },
        label="awards",
    )
    _require_columns(
        supplier_xref,
        {"supplier_norm", "company_num", "n_cro", "match_method", "match_confidence"},
        label="supplier xref",
    )
    _require_columns(
        buyer_xref,
        {"etenders_name", "payments_publisher_name", "match_tier"},
        label="buyer xref",
    )
    exact_suppliers = _exact_supplier_links(supplier_xref)
    exact_buyers = buyer_xref.filter(
        (pl.col("match_tier") == "curated_exact")
        & pl.col("etenders_name").is_not_null()
        & (pl.col("etenders_name").str.strip_chars() != "")
        & pl.col("payments_publisher_name").is_not_null()
        & (pl.col("payments_publisher_name").str.strip_chars() != "")
    ).select("etenders_name", "payments_publisher_name")
    relationships = (
        awards.join(exact_suppliers, on="supplier_norm", how="inner", validate="m:1")
        .join(
            exact_buyers,
            left_on="contracting_authority",
            right_on="etenders_name",
            how="inner",
            validate="m:1",
        )
        .with_columns(
            (pl.col("award_date").dt.year() * 4 + ((pl.col("award_date").dt.month() - 1) // 3 + 1)).alias(
                "_award_quarter_index"
            ),
            _normalised_text("tender_id").alias("_tender_key"),
            _normalised_text("tender_title").alias("_title_key"),
        )
    )
    # Several supplier spellings can resolve to the same company on one tender. The review
    # crosswalk is tender/company/buyer grain, so collapse those source duplicates before joining.
    return relationships.group_by("tender_id", "payments_publisher_name", "company_num", maintain_order=True).agg(
        pl.first("contracting_authority"),
        pl.first("award_date"),
        pl.first("contract_duration_months"),
        pl.first("tender_title"),
        pl.first("_award_quarter_index"),
        pl.first("_tender_key"),
        pl.first("_title_key"),
    )


def build_shadow_candidates(
    awards: pl.DataFrame,
    payments: pl.DataFrame,
    supplier_xref: pl.DataFrame,
    buyer_xref: pl.DataFrame,
) -> pl.DataFrame:
    """Return one fail-closed shadow row per exact buyer/company payment relationship."""
    _require_columns(
        payments,
        {
            "_fact_row_number",
            "publisher_id",
            "publisher_name",
            "payment_supplier",
            "supplier_normalised",
            "cro_company_num",
            "period",
            "year",
            "quarter",
            "amount_eur",
            "value_kind",
            "realisation_tier",
            "value_safe_to_sum",
            "vat_status",
            "po_number",
            "description",
            "source_landing_url",
            "source_file_url",
            "source_file_hash",
            "source_row_number",
        },
        label="payments",
    )
    award_relationships = _prepare_award_relationships(awards, supplier_xref, buyer_xref)
    exact_suppliers = _exact_supplier_links(supplier_xref)
    payments = payments.join(
        exact_suppliers,
        left_on=["supplier_normalised", "cro_company_num"],
        right_on=["supplier_norm", "company_num"],
        how="inner",
        validate="m:1",
    )
    payment_rows = payments.filter(pl.col("cro_company_num").is_not_null()).with_columns(
        pl.concat_str(
            pl.col("publisher_id").fill_null("unknown"),
            pl.coalesce(pl.col("source_file_hash"), pl.col("source_file_url"), pl.lit("unknown-source")),
            pl.col("source_row_number").cast(pl.String).fill_null("unknown-row"),
            pl.col("_fact_row_number").cast(pl.String),
            separator="|",
        ).alias("payment_line_id"),
        (pl.col("year") * 4 + pl.col("quarter")).alias("_payment_quarter_index"),
        pl.concat_str(pl.col("po_number").fill_null(""), pl.col("description").fill_null(""), separator=" ")
        .str.to_lowercase()
        .str.extract_all(r"[a-z0-9]+")
        .alias("_payment_tokens"),
        _normalised_text("description").alias("_description_key"),
    )
    pairs = payment_rows.join(
        award_relationships,
        left_on=["publisher_name", "cro_company_num"],
        right_on=["payments_publisher_name", "company_num"],
        how="inner",
        validate="m:m",
    ).with_columns(
        _has_dynamic_token("_payment_tokens", "_tender_key", minimum_needle_length=6).alias("_explicit_reference"),
        (
            _contains_dynamic("_description_key", "_title_key", minimum_needle_length=12)
            | _contains_dynamic("_title_key", "_description_key", minimum_needle_length=12)
        ).alias("_title_containment"),
        (
            (pl.col("_payment_quarter_index") >= pl.col("_award_quarter_index"))
            & (
                pl.col("contract_duration_months").is_not_null()
                & (
                    pl.col("_payment_quarter_index")
                    <= pl.col("_award_quarter_index") + (pl.col("contract_duration_months") / 3).ceil().cast(pl.Int64)
                )
            )
        ).alias("_within_published_duration"),
    )
    grouped = (
        pairs.group_by("payment_line_id", maintain_order=True)
        .agg(
            pl.first("publisher_id"),
            pl.first("publisher_name"),
            pl.first("payment_supplier"),
            pl.first("supplier_normalised"),
            pl.first("cro_company_num"),
            pl.first("period"),
            pl.first("year"),
            pl.first("quarter"),
            pl.first("amount_eur"),
            pl.first("value_kind"),
            pl.first("realisation_tier"),
            pl.first("value_safe_to_sum"),
            pl.first("vat_status"),
            pl.first("po_number"),
            pl.first("description"),
            pl.first("source_landing_url"),
            pl.first("source_file_url"),
            pl.first("source_file_hash"),
            pl.first("source_row_number"),
            pl.col("tender_id").drop_nulls().unique().sort().alias("_relationship_ids"),
            pl.col("tender_id")
            .filter(pl.col("_explicit_reference"))
            .drop_nulls()
            .unique()
            .sort()
            .alias("_explicit_ids"),
            pl.col("tender_id")
            .filter(pl.col("_within_published_duration") & pl.col("_title_containment"))
            .drop_nulls()
            .unique()
            .sort()
            .alias("_review_ids"),
        )
        .with_columns(
            pl.col("_relationship_ids").list.len().alias("relationship_tender_count"),
            pl.col("_explicit_ids").list.len().alias("explicit_reference_count"),
            pl.col("_review_ids").list.len().alias("review_candidate_count"),
        )
    )
    selected = grouped.with_columns(
        pl.when(pl.col("explicit_reference_count") == 1)
        .then(pl.col("_explicit_ids").list.first())
        .when(pl.col("review_candidate_count") == 1)
        .then(pl.col("_review_ids").list.first())
        .otherwise(None)
        .alias("candidate_tender_id"),
        pl.when(pl.col("explicit_reference_count") == 1)
        .then(pl.lit("explicit_reference"))
        .when((pl.col("explicit_reference_count") == 0) & (pl.col("review_candidate_count") == 1))
        .then(pl.lit("review_candidate"))
        .otherwise(pl.lit("relationship_only"))
        .alias("link_state"),
        pl.when(pl.col("explicit_reference_count") == 1)
        .then(pl.lit("one exact tender ID is stated in the payment PO/description text"))
        .when(pl.col("explicit_reference_count") > 1)
        .then(pl.lit("multiple tender IDs are stated; attribution withheld"))
        .when(pl.col("review_candidate_count") == 1)
        .then(pl.lit("one exact-buyer/exact-company award matches published duration and literal title text"))
        .when(pl.col("review_candidate_count") > 1)
        .then(pl.lit("multiple temporal/title candidates; attribution withheld"))
        .otherwise(pl.lit("exact buyer/company relationship only; no unique tender evidence"))
        .alias("state_reason"),
    )
    award_lookup = award_relationships.select(
        pl.col("tender_id").alias("candidate_tender_id"),
        pl.col("contracting_authority").alias("candidate_contracting_authority"),
        pl.col("award_date").alias("candidate_award_date"),
        pl.col("contract_duration_months").alias("candidate_contract_duration_months"),
        pl.col("tender_title").alias("candidate_tender_title"),
    ).unique(subset=["candidate_tender_id"], keep="first")
    return (
        selected.join(award_lookup, on="candidate_tender_id", how="left", validate="m:1")
        .with_columns(
            pl.when(pl.col("candidate_tender_id").is_not_null())
            .then(
                pl.concat_str(
                    pl.lit("https://www.etenders.gov.ie/epps/cft/prepareViewCfTWS.do?resourceId="),
                    pl.col("candidate_tender_id"),
                )
            )
            .otherwise(None)
            .alias("candidate_etenders_notice_url"),
            pl.lit("shadow_only").alias("publication_status"),
            pl.lit("unreviewed").alias("review_status"),
            pl.lit(False).alias("contract_attribution_permitted"),
            pl.lit(NEVER_SUM_WITH).alias("never_sum_with"),
            pl.when(pl.col("realisation_tier") == "SPENT")
            .then(pl.lit("disclosed payment line; not attributed to the candidate tender"))
            .when(pl.col("realisation_tier") == "COMMITTED")
            .then(
                pl.lit("disclosed purchase-order commitment; not cash paid and not attributed to the candidate tender")
            )
            .otherwise(pl.lit("disclosed line of unknown lifecycle; not attributed to the candidate tender"))
            .alias("money_caveat"),
        )
        .drop("_relationship_ids", "_explicit_ids", "_review_ids")
        .select(
            "payment_line_id",
            "link_state",
            "state_reason",
            "publication_status",
            "review_status",
            "contract_attribution_permitted",
            "relationship_tender_count",
            "explicit_reference_count",
            "review_candidate_count",
            "candidate_tender_id",
            "candidate_contracting_authority",
            "candidate_award_date",
            "candidate_contract_duration_months",
            "candidate_tender_title",
            "candidate_etenders_notice_url",
            "publisher_id",
            "publisher_name",
            "payment_supplier",
            "supplier_normalised",
            "cro_company_num",
            "period",
            "year",
            "quarter",
            "amount_eur",
            "value_kind",
            "realisation_tier",
            "value_safe_to_sum",
            "vat_status",
            "never_sum_with",
            "money_caveat",
            "po_number",
            "description",
            "source_landing_url",
            "source_file_url",
            "source_file_hash",
            "source_row_number",
        )
        .sort("link_state", "publisher_name", "period", "payment_line_id")
    )


def build_summary(rows: pl.DataFrame) -> dict:
    """Small provenance/status sidecar; no award/payment totals or ratios."""
    by_state = {row["link_state"]: row["len"] for row in rows.group_by("link_state").len().iter_rows(named=True)}
    by_tier = {
        row["realisation_tier"]: row["len"] for row in rows.group_by("realisation_tier").len().iter_rows(named=True)
    }
    return {
        "schema": "procurement-award-payment-shadow-candidates/1",
        "generated_at": datetime.now(UTC).isoformat(),
        "rows": rows.height,
        "grain": "one row per public payment/PO line with an exact curated buyer and exact-unique CRO award relationship",
        "states": by_state,
        "lifecycle_tiers": by_tier,
        "contract_attribution_permitted": False,
        "contains_confidence_score": False,
        "caveat": (
            "Shadow review queue only. explicit_reference means a tender ID is written in the payment text; "
            "review_candidate is deterministic identity/time/text triage; relationship_only carries no tender attribution. "
            "SPENT and COMMITTED remain separate and no award/payment values are combined."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the private award/payment shadow review queue")
    parser.add_argument("--output", type=Path, default=OUT)
    parser.add_argument("--summary", type=Path, default=OUT_SUMMARY)
    args = parser.parse_args()
    rows = build_shadow_candidates(load_awards(), load_payments(), load_supplier_xref(), load_buyer_xref())
    summary = build_summary(rows)
    save_parquet(rows, args.output)
    save_coverage(summary, args.summary)
    print(summary)


if __name__ == "__main__":
    run_extractor(main)
