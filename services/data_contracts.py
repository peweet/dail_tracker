"""Pipeline data contracts — the runtime drift gate for the payment-grain facts.

The procurement payment facts are PDF/spreadsheet-derived, so the failure mode is
never a crash — it is a *silent* one: a layout change makes a parser mis-align a
column (an amount or a description leaks into ``paid_flag``), or a source starts
emitting a value the classifier has never seen, which the consolidation maps to a
silent ``"unknown"`` / ``"UNKNOWN"`` fallback. Either way the run stays green and the
bad rows propagate all the way to the gold fact and the UI.

This module is the chokepoint that refuses to let that happen. It enforces a
declarative CONTRACT over a frame:

  * **structural** — required columns present, key columns non-null;
  * **closed-vocabulary enums** — every disciplined classification column
    (``value_kind``, ``realisation_tier``, ``supplier_class`` …) must hold only
    values the pipeline knows how to interpret. An *out-of-vocabulary* value is, by
    definition, an unclassified value — exactly the drift we must catch;
  * **quarantine** — offending rows are written to ``data/_meta/quarantine/`` (full
    rows as parquet for investigation + a small tracked JSON summary) BEFORE any
    decision to halt, so the evidence survives even when the run is failed.

Two severities:

  ``HARD``        any out-of-vocab value raises :class:`ContractViolation` and stops
                  the pipeline (used for closed enums that are clean today — a new
                  value is genuine drift, not noise).
  ``QUARANTINE``  offending rows are recorded for investigation but do NOT halt the
                  run, UNLESS their fraction exceeds a tolerance (used for known-dirty
                  free-text-contaminated columns like ``paid_flag``: ~6% of gold rows
                  already carry a leaked amount/description there, so we report every
                  run and only escalate to a halt if the contamination jumps).

This is **pure Polars on purpose** — no Pandera import — so the ETL never depends on
a test-only dependency (Streamlit Cloud installs core deps only; ETL must import
cleanly everywhere). The Pandera ``DataFrameModel`` schemas live in the test layer
and import the SAME vocabulary constants from here, so there is one source of truth.

Grounded against the live facts on 2026-06-20 (every distinct value of each column
across the five silver facts + the LA fact + the gold consolidation).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
QUARANTINE_DIR = PROJECT_ROOT / "data" / "_meta" / "quarantine"

# --------------------------------------------------------------------------- vocab
# Closed vocabularies for the disciplined classification columns. "unknown"/"UNKNOWN"
# are DELIBERATELY EXCLUDED from value_kind / realisation_tier: the consolidation maps
# an unrecognised amount_semantics to those sentinels, so a row carrying one is, by
# construction, unclassified — we want it flagged, not waved through.
AMOUNT_SEMANTICS: frozenset[str] = frozenset({"payment_actual", "po_committed"})
VALUE_KIND: frozenset[str] = frozenset({"payment_actual", "po_committed"})
REALISATION_TIER: frozenset[str] = frozenset({"SPENT", "COMMITTED"})
EXTRACTION_STATUS: frozenset[str] = frozenset({"extracted"})
EXTRACTION_CONFIDENCE: frozenset[str] = frozenset({"high", "medium", "low"})
# vat_status: "unknown" IS legitimate here — it means the VAT basis was not confirmed
# (the honest default), distinct from a confirmed inclusive figure.
VAT_STATUS: frozenset[str] = frozenset({"incl_vat", "excl_vat", "unknown"})
SUPPLIER_CLASS: frozenset[str] = frozenset(
    {
        "company",
        "foreign_company",
        "sole_trader",
        "sole_trader_or_individual",
        "public_body",
        "id_code",
        "unknown",
    }
)
# Union across stages: source facts carry the legacy public/quarantined vocab, the
# consolidation remaps to ok/review_personal_data. Both are valid depending on layer.
PRIVACY_STATUS: frozenset[str] = frozenset({"ok", "review_personal_data", "public", "quarantined"})
# paid_flag SHOULD be a flag. Anything outside this (case-insensitive) set is a
# leaked amount / description / date from a mis-aligned PDF column → quarantine.
PAID_FLAG_CLEAN: frozenset[str] = frozenset({"y", "n", "yes", "no", "paid", "unpaid", "true", "false", "1", "0", ""})

# The 29 columns every payment-grain SILVER fact must carry (la_payments_fact adds a
# few; the consolidation adds the regime/CRO/spend_category columns on top). Structural
# floor — a missing one means a parser dropped a column.
PAYMENT_FACT_REQUIRED_COLUMNS: tuple[str, ...] = (
    "publisher_id",
    "publisher_name",
    "publisher_type",
    "supplier_raw",
    "supplier_normalised",
    "amount_eur",
    "amount_semantics",
    "value_safe_to_sum",
    "supplier_class",
    "privacy_status",
    "public_display",
)
# Columns that may never be null without the row being meaningless.
PAYMENT_FACT_NONNULL_COLUMNS: tuple[str, ...] = ("publisher_id", "amount_eur")


# --------------------------------------------------------------------------- rules
@dataclass(frozen=True)
class ColumnRule:
    """A closed-vocabulary check on one column.

    ``allowed`` is the set of acceptable values; a non-null value outside it is a
    violation. Null is tolerated (some facts carry Null-typed columns, e.g. nphdb's
    ``paid_flag``) — nullness is a structural concern handled separately.
    """

    column: str
    allowed: frozenset[str]
    severity: str = "hard"  # "hard" | "quarantine"
    case_insensitive: bool = False
    # quarantine-only: escalate to a hard halt if the offending fraction exceeds this.
    max_offending_frac: float = 1.0


# The contract for a procurement payment-grain fact (silver source facts and the gold
# consolidation alike). Order is cosmetic; reasons are reported per column.
PAYMENT_FACT_RULES: tuple[ColumnRule, ...] = (
    ColumnRule("amount_semantics", AMOUNT_SEMANTICS, "hard"),
    ColumnRule("value_kind", VALUE_KIND, "hard"),
    ColumnRule("realisation_tier", REALISATION_TIER, "hard"),
    ColumnRule("extraction_status", EXTRACTION_STATUS, "hard"),
    ColumnRule("extraction_confidence", EXTRACTION_CONFIDENCE, "hard"),
    ColumnRule("vat_status", VAT_STATUS, "hard"),
    ColumnRule("supplier_class", SUPPLIER_CLASS, "hard"),
    ColumnRule("privacy_status", PRIVACY_STATUS, "hard"),
    # Known-dirty: ~6% of gold rows already carry a leaked amount/description here.
    # Quarantine every run for investigation; only halt if it jumps past 12%.
    ColumnRule("paid_flag", PAID_FLAG_CLEAN, "quarantine", case_insensitive=True, max_offending_frac=0.12),
)


class ContractViolation(RuntimeError):
    """Raised when a HARD contract check fails — halts the pipeline before gold is written."""


@dataclass
class ContractReport:
    name: str
    n_rows: int
    structural_errors: list[str] = field(default_factory=list)
    # column -> {"severity", "n_offending", "frac", "samples": [...], "escalated": bool}
    vocab_breaches: dict[str, dict] = field(default_factory=dict)
    # cross-column rules that must hold for every row (the consolidation documents these
    # but historically never re-asserted them on its own output).
    invariant_errors: list[str] = field(default_factory=list)
    n_quarantined_rows: int = 0
    quarantine_parquet: str | None = None
    quarantine_summary: str | None = None

    @property
    def ok(self) -> bool:
        """True iff nothing forces a halt (no structural/invariant error, no hard/escalated breach)."""
        if self.structural_errors or self.invariant_errors:
            return False
        return not any(b.get("severity") == "hard" or b.get("escalated") for b in self.vocab_breaches.values())

    def raise_if_failed(self) -> ContractReport:
        if self.ok:
            return self
        lines = [f"data contract FAILED for '{self.name}' ({self.n_rows:,} rows):"]
        lines += [f"  [STRUCTURE] {e}" for e in self.structural_errors]
        lines += [f"  [INVARIANT] {e}" for e in self.invariant_errors]
        for col, b in self.vocab_breaches.items():
            if b.get("severity") == "hard" or b.get("escalated"):
                why = "ESCALATED" if b.get("escalated") else "HARD"
                lines.append(
                    f"  [{why}] {col}: {b['n_offending']:,} rows ({b['frac'] * 100:.1f}%) "
                    f"out-of-vocab; e.g. {b['samples'][:5]}"
                )
        if self.quarantine_parquet:
            lines.append(f"  offending rows quarantined -> {self.quarantine_parquet}")
        lines.append("  Investigate the source parser, then re-run. This guard exists to stop bad rows reaching gold.")
        raise ContractViolation("\n".join(lines))


# --------------------------------------------------------------------------- engine
def _offending_expr(rule: ColumnRule) -> pl.Expr:
    """Polars expression: True for rows whose ``rule.column`` value is non-null and not
    in ``rule.allowed``. Caller must ensure the column exists and is not Null-typed."""
    col = pl.col(rule.column).cast(pl.Utf8)
    if rule.case_insensitive:
        col = col.str.strip_chars().str.to_lowercase()
        allowed = [a.lower() for a in rule.allowed]
    else:
        allowed = list(rule.allowed)
    return col.is_not_null() & ~col.is_in(allowed)


def _checkable(df: pl.DataFrame, rule: ColumnRule) -> bool:
    """A rule is checkable only if its column is present and carries a non-Null dtype."""
    return rule.column in df.columns and df.schema[rule.column] != pl.Null


def _offending_mask(df: pl.DataFrame, rule: ColumnRule) -> pl.Series | None:
    """Boolean Series for a rule, or None if the column is absent/Null-typed."""
    if not _checkable(df, rule):
        return None
    return df.select(_offending_expr(rule).alias("_m")).to_series()


def check_structure(
    df: pl.DataFrame,
    *,
    required_columns: tuple[str, ...],
    nonnull_columns: tuple[str, ...] = (),
) -> list[str]:
    """Return a list of structural error strings (empty == passes). Pure; no side effects."""
    errors: list[str] = []
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        errors.append(f"missing required columns: {missing}")
    if df.height == 0:
        errors.append("frame is empty (0 rows)")
    for c in nonnull_columns:
        if c in df.columns and df.schema[c] != pl.Null:
            n_null = int(df.select(pl.col(c).is_null().sum()).item())
            if n_null:
                errors.append(f"column '{c}' has {n_null:,} null(s) but must be non-null")
    return errors


def enforce_contract(
    df: pl.DataFrame,
    *,
    name: str,
    rules: tuple[ColumnRule, ...] = PAYMENT_FACT_RULES,
    required_columns: tuple[str, ...] = PAYMENT_FACT_REQUIRED_COLUMNS,
    nonnull_columns: tuple[str, ...] = PAYMENT_FACT_NONNULL_COLUMNS,
    quarantine_dir: Path = QUARANTINE_DIR,
    write_quarantine: bool = True,
) -> ContractReport:
    """Validate ``df`` against the contract, quarantine offending rows, and report.

    Does NOT raise on its own — call :meth:`ContractReport.raise_if_failed` (or check
    ``report.ok``) at the call site so the caller controls the halt. The quarantine
    artifacts are always written first, so evidence survives a subsequent halt.
    """
    report = ContractReport(name=name, n_rows=df.height)
    report.structural_errors = check_structure(df, required_columns=required_columns, nonnull_columns=nonnull_columns)

    # Accumulate a union mask across all vocab rules plus a per-row reason expression
    # (one boolean "_q_<col>" column per failing rule — vectorised, no Python row loop).
    union_mask = pl.Series("_m", [False] * df.height, dtype=pl.Boolean)
    fired_rules: list[ColumnRule] = []
    for rule in rules:
        mask = _offending_mask(df, rule)
        if mask is None:
            continue
        n_off = int(mask.sum())
        if n_off == 0:
            continue
        frac = n_off / df.height if df.height else 0.0
        bad_vals = df.filter(mask).select(pl.col(rule.column).cast(pl.Utf8)).to_series().value_counts(sort=True)
        samples = [r[0] for r in bad_vals.head(8).iter_rows()]
        escalated = rule.severity == "quarantine" and frac > rule.max_offending_frac
        report.vocab_breaches[rule.column] = {
            "severity": rule.severity,
            "n_offending": n_off,
            "frac": round(frac, 4),
            "samples": samples,
            "escalated": escalated,
        }
        union_mask = union_mask | mask
        fired_rules.append(rule)

    n_q = int(union_mask.sum())
    report.n_quarantined_rows = n_q
    if n_q and write_quarantine:
        # Build "_quarantine_reason" by concatenating the names of the rules each row failed.
        reason_expr = pl.concat_str(
            [pl.when(_offending_expr(r)).then(pl.lit(f"{r.column};")).otherwise(pl.lit("")) for r in fired_rules]
        ).str.strip_chars_end(";")
        q = df.with_columns(reason_expr.alias("_quarantine_reason")).filter(union_mask)
        report.quarantine_parquet, report.quarantine_summary = _write_quarantine(
            name=name, frame=q, report=report, quarantine_dir=quarantine_dir
        )
    return report


def _write_quarantine(
    *, name: str, frame: pl.DataFrame, report: ContractReport, quarantine_dir: Path
) -> tuple[str, str]:
    """Write the offending rows (parquet, full detail) + a small JSON summary (tracked)."""
    from services.parquet_io import save_parquet

    quarantine_dir.mkdir(parents=True, exist_ok=True)
    pq_path = quarantine_dir / f"{name}_quarantine.parquet"
    json_path = quarantine_dir / f"{name}_quarantine.json"
    save_parquet(frame, pq_path)
    summary = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "fact": name,
        "n_rows_total": report.n_rows,
        "n_rows_quarantined": frame.height,
        "frac_quarantined": round(frame.height / report.n_rows, 4) if report.n_rows else 0.0,
        "breaches": report.vocab_breaches,
        "note": "Out-of-vocabulary / mis-aligned values held for investigation. Full rows in the "
        "sibling .parquet. HARD/ESCALATED breaches also halt the pipeline.",
    }
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.warning("quarantined %s rows for %s -> %s", frame.height, name, pq_path)
    return str(pq_path), str(json_path)


# --------------------------------------------------------------------------- invariants
# Cross-column rules the consolidation's own logic is supposed to guarantee. They all hold
# on the current gold fact (verified 2026-06-20: 0 violations each), so they are REGRESSION
# insurance — a refactor that breaks one (e.g. stops excluding public-body recipients from
# summable spend, or lets a CRO match attach to a non-company row) fails the run instead of
# silently shipping double-counted or mis-classed money. Each entry: (label, offending expr).
_PAYMENT_FACT_INVARIANTS: tuple[tuple[str, pl.Expr], ...] = (
    (
        "summable public-body recipient (intergovernmental transfer counted as spend → double-count)",
        pl.col("value_safe_to_sum") & (pl.col("supplier_class") == "public_body"),
    ),
    (
        "summable row with a blank supplier (un-identifiable spend left summable)",
        pl.col("value_safe_to_sum") & (pl.col("supplier_normalised").fill_null("").str.strip_chars() == ""),
    ),
    (
        "CRO company number attached to a non-company-class row (CRO match ⇒ company invariant)",
        pl.col("cro_company_num").is_not_null() & (pl.col("supplier_class") != "company"),
    ),
    (
        "likely-person row left publicly displayable (privacy invariant)",
        pl.col("public_display")
        & (
            (pl.col("supplier_class") == "sole_trader_or_individual")
            | (pl.col("privacy_status") == "review_personal_data")
        ),
    ),
    (
        "summable row with a non-positive amount (a 0/negative payment is a parse error)",
        pl.col("value_safe_to_sum") & (pl.col("amount_eur") <= 0),
    ),
    (
        "value_kind / realisation_tier disagree (payment_actual⇔SPENT, po_committed⇔COMMITTED)",
        ((pl.col("value_kind") == "payment_actual") & (pl.col("realisation_tier") != "SPENT"))
        | ((pl.col("value_kind") == "po_committed") & (pl.col("realisation_tier") != "COMMITTED")),
    ),
)


def payment_fact_invariant_violations(df: pl.DataFrame) -> list[str]:
    """Return a human-readable error per violated cross-column invariant (empty == clean).

    Each invariant whose offending columns are all present is evaluated; one referencing an
    absent column is skipped (so this works on silver facts that lack the gold-only columns)."""
    errors: list[str] = []
    for label, expr in _PAYMENT_FACT_INVARIANTS:
        try:
            n = int(df.select(expr.alias("_v")).to_series().sum())
        except pl.exceptions.ColumnNotFoundError:
            continue  # a gold-only invariant evaluated on a silver fact — not applicable
        if n:
            errors.append(f"{n:,} rows violate: {label}")
    return errors


# --------------------------------------------------------------------------- reconciliation
def reconciliation_violations(
    expected: dict[str, tuple[int, float]],
    actual: dict[str, tuple[int, float]],
    *,
    allowed_row_delta: dict[str, int] | None = None,
    eur_tolerance: float = 1.0,
) -> list[str]:
    """Compare per-source (rows, summed €) between two layers; return a message per drift.

    This is the *audit* step of write-audit-publish: the consolidation maps several silver
    facts into one gold fact WITHOUT re-parsing, so every source's rows and money must
    survive exactly (modulo a documented, explicit carry-forward in ``allowed_row_delta``).
    A non-zero unexplained delta means a concat/dedup/join bug silently dropped or duplicated
    rows — the classic green-pipeline-partial-data failure the row-count baseline (which only
    trips on a >50% loss) cannot see. ``eur_tolerance`` absorbs float-sum noise only."""
    allowed_row_delta = allowed_row_delta or {}
    errors: list[str] = []
    for key, (exp_rows, exp_eur) in expected.items():
        if key not in actual:
            errors.append(f"{key}: present in source but ABSENT from output")
            continue
        act_rows, act_eur = actual[key]
        drow = act_rows - exp_rows - allowed_row_delta.get(key, 0)
        if drow != 0:
            errors.append(
                f"{key}: row count drift {act_rows - exp_rows:+,} "
                f"(allowed {allowed_row_delta.get(key, 0):+,}) — concat/dedup/join lost or duplicated rows"
            )
        if abs((act_eur or 0) - (exp_eur or 0)) > eur_tolerance:
            errors.append(f"{key}: € total drift {(act_eur or 0) - (exp_eur or 0):+,.2f} — money not preserved")
    return errors


def guard_payment_fact(
    df: pl.DataFrame, *, name: str, hard: bool = True, quarantine_dir: Path = QUARANTINE_DIR
) -> ContractReport:
    """Convenience wrapper: enforce the payment-fact contract and (optionally) halt.

    ``hard=True`` raises :class:`ContractViolation` on any structural/hard/escalated
    breach — the pipeline stops before the bad frame is written onward. ``hard=False``
    only reports (used by tests / dry-runs).
    """
    report = enforce_contract(df, name=name, quarantine_dir=quarantine_dir)
    report.invariant_errors = payment_fact_invariant_violations(df)
    if report.vocab_breaches or report.structural_errors or report.invariant_errors:
        logger.info(
            "contract report for %s: vocab=%s struct=%s invariants=%s",
            name,
            report.vocab_breaches,
            report.structural_errors,
            report.invariant_errors,
        )
    if hard:
        report.raise_if_failed()
    return report


# --------------------------------------------------------------------------- plausibility
# The vocab gate above catches *unknown labels*; this catches *absurd numbers* — a stray,
# fat-fingered or mis-OCR'd figure (a €999,999,999 donation) that is the right TYPE, in the
# right column, but is simply not a real value. Trust is the product, so a single ridiculous
# number reaching a published total is the failure to avoid. The pattern is dead-letter /
# quarantine: split the frame, let the plausible rows flow, divert the rest to the quarantine
# dir for review, and HALT only if the bad FRACTION spikes (which means the source changed
# shape — a units change, a parser break — not one bad cell).

_NUMERIC_DTYPES = (
    pl.Float64,
    pl.Float32,
    pl.Int64,
    pl.Int32,
    pl.Int16,
    pl.Int8,
    pl.UInt64,
    pl.UInt32,
    pl.UInt16,
    pl.UInt8,
)


@dataclass(frozen=True)
class BoundRule:
    """An 'absurd-only' numeric plausibility bound on one column.

    A non-null value ``< min_value`` or ``> max_value`` is implausible. Bounds are meant to
    be set WIDE — so wide that only genuine garbage trips them (a single Irish political
    donation over €1bn is physically impossible) — NOT as tight business rules: a tight
    bound is a straitjacket coupled to the source. Use ``None`` for an open side.
    """

    column: str
    min_value: float | None = None
    max_value: float | None = None
    # A few stray cells are quarantined and the run continues; a fraction past this means
    # the SOURCE changed shape (units, a parser break) rather than a fat-finger — halt.
    max_offending_frac: float = 0.02


class ImplausibleValueSpike(ContractViolation):
    """Raised when implausible values exceed tolerance — a structural change, not a stray
    cell. Subclasses :class:`ContractViolation` so existing pipeline halts still catch it."""


def _bound_offending_expr(rule: BoundRule) -> pl.Expr:
    col = pl.col(rule.column)
    cond = pl.lit(False)
    if rule.min_value is not None:
        cond = cond | (col < rule.min_value)
    if rule.max_value is not None:
        cond = cond | (col > rule.max_value)
    return col.is_not_null() & cond


def partition_implausible(
    df: pl.DataFrame,
    *,
    name: str,
    bounds: tuple[BoundRule, ...],
    quarantine_dir: Path = QUARANTINE_DIR,
    write_quarantine: bool = True,
) -> tuple[pl.DataFrame, pl.DataFrame, ContractReport]:
    """Split ``df`` into ``(plausible, implausible)`` rows by absurd-only numeric bounds.

    The plausible frame is safe to propagate; implausible rows are quarantined to disk
    (full rows + a tracked JSON summary, the SAME convention as the vocab gate) tagged with
    a per-row ``_quarantine_reason``, and the event is reported. Surviving rows keep their
    order; the only side effect is the quarantine write.

    Raises :class:`ImplausibleValueSpike` if any rule's offending fraction exceeds its
    ``max_offending_frac`` (evidence is quarantined first, so it survives the halt). A bounded
    column that is ABSENT is skipped (a loader may not carry it); a column that is PRESENT but
    non-numeric raises ``ValueError`` — a mis-wired gate, caught at dev time, not waved through.
    """
    for rule in bounds:
        if rule.column in df.columns:
            dt = df.schema[rule.column]
            if dt != pl.Null and dt not in _NUMERIC_DTYPES:
                raise ValueError(
                    f"partition_implausible[{name}]: column '{rule.column}' is {dt}, not numeric — "
                    f"cast it before gating (this is a wiring error, not data drift)."
                )

    report = ContractReport(name=name, n_rows=df.height)
    union = pl.Series("_m", [False] * df.height, dtype=pl.Boolean)
    fired: list[BoundRule] = []
    spike = False
    for rule in bounds:
        if rule.column not in df.columns or df.schema[rule.column] == pl.Null:
            continue
        mask = df.select(_bound_offending_expr(rule).alias("_m")).to_series()
        n_off = int(mask.sum())
        if n_off == 0:
            continue
        frac = n_off / df.height if df.height else 0.0
        samples = df.filter(mask).select(pl.col(rule.column)).to_series().head(8).to_list()
        escalated = frac > rule.max_offending_frac
        spike = spike or escalated
        report.vocab_breaches[rule.column] = {
            "severity": "bound",
            "n_offending": n_off,
            "frac": round(frac, 4),
            "samples": [str(s) for s in samples],
            "escalated": escalated,
            "bounds": [rule.min_value, rule.max_value],
        }
        union = union | mask
        fired.append(rule)

    n_q = int(union.sum())
    report.n_quarantined_rows = n_q
    plausible = df.filter(~union)
    implausible = df.filter(union)

    if n_q and write_quarantine:
        reason_expr = pl.concat_str(
            [pl.when(_bound_offending_expr(r)).then(pl.lit(f"{r.column};")).otherwise(pl.lit("")) for r in fired]
        ).str.strip_chars_end(";")
        q = df.with_columns(reason_expr.alias("_quarantine_reason")).filter(union)
        report.quarantine_parquet, report.quarantine_summary = _write_quarantine(
            name=name, frame=q, report=report, quarantine_dir=quarantine_dir
        )

    if spike:
        worst = {c: b for c, b in report.vocab_breaches.items() if b.get("escalated")}
        raise ImplausibleValueSpike(
            f"implausible-value SPIKE in '{name}': {worst} exceeded tolerance — the SOURCE likely "
            f"changed shape (units / parser), this is not a stray cell. Investigate before publishing. "
            + (f"Offending rows quarantined -> {report.quarantine_parquet}" if report.quarantine_parquet else "")
        )

    return plausible, implausible, report
