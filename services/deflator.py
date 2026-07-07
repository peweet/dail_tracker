"""services/deflator.py — the single, tested CPI deflation function ("inflation API").

Mirrors the canonical OSS pattern (palewire `cpi.inflate`, R `priceR::adjust_for_inflation`):
a PURE function backed by a PRECOMPUTED, cached index — here the gold table
``data/gold/parquet/cso_cpi_deflator.parquet`` built by
``extractors/cso_pxstat_extract.py:build_cpi_deflator`` (chain-linked CSO CPA07, base 2025).

Design contract (why this exists as its own module):
  * ONE code path for every euro adjusted for inflation — the app's SQL views precompute the
    same arithmetic, and a parity test pins SQL == this function, so there is a single,
    unit-tested source of truth.
  * Deflation is multiplicative and order-preserving: it RE-EXPRESSES a value, it does not
    correct it. It therefore neither creates nor fixes magnitude errors in the nominal input —
    it scales them. Data-quality guarding is a FACT-LAYER concern (see ``implausible_mask``),
    deliberately kept OUT of the math so the two never get conflated.

Usage:
    d = Deflator.load()
    d.inflate(100_000, 2013)            # -> ~124_038.0  (2013 € in 2025 €)
    d.inflate(100_000, 2013, to=2019)   # -> value in 2019 €
    d.factor(2025)                      # -> 1.0  (base year is identity)
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_GOLD = _ROOT / "data" / "gold" / "parquet"
_DEFAULT_PATH = _GOLD / "cso_cpi_deflator.parquet"

# ---- multi-index registry --------------------------------------------------
# Different value types need different price indices — CPI is a household basket and is NOT the
# right deflator for public money (use the government-consumption deflator) or construction
# (use the tender-price index). Each reference deflator is a gold parquet sharing the schema
# (year, <index_col>, deflator_to_base, base_year), built by extractors/cso_pxstat_extract.py.
# The chosen index is always recorded alongside any adjusted figure (deflator_index column /
# .index_code) so a real-terms number can never be shown without its provenance + caveat.
DEFAULT_INDEX = "CSO_CPA07_CPI"
INDEX_REGISTRY: dict[str, dict] = {
    "CSO_CPA07_CPI": {
        "file": "cso_cpi_deflator.parquet",
        "index_col": "cpi_index_chained",
        "label": "Consumer prices (CSO CPI)",
        "applies_to": "general",
        "source": "CSO CPA07 (chain-linked CPI, annual)",
        "caveat": (
            "General consumer-price inflation — a household basket. NOT construction, materials, "
            "labour-rate or tender-price inflation, and NOT the government-consumption deflator "
            "agencies use for public-spend real terms. Re-expresses purchasing power; not a cost "
            "today and not a bid price."
        ),
    },
    "CSO_GOV_CONSUMPTION": {
        "file": "cso_govt_consumption_deflator.parquet",
        "index_col": "gov_price_index",
        "label": "Government consumption (CSO National Accounts)",
        "applies_to": "public_spend",
        "source": "CSO NA007/NA008 (govt final-consumption current/constant implied deflator)",
        "caveat": (
            "The agency-standard deflator for public spending (the HM-Treasury GDP-deflator analog; "
            "uses the government-consumption component because a raw Irish GDP deflator is distorted "
            "by multinational activity). Annual; currently covers years ≤2024."
        ),
    },
    "CSO_WPM39_MATERIALS": {
        "file": "cso_construction_materials_deflator.parquet",
        "index_col": "wpm_index",
        "label": "Construction materials (CSO WPI, WPM39)",
        "applies_to": "construction_materials",
        "source": "CSO WPM39 (building & construction materials WPI)",
        "caveat": (
            "Construction MATERIALS only — excludes labour, plant and contractor margins. Short "
            "coverage (complete years 2021+); a secondary lens, not a whole-project cost index."
        ),
    },
    "SCSI_TPI_CONSTRUCTION": {
        "file": "scsi_tpi_deflator.parquet",
        "index_col": "tpi_index",
        "label": "Construction tender prices (SCSI TPI)",
        "applies_to": "construction",
        "source": "SCSI Tender Price Index (data/_meta)",
        "caveat": (
            "Tender prices including contractor margins — the quantity-surveyor 'cost to procure' "
            "lens, the right index for 'what would this construction work cost to award today'."
        ),
    },
}


def list_indices() -> list[dict]:
    """The registry as a list of {code, label, applies_to, source, caveat} — for a UI/API picker."""
    return [{"code": c, **{k: v for k, v in spec.items() if k != "index_col" and k != "file"}} for c, spec in INDEX_REGISTRY.items()]


class Deflator:
    """Year -> CPI index lookup with a pure ``inflate`` method. Immutable after construction."""

    def __init__(self, index_by_year: dict[int, float], base_year: int):
        if base_year not in index_by_year:
            raise ValueError(f"base_year {base_year} absent from index")
        self._index = dict(index_by_year)  # year -> chain-linked index level
        self.base_year = int(base_year)
        self.index_code = DEFAULT_INDEX  # overridden by load_index/load to the actual series
        self.meta: dict | None = None

    # ---- constructors -------------------------------------------------------
    @classmethod
    def load(cls, path: Path | str = _DEFAULT_PATH) -> "Deflator":
        """Load the CPI deflator from a precomputed gold parquet (the cached index). Kept for
        backward compatibility; for any other index use ``load_index``."""
        import polars as pl

        df = pl.read_parquet(path)
        base = int(df["base_year"][0])
        idx = {int(y): float(v) for y, v in zip(df["year"], df["cpi_index_chained"])}
        d = cls(idx, base)
        d.index_code = DEFAULT_INDEX
        d.meta = INDEX_REGISTRY[DEFAULT_INDEX]
        return d

    @classmethod
    def load_index(cls, index_code: str = DEFAULT_INDEX, gold_dir: Path | str = _GOLD) -> "Deflator":
        """Load ANY registered index by code (CPI / government-consumption / construction TPI /
        materials). Mirrors palewire ``cpi``'s ``series_id`` selector. The returned Deflator
        carries ``.index_code`` and ``.meta`` (label/source/caveat) so a consumer can always
        attach provenance to an adjusted figure. Unknown code → KeyError (never a silent CPI)."""
        import polars as pl

        spec = INDEX_REGISTRY.get(index_code)
        if spec is None:
            raise KeyError(f"unknown index_code {index_code!r}; known: {sorted(INDEX_REGISTRY)}")
        df = pl.read_parquet(Path(gold_dir) / spec["file"])
        base = int(df["base_year"][0])
        idx = {int(y): float(v) for y, v in zip(df["year"], df[spec["index_col"]])}
        d = cls(idx, base)
        d.index_code = index_code
        d.meta = spec
        return d

    # ---- core API -----------------------------------------------------------
    def has_year(self, year: int | None) -> bool:
        return year is not None and int(year) in self._index

    def factor(self, year: int, to: int | None = None) -> float | None:
        """Multiplicative factor to convert a value FROM ``year`` INTO ``to`` (default base).

        factor = index[to] / index[year]. Returns None if either year is missing — callers
        MUST treat None as "leave nominal / exclude", never as 1.0, so a missing year can
        never silently masquerade as 'no inflation'.
        """
        to = self.base_year if to is None else int(to)
        if year is None or int(year) not in self._index or to not in self._index:
            return None
        return self._index[to] / self._index[int(year)]

    def inflate(self, value: float | None, year: int, to: int | None = None) -> float | None:
        """Adjust ``value`` (expressed in ``year`` prices) into ``to`` prices (default base).

        Returns None if the value or year cannot be adjusted (missing year / null value) —
        never a silently-unadjusted number.
        """
        if value is None:
            return None
        f = self.factor(year, to)
        return None if f is None else float(value) * f

    # ---- vectorised precompute (Polars) -------------------------------------
    def deflate_series(self, df, value_col: str, year_col: str, out_col: str, to: int | None = None):
        """Return ``df`` with ``out_col`` = value_col adjusted from year_col into ``to`` prices.

        Rows whose year is missing from the index get NULL in out_col (not the nominal value),
        matching ``factor``'s None contract. Pure: never mutates value_col.
        """
        import polars as pl

        to = self.base_year if to is None else int(to)
        base_idx = self._index[to]
        factor_map = pl.DataFrame(
            {"_dfl_year": list(self._index), "_dfl_factor": [base_idx / v for v in self._index.values()]}
        )
        return (
            df.join(factor_map, left_on=year_col, right_on="_dfl_year", how="left")
            .with_columns((pl.col(value_col) * pl.col("_dfl_factor")).alias(out_col))
            .drop("_dfl_factor")
        )


# ---- fact-layer data-quality guard (separate from the math on purpose) -----
# A magnitude is "plausible" for a single award/payment line when it sits inside this band.
# This is DISTINCT from value_safe_to_sum (a dedup/ceiling concern): it catches parse
# artefacts — sub-€100 noise (e.g. a year/qty mis-read into the value), giant typos — BEFORE
# any deflation can scale them. Tuned to the eTenders extractor's own constants.
PLAUSIBLE_FLOOR_EUR = 100.0          # below this, a "contract award/payment" is almost certainly noise
PLAUSIBLE_CEILING_EUR = 5e8          # generic upper sanity bound; awards pass the tighter €50m review floor


def value_plausible_expr(col, lo: float = PLAUSIBLE_FLOOR_EUR, hi: float = PLAUSIBLE_CEILING_EUR):
    """Polars Expr: TRUE when a monetary value is a plausible single-line magnitude, NULL when
    the value itself is null (unknown, not 'implausible'). The single definition reused by the
    extractors (to persist the flag) and the gold patch (to backfill it)."""
    import polars as pl

    c = pl.col(col) if isinstance(col, str) else col
    return pl.when(c.is_null()).then(None).otherwise((c >= lo) & (c <= hi))


def implausible_mask(values, lo: float = PLAUSIBLE_FLOOR_EUR, hi: float = PLAUSIBLE_CEILING_EUR):
    """Boolean Polars expression flagging nominal values OUTSIDE a plausible award/payment
    range. Deflation cannot detect a mis-formatted magnitude (it just scales it), so the
    honest place to catch a parse error (e.g. €0.99 'awards', €2.5bn typos) is here, on the
    NOMINAL value, before any adjustment. ``hi`` aligns with the pipeline's is_large_award_review
    quarantine (>=€50m flagged); ``lo`` catches sub-€100 artefacts.

    Returns a Polars Expr; pass a column name or pl.col(...). Use to COUNT/REVIEW, never to
    silently drop.
    """
    import polars as pl

    col = pl.col(values) if isinstance(values, str) else values
    return (col.is_not_null()) & ((col < lo) | (col > hi))
