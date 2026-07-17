"""FACT CARDS — a machine-readable metadata index of every silver/gold parquet.

WHY THIS EXISTS. The project has 55 excellent *domain* MCP tools but zero *metadata* tools, so
the cheapest question an agent can ask — "what columns does this fact have?", "how many rows?",
"what years?" — forces the most expensive action: a throwaway polars script, or a Read that
blows the context window (speeches_fact_full.parquet = 204 MB ≈ 496k tokens). Yet a parquet
gives up its full schema + row count from the FOOTER in ~30ms.

This builds `data/_meta/fact_cards.json` — one small card per fact — by unioning the metadata that
already exists (nothing is recomputed that a regression test already stored) and adding the three
things nobody records: grain, year-span, and the never-sum money class:

  rows + columns        ← data/_meta/output_baseline.json      (the regression baseline)
  null / dup / sentinels← data/_meta/gold_quality_baseline.json
  size + read_by_views  ← data/_meta/runtime_data_manifest.json (its reader reverse-index)
  freshness             ← data/_meta/freshness.json
  columns + year-span   ← the parquet FOOTER (scan_parquet, no data read) for anything the
                          baseline misses (all silver, ~30 gold)
  grain/purpose/never-sum← data/_meta/fact_grain.csv  (hand-curated seed; optional, enriches only)

Served by the MCP `describe_dataset` / `list_datasets` tools and safe to Read directly (~80 KB).

Run:  python tools/build_fact_cards.py           (writes fact_cards.json)
      python tools/build_fact_cards.py --check   (CI: fail if a fact is missing a card)
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
META = ROOT / "data" / "_meta"
OUT = META / "fact_cards.json"
GRAIN_SEED_YAML = META / "fact_contracts.yaml"
GRAIN_SEED_CSV = META / "fact_grain.csv"  # legacy fallback (fragile: unquoted grain commas)
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

LAYERS = {"gold": ROOT / "data/gold/parquet", "silver": ROOT / "data/silver/parquet"}
_YEAR_COL = re.compile(r"(^|_)(year|yr)$", re.I)
_DATE_COL = re.compile(r"(date|_at$|_on$|period)", re.I)


def _load(name: str) -> dict:
    p = META / name
    if not p.exists():
        return {}
    with contextlib.suppress(Exception):
        return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _footer(path: Path) -> tuple[int, dict[str, str], list[int] | None]:
    """Schema + row count from the parquet FOOTER only (no row data read), plus a best-effort
    year span from any year/date column. This is the ~30ms move the whole file exists to enable."""
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    cols = {n: str(t) for n, t in schema.items()}
    rows = lf.select(pl.len()).collect().item()
    span = None
    yr_col = next((n for n in cols if _YEAR_COL.search(n)), None)
    dt_col = None if yr_col else next((n for n in cols if _DATE_COL.search(n)), None)
    if yr_col:
        with contextlib.suppress(Exception):
            mn, mx = lf.select(pl.col(yr_col).min(), pl.col(yr_col).max()).collect().row(0)
            if mn is not None and mx is not None:
                yrs = [int(re.sub(r"\D", "", str(mn))[:4] or 0), int(re.sub(r"\D", "", str(mx))[:4] or 0)]
                span = yrs if all(1900 < y < 2100 for y in yrs) else None
    elif dt_col:
        with contextlib.suppress(Exception):
            vals = lf.select(pl.col(dt_col).cast(pl.Utf8)).collect().to_series().drop_nulls()
            yrs = sorted({int(m.group()) for v in vals if (m := re.search(r"(19|20)\d{2}", str(v)))})
            span = [yrs[0], yrs[-1]] if yrs else None
    return rows, cols, span


def _grain_seed() -> dict[str, dict]:
    """The hand-curated semantic layer (grain / money_grain / never_sum_with / purpose).

    Prefers the YAML contract (`fact_contracts.yaml`), which holds commas/€/em-dashes
    in grain strings without the quoting fragility that corrupted the legacy CSV.
    Falls back to `fact_grain.csv` only if the YAML is absent.
    """
    if GRAIN_SEED_YAML.exists():
        import yaml  # pyyaml — already a dep (page_contracts are YAML)

        with contextlib.suppress(Exception):
            raw = yaml.safe_load(GRAIN_SEED_YAML.read_text(encoding="utf-8")) or {}
            out: dict[str, dict] = {}
            for fact, fields in raw.items():
                if not isinstance(fields, dict):
                    continue
                clean: dict[str, str] = {}
                for k, v in fields.items():
                    if k == "fact" or v is None:
                        continue
                    if isinstance(v, (list, tuple)):  # tolerate never_sum_with as a real list
                        v = "|".join(str(x) for x in v)
                    s = str(v).strip()
                    if s:
                        clean[k] = s
                if str(fact).strip():
                    out[str(fact).strip()] = clean
            return out
    if not GRAIN_SEED_CSV.exists():
        return {}
    with GRAIN_SEED_CSV.open(encoding="utf-8", newline="") as fh:
        out = {}
        for r in csv.DictReader(fh):
            key = (r.get("fact") or "").strip()
            if not key:
                continue
            # guard str-only: a stray comma can push overflow under DictReader's None restkey (a list)
            out[key] = {
                k: v.strip()
                for k, v in r.items()
                if isinstance(k, str) and k != "fact" and isinstance(v, str) and v.strip()
            }
        return out


def build() -> dict:
    quality = _load("gold_quality_baseline.json")
    quality = quality.get("baseline", quality)  # tolerate either shape
    manifest = _load("runtime_data_manifest.json")
    freshness = _load("freshness.json")
    seed = _grain_seed()

    # reader reverse-index (view/sql → parquet) from the manifest, keyed by basename
    readers: dict[str, list[str]] = {}
    for section in ("files", "entries", "optional_untracked", "runtime"):
        for e in (manifest.get(section) or []) if isinstance(manifest.get(section), list) else []:
            if isinstance(e, dict) and e.get("path"):
                rd = e.get("readers") or e.get("kept_because")
                if rd:
                    readers[Path(e["path"]).name] = rd if isinstance(rd, list) else [rd]

    # freshness keyed loosely by dataset name fragment
    fresh_by = {k: v for k, v in freshness.items() if isinstance(v, dict)} if isinstance(freshness, dict) else {}

    cards: dict[str, dict] = {}
    for layer, d in LAYERS.items():
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.parquet")):
            fname, stem = p.name, p.stem
            card: dict = {"file": f"data/{'gold' if layer == 'gold' else 'silver'}/parquet/{fname}", "layer": layer}
            # rows + columns + year-span ALWAYS from the live footer (the regression baseline goes
            # stale the moment a fact is rebuilt — as procurement_payments_fact did this session —
            # and the footer read is ~30ms, so there is no reason to trust a snapshot).
            with contextlib.suppress(Exception):
                rows, cols, span = _footer(p)
                card["rows"] = rows
                card["columns"] = list(cols)
                card["column_types"] = cols
                if span:
                    card["year_span"] = span
            q = quality.get(fname, {})
            if q:
                card["dq"] = {
                    k: q[k] for k in ("dup_rows", "all_null_cols", "sentinels") if q.get(k)
                }
            if fname in readers:
                card["read_by"] = readers[fname]
            card["size_bytes"] = p.stat().st_size
            # freshness by best-effort name match
            fr = fresh_by.get(stem) or next((v for k, v in fresh_by.items() if k and k in stem), None)
            if fr:
                card["freshness"] = {k: fr[k] for k in ("built_at", "age_days", "status") if k in fr}
            # hand-curated grain / purpose / never-sum
            if stem in seed:
                card.update(seed[stem])
            cards[stem] = card
    return {
        "generated_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "note": "Metadata index of every silver/gold parquet. Read a card before scanning a fact. "
        "money facts carry never_sum_with — obey the 3-money-grain rule.",
        "count": len(cards),
        "facts": dict(sorted(cards.items())),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="CI: fail if a parquet has no card")
    args = ap.parse_args()

    data = build()
    if args.check:
        have = set(data["facts"])
        missing = []
        for layer, d in LAYERS.items():  # noqa: B007
            if d.is_dir():
                missing += [p.stem for p in d.glob("*.parquet") if p.stem not in have]
        # contract drift: a semantic-contract entry that names a fact with no parquet
        orphan = sorted(k for k in _grain_seed() if k not in have)
        problems = []
        if missing:
            problems.append(f"{len(missing)} parquet(s) without a card: {sorted(missing)[:8]}")
        if orphan:
            problems.append(
                f"{len(orphan)} contract entr(ies) in fact_contracts.yaml name a fact "
                f"with no parquet (drift): {orphan}"
            )
        if problems:
            for p in problems:
                print(f"fact-cards: {p}")
            return 1
        print(f"fact-cards: OK — {data['count']} facts carded, contract clean")
        return 0

    OUT.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    n_span = sum(1 for c in data["facts"].values() if "year_span" in c)
    n_grain = sum(1 for c in data["facts"].values() if "grain" in c)
    print(f"wrote {OUT}  ({data['count']} facts · {n_span} with year-span · {n_grain} with curated grain)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
