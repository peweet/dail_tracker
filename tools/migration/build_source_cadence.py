"""Seed / sync the curated source-cadence ledger — the UNIFIED index of every
source we target, across every registry, with its refresh cadence + poller + tests.

The registries in this repo form a LAYER, not a duplicate set (see
doc/SOURCE_CADENCE_PROCEDURE.md):
  * `source_registry.generated.json` (129) — the canonical LIVE pipeline sources,
    generated from 5 in-code configs. THE spine.
  * `planning_rules/_corpus_registry/planning_corpus_seed.csv` (36) — a PARALLEL
    live registry for the planning corpus, which independently grew its own
    `update_cadence` column. Folded in here so cadence lives in ONE place.
  * seeds that FEED the canonical registry (procurement publishers_seed, la_seed)
    — inputs, not sources; NOT ingested (they arrive via the generated registry).
  * sandbox seeds (council_minutes, public_decisions, disclosed_po) — experimental
    TARGETS, not live; listed in the doc, not ingested until promoted.

This ledger adds the one thing no registry had for all sources: refresh CADENCE
(curated knowledge — council AFS each autumn, SIPO Jan/May/Sep) plus, per source,
the test package(s) that cover its extractor. It does NOT own cadence values — it
seeds a guess flagged `curated=no`; a human sets the real value and flips to `yes`.

Columns: source_id, name, group, registry, cadence, cadence_days, next_expected,
release_window, poller, runner, test_packages, curated, notes.

Usage
-----
    python tools/build_source_cadence.py            # create/sync + report drift
    python tools/build_source_cadence.py --check    # exit 1 on drift (CI ratchet)
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from tools.cadence_constants import CADENCE_DAYS  # noqa: E402 — one home for the label→days map

REGISTRY = PROJECT_ROOT / "data" / "_meta" / "source_registry.generated.json"
PLANNING_CORPUS = PROJECT_ROOT / "planning_rules" / "_corpus_registry" / "planning_corpus_seed.csv"
TEST_DIR = PROJECT_ROOT / "test"
LEDGER = PROJECT_ROOT / "data" / "_meta" / "source_cadence.csv"

FIELDS = [
    "source_id",
    "name",
    "group",
    "registry",
    "cadence",
    "cadence_days",
    "next_expected",
    "release_window",
    "poller",
    "runner",
    "test_packages",
    "curated",
    "notes",
]


def _label_for_days(days: int) -> str:
    best, name = 10**9, "review"
    for label, d in CADENCE_DAYS.items():
        if d and abs(d - days) < best:
            best, name = abs(d - days), label
    return name


def cadence_from_text(text: str) -> tuple[str, int]:
    """Map a free-text cadence hint (planning corpus `update_cadence`) to a band."""
    t = (text or "").lower()
    for label in ("daily", "weekly", "fortnightly", "monthly", "quarterly", "annual"):
        if label in t:
            return label, CADENCE_DAYS[label]
    if "near-weekly" in t or "near weekly" in t:
        return "weekly", 7
    return "review", 0


def guess_cadence(group: str, check_type: str, stale_after_days) -> tuple[str, int]:
    if stale_after_days:
        return _label_for_days(int(stale_after_days)), int(stale_after_days)
    g = (group or "").lower()
    if "afs" in g:
        return "annual", 365
    if check_type == "fixed_file":
        return "one_off", 0
    if "ipas" in g:
        return "quarterly", 90
    if "lobby" in g or "sipo" in g:
        return "triannual", 120
    if any(t in g for t in ("oireachtas", "vote", "question", "debate")):
        return "daily", 1
    if check_type == "index_poll":
        return "monthly", 30
    return "review", 0


def guess_runner(urls: list[str]) -> str:
    for u in urls:
        if u and str(u).startswith("http") and "gov.ie" in urlparse(u).netloc:
            return "residential"
    return "cloud"


@lru_cache(maxsize=1)
def _test_corpus() -> list[tuple[str, str]]:
    """(relpath, lowercased text) for every test module — read once."""
    out: list[tuple[str, str]] = []
    if not TEST_DIR.exists():
        return out
    for p in TEST_DIR.rglob("test_*.py"):
        if "__pycache__" in p.parts:
            continue
        try:
            out.append((p.relative_to(TEST_DIR).as_posix(), p.read_text(encoding="utf-8").lower()))
        except (OSError, UnicodeDecodeError):
            continue
    return out


def test_packages_for(token: str, limit: int = 4) -> str:
    """Rough map: test modules whose source names the extractor module/keyword.

    Not a coverage metric — a 'which tests to run when this source changes' hint.
    """
    if not token:
        return ""
    needle = token.lower()
    hits = [rel for rel, text in _test_corpus() if re.search(rf"\b{re.escape(needle)}\b", text)]
    if not hits:
        return "<none>"
    if len(hits) > limit:
        return "; ".join(sorted(hits)[:limit]) + f"; +{len(hits) - limit}"
    return "; ".join(sorted(hits))


def load_registry() -> list[dict]:
    if not REGISTRY.exists():
        print(f"registry not found: {REGISTRY}", file=sys.stderr)
        return []
    return json.loads(REGISTRY.read_text(encoding="utf-8")).get("sources", [])


def load_planning_corpus() -> list[dict]:
    if not PLANNING_CORPUS.exists():
        return []
    with PLANNING_CORPUS.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def load_ledger() -> dict[str, dict]:
    if not LEDGER.exists():
        return {}
    with LEDGER.open(encoding="utf-8", newline="") as fh:
        return {row["source_id"]: row for row in csv.DictReader(fh)}


def seed_registry_row(src: dict) -> dict:
    urls = [src.get("listing_url")] + (src.get("direct_files") or [])
    cadence, days = guess_cadence(src.get("group", ""), src.get("check_type", ""), src.get("stale_after_days"))
    poller = src.get("owner_module", "")
    return {
        "source_id": src["source_id"],
        "name": src.get("name", ""),
        "group": src.get("group", ""),
        "registry": "source_registry",
        "cadence": cadence,
        "cadence_days": days,
        "next_expected": "",
        "release_window": "",
        "poller": poller,
        "runner": guess_runner(urls),
        "test_packages": test_packages_for(poller),
        "curated": "no",
        "notes": "AUTO-SEEDED — set cadence + next_expected, then curated=yes",
    }


def seed_planning_row(row: dict) -> dict:
    cadence, days = cadence_from_text(row.get("update_cadence", ""))
    urls = [row.get("resolved_url", "")]
    return {
        "source_id": f"planning_corpus:{row.get('id', '?')}",
        "name": row.get("dataset", ""),
        "group": f"planning_{row.get('category', '')}",
        "registry": "planning_corpus",
        "cadence": cadence,
        "cadence_days": days,
        "next_expected": "",
        "release_window": row.get("update_cadence", ""),
        "poller": (row.get("poll_method", "") or "")[:60],
        "runner": guess_runner(urls),
        "test_packages": test_packages_for("planning_layers") or "<none>",
        "curated": "no",
        "notes": f"AUTO-SEEDED from planning_corpus_seed (status={row.get('status', '?')})",
    }


def all_seed_rows() -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for s in load_registry():
        rows[s["source_id"]] = seed_registry_row(s)
    for r in load_planning_corpus():
        row = seed_planning_row(r)
        rows[row["source_id"]] = row
    return rows


def write_ledger(rows: list[dict]) -> None:
    rows.sort(key=lambda r: r["source_id"])
    with LEDGER.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in FIELDS})


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="exit 1 on registry/ledger drift")
    args = ap.parse_args()

    seeds = all_seed_rows()
    if not seeds:
        print("no sources found in any registry", file=sys.stderr)
        return 1
    all_ids = set(seeds)
    existing = load_ledger()

    new_ids = all_ids - set(existing)
    gone_ids = set(existing) - all_ids

    if args.check:
        drift = False
        if new_ids:
            drift = True
            print(f"DRIFT — {len(new_ids)} source(s) in a registry not in the ledger:", file=sys.stderr)
            for sid in sorted(new_ids):
                print(f"  + {sid}", file=sys.stderr)
        if gone_ids:
            drift = True
            print(f"DRIFT — {len(gone_ids)} ledger row(s) whose source left every registry:", file=sys.stderr)
            for sid in sorted(gone_ids):
                print(f"  - {sid}", file=sys.stderr)
        if drift:
            print("\nRun `python tools/build_source_cadence.py` to sync.", file=sys.stderr)
            return 1
        print(f"OK — ledger covers all {len(all_ids)} sources across all registries, no orphans.")
        return 0

    # Preserve curated rows verbatim, but BACKFILL any newly-added column onto them.
    rows: list[dict] = []
    for sid in existing:
        if sid not in all_ids:
            continue
        row = dict(existing[sid])
        for col in FIELDS:
            row.setdefault(col, "")
        if not row.get("registry"):
            row["registry"] = seeds[sid]["registry"]
        if not row.get("test_packages"):
            row["test_packages"] = seeds[sid]["test_packages"]
        rows.append(row)
    for sid in sorted(new_ids):
        rows.append(seeds[sid])
    write_ledger(rows)

    by_reg: dict[str, int] = {}
    for r in rows:
        by_reg[r.get("registry", "?")] = by_reg.get(r.get("registry", "?"), 0) + 1
    curated = sum(1 for r in rows if r.get("curated") == "yes")
    print(f"[wrote {LEDGER}] {len(rows)} rows ({curated} curated) — by registry: {by_reg}")
    if new_ids:
        print(f"  +{len(new_ids)} newly seeded")
    if gone_ids:
        print(f"  -{len(gone_ids)} orphan row(s) dropped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
