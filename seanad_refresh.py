"""seanad_refresh.py — Seanad (Senator) parity chain.

Produces the Senator equivalents of the Dáil gold datasets by REUSING the
production parsers/enrichers with Senator inputs — no parser logic is
duplicated. Mirrors the structure of payments_refresh.py / attendance_refresh.py
but calls the (now chamber-parameterised) functions in-process, since the
Senator steps need explicit dir/output/house arguments the standalone
__main__ entrypoints don't take.

Steps:
    1. poll      Senator payment + attendance PDFs (reuse oireachtas_pdf_poller)
    2. votes     fetch chamber=seanad + transform_votes.build_seanad_votes_silver
    3. attendance attendance.main(Senator dir + house="Seanad")
    4. payments  payments_full_psa_etl.build_full_psa(Senator dir + house="Seanad")
    5. gold      enrich.main_seanad()  (reuses every _build_* helper)

Depends on bootstrap having flattened flattened_seanad_members.csv. Safe to run
standalone (step 1 polls the PDFs it needs).

CLI:
    python seanad_refresh.py
"""

from __future__ import annotations

import logging
import time

import attendance
import enrich
import payments_full_psa_etl as payments
import payments_member_enrichment
import transform_votes
from config import (
    ATTENDANCE_PDF_DIR_SEANAD,
    GOLD_CSV_DIR,
    GOLD_PARQUET_DIR,
    PAYMENTS_PDF_DIR_SEANAD,
    SEANAD_PAYMENTS_PARQUET,
    SILVER_DIR,
    SILVER_PARQUET_DIR,
)
from pdf_infra.oireachtas_pdf_poller import SOURCES, run_one
from services.votes import fetch_votes

_SEANAD_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"

_log = logging.getLogger("seanad_refresh")

# Senator silver outputs (distinct names so the Dáil ETL globs never collide).
_VOTES_CSV = SILVER_DIR / "seanad_pretty_votes.csv"
_ATT_SILVER_CSV = SILVER_DIR / "seanad_aggregated_tables.csv"
_ATT_FACT_CSV = SILVER_DIR / "seanad_attendance_fact_table.csv"
_ATT_FACT_PARQUET = SILVER_PARQUET_DIR / "seanad_attendance_fact_table.parquet"
_PAY_CSV = GOLD_CSV_DIR / "seanad_payments_full_psa.csv"
_PAY_QUARANTINE = GOLD_PARQUET_DIR / "seanad_payments_full_psa_quarantine.parquet"


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def step_poll() -> bool:
    _hr("[1/5] poll — Senator payment + attendance PDFs")
    ok = True
    for key in ("payments_seanad", "attendance_seanad"):
        result = run_one(SOURCES[key])
        print(f"  {key}: status={result['status']} downloaded={result['downloaded']}")
        ok = ok and result["status"] in {"ok", "no_entries"}
    return ok


def step_votes() -> bool:
    _hr("[2/5] votes — fetch chamber=seanad + reuse normalize_vote_data")
    payloads, _bytes = fetch_votes("seanad")
    results = [div for payload in payloads for div in payload.get("results", [])]
    transform_votes.build_seanad_votes_silver(results, _VOTES_CSV)
    return True


def step_attendance() -> bool:
    _hr("[3/5] attendance — reuse attendance.main (Senator dir, house=Seanad)")
    rc = attendance.main(
        pdf_dir=ATTENDANCE_PDF_DIR_SEANAD,
        silver_csv=_ATT_SILVER_CSV,
        fact_csv=_ATT_FACT_CSV,
        fact_parquet=_ATT_FACT_PARQUET,
        house="Seanad",
    )
    return rc == 0


def step_payments() -> bool:
    _hr("[4/5] payments — reuse build_full_psa (Senator dir, house=Seanad)")
    stats = payments.build_full_psa(
        pdf_dir=PAYMENTS_PDF_DIR_SEANAD,
        out_parquet=SEANAD_PAYMENTS_PARQUET,
        out_csv=_PAY_CSV,
        quarantine_parquet=_PAY_QUARANTINE,
        house="Seanad",
    )
    print(f"  clean={stats['clean_rows']:,} quarantine={stats['quarantine_rows']:,}")
    # Attach unique_member_code/party/constituency so the per-member payments
    # panel resolves senators (reuses the Dáil fuzzy-key enricher).
    enr = payments_member_enrichment.enrich(
        payments_parquet=SEANAD_PAYMENTS_PARQUET, members_parquet=_SEANAD_MEMBERS_PARQUET
    )
    print(f"  enriched: {enr['members_resolved']} senators resolved ({enr['rows_matched']}/{enr['rows_total']} rows)")
    return True


def step_gold() -> bool:
    _hr("[5/5] gold — reuse enrich.main_seanad()")
    return enrich.main_seanad() == 0


def main() -> int:
    steps = [
        ("poll", step_poll),
        ("votes", step_votes),
        ("attendance", step_attendance),
        ("payments", step_payments),
        ("gold", step_gold),
    ]
    failed: list[str] = []
    for name, fn in steps:
        t = time.monotonic()
        try:
            ok = fn()
        except Exception as exc:  # noqa: BLE001 — keep going; report at end
            _log.exception("seanad step %s crashed", name)
            print(f"  ERROR in {name}: {exc}")
            ok = False
        print(f"  {name} done in {time.monotonic() - t:.1f}s ({'ok' if ok else 'FAILED'})")
        if not ok:
            failed.append(name)

    print("\n=== seanad_refresh summary ===")
    if failed:
        print("Failed steps: " + ", ".join(failed))
        return 1
    print("Seanad chain complete.")
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
