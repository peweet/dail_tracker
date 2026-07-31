"""Public-body PO/payment extractor -> silver public_payments_fact. THIN CLI SHIM.

Wired into pipeline.py as the ``public_body_payments`` chain (invoked as this literal
script path -- pipeline.py is NOT edited by the C7 split, doc/REFACTORING_CANDIDATES.md).
The implementation lives in extractors/public_body_payments/ (config/readers/harvest/
emit/main.py); this file just re-exports the names sibling parsers and tests reach via
``pbe.X`` / ``from extractors.procurement_public_body_extract import X`` (several load
it by file path with importlib, so it must stay import-safe under a flat module name too)
and preserves the original CLI.

PRIVACY: supplier_class -> privacy_status, and the quarantine IS APPLIED (see
extractors/public_body_payments/emit.py:classify_and_flag -- moved verbatim, one-way and
safety-critical). Any row whose supplier looks like a sole trader / individual is marked
public_display=False so it can never surface in a UI or be promoted; rows are RETAINED
(nothing dropped) for analysis/coverage.

Run:
  ./.venv/Scripts/python.exe extractors/procurement_public_body_extract.py --list            # harvest-only (lock URLs)
  ./.venv/Scripts/python.exe extractors/procurement_public_body_extract.py --list --only ie_opw,ie_tii
  ./.venv/Scripts/python.exe extractors/procurement_public_body_extract.py                   # full ingest
  ./.venv/Scripts/python.exe extractors/procurement_public_body_extract.py --only ie_hse --max-files 2
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before numpy/pandas load anywhere downstream -- this script is
# a pipeline.py chain entry point, run as its own process. Ordering is the contract; see
# services/runtime_env.py.
import services.runtime_env  # noqa: F401,E402
# isort: on

import contextlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from extractors.public_body_payments.config import (  # noqa: E402
    PARSER_VERSION,
    PAYMENTS_FACT_SCHEMA_COLS,
    PUBLISHERS,
)
from extractors.public_body_payments.emit import (  # noqa: E402
    DEDUP_SIG,
    canonicalise_supplier_raw,
    classify_and_flag,
    dedup_source_repeats,
    emit_rows,
    flag_unidentifiable_suppliers,
)
from extractors.public_body_payments.harvest import (  # noqa: E402
    HREF_RE,
    fetch_bytes,
    fetch_text,
    fetch_to_bronze,
    harvest_files,
)
from extractors.public_body_payments.main import main  # noqa: E402
from extractors.public_body_payments.readers import (  # noqa: E402
    clean_supplier,
    detect_roles_tab,
    period_from_url,
    read_courts,
    read_csv,
    read_defence,
    read_housing,
    read_pdf_reading_order_fallback,
    read_reading_order,
    read_xls,
    read_xlsx,
    to_eur,
)

__all__ = [
    "DEDUP_SIG",
    "HREF_RE",
    "PARSER_VERSION",
    "PAYMENTS_FACT_SCHEMA_COLS",
    "PUBLISHERS",
    "canonicalise_supplier_raw",
    "classify_and_flag",
    "clean_supplier",
    "dedup_source_repeats",
    "detect_roles_tab",
    "emit_rows",
    "fetch_bytes",
    "fetch_text",
    "fetch_to_bronze",
    "flag_unidentifiable_suppliers",
    "harvest_files",
    "main",
    "period_from_url",
    "read_courts",
    "read_csv",
    "read_defence",
    "read_housing",
    "read_pdf_reading_order_fallback",
    "read_reading_order",
    "read_xls",
    "read_xlsx",
    "to_eur",
]

if __name__ == "__main__":
    main()
