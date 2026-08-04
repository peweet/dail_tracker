"""Build the minimal external dataset used by the installed-wheel API smoke.

The fixture contains the two empty, schema-correct payment parquet files needed
to register ``v_payments_base``. It intentionally contains no real records and
is written outside the wheel, proving that ``DAIL_DATA_DIR`` is the deployment
data seam rather than accidentally reading from the source checkout.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

_EMPTY_PAYMENTS_SQL = """
SELECT
    NULL::VARCHAR AS member_name,
    NULL::VARCHAR AS position,
    NULL::VARCHAR AS payment_kind,
    NULL::VARCHAR AS taa_band_raw,
    NULL::VARCHAR AS taa_band_label,
    NULL::DATE AS date_paid,
    NULL::VARCHAR AS narrative,
    NULL::DOUBLE AS amount,
    NULL::VARCHAR AS source_pdf,
    NULL::VARCHAR AS schema,
    NULL::VARCHAR AS unique_member_code,
    NULL::VARCHAR AS party_name,
    NULL::VARCHAR AS constituency,
    NULL::VARCHAR AS house
WHERE FALSE
"""


def build_fixture(data_dir: Path) -> tuple[Path, Path]:
    """Write schema-only Dáil and Seanad payment facts under ``data_dir``."""
    if not data_dir.is_absolute():
        raise ValueError("data_dir must be absolute")
    destination = data_dir.expanduser().resolve() / "gold" / "parquet"
    destination.mkdir(parents=True, exist_ok=True)
    outputs = (
        destination / "payments_full_psa.parquet",
        destination / "seanad_payments_full_psa.parquet",
    )
    connection = duckdb.connect()
    try:
        relation = connection.sql(_EMPTY_PAYMENTS_SQL)
        for output in outputs:
            relation.write_parquet(str(output))
    finally:
        connection.close()
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True, type=Path, help="absolute external data root")
    args = parser.parse_args()
    try:
        outputs = build_fixture(args.data_dir)
    except (OSError, ValueError, duckdb.Error) as exc:
        parser.error(str(exc))
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
