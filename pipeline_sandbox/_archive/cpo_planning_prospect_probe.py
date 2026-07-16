"""Sandbox probe: one-off-dwelling PLANNING PROSPECT around a coordinate.

Context (own workstream): the CPO compensation thread. To value agricultural land taken under
CPO, the "no-scheme world" asks what residential/dwelling potential the land realistically had.
A data-grounded proxy for that prospect = the actual one-off-house DECISION record near the site:
how many one-off dwellings were applied for nearby, and the grant vs refusal split.

This replaces an earlier ad-hoc query that used a ±0.03 DEGREE SQUARE box and mislabelled it
"~3 km". That box is wrong two ways: (1) it is a square, not a radius, and (2) it never corrects
longitude for latitude, so at ~53.3 N a 0.03 deg box is ~3.3 km N-S but only ~2.0 km E-W. This
script uses a TRUE great-circle (haversine) radius so the catchment is an honest circle.

NO-INFERENCE: `decision_normalised` is the pipeline's own normalisation of the council decision;
this script only counts it. It does NOT read refusal REASONS (the silver has no reason column), so
it CANNOT attribute a refusal to the ring road / route safeguarding vs ordinary planning
constraints (SAC, landscape, wastewater). `LinkAppDetails` is emitted so a human can read the
actual reason on the council portal. Planning-application data is public record.

Run:
    python pipeline_sandbox/cpo_planning_prospect_probe.py                      # default site
    python pipeline_sandbox/cpo_planning_prospect_probe.py --lat 53.3003 --lon -9.0597 --km 3
    python pipeline_sandbox/cpo_planning_prospect_probe.py --km 1.5 --dump-refusals
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "pipeline_sandbox/_planning_output/planning_applications_silver.parquet"

# Great-circle distance (km) from the probe point (:lat0,:lon0) to each row's lat/lon.
_DIST_KM = """
2 * 6371 * asin(sqrt(
    pow(sin(radians(lat - {lat0}) / 2), 2)
    + cos(radians({lat0})) * cos(radians(lat))
      * pow(sin(radians(lon - {lon0}) / 2), 2)
))
"""


def probe(lat: float, lon: float, km: float, dump_refusals: bool = False) -> None:
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER}")
    con = duckdb.connect()
    dist = _DIST_KM.format(lat0=lat, lon0=lon)
    # Materialise the in-radius slice once, with the computed distance, so every later
    # query is an honest circle (not a square box).
    con.execute(f"""
        CREATE TABLE near AS
        SELECT *, {dist} AS dist_km
        FROM read_parquet('{SILVER.as_posix()}')
        WHERE lat IS NOT NULL AND lon IS NOT NULL AND geo_in_bounds
          AND {dist} <= {km}
    """)

    tot = con.execute("SELECT count(*) FROM near").fetchone()[0]
    oo = con.execute("SELECT count(*) FROM near WHERE is_one_off_house").fetchone()[0]
    print(f"\nProbe point ({lat}, {lon}) · TRUE radius {km} km")
    print(f"  total applications in radius : {tot}")
    print(f"  one-off-house applications   : {oo}")

    print("\n  one-off-house decisions (decision_normalised):")
    rows = con.execute(
        "SELECT decision_normalised, count(*) FROM near WHERE is_one_off_house "
        "GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()
    granted = refused = 0
    for d, n in rows:
        print(f"    {str(d):22s} {n}")
        if d in ("Granted", "Granted-Conditional"):
            granted += n
        elif d == "Refused":
            refused += n
    decided = granted + refused
    if decided:
        print(f"\n  GRANT RATE among decided one-off houses: "
              f"{granted}/{decided} = {granted/decided*100:.1f}%")
    else:
        print("\n  GRANT RATE: no decided one-off houses in radius")

    # authorities present (which DM-standards rulebook governs)
    print("\n  planning authorities in radius:")
    for a, n in con.execute(
        "SELECT PlanningAuthority, count(*) FROM near GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall():
        print(f"    {str(a):28s} {n}")

    if dump_refusals:
        print("\n  REFUSED one-off houses (read the reason yourself on the portal):")
        for addr, dt, dkm, link, desc in con.execute(
            "SELECT DevelopmentAddress, DecisionDate, dist_km, LinkAppDetails, "
            "DevelopmentDescription FROM near "
            "WHERE is_one_off_house AND decision_normalised = 'Refused' "
            "ORDER BY dist_km"
        ).fetchall():
            print(f"    {dkm:4.1f}km | {str(addr)[:34]:34s} | {dt} | {str(link or '')[:48]}")
            print(f"           {(str(desc) or '')[:80]}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--lat", type=float, default=53.3003439)
    ap.add_argument("--lon", type=float, default=-9.0596743)
    ap.add_argument("--km", type=float, default=3.0, help="true great-circle radius in km")
    ap.add_argument("--dump-refusals", action="store_true", help="list each refusal + portal link")
    a = ap.parse_args()
    probe(a.lat, a.lon, a.km, a.dump_refusals)


if __name__ == "__main__":
    main()
