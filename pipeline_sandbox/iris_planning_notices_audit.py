"""SPIKE (sandbox): audit the planning-related notices in the Iris Oifigiúil corpus.

Question being answered: "the Iris notices have a lot of planning declarations —
have we ever checked?" + a follow-up "check for CPOs".

Input : data/silver/iris_oifigiuil/iris_notice_events_clean.csv
        (50,042 cleaned notices, 2016-01 .. 2026-06; columns incl. notice_category,
        notice_subtype, classification_flags, title, raw_text, issue_date,
        si_policy_domain_primary, si_parent_legislation.)

Output: pipeline_sandbox/_iris_planning_output/
        - iris_planning_notices.parquet / .csv   one row per planning notice + planning_subtype
        - iris_planning_by_subtype.csv            counts per subtype
        - iris_planning_by_subtype_year.csv       counts per subtype × year
        - iris_cpo_notices.csv                    the CPO / road-scheme slice (the follow-up)
        - SUMMARY.md                              human-readable findings

Self-contained: reads silver, writes a sandbox dir, does NOT import the main
pipeline and does NOT touch gold. Discardable. (Pandas/DuckDB for the spike;
Polars + sql_views on integration.)

KEY FINDING (no-inference): Iris carries the national planning LEGISLATION
(Planning & Development Act SIs/orders), State self-development approvals
(Section 181), marine consents (Foreshore Act), An Bord Pleanála items, and a
thin band of local-authority planning + CPO/road-scheme notices. It does NOT
carry per-application planning permission decisions or per-site Section 5
exempted-development declarations — those live on council registers (consistent
with doc/PLANNING_PERMISSION_SCOPING.md §10.1).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

SILVER = Path("data/silver/iris_oifigiuil/iris_notice_events_clean.csv")
OUT = Path("pipeline_sandbox/_iris_planning_output")

# ---- planning sub-type classification ----------------------------------------
# Priority-ordered SQL CASE: the first matching branch wins, so put the most
# specific / highest-value sub-types first. All matching is upper-cased.
# `U` = title (upper), `R` = raw_text (upper), `D` = si_policy_domain_primary,
# `F` = classification_flags, `C` = notice_category.
SUBTYPE_CASE = """
CASE
  -- CPO / road scheme (the follow-up check): explicit compulsory-purchase wording,
  -- OR a Roads Act 1993 SCHEME notice (notice of intention/decision / motorway /
  -- acquisition). Deliberately EXCLUDES Roads Act classification orders, the NRA
  -- superannuation scheme, and EU EIA-of-roads regulations (not CPOs).
  WHEN R LIKE '%COMPULSORY PURCHASE%' OR R LIKE '%COMPULSORILY ACQUIR%'
       OR R LIKE '%ACQUISITION OF LAND%'
       OR ((R LIKE '%ROADS ACT 1993%' OR R LIKE '%ROADS ACT, 1993%')
           AND (R LIKE '%NOTICE OF INTENTION%' OR R LIKE '%NOTICE OF DECISION%'
                OR R LIKE '%MOTORWAY%' OR R LIKE '%ACQUIR%'
                OR R LIKE '%APPROVED ROAD DEVELOPMENT%' OR R LIKE '%PROTECTED ROAD%'))
    THEN 'cpo_or_road_scheme'
  -- Section 181: State's own developments approved by the Minister.
  WHEN F LIKE '%si_section_181_state_development%'
       OR U LIKE '%SECTION 181%' OR U LIKE '%SECTION 181(2)%'
    THEN 'section_181_state_development'
  -- Foreshore: ministerial marine/coastal planning consents.
  WHEN F LIKE '%si_foreshore_act_decision%' OR U LIKE '%FORESHORE%' OR R LIKE '%FORESHORE ACT%'
    THEN 'foreshore_consent'
  -- Exempted-development regulations (national class-exemption rules).
  WHEN U LIKE '%EXEMPTED DEVELOPMENT%'
    THEN 'exempted_development_regs'
  -- An Bord Pleanála / An Coimisiún Pleanála items (excluding board appointments,
  -- which sit in public_appointment and are handled by that enrichment).
  WHEN (R LIKE '%BORD PLEAN%LA%' OR U LIKE '%PLEAN%LA%') AND C <> 'public_appointment'
    THEN 'an_bord_pleanala_notice'
  -- Local-authority planning notices: development plan / LAP / variation / SDZ /
  -- Part 8 (LA own development) / Section 12 plan-making notices.
  WHEN C = 'local_authority_notice'
       AND (U LIKE '%DEVELOPMENT PLAN%' OR R LIKE '%DEVELOPMENT PLAN%'
            OR U LIKE '%LOCAL AREA PLAN%' OR U LIKE '%VARIATION%'
            OR U LIKE '%STRATEGIC DEVELOPMENT ZONE%' OR U LIKE '%PART 8%'
            OR U LIKE '%PART XI%' OR R LIKE '%PLANNING AND DEVELOPMENT ACT%'
            OR R LIKE '%SECTION 12%')
    THEN 'la_planning_notice'
  -- National planning legislation in the SI bucket: Planning & Development Act
  -- titles, or the SI taxonomy's planning policy domain.
  WHEN C = 'statutory_instrument'
       AND (U LIKE '%PLANNING AND DEVELOPMENT%' OR D = 'housing_planning_local_gov')
    THEN 'planning_legislation_si'
  ELSE NULL
END
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    base = f"""
        SELECT issue_date, issue_number, notice_category, notice_subtype, title,
               display_title, si_number, si_year, si_parent_legislation,
               si_policy_domain_primary, classification_flags, raw_text,
               upper(CAST(title AS VARCHAR))     AS U,
               upper(CAST(raw_text AS VARCHAR))  AS R,
               CAST(si_policy_domain_primary AS VARCHAR) AS D,
               CAST(classification_flags AS VARCHAR)     AS F,
               CAST(notice_category AS VARCHAR)          AS C
        FROM read_csv_auto('{SILVER.as_posix()}')
    """
    con.execute(f"CREATE TABLE notices AS {base}")
    con.execute(f"ALTER TABLE notices ADD COLUMN planning_subtype VARCHAR")
    con.execute(f"UPDATE notices SET planning_subtype = ({SUBTYPE_CASE})")

    total = con.execute("SELECT count(*) FROM notices").fetchone()[0]
    planning = con.execute("SELECT count(*) FROM notices WHERE planning_subtype IS NOT NULL").fetchone()[0]

    # --- row-level planning notices -----------------------------------------
    cols = ("issue_date, issue_number, planning_subtype, notice_category, notice_subtype, "
            "title, display_title, si_number, si_year, si_parent_legislation, classification_flags")
    df = con.execute(
        f"SELECT {cols} FROM notices WHERE planning_subtype IS NOT NULL "
        "ORDER BY planning_subtype, issue_date"
    ).df()
    df.to_parquet(OUT / "iris_planning_notices.parquet", compression="zstd")
    df.to_csv(OUT / "iris_planning_notices.csv", index=False)

    # --- by subtype ----------------------------------------------------------
    by_sub = con.execute(
        "SELECT planning_subtype, count(*) n, min(issue_date) first_seen, max(issue_date) last_seen "
        "FROM notices WHERE planning_subtype IS NOT NULL GROUP BY 1 ORDER BY 2 DESC"
    ).df()
    by_sub.to_csv(OUT / "iris_planning_by_subtype.csv", index=False)

    # --- by subtype x year ---------------------------------------------------
    by_year = con.execute(
        "SELECT planning_subtype, year(issue_date) yr, count(*) n "
        "FROM notices WHERE planning_subtype IS NOT NULL GROUP BY 1,2 ORDER BY 1,2"
    ).df()
    by_year.to_csv(OUT / "iris_planning_by_subtype_year.csv", index=False)

    # --- CPO slice (the follow-up) ------------------------------------------
    cpo = con.execute(
        f"SELECT {cols}, raw_text FROM notices WHERE planning_subtype = 'cpo_or_road_scheme' "
        "ORDER BY issue_date"
    ).df()
    cpo.drop(columns=["raw_text"]).to_csv(OUT / "iris_cpo_notices.csv", index=False)

    # --- sample titles per subtype ------------------------------------------
    samples = {}
    for st in by_sub["planning_subtype"]:
        rows = con.execute(
            "SELECT DISTINCT title FROM notices WHERE planning_subtype = ? AND title IS NOT NULL LIMIT 6",
            [st],
        ).fetchall()
        samples[st] = [(r[0] or "").replace("\n", " ").strip()[:100] for r in rows]

    # --- SUMMARY.md ----------------------------------------------------------
    lines = [
        "# Iris Oifigiúil — Planning Notices Audit",
        "",
        f"Source: `{SILVER.as_posix()}` · notices: **{total:,}** · date range 2016-01 .. 2026-06.",
        f"Planning-related notices identified: **{planning:,}** ({planning/total*100:.1f}% of corpus).",
        "",
        "> No-inference: Iris carries national planning **legislation** + State (Section 181) /",
        "> marine (Foreshore) consents + An Bord Pleanála items + a thin band of local-authority",
        "> planning & CPO/road-scheme notices. It does **not** carry per-application planning",
        "> permission decisions or per-site Section 5 exempted-development declarations (those are",
        "> on council registers — see doc/PLANNING_PERMISSION_SCOPING.md §10.1).",
        "",
        "## Counts by sub-type",
        "",
        "| Sub-type | Count | First | Last |",
        "|---|---|---|---|",
    ]
    for _, r in by_sub.iterrows():
        lines.append(f"| {r.planning_subtype} | {r.n} | {r.first_seen} | {r.last_seen} |")
    lines += ["", "## Sample titles", ""]
    for st, ts in samples.items():
        lines.append(f"### {st}")
        lines += [f"- {t}" for t in ts] + [""]
    cpo_n = len(cpo)
    lines += [
        "## CPO / road-scheme slice (follow-up check)",
        "",
        f"**{cpo_n}** notices matched CPO / Roads-Act-1993-scheme wording. CPOs are **sparse** in",
        "Iris — most compulsory-purchase notices are published in newspapers and on An Bord",
        "Pleanála / council sites, not Iris Oifigiúil. The matches here are mainly Roads Act 1993",
        "motorway/protected-road scheme notices plus a few explicit CPOs. See `iris_cpo_notices.csv`.",
    ]
    (OUT / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")

    # --- console report ------------------------------------------------------
    print(f"Iris notices: {total:,} | planning-related: {planning:,} ({planning/total*100:.1f}%)")
    print("\nBy sub-type:")
    for _, r in by_sub.iterrows():
        print(f"  {r.n:>5}  {r.planning_subtype}  ({r.first_seen} .. {r.last_seen})")
    print(f"\nCPO / road-scheme slice: {cpo_n} notices -> iris_cpo_notices.csv")
    print(f"\nOutputs written to {OUT}/")


if __name__ == "__main__":
    main()
