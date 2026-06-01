# ec_constituency_pop_2022.parquet — source citation

Companion to the gold parquet `data/gold/parquet/ec_constituency_pop_2022.parquet`
(gitignored — regenerated, not committed). One row per current Dáil constituency
with Census 2022 population, population-per-TD and seat count.

**In use:** powers the constituency civic-context card on the Member Overview
page (`v_member_constituency_demographics` → `member_overview.py`). Per the
project's editorial rule ("cite real-world claims"), the figures are sourced
below and the card carries an inline verify link to the report.

## Source

- **Electoral Commission (An Coimisiún Toghcháin), _Constituency Review Report
  2023_, Appendix 2 — "Statistics Relating to Recommended Dáil Constituencies."**
  Census 2022 population for each of the 43 recommended constituencies, on the
  **2023 boundaries** used from the November 2024 general election onward.
  - Report landing page: <https://www.electoralcommission.ie/publications/constituency-review-reports/>
  - The underlying population is CSO Census 2022; the Electoral Commission
    re-aggregated it to the new boundaries to balance the review.

## Why this source (not CSO PxStat FY005)

CSO FY005 ("Population of each Constituency of Dáil Éireann") is the only
natively constituency-keyed PxStat table, but it is drawn on the **2017**
boundaries (39 constituencies). The 34th Dáil sits on the **2023** boundaries
(43), so FY005 left the four split/new constituencies (Dublin Fingal East/West,
Tipperary North/South, Laois, Offaly, Wicklow-Wexford) with no clean row and
risked attaching 2017-shape headcounts to 2023-shape constituencies. The
Electoral Commission table fixes this: a verified **43/43** join to
`v_member_registry`, no aliasing.

**No 2016 comparison is published on the 2023 boundaries** (Census 2016 is on
yet-older boundaries), so the card intentionally shows 2022 figures only — a
2016→2022 growth across mixed boundary vintages would be misleading.

## How it is built

`pipeline_sandbox/ec_constituency_pop_extract.py` downloads the report PDF
(cached at `data/_meta/ec_review_2023.pdf`, gitignored), parses Appendix 2 via
PyMuPDF, and self-checks before writing: the 43 populations must sum to the
report's national total **5,149,139** and the derived seats to **174**.

```
python pipeline_sandbox/ec_constituency_pop_extract.py --write
```

## Integrity (verified 2026-06-01)

- 43 constituencies, population sum 5,149,139 (matches report total)
- seats sum 174 (matches the recommended Dáil size)
- 43/43 clean join to the live member registry
