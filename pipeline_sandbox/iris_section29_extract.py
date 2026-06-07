"""SPIKE (sandbox): structure the unextracted Iris Oifigiúil Section-29
member-interest notices.

Input : data/silver/iris_oifigiuil/iris_member_interests_raw_pages.json
        (116 raw OCR'd notices — Statements of Registrable Interests under the
        Ethics in Public Office Acts, published in Iris Oifigiúil; only raw text +
        a rough detected_member_names tag, nothing structured.)

Output: pipeline_sandbox/_iris_s29_output/
        - iris_s29_statements.(parquet|csv)   one row per (notice × member)
        - iris_s29_member_summary.csv         per-member rollup + red-flag metric

Self-contained. Reads silver, writes a sandbox dir. Does NOT import the main
pipeline and does NOT touch gold. Discardable. (Pandas for the spike; Polars on
integration.)

The high-value signal isn't item-level detail — it's the RED FLAG metric:
`max_years_one_notice` = the most calendar years a member back-filled in a single
notice. A late bulk supplement (e.g. declaring 7+ past years at once) is the data
signature of the kind of under-declaration that forced Robert Troy's 2022
resignation. We also capture which interest CATEGORIES appear (Directorships,
Shares, Land/Property, Occupations, Gifts, Travel, Contracts).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

SRC = Path("data/silver/iris_oifigiuil/iris_member_interests_raw_pages.json")
OUT = Path("pipeline_sandbox/_iris_s29_output")
OUT.mkdir(parents=True, exist_ok=True)

_CATS: dict[str, tuple[str, ...]] = {
    "Directorships": ("directorship",),
    "Shares": ("shares",),
    "Land/Property": ("land", "property", "premises", "dwelling", "residence", "acres", "hectares"),
    "Occupations": ("occupation", "remunerated", "profession", "employment", "trade", "consultanc"),
    "Gifts": ("gift",),
    "Travel/Hospitality": ("travel", "hospitality", "supply of property"),
    "Contracts": ("contract",),
}

_TITLE_RE = re.compile(r"\b(Senator|Deputy|Minister of State|Minister|TD|T\.D\.|Cllr)\b", re.I)
_YEAR_RE = re.compile(r"to\s+\d{1,2}(?:st|nd|rd|th)?\s+december\s+(\d{4})", re.I)  # Jan–Dec period → year
_ANCHOR = "Name of Member concerned"


def _norm(t):
    s = t or ""
    for a, b in (("’", "'"), ("\xa0", " "), (" ", " ")):
        s = s.replace(a, b)
    return s


def clean_name(raw):
    if not raw:
        return None
    n = _TITLE_RE.sub("", raw)
    n = re.split(r"[,(]", n)[0]  # drop constituency after a comma / open-paren
    n = re.sub(r"\s+", " ", n).strip(" .-,:–")
    return n or None


def _cats(text):
    low = text.lower()
    return {c for c, kws in _CATS.items() if any(k in low for k in kws)}


def parse_notice(it):
    """One row per (notice × distinct member) — years back-filled + categories unioned."""
    issue_date, src = it.get("issue_date"), it.get("source_file")
    full = "\n".join(_norm(p.get("raw_text", "")) for p in (it.get("pages") or []))
    detected = [clean_name(d) for d in (it.get("detected_member_names") or []) if d]

    segs = full.split(_ANCHOR)
    if len(segs) <= 1:
        years = sorted(set(_YEAR_RE.findall(full)))
        cats = _cats(full)
        return [_row(issue_date, src, m, years, cats, parsed=False) for m in (detected or [None])]

    by_member = {}
    for seg in segs[1:]:
        nm = re.match(r"\s*:?\s*([^\n]+)", seg)
        member = clean_name(nm.group(1)) if nm else None
        body = seg[:1500]
        b = by_member.setdefault(member, {"years": set(), "cats": set()})
        b["years"].update(_YEAR_RE.findall(body))
        b["cats"].update(_cats(body))
    return [_row(issue_date, src, m, sorted(b["years"]), sorted(b["cats"]), parsed=True) for m, b in by_member.items()]


def _row(issue_date, src, member, years, cats, *, parsed):
    return {
        "issue_date": issue_date,
        "source_file": src,
        "member": member,
        "years_declared": ", ".join(years) or None,
        "n_years_declared": len(years),  # many years in ONE notice = late bulk back-fill
        "categories": ", ".join(sorted(cats)) or None,
        "parsed": parsed,
    }


def main():
    data = json.loads(SRC.read_text(encoding="utf-8"))
    rows = [r for it in data for r in parse_notice(it)]
    df = pd.DataFrame(rows)

    df.to_parquet(OUT / "iris_s29_statements.parquet", compression="zstd", index=False)
    df.to_csv(OUT / "iris_s29_statements.csv", index=False, encoding="utf-8")

    named = df[df["member"].notna()].copy()

    def _union_years(s):
        ys = sorted({y.strip() for v in s.dropna() for y in v.split(",") if y.strip()})
        return ", ".join(ys)

    summ = (
        named.groupby("member")
        .agg(
            n_notices=("source_file", "nunique"),
            max_years_one_notice=("n_years_declared", "max"),
            distinct_years=("years_declared", _union_years),
            categories_ever=("categories", lambda s: ", ".join(sorted({c.strip() for v in s.dropna() for c in v.split(",")}))),
        )
        .reset_index()
        .sort_values(["max_years_one_notice", "n_notices"], ascending=False)
    )
    summ.to_csv(OUT / "iris_s29_member_summary.csv", index=False, encoding="utf-8")

    print(f"notices in source       : {len(data)}")
    print(f"(notice × member) rows  : {len(df)}  (parsed={int(df['parsed'].sum())})")
    print(f"distinct members        : {named['member'].nunique()}")
    print(f"output                  : {OUT}/")

    print("\n=== RED FLAG: most years back-filled in a single notice (Troy = the known case) ===")
    show = summ[["member", "n_notices", "max_years_one_notice", "categories_ever"]].head(12)
    print(show.to_string(index=False))

    print("\n=== Troy + Crowe rows ===")
    chk = df[df["member"].astype(str).str.contains("Troy|Crowe", case=False, na=False)]
    print(chk[["issue_date", "source_file", "member", "n_years_declared", "years_declared", "categories"]].to_string(index=False))


if __name__ == "__main__":
    main()
