"""
probe_register_parse_quality.py  (SANDBOX)
------------------------------------------
"The backfill is only as good as the parser." Older Register-of-Interests PDFs
change format (layout shifts, eventually scanned images) and the prod parser
degrades to garbage. This harness finds the breakpoint EMPIRICALLY so we cap the
ingest at the last clean year instead of silently importing a mess.

For each interest year it:
  1. discovers the annual Dáil register URL from the oireachtas.ie publications
     index (published Feb/Mar of year+1, per the existing convention),
  2. downloads it to ./_pdfs/ (sandbox — never touches INTERESTS_PDF_DIR),
  3. runs the PROD parser, and
  4. scores quality: parsed count, match-rate vs the wide roster, and a
     garbage signal (median interest length, share of no-interest rows).

Run:  python -m pipeline_sandbox.historic_members.probe_register_parse_quality
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

from members.member_interests import (
    CATEGORIES_PATTERN,
    MEMBER_NAME_PATTERN,
    clean_interests,
    extract_raw_lines,
    group_lines,
    parse_members,
    split_embedded_names,
)
from shared import normalise_join_key

UA = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
INDEX = "https://www.oireachtas.ie/en/publications/?topic%5B%5D=register-of-members-interests&resultsPerPage=100"
PDF_DIR = Path(__file__).parent / "_pdfs"
WIDE_ROSTER = Path(__file__).parent / "_out" / "member_roster_wide.parquet"

# Interest years to probe — span the format change. Roster covers 31st Dáil
# (2011) onward, so match-rate is a valid signal from 2011 up.
TARGET_YEARS = list(range(2011, 2020))


def discover_dail_registers() -> dict[int, str]:
    """interest_year -> annual Dáil register URL (excludes supplements)."""
    r = requests.get(INDEX, headers={"User-Agent": UA}, timeout=30)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    out: dict[int, str] = {}
    for card in soup.select("div.c-publications-list__item"):
        a = card.select_one("p.c-publications-list__view a[href]")
        if not a:
            continue
        href = a["href"]
        fname = href.rsplit("/", 1)[-1]
        if "dail" not in href or "register-of-member" not in href or "supplement" in href:
            continue
        m = re.match(r"(\d{4})-\d{2}-\d{2}_", fname)
        if not m:
            continue
        # Convention: register published in Feb/Mar of year Y reports interests for Y-1.
        interest_year = int(m.group(1)) - 1
        out.setdefault(interest_year, href)
    return out


def roster_keys() -> set[str]:
    df = pl.read_parquet(WIDE_ROSTER).select(["first_name", "last_name"])
    df = df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    df = normalise_join_key.normalise_df_td_name(df, "join_key")
    return set(df.get_column("join_key").to_list())


def download(url: str) -> Path | None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    dest = PDF_DIR / url.rsplit("/", 1)[-1]
    if dest.exists() and dest.stat().st_size > 10_000:
        return dest
    try:
        with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(8192):
                    f.write(chunk)
        if dest.stat().st_size < 10_000 or open(dest, "rb").read(5) != b"%PDF-":
            dest.unlink(missing_ok=True)
            return None
        return dest
    except Exception as exc:  # noqa: BLE001
        print(f"  download failed {url}: {exc}")
        return None


def score(year: int, pdf: Path, keys: set[str]) -> dict:
    lines = split_embedded_names(extract_raw_lines(pdf))
    grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)
    members = parse_members(grouped, MEMBER_NAME_PATTERN)
    parsed = len(members)
    if parsed == 0:
        return {"year": year, "parsed": 0, "matched": 0, "match_rate": 0.0, "verdict": "EMPTY (likely scanned/unparseable)"}
    df = clean_interests(pl.DataFrame(members), year)
    df = normalise_join_key.normalise_df_td_name(df, "join_key")
    declarers = {k for k in df.get_column("join_key").to_list() if k}
    matched = len(declarers & keys)
    match_rate = matched / max(len(declarers), 1)
    # garbage signal: real registers run ~150-170 declarers; very low parsed OR
    # very low match rate => format the parser can't handle.
    verdict = "clean" if (match_rate >= 0.9 and parsed >= 120) else (
        "DEGRADED" if match_rate >= 0.6 else "GARBAGE"
    )
    return {
        "year": year,
        "parsed": parsed,
        "declarers": len(declarers),
        "matched": matched,
        "match_rate": round(match_rate, 3),
        "verdict": verdict,
    }


def main() -> None:
    if not WIDE_ROSTER.exists():
        raise SystemExit("run historic_member_pull.py first (need _out/member_roster_wide.parquet)")
    keys = roster_keys()
    registers = discover_dail_registers()
    print(f"discovered {len(registers)} Dáil annual registers; probing {TARGET_YEARS}\n")
    rows = []
    for year in TARGET_YEARS:
        url = registers.get(year)
        if not url:
            rows.append({"year": year, "verdict": "no register found"})
            print(rows[-1])
            continue
        pdf = download(url)
        if not pdf:
            rows.append({"year": year, "verdict": "download failed"})
            print(rows[-1])
            continue
        rows.append(score(year, pdf, keys))
        print(rows[-1])
    (PDF_DIR / "parse_quality_report.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
