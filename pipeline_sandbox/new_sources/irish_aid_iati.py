"""P0 grants — Irish Aid ODA activities via IATI (SANDBOX).

Grants are a THIRD public-money channel: never summed with procurement awards
or payments facts. This ingest is TRANSACTION-grain IATI data: each row is one
transaction on one aid activity, and ``transaction_type`` labels the grain
(commitment vs disbursement vs expenditure...). Commitments and disbursements
are DIFFERENT grains of the same money — never mix or sum across types, and
``value_safe_to_sum=False`` on every row regardless.

Source: IATI Registry (CKAN, no API key) publisher ``irishaid`` — "Ireland -
Department of Foreign Affairs". Activity XML files are downloaded directly
from the registry resource URLs (assets.ireland.ie). The IATI *datastore* API
(needs a key) is deliberately NOT used.

Licence: the registry package metadata declares ``cc-zero`` (CC0) for every
irishaid dataset (recorded per-row from the live metadata).

Handles both IATI 1.x (letter transaction codes, text org roles) and 2.x
(numeric codes, narrative elements) since the 2013-2015 files predate 2.x.

Output: c:/tmp/dail_new_sources/silver/irish_aid_iati.parquet
"""
from __future__ import annotations

import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import cache_raw, now_iso, write_silver  # noqa: E402

REGISTRY_SEARCH = "https://iatiregistry.org/api/3/action/package_search"
PUBLISHER = "irishaid"
# assets.ireland.ie sits on the gov.ie CDN family — browser spoof per
# extractors/procurement_etenders_extract.py GOVIE_HEADERS pattern.
GOVIE_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Referer": "https://www.gov.ie/"}

# IATI transaction-type code -> grain label. 2.x numeric; 1.x letters.
TTYPE = {
    "1": "incoming_funds", "2": "commitment", "3": "disbursement", "4": "expenditure",
    "5": "interest_payment", "6": "loan_repayment", "7": "reimbursement",
    "8": "purchase_of_equity", "9": "sale_of_equity", "11": "credit_guarantee",
    "12": "incoming_commitment", "13": "commitment_cancellation",
    "C": "commitment", "D": "disbursement", "E": "expenditure", "IF": "incoming_funds",
    "IR": "interest_repayment", "LR": "loan_repayment", "R": "reimbursement",
    "QP": "purchase_of_equity", "QS": "sale_of_equity", "CG": "credit_guarantee",
}
# participating-org roles: 1.x text / 2.x numeric
ROLE_FUNDING = {"1", "funding"}
ROLE_IMPLEMENTING = {"4", "implementing"}


def _narrative(el: ET.Element | None) -> str | None:
    """2.x narrative child, else 1.x element text."""
    if el is None:
        return None
    n = el.find("narrative")
    txt = (n.text if n is not None else el.text) or ""
    txt = " ".join(txt.split())
    return txt or None


def list_packages() -> list[dict]:
    r = requests.get(REGISTRY_SEARCH, params={"fq": f"organization:{PUBLISHER}", "rows": 100}, timeout=60)
    r.raise_for_status()
    res = r.json()["result"]
    pkgs = []
    for p in res["results"]:
        extras = {e["key"]: e["value"] for e in p.get("extras", [])}
        if extras.get("filetype") != "activity":  # skip the organisation file
            continue
        url = next((rs.get("url") for rs in p.get("resources", []) if rs.get("url")), None)
        if not url:
            continue
        pkgs.append({
            "name": p["name"],
            "url": url,
            "licence": p.get("license_id") or "unknown",
            "source_published_date": extras.get("data_updated") or p.get("metadata_modified"),
            "activity_count": extras.get("activity_count"),
        })
    return sorted(pkgs, key=lambda x: x["name"])


def parse_activities(xml_bytes: bytes, pkg: dict, sha: str, fetched_at: str,
                     last_modified: str | None) -> list[dict]:
    root = ET.fromstring(xml_bytes)
    iati_version = root.get("version")
    rows: list[dict] = []
    for act in root.iter("iati-activity"):
        ident = (act.findtext("iati-identifier") or "").strip() or None
        title = _narrative(act.find("title"))
        rc = act.find("recipient-country")
        rr = act.find("recipient-region")
        sectors = act.findall("sector")
        sector_el = sectors[0] if sectors else None
        implementing = funding = None
        for org in act.findall("participating-org"):
            role = (org.get("role") or "").strip().lower()
            if implementing is None and role in ROLE_IMPLEMENTING:
                implementing = _narrative(org)
            elif funding is None and role in ROLE_FUNDING:
                funding = _narrative(org)
        act_sector_code = sector_el.get("code") if sector_el is not None else None
        base = {
            "iati_identifier": ident,
            "activity_title": title,
            "recipient_country_code": rc.get("code") if rc is not None else None,
            "recipient_country": _narrative(rc),
            "recipient_region_code": rr.get("code") if rr is not None else None,
            "recipient_region": _narrative(rr),
            "sector_name": _narrative(sector_el),
            "n_sectors": len(sectors),
            "implementing_org": implementing,
            "funding_org": funding,
            "default_currency": act.get("default-currency"),
        }
        for tx in act.findall("transaction"):
            tt = tx.find("transaction-type")
            code = (tt.get("code") if tt is not None else None) or None
            val = tx.find("value")
            amount = None
            if val is not None and val.text:
                try:
                    amount = float(val.text.strip().replace(",", ""))
                except ValueError:
                    amount = None
            td = tx.find("transaction-date")
            tx_date = (td.get("iso-date") if td is not None else None) or \
                      (val.get("value-date") if val is not None else None)
            year = None
            if tx_date and len(tx_date) >= 4 and tx_date[:4].isdigit():
                year = int(tx_date[:4])
            # IATI 2.x may carry sector on the transaction itself; fall back to activity level
            tx_sector = tx.find("sector")
            sector_code = (tx_sector.get("code") if tx_sector is not None else None) or act_sector_code
            rows.append({
                **base,
                "sector_code": sector_code,
                # source data contains a handful of obvious date typos (e.g. 1913-08-23)
                "dq_suspect_date": bool(year is not None and not (2000 <= year <= 2027)),
                "transaction_type_code": code,
                "transaction_type": TTYPE.get((code or "").upper() if code and code.isalpha() else (code or ""),
                                              None) or (f"other({code})" if code else None),
                "transaction_date": tx_date,
                "year": year,
                "value": amount,
                "currency": (val.get("currency") if val is not None else None) or act.get("default-currency"),
                "value_safe_to_sum": False,
                "receiver_org": _narrative(tx.find("receiver-org")),
                "provider_org": _narrative(tx.find("provider-org")),
                "tx_description": _narrative(tx.find("description")),
                "source_package": pkg["name"],
                "iati_version": iati_version,
                "source_url": pkg["url"],
                "source_document_hash": sha,
                "fetched_at": fetched_at,
                "source_published_date": pkg["source_published_date"],
                "source_last_modified": last_modified,
                "extraction_method": "iati_registry_xml",
                "confidence": "high",
                "privacy_tier": "public",  # organisations, not individuals
                "licence": pkg["licence"],
            })
    return rows


def run() -> None:
    pkgs = list_packages()
    print(f"irishaid activity packages on IATI registry: {len(pkgs)}")
    all_rows: list[dict] = []
    for pkg in pkgs:
        try:
            time.sleep(0.4)
            r = requests.get(pkg["url"], headers=GOVIE_HEADERS, timeout=120)
            r.raise_for_status()
        except Exception as e:  # noqa: BLE001 — keep partials if one year's file is down
            print(f"  {pkg['name']}: FETCH FAILED {type(e).__name__}: {e}")
            continue
        fname = pkg["url"].rsplit("/", 1)[-1]
        _, sha = cache_raw("irish_aid_iati", f"{pkg['name']}__{fname}", r.content)
        try:
            rows = parse_activities(r.content, pkg, sha, now_iso(), r.headers.get("last-modified"))
        except ET.ParseError as e:
            print(f"  {pkg['name']}: XML PARSE FAILED: {e}")
            continue
        n_act = len({x['iati_identifier'] for x in rows})
        print(f"  {pkg['name']}: {len(r.content):,}B, activities~{pkg['activity_count']}, "
              f"parsed acts w/tx={n_act}, tx rows={len(rows)}")
        all_rows.extend(rows)

    df = pl.DataFrame(all_rows, infer_schema_length=None)
    out = write_silver("irish_aid_iati", df)

    # ---- profile (counts only; commitments/disbursements are different grains, NEVER summed) ----
    print(f"\nSILVER: {out}  rows={df.height}")
    print(f"  year range: {df['year'].min()} … {df['year'].max()}")
    print("  rows by transaction_type (grain — never mix):")
    for r_ in df.group_by("transaction_type").len().sort("len", descending=True).to_dicts():
        print(f"    {r_['len']:>6}  {r_['transaction_type']}")
    for col in ("iati_identifier", "receiver_org", "implementing_org", "recipient_country",
                "sector_name", "value", "transaction_date"):
        nulls = df[col].null_count()
        print(f"  null rate {col}: {nulls}/{df.height} ({100*nulls/df.height:.1f}%)")
    print("  top receiver orgs by tx COUNT (not €):")
    for r_ in (df.filter(pl.col("receiver_org").is_not_null())
               .group_by("receiver_org").len().sort("len", descending=True).head(8).to_dicts()):
        print(f"    {r_['len']:>5}  {r_['receiver_org'][:70]}")
    print("  top recipient countries by tx COUNT:")
    for r_ in (df.filter(pl.col("recipient_country").is_not_null())
               .group_by("recipient_country").len().sort("len", descending=True).head(6).to_dicts()):
        print(f"    {r_['len']:>5}  {r_['recipient_country']}")
    print("  currencies:", df.group_by("currency").len().sort("len", descending=True).head(5).to_dicts())


if __name__ == "__main__":
    run()
