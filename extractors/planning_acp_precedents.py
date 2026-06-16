"""ACP precedent curation harness — surface real appeal decisions for the issue catalogue.

Each issue node may link a real An Coimisiún Pleanála (ex-An Bord Pleanála) decision so the
siting brief can quote "the Board's own words". Some catalogue precedents are `curate: true`
stubs (keywords only). This harness queries the ACP Cases_2016_Onwards feed (CC-BY) for
candidate cases matching each stub's keywords + a decision filter, and prints them with the
case page + the (usually text-extractable) inspector-report URL for a human to confirm.

Phase 1 = surface candidates for hand-curation (this script). Phase 2 = keyword auto-tag the
full inspector-report corpus. We do NOT auto-write the catalogue — curation is a human step
(a candidate is a lead, confirmed against the inspector report's "Reasons" section).

    python extractors/planning_acp_precedents.py            # all curate:true stubs
    python extractors/planning_acp_precedents.py --issue floodplain --region Galway
"""

from __future__ import annotations

import argparse
from pathlib import Path

import requests
import yaml

ACP = ("https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/"
       "Cases_2016_Onwards/FeatureServer/3")
CATALOGUE = Path("planning_rules/issue_catalogue.yaml")
_FIELDS = "ABPCASEID,DEVDESC,DECISION,PLANINGATY,DECIDED_ON,LINKABPWEB"


def report_url(case_id: str) -> str:
    """Inspector-report PDF (usually text-extractable). {ddd} = first 3 digits of the case no."""
    digits = "".join(c for c in str(case_id) if c.isdigit())
    return f"https://www.pleanala.ie/anbordpleanala/media/abp/cases/reports/{digits[:3]}/r{digits}.pdf"


def query_candidates(keywords: list[str], region: str | None = None,
                     decision: str = "REFUSE", n: int = 6) -> list[dict]:
    kw = " OR ".join(f"UPPER(DEVDESC) LIKE '%{k.upper()}%'" for k in keywords)
    where = f"({kw})"
    if decision:
        where += f" AND UPPER(DECISION) LIKE '%{decision.upper()}%'"
    if region:
        where += f" AND UPPER(PLANINGATY) LIKE '%{region.upper()}%'"
    params = {"where": where, "outFields": _FIELDS, "orderByFields": "DECIDED_ON DESC",
              "resultRecordCount": n, "returnGeometry": "false", "f": "json"}
    r = requests.get(ACP + "/query", params=params, timeout=60)
    r.raise_for_status()
    out = []
    for f in r.json().get("features", []):
        a = f["attributes"]
        cid = a.get("ABPCASEID")
        out.append({
            "case": cid, "decision": a.get("DECISION"), "authority": a.get("PLANINGATY"),
            "desc": (a.get("DEVDESC") or "")[:90],
            "case_page": a.get("LINKABPWEB"), "report": report_url(cid),
        })
    return out


def verify_in_report(case_id: str, keywords: list[str]) -> tuple[bool, str]:
    """Fetch the inspector-report PDF and confirm a keyword appears in its TEXT (the reasons).

    This is the real curation signal — the ground issue (peat/flood) lives in the report's
    reasons, not in DEVDESC. Text-only (no OCR); returns (matched, snippet). Inspector reports
    are usually text-extractable; the older scanned-image ones return ('', no match) honestly.
    """
    try:
        from io import BytesIO

        from pypdf import PdfReader

        r = requests.get(report_url(case_id), timeout=60)
        if r.status_code != 200 or "pdf" not in r.headers.get("content-type", "").lower():
            return False, "no report PDF"
        reader = PdfReader(BytesIO(r.content))
        text = " ".join((p.extract_text() or "") for p in reader.pages[:8]).lower()
        if len(text.strip()) < 200:
            return False, "scanned/no text (OCR needed)"
        for k in keywords:
            i = text.find(k.lower())
            if i >= 0:
                return True, "…" + text[max(0, i - 40):i + 60].replace("\n", " ") + "…"
        return False, "keywords not found in first pages"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}"


def _curate_stubs() -> dict[str, list[str]]:
    d = yaml.safe_load(CATALOGUE.read_text(encoding="utf-8"))
    out = {}
    for node in d.get("nodes", []):
        for p in (node.get("precedents") or []):
            if p.get("curate"):
                out[node["id"]] = p.get("keywords") or []
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--issue", help="single node id, else all curate:true stubs")
    ap.add_argument("--region", help="filter by PLANINGATY (e.g. Galway)")
    ap.add_argument("--n", type=int, default=6)
    ap.add_argument("--verify", action="store_true", help="confirm keyword in the inspector report text")
    args = ap.parse_args()

    stubs = _curate_stubs()
    targets = {args.issue: stubs.get(args.issue, [])} if args.issue else stubs
    for issue, keywords in targets.items():
        print(f"\n### {issue}  keywords={keywords}  region={args.region or 'national'}")
        cands = query_candidates(keywords, region=args.region, n=args.n)
        if not cands:
            print("  (no candidates — broaden keywords or drop the region filter)")
        for c in cands:
            mark = ""
            if args.verify:
                ok, snip = verify_in_report(c["case"], keywords)
                mark = f"  [{'CONFIRMED' if ok else 'unconfirmed'}: {snip}]"
            print(f"  ABP {c['case']} | {c['decision']} | {c['authority']}{mark}")
            print(f"     {c['desc']}")
            print(f"     case: {c['case_page']}  | report: {c['report']}")


if __name__ == "__main__":
    main()
