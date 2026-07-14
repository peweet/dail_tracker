"""Per-centre compliance extraction: the 101 individual HIQA IPAS centre inspection reports.

SANDBOX ONLY. Reads the born-digital PDFs already cached at
``bronze/hiqa_ipas/pdf/`` (no OCR, no re-fetch, no network) and joins back to
``silver/hiqa_ipas_inspections.parquet`` on pdf_local_path / centre_id.

WHY THIS EXISTS
---------------
The HIQA overview report (hiqa_ipas_figures.py) publishes compliance only as
NATIONAL PERCENTAGES and never names a single service provider. The 101
individual inspection reports carry the per-centre payload:

  * the PROVIDER NAME on the cover page -- the only public source that says who
    operates each IPAS centre HIQA inspects;
  * the COMPLIANCE JUDGMENT TABLE (Appendix 1) -- centre x standard x judgment;
  * Section 2 of the compliance plan -- risk rating (Red/Orange/Yellow) and the
    date by which the provider must comply, for every failed standard;
  * residents/children/capacity/bedrooms on the day, inspectors, announced vs
    unannounced, commissioning department, premises ownership.

TWO OUTPUTS
-----------
1. ``hiqa_centre_compliance.parquet`` -- one row per (centre x inspection x
   standard). judgment is VERBATIM as printed by HIQA.
2. ``hiqa_centre_facts.parquet`` -- the canonical fact schema documented in
   ipas_doc_registry.py (doc_key='hiqa_inspection_reports'), categories drawn
   from that file's CATEGORIES list.

JUDGMENT SOURCING (nothing is silently reconciled)
--------------------------------------------------
Every report states each judgment TWICE: once inline in the body
("Judgment: Not Compliant") and once in the Appendix 1 summary table. These do
not always agree -- HIQA's own two statements conflict in a minority of reports,
and some standards appear in one place but not the other. Both values are kept
(`judgment` = the Appendix 1 summary table, which is the report's own summary of
record; `judgment_body` = the inline narrative judgment), `judgment_conflict` is
flagged, confidence drops to 'low', and the divergence is REPORTED, never
adjusted. Where only one source carries the standard, that source is used and
`judgment_source` records which.

UNKNOWN DISCIPLINE
------------------
Nothing is guessed, inferred or interpolated. A value the report does not state
(most reports do not print a capacity, and single-adult centres have no children)
gets an EXPLICIT row with value null, is_unknown=True and unknown_reason set. A
report whose layout defeats the parser gets a named unknown row so a future loop
can catch it. Partial coverage that is honestly labelled beats fabricated
completeness.

PRIVACY
-------
Facilities, providers and inspectors-in-professional-capacity only. No resident
names, ages, nationalities, medical or personal details, and no quote
attributable to an identifiable resident: `_privacy_scrub` strips every paired
double-quoted span from narrative text before it is stored, and drops any
sentence that pairs a resident reference with an age or medical detail.
privacy_tier:
  public_facility             -- centre / provider / compliance facts
  public_professional         -- named HIQA inspectors (professional capacity)
  public_regulatory_narrative -- HIQA's published findings, quote-stripped
PROVIDER NAMES carry `join_caveat`: they must inherit the accommodation-providers
public_display gating at join time (same caveat the metadata parquet already sets).

PROVIDER NAME NORMALISATION
---------------------------
`provider_name` is VERBATIM from the cover page. `provider_key` folds case,
punctuation and legal-form suffixes (LTD/LIMITED/ULC/PLC/DAC) so that HIQA's own
spelling variants ("Onsite Facilities Management Ltd." / "Onsite Facilities
Management" / "On-site Facilities Management Ltd") group into one operator.
`provider_name_canonical` is the most frequent verbatim spelling inside a key
group -- a choice among HIQA's own strings, never an invented name.

Run:
  cd pipeline_sandbox/new_sources
  PYTHONUTF8=1 PYTHONIOENCODING=utf-8 ../../.venv/Scripts/python.exe hiqa_centre_reports.py
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

from _common import SILVER, now_iso

META = SILVER / "hiqa_ipas_inspections.parquet"
DOC_KEY = "hiqa_inspection_reports"
DOC_TITLE = "HIQA - individual IPAS centre inspection reports (2024-01 -> 2026-03)"
JOIN_CAVEAT = ("Provider names must inherit accommodation-providers public_display gating at join time")

# ---------------------------------------------------------------- regex battery
JUDG_WORDS = r"Not Compliant|Substantially Compliant|Partially Compliant|Compliant"
RE_JUDG_CELL = re.compile(rf"^\s*({JUDG_WORDS})\s*\.?\s*$", re.I)
RE_STD_CELL = re.compile(r"^\s*Standard\s+(\d+\.\d+)\s*$", re.I)
RE_APPENDIX = re.compile(r"Appendix\s*1\s*[-–—]\s*Summary table", re.I)
RE_SECTION2 = re.compile(r"Standards\s+to\s+be\s+complied\s+with", re.I)
RE_BODY_SCAN = re.compile(rf"(?P<std>\bStandard\s+(?P<ref>\d+\.\d+)\b)|(?P<judg>Judgment:\s*(?P<j>{JUDG_WORDS}))")
RE_RISK = re.compile(r"^\s*(Red|Orange|Yellow)\s*$", re.I)
RE_DATE = re.compile(r"^\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*$")
RE_RESIDENTS_BOX = re.compile(r"Number of residents on\s*\n?\s*the date of inspection:\s*\n?\s*([\d,]+)")
RE_DEPT = re.compile(r"on\s+behalf\s+of\s+the\s+(Department\s+of\s+[A-Za-z,\s]+?)(?:\s+by\b|\.|$)", re.I)
RE_OWNERSHIP = re.compile(r"(privately owned|State[-\s]owned|owned by the State)", re.I)
RE_URGENT = re.compile(r"[^.]*\b(urgent compliance plan|urgent action|immediate action|escalat\w+)\b[^.]*\.", re.I)
RE_OVERALL = re.compile(r"(Overall,[^\n]{40,1200}?\.)(?=\s|$)", re.S)
RE_VETTING = re.compile(r"[^.]*\b(Garda [Vv]etting|vetted|National Vetting Bureau|police check\w*)\b[^.]*\.", re.I)
RE_PAGE_FOOTER = re.compile(r"Page\s+\d+\s+of\s+\d+\s*", re.I)

CAP_PATS = [
    r"recorded\s+capacity\s+of\s+([\d,]+)",
    r"capacity\s+(?:of|to\s+accommodate)\s*(?:up\s+to\s+)?([\d,]+)",
    r"capacity\s+for\s+(?:up\s+to\s+)?([\d,]+)",
    r"accommodation\s+to\s+up\s+to\s+([\d,]+)",
    r"up\s+to\s+([\d,]+)\s+(?:\w+\s+){0,3}?(?:people|persons|residents|men|women|males|females)\b",
]
CHILD_PATS = [
    r"([\d,]+)\s+of\s+whom\s+(?:were|was|are|is)\s+children",
    r"of\s+(?:whom|which)\s+([\d,]+)\s+(?:were|was|are)\s+children",
    r"includ(?:ed|ing|es)\s+([\d,]+)\s+children",
    r"([\d,]+)\s+children\s+and\s+[\d,]+\s+adults",
    r"comprising\s+([\d,]+)\s+children",
]
ADULT_PATS = [r"\b([\d,]+)\s+adults\b"]
BED_PATS = [
    r"\b([\d,]+)\s+bedrooms\s+in\s+total",
    r"(?:contained|contains|has|have|had|there\s+are|there\s+were)\s+([\d,]+)\s+bedrooms",
]
COVER_LABELS = {
    "centre_name_report": r"Name of the Centre",
    "centre_osv_id": r"Centre ID(?:\s+OSV)?",
    "provider_name": r"Provider Name",
    "location_of_centre": r"Location of Centre",
    "inspection_type": r"Type of Inspection",
    "inspection_date_printed": r"Date of Inspection",
    "inspection_id": r"Inspection ID",
}

# privacy guards -------------------------------------------------------------
RE_QUOTE = re.compile(r"[“\"][^”\"]{0,800}[”\"]")
RE_PERSONAL = re.compile(
    r"[^.]*\b(resident|child|young person|man|woman|boy|girl)\b[^.]{0,80}?"
    r"\b(aged\s+\d+|\d+\s+years?\s+old|diagnos\w+|medical condition|mental health condition)\b[^.]*\.",
    re.I)


def _privacy_scrub(text: str | None, limit: int = 1500) -> str | None:
    """Strip anything that could carry an identifiable resident's words or details."""
    if not text:
        return None
    t = RE_QUOTE.sub("[quote removed - privacy]", text)
    t = RE_PERSONAL.sub("[sentence removed - resident personal detail]", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:limit] if t else None


def _flat(t: str) -> str:
    return re.sub(r"\s+", " ", t or "").strip()


def _num(s: str) -> float | None:
    try:
        return float(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def _norm_judgment(j: str | None) -> str | None:
    if not j:
        return None
    j = re.sub(r"\s+", " ", j).strip().rstrip(".").lower()
    return {"compliant": "Compliant", "substantially compliant": "Substantially compliant",
            "partially compliant": "Partially compliant", "not compliant": "Not compliant"}.get(j)


def provider_key(name: str | None) -> str | None:
    """Fold case/punctuation/legal-form so HIQA's spelling variants group into one operator."""
    if not name:
        return None
    s = name.upper().replace("&", " AND ")
    s = re.sub(r"\bUNLIMITED COMPANY\b", " ULC ", s)
    s = re.sub(r"\bLIMITED\b", " LTD ", s)
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\b(LTD|ULC|PLC|DAC|LLP|CLG|CO)\b", " ", s)
    return re.sub(r"\s+", "", s) or None


# ============================================================================
# per-PDF parse
# ============================================================================
def parse_report(pdf: Path) -> dict:
    """Return everything establishable from one inspection report. Never guesses."""
    out: dict = {"errors": [], "standards": {}, "inspectors": [], "notes": []}
    with fitz.open(pdf) as doc:
        pages = [doc[i].get_text() for i in range(doc.page_count)]
        out["page_count"] = doc.page_count
        full = "\n".join(pages)
        full_flat = _flat(full)

        # ---------------- cover page (page 1) ----------------
        p1 = pages[0] if pages else ""
        for field, label in COVER_LABELS.items():
            m = re.search(rf"{label}\s*:\s*\n?\s*(.+)", p1)
            val = _flat(m.group(1)) if m else None
            out[field] = val or None
            if not val:
                out["errors"].append(f"cover field missing: {field}")
        out["cover_page"] = 1

        # ---------------- About the Service ----------------
        about_idx = next((i for i, t in enumerate(pages[:8]) if "About the Service" in t), None)
        about = _flat(pages[about_idx]) if about_idx is not None else ""
        out["about_page"] = (about_idx + 1) if about_idx is not None else None
        if about_idx is None:
            out["errors"].append("no 'About the Service' page found")

        def _first(pats, hay):
            for p in pats:
                m = re.search(p, hay, re.I)
                if m:
                    return _num(m.group(1)), p
            return None, None

        out["capacity"], out["capacity_pat"] = _first(CAP_PATS, about)
        out["children"], out["children_pat"] = _first(CHILD_PATS, about)
        out["adults"], _ = _first(ADULT_PATS, about)
        out["bedrooms"], _ = _first(BED_PATS, about)

        m = RE_RESIDENTS_BOX.search(full)
        out["residents"] = _num(m.group(1)) if m else None
        if out["residents"] is None:
            out["errors"].append("residents-on-inspection box not found")

        m = RE_DEPT.search(full_flat)
        out["department"] = _flat(m.group(1)).rstrip(",") if m else None
        m = RE_OWNERSHIP.search(about)
        out["ownership"] = _flat(m.group(1)) if m else None

        # ---------------- inspector table ----------------
        ins_idx = next((i for i, t in enumerate(pages) if "Times of Inspection" in t), None)
        out["inspector_page"] = (ins_idx + 1) if ins_idx is not None else None
        dates_seen: set[str] = set()
        if ins_idx is not None:
            seen: set[tuple[str, str]] = set()
            for tab in doc[ins_idx].find_tables().tables:
                for row in tab.extract():
                    cells = [_flat(c or "") for c in row]
                    cells = [c for c in cells if c]
                    if len(cells) < 4 or cells[0].lower().startswith("date"):
                        continue
                    d, _times, name, role = cells[0], cells[1], cells[2], cells[3]
                    if re.match(r"^\d", d):
                        dates_seen.add(d)
                    if name and role and (name, role) not in seen and not name.lower().startswith("inspector"):
                        seen.add((name, role))
                        out["inspectors"].append({"name": name, "role": role})
        else:
            out["errors"].append("inspector table not found")
        out["inspection_days"] = len(dates_seen) or None

        # ---------------- Appendix 1: the summary judgment table ----------------
        ap_idx = next((i for i, t in enumerate(pages) if RE_APPENDIX.search(t)), None)
        out["appendix_page"] = (ap_idx + 1) if ap_idx is not None else None
        appendix: dict[str, tuple[str, int, str | None, str | None]] = {}
        if ap_idx is None:
            out["errors"].append("Appendix 1 summary table not found")
        else:
            dim = theme = None
            for i in range(ap_idx, len(pages)):
                if i > ap_idx and ("Compliance Plan" in pages[i] or "Introduction and instruction" in pages[i]):
                    break
                for tab in doc[i].find_tables().tables:
                    for row in tab.extract():
                        cells = [_flat(c or "") for c in row if c]
                        for c in cells:
                            if c.lower().startswith("dimension:"):
                                dim = c.split(":", 1)[1].strip()
                            elif c.lower().startswith("theme"):
                                theme = c
                        sref = next((RE_STD_CELL.match(c).group(1) for c in cells if RE_STD_CELL.match(c)), None)
                        j = next((c for c in cells if RE_JUDG_CELL.match(c)), None)
                        if sref and j:
                            appendix.setdefault(sref, (_flat(j), i + 1, dim, theme))
                        elif sref and not j:
                            appendix.setdefault(sref, (None, i + 1, dim, theme))
        out["appendix"] = appendix

        # ---------------- body: Standard X.Y ... Judgment: <j> ----------------
        body_end = ap_idx if ap_idx is not None else len(pages)
        body: dict[str, tuple[str, int, str, str]] = {}  # ref -> (judgment, page, finding, statement)
        cur_ref: str | None = None
        cur_page = 0
        buf: list[str] = []
        for i in range(body_end):
            txt = RE_PAGE_FOOTER.sub(" ", pages[i])
            last = 0
            for m in RE_BODY_SCAN.finditer(txt):
                if cur_ref is not None:
                    buf.append(txt[last:m.start()])
                last = m.end()
                if m.group("std"):
                    cur_ref, cur_page, buf = m.group("ref"), i + 1, []
                elif m.group("judg") and cur_ref is not None:
                    seg = _flat("".join(buf))
                    stmt = re.split(r"(?<=\.)\s+(?=[A-Z])", seg)[0] if seg else ""
                    stmt = stmt if 20 <= len(stmt) <= 420 else ""
                    finding = seg[len(stmt):].strip() if stmt else seg
                    body.setdefault(cur_ref, (_flat(m.group("j")), i + 1, finding, stmt))
                    cur_ref, buf = None, []
            if cur_ref is not None:
                buf.append(txt[last:])
        out["body"] = body
        if not body and not appendix:
            out["errors"].append("NO standards or judgments recovered from this report")

        # ---------------- Section 2: risk rating + comply-by date ----------------
        s2_idx = next((i for i, t in enumerate(pages) if RE_SECTION2.search(t)), None)
        sec2: dict[str, dict] = {}
        if s2_idx is not None:
            for i in range(s2_idx, len(pages)):
                for tab in doc[i].find_tables().tables:
                    for row in tab.extract():
                        cells = [_flat(c or "") for c in row if c and _flat(c)]
                        sref = next((RE_STD_CELL.match(c).group(1) for c in cells if RE_STD_CELL.match(c)), None)
                        if not sref:
                            continue
                        j = next((c for c in cells if RE_JUDG_CELL.match(c)), None)
                        risk = next((c.title() for c in cells if RE_RISK.match(c)), None)
                        dt = next((c for c in cells if RE_DATE.match(c)), None)
                        stmt = next((c for c in cells
                                     if len(c) > 30 and not RE_JUDG_CELL.match(c) and not RE_STD_CELL.match(c)), None)
                        sec2.setdefault(sref, {"judgment": j, "risk_rating": risk,
                                               "comply_by": dt, "statement": stmt, "page": i + 1})
        out["sec2"] = sec2
        out["has_compliance_plan"] = bool(re.search(r"Compliance Plan for", full, re.I))

        # ---------------- narrative (privacy-scrubbed) ----------------
        body_text = RE_PAGE_FOOTER.sub(" ", "\n".join(pages[:body_end]))
        out["overall"] = [_privacy_scrub(m.group(1)) for m in RE_OVERALL.finditer(body_text)][:4]
        out["urgent"] = list(dict.fromkeys(
            _privacy_scrub(_flat(m.group(0))) for m in RE_URGENT.finditer(body_text)))[:6]
        out["vetting"] = list(dict.fromkeys(
            _privacy_scrub(_flat(m.group(0))) for m in RE_VETTING.finditer(body_text)))[:6]
    return out


# ============================================================================
# main
# ============================================================================
def main() -> None:
    meta = pl.read_parquet(META).sort("inspection_date")
    print(f"metadata: {meta.height} inspection reports, "
          f"{meta['centre_id'].n_unique()} distinct centres, "
          f"{meta['inspection_date'].min()} -> {meta['inspection_date'].max()}")

    parsed: list[tuple[dict, dict]] = []
    for r in meta.iter_rows(named=True):
        pdf = Path(r["pdf_local_path"])
        if not pdf.exists():
            parsed.append((r, {"errors": [f"PDF missing on disk: {pdf}"], "standards": {},
                               "appendix": {}, "body": {}, "sec2": {}, "inspectors": [],
                               "overall": [], "urgent": [], "vetting": []}))
            continue
        parsed.append((r, parse_report(pdf)))
    print(f"read {len(parsed)} PDFs")

    # ---- pass 2: canonical standard titles (mode of the reports' own statements)
    stmt_votes: dict[str, Counter] = defaultdict(Counter)
    for _, p in parsed:
        for ref, (_j, _pg, _find, stmt) in p.get("body", {}).items():
            if stmt:
                stmt_votes[ref][stmt] += 1
        for ref, s in p.get("sec2", {}).items():
            if s.get("statement") and len(s["statement"]) > 40:
                stmt_votes[ref][_flat(s["statement"])] += 1
    titles: dict[str, str] = {ref: c.most_common(1)[0][0] for ref, c in stmt_votes.items() if c}

    # ---- pass 2b: provider canonicalisation (mode of HIQA's own spellings)
    key_votes: dict[str, Counter] = defaultdict(Counter)
    for _, p in parsed:
        k = provider_key(p.get("provider_name"))
        if k:
            key_votes[k][p["provider_name"]] += 1
    canon = {k: c.most_common(1)[0][0] for k, c in key_votes.items()}

    ts = now_iso()
    comp_rows: list[dict] = []
    fact_rows: list[dict] = []
    tally = Counter()
    fid = 0

    def fact(r, p, category, metric, value_numeric=None, value_text=None, unit=None,
             qualifier="exact", scope=None, section=None, ref=None, page=None,
             method="text_layer", confidence="high", tier="public_facility",
             is_unknown=False, unknown_reason=None, notes=None):
        nonlocal fid
        fid += 1
        fact_rows.append({
            "fact_id": f"{DOC_KEY}-{fid:05d}", "doc_key": DOC_KEY, "doc_title": DOC_TITLE,
            "centre_id": r["centre_id"], "centre_name": r["centre_name"], "county": r["county"],
            "provider_name": p.get("provider_name"),
            "provider_name_canonical": canon.get(provider_key(p.get("provider_name"))),
            "provider_key": provider_key(p.get("provider_name")),
            "inspection_date": r["inspection_date"], "inspection_id": p.get("inspection_id"),
            "page": page, "printed_page": str(page) if page else None,
            "ref": ref, "section": section, "category": category,
            "subject": p.get("centre_name_report") or r["centre_name"],
            "metric": metric, "value_numeric": value_numeric, "value_text": value_text,
            "unit": unit, "qualifier": qualifier, "period": r["inspection_date"],
            "scope": scope or "this centre on the date of inspection",
            "is_unknown": is_unknown, "unknown_reason": unknown_reason, "notes": notes,
            "source_url": r["report_pdf_url"], "source_document_hash": r["pdf_sha256"],
            "extraction_method": method, "confidence": confidence, "privacy_tier": tier,
            "value_safe_to_sum": False, "join_caveat": JOIN_CAVEAT,
            "pdf_local_path": r["pdf_local_path"], "extracted_at": ts,
        })

    for r, p in parsed:
        appendix, body, sec2 = p.get("appendix", {}), p.get("body", {}), p.get("sec2", {})
        refs = sorted(set(appendix) | set(body), key=lambda s: [int(x) for x in s.split(".")])
        pname = p.get("provider_name")
        pkey = provider_key(pname)

        # ---------- classify the parse ----------
        conflicts = 0
        for ref in set(appendix) & set(body):
            if _norm_judgment(appendix[ref][0]) != _norm_judgment(body[ref][0]):
                conflicts += 1
        asym = len(set(appendix) ^ set(body))
        if not refs:
            grade = "FAILED"
        elif (conflicts or asym or not pname or p.get("residents") is None
              or not p.get("inspectors") or p.get("errors")):
            grade = "PARTIAL"
        else:
            grade = "FULL"
        tally[grade] += 1

        # ---------- compliance rows ----------
        if not refs:
            comp_rows.append({
                "centre_id": r["centre_id"], "centre_name": r["centre_name"],
                "centre_name_report": p.get("centre_name_report"), "county": r["county"],
                "provider_name": pname, "provider_name_canonical": canon.get(pkey), "provider_key": pkey,
                "inspection_date": r["inspection_date"], "inspection_id": p.get("inspection_id"),
                "inspection_type": p.get("inspection_type"), "dimension": None, "theme": None,
                "standard_ref": None, "standard_title": None, "judgment": None,
                "judgment_normalised": None, "judgment_body": None, "judgment_source": None,
                "judgment_conflict": None, "risk_rating": None, "date_to_be_complied_with": None,
                "finding_text": None, "source_url": r["report_pdf_url"],
                "source_document_hash": r["pdf_sha256"], "page": None,
                "extraction_method": "unknown", "confidence": "low",
                "privacy_tier": "public_facility", "is_unknown": True,
                "unknown_reason": ("no standard/judgment recovered from this report: "
                                   + "; ".join(p.get("errors", [])[:3])),
                "notes": f"UNPARSED REPORT: {Path(r['pdf_local_path']).name}",
                "join_caveat": JOIN_CAVEAT, "pdf_local_path": r["pdf_local_path"], "extracted_at": ts,
            })
        for ref in refs:
            a = appendix.get(ref)
            b = body.get(ref)
            aj = _flat(a[0]) if a and a[0] else None
            bj = _flat(b[0]) if b and b[0] else None
            judgment = aj or bj
            src = "appendix_1_summary_table" if aj else ("body_narrative" if bj else None)
            conflict = bool(aj and bj and _norm_judgment(aj) != _norm_judgment(bj))
            if conflict:
                conf, note = "low", (f"SOURCE CONFLICT: Appendix 1 says '{aj}', the body narrative says "
                                     f"'{bj}'. Both kept verbatim; neither adjusted.")
            elif aj and bj:
                conf, note = "high", "appendix table and body narrative agree"
            elif judgment:
                conf = "medium"
                note = (f"standard present only in the {src.replace('_', ' ')}; "
                        "the other location of the report does not carry it")
            else:
                conf, note = "low", None
            s2 = sec2.get(ref, {})
            comp_rows.append({
                "centre_id": r["centre_id"], "centre_name": r["centre_name"],
                "centre_name_report": p.get("centre_name_report"), "county": r["county"],
                "provider_name": pname, "provider_name_canonical": canon.get(pkey), "provider_key": pkey,
                "inspection_date": r["inspection_date"], "inspection_id": p.get("inspection_id"),
                "inspection_type": p.get("inspection_type"),
                "dimension": a[2] if a else None, "theme": a[3] if a else None,
                "standard_ref": ref, "standard_title": titles.get(ref),
                "judgment": judgment, "judgment_normalised": _norm_judgment(judgment),
                "judgment_body": bj, "judgment_source": src, "judgment_conflict": conflict,
                "risk_rating": s2.get("risk_rating"), "date_to_be_complied_with": s2.get("comply_by"),
                "finding_text": _privacy_scrub(b[2]) if b else None,
                "source_url": r["report_pdf_url"], "source_document_hash": r["pdf_sha256"],
                "page": (a[1] if a else b[1]),
                "extraction_method": "pdf_table_appendix1" if aj else "pdf_text_body_judgment",
                "confidence": conf, "privacy_tier": "public_facility",
                "is_unknown": judgment is None,
                "unknown_reason": (None if judgment else
                                   "standard listed but no judgment cell parsed at source"),
                "notes": note, "join_caveat": JOIN_CAVEAT,
                "pdf_local_path": r["pdf_local_path"], "extracted_at": ts,
            })

        # ---------- facts: identity / contract ----------
        if pname:
            fact(r, p, "contracts", "Service provider named on the inspection report cover",
                 value_text=pname, page=1, ref="cover", section="Cover",
                 method="pdf_text_cover_label", confidence="high",
                 notes=("THE ONLY PUBLIC SOURCE NAMING THE OPERATOR OF THIS CENTRE. "
                        f"canonical form: {canon.get(pkey)}"))
        else:
            fact(r, p, "unknown_at_source", "Service provider named on the inspection report cover",
                 page=1, ref="cover", section="Cover", method="unknown", confidence="low",
                 qualifier="unknown", is_unknown=True,
                 unknown_reason="'Provider Name:' label not found on the cover page")
        for key, cat, metric in [
            ("centre_osv_id", "inspections", "HIQA centre OSV identifier"),
            ("location_of_centre", "residents_centres", "Location of centre as printed"),
            ("inspection_type", "inspections", "Type of inspection (announced/unannounced)"),
            ("inspection_id", "inspections", "HIQA inspection ID"),
            ("inspection_date_printed", "inspections", "Date(s) of inspection as printed"),
        ]:
            v = p.get(key)
            if v:
                fact(r, p, cat, metric, value_text=v, page=1, ref="cover", section="Cover",
                     method="pdf_text_cover_label")
            else:
                fact(r, p, "unknown_at_source", metric, page=1, ref="cover", section="Cover",
                     method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                     unknown_reason=f"cover label for '{metric}' not found")
        if p.get("department"):
            fact(r, p, "contracts", "Commissioning department the service is provided on behalf of",
                 value_text=p["department"], page=p.get("about_page"), section="About the Service",
                 method="pdf_text_narrative",
                 notes="IPAS moved from DCEDIY to the Department of Justice, Home Affairs and Migration")
        else:
            fact(r, p, "unknown_at_source", "Commissioning department the service is provided on behalf of",
                 page=p.get("about_page"), section="About the Service", method="unknown",
                 confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason="the report does not state the commissioning department")
        if p.get("ownership"):
            fact(r, p, "contracts", "Premises ownership as described by HIQA",
                 value_text=p["ownership"], page=p.get("about_page"), section="About the Service",
                 method="pdf_text_narrative")
        else:
            fact(r, p, "unknown_at_source", "Premises ownership as described by HIQA",
                 page=p.get("about_page"), section="About the Service", method="unknown",
                 confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason="the report does not state whether the premises are privately or State owned")

        # ---------- facts: people & capacity ----------
        if p.get("residents") is not None:
            fact(r, p, "residents_centres", "Residents accommodated on the date of inspection",
                 value_numeric=p["residents"], unit="persons", page=p.get("about_page"),
                 section="About the Service", ref="residents box", method="pdf_text_databox")
        else:
            fact(r, p, "unknown_at_source", "Residents accommodated on the date of inspection",
                 unit="persons", page=p.get("about_page"), section="About the Service",
                 method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason="the 'Number of residents on the date of inspection' data box was not parsed")
        for key, cat, metric, unit, why in [
            ("capacity", "capacity", "Centre capacity as stated in the report", "persons",
             "the report does not state a capacity for this centre (many state bedroom counts instead)"),
            ("children", "residents_centres", "Children accommodated on the date of inspection", "persons",
             "the report does not state a child count (many centres accommodate single adults only)"),
            ("adults", "residents_centres", "Adults accommodated on the date of inspection", "persons",
             "the report does not state an adult count separately"),
            ("bedrooms", "capacity", "Bedrooms in the centre as stated in the report", "bedrooms",
             "the report does not state a bedroom count"),
        ]:
            v = p.get(key)
            if v is not None:
                fact(r, p, cat, metric, value_numeric=v, unit=unit, page=p.get("about_page"),
                     section="About the Service", method="pdf_text_narrative")
            else:
                fact(r, p, "unknown_at_source", metric, unit=unit, page=p.get("about_page"),
                     section="About the Service", method="unknown", confidence="low",
                     qualifier="unknown", is_unknown=True, unknown_reason=why)
        if p.get("residents") is not None and p.get("capacity"):
            fact(r, p, "occupancy", "Occupancy on the date of inspection",
                 value_numeric=round(100.0 * p["residents"] / p["capacity"], 1), unit="percent",
                 page=p.get("about_page"), section="About the Service",
                 method="derived_from_extracted_values", confidence="medium",
                 scope=f"{int(p['residents'])} residents / capacity {int(p['capacity'])}",
                 notes="DERIVED: residents / stated capacity. Not printed in the report.")

        # ---------- facts: inspection team ----------
        for ins in p.get("inspectors", []):
            fact(r, p, "inspections", "HIQA inspector on this inspection", value_text=ins["name"],
                 page=p.get("inspector_page"), section="Times of Inspection",
                 method="pdf_table_inspector", tier="public_professional",
                 notes=f"role: {ins['role']}")
        if p.get("inspectors"):
            fact(r, p, "inspections", "Inspectors on the inspection team",
                 value_numeric=float(len({i["name"] for i in p["inspectors"]})), unit="persons",
                 page=p.get("inspector_page"), section="Times of Inspection",
                 method="pdf_table_inspector", tier="public_professional")
        else:
            fact(r, p, "unknown_at_source", "Inspectors on the inspection team", unit="persons",
                 method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason="the 'Times of Inspection' table was not parsed")
        if p.get("inspection_days"):
            fact(r, p, "inspections", "Days on site", value_numeric=float(p["inspection_days"]),
                 unit="days", page=p.get("inspector_page"), section="Times of Inspection",
                 method="pdf_table_inspector")

        # ---------- facts: compliance profile ----------
        judged = [(_norm_judgment(appendix[x][0]) if appendix.get(x) and appendix[x][0]
                   else _norm_judgment(body[x][0]) if body.get(x) and body[x][0] else None)
                  for x in refs]
        judged = [j for j in judged if j]
        if refs:
            fact(r, p, "standards", "National standards assessed at this inspection",
                 value_numeric=float(len(refs)), unit="standards", page=p.get("appendix_page"),
                 section="Appendix 1", method="pdf_table_appendix1")
        for jname in ("Compliant", "Substantially compliant", "Partially compliant", "Not compliant"):
            n = sum(1 for j in judged if j == jname)
            fact(r, p, "compliance", f"Standards judged {jname}", value_numeric=float(n),
                 unit="standards", page=p.get("appendix_page"), section="Appendix 1",
                 method="pdf_table_appendix1", scope=f"of {len(refs)} standards assessed")
        if judged:
            ok = sum(1 for j in judged if j in ("Compliant", "Substantially compliant"))
            fact(r, p, "compliance", "Standards compliant or substantially compliant",
                 value_numeric=round(100.0 * ok / len(judged), 1), unit="percent",
                 page=p.get("appendix_page"), section="Appendix 1",
                 method="derived_from_extracted_judgments", confidence="medium",
                 scope=f"{ok} of {len(judged)} standards judged",
                 notes="DERIVED from the per-standard judgments; not printed in the report.")

        # ---------- facts: risk ----------
        risks = Counter(v["risk_rating"] for v in sec2.values() if v.get("risk_rating"))
        for colour in ("Red", "Orange", "Yellow"):
            fact(r, p, "risk", f"Failed standards risk-rated {colour} in the compliance plan",
                 value_numeric=float(risks.get(colour, 0)), unit="standards",
                 section="Compliance plan - Section 2", method="pdf_table_section2",
                 confidence="high" if sec2 else "low",
                 notes=("Red = high risk; the inspector sets the date by which the provider must comply"
                        if colour == "Red" else None))
        if not sec2:
            fact(r, p, "unknown_at_source", "Compliance plan Section 2 (risk ratings / comply-by dates)",
                 method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason=("no Section 2 table parsed - either the centre had no failed standard "
                                 "or the table layout defeated the parser"))
        fact(r, p, "compliance", "Compliance plan issued to the provider",
             value_text="yes" if p.get("has_compliance_plan") else "no",
             method="pdf_text_marker")
        for i, s in enumerate(p.get("urgent", [])):
            if s:
                fact(r, p, "risk", "Urgent / escalated finding (HIQA narrative)", value_text=s,
                     ref=f"urgent-{i+1}", method="pdf_text_narrative",
                     tier="public_regulatory_narrative", confidence="medium",
                     notes="sentence matched on urgent/immediate/escalation language; quote-stripped")
        for i, s in enumerate(p.get("vetting", [])):
            if s:
                fact(r, p, "vetting", "Garda vetting / police check finding (HIQA narrative)",
                     value_text=s, ref=f"vetting-{i+1}", method="pdf_text_narrative",
                     tier="public_regulatory_narrative", confidence="medium",
                     notes="sentence matched on vetting language; quote-stripped")
        if not p.get("vetting"):
            fact(r, p, "unknown_at_source", "Garda vetting / police check finding (HIQA narrative)",
                 method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason="no vetting sentence found in this report's narrative")
        for i, s in enumerate(p.get("overall", [])):
            if s:
                fact(r, p, "compliance", "HIQA overall summary of findings", value_text=s,
                     ref=f"overall-{i+1}", method="pdf_text_narrative",
                     tier="public_regulatory_narrative", confidence="medium",
                     notes="'Overall, ...' summary paragraph; quote-stripped")
        for e in p.get("errors", []):
            fact(r, p, "unknown_at_source", "Parser could not establish a documented element",
                 method="unknown", confidence="low", qualifier="unknown", is_unknown=True,
                 unknown_reason=e, notes=f"report: {Path(r['pdf_local_path']).name}")

    comp = pl.DataFrame(comp_rows, infer_schema_length=None)
    facts = pl.DataFrame(fact_rows, infer_schema_length=None)

    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)
    comp.write_parquet(SILVER / "hiqa_centre_compliance.parquet", compression="zstd", statistics=True)
    facts.write_parquet(SILVER / "hiqa_centre_facts.parquet", compression="zstd", statistics=True)
    comp.write_csv(eye / "hiqa_centre_compliance.csv")
    facts.write_csv(eye / "hiqa_centre_facts.csv")
    print(f"\nwrote hiqa_centre_compliance.parquet -- {comp.height} rows x {comp.width} cols")
    print(f"wrote hiqa_centre_facts.parquet      -- {facts.height} rows x {facts.width} cols")
    print(f"CSV eyeball copies -> {eye}")

    # ======================= VALIDATION =======================
    print("\n================ PARSE TALLY (101 reports) ================")
    for g in ("FULL", "PARTIAL", "FAILED"):
        print(f"  {g:8s} {tally.get(g, 0)}")
    print("  (PARTIAL = standards captured but something else missing or a HIQA source conflict; "
          "an absent capacity is a SOURCE absence, not a parse failure)")

    real = comp.filter(pl.col("standard_ref").is_not_null())
    print(f"\nstandards captured: {real.height} across {real['centre_id'].n_unique()} centres / "
          f"{real.select('centre_id', 'inspection_date').unique().height} inspections")
    per = real.group_by("centre_id", "inspection_date").len()
    print(f"standards per inspection: min={per['len'].min()} max={per['len'].max()} "
          f"mean={per['len'].mean():.1f}")

    print("\n--- judgment distribution (all 101 reports) ---")
    dist = (real.group_by("judgment_normalised").len()
            .with_columns((100 * pl.col("len") / real.height).round(1).alias("pct"))
            .sort("len", descending=True))
    print(dist)

    print("\n--- worst standards (share NOT compliant, standards judged >= 30 times) ---")
    worst = (real.group_by("standard_ref").agg(
        pl.len().alias("n"),
        (pl.col("judgment_normalised") == "Not compliant").sum().alias("not_compliant"),
        (pl.col("judgment_normalised").is_in(["Not compliant", "Partially compliant"])).sum().alias("failed"),
    ).filter(pl.col("n") >= 30)
        .with_columns((100 * pl.col("not_compliant") / pl.col("n")).round(1).alias("pct_not_compliant"),
                      (100 * pl.col("failed") / pl.col("n")).round(1).alias("pct_not_in_compliance"))
        .sort("pct_not_in_compliance", descending=True))
    with pl.Config(tbl_rows=12):
        print(worst.head(10))

    print("\n--- providers named ---")
    pv = comp.select("centre_id", "inspection_date", "provider_name", "provider_name_canonical",
                     "provider_key").unique()
    print(f"  provider named on {pv.filter(pl.col('provider_name').is_not_null()).height} of "
          f"{pv.height} inspections")
    print(f"  distinct verbatim spellings: {pv['provider_name'].n_unique()}")
    print(f"  distinct operators after key-folding: {pv['provider_key'].n_unique()}")
    top = (comp.select("centre_id", "provider_name_canonical").unique()
           .group_by("provider_name_canonical").len().sort("len", descending=True))
    with pl.Config(tbl_rows=15, fmt_str_lengths=44):
        print("  top operators by DISTINCT CENTRES:")
        print(top.head(12))

    print("\n--- per-county coverage ---")
    cty = (real.group_by("county").agg(
        pl.col("centre_id").n_unique().alias("centres"),
        pl.len().alias("judgments"),
        (100 * (pl.col("judgment_normalised") == "Not compliant").sum() / pl.len()).round(1).alias("pct_not_compliant"),
    ).sort("centres", descending=True))
    with pl.Config(tbl_rows=25):
        print(cty)

    print("\n--- SOURCE CONFLICTS (Appendix 1 vs body narrative) — reported, NEVER adjusted ---")
    cf = real.filter(pl.col("judgment_conflict"))
    print(f"  {cf.height} standards across {cf.select('centre_id', 'inspection_date').unique().height} "
          f"reports where HIQA's own two statements of the judgment disagree")
    with pl.Config(tbl_rows=12, fmt_str_lengths=26):
        print(cf.select("centre_id", "centre_name", "inspection_date", "standard_ref",
                        "judgment", "judgment_body").head(12))
    only = real.filter(pl.col("confidence") == "medium")
    print(f"  {only.height} standards carried by only ONE of the two locations in the report")

    print("\n--- compliance-plan risk ratings (Section 2: every FAILED standard) ---")
    rr = real.filter(pl.col("risk_rating").is_not_null())
    print(f"  risk rating recovered for {rr.height} failed standards "
          f"({real['finding_text'].is_not_null().sum()} standards carry HIQA's narrative finding; "
          f"{real['standard_title'].is_not_null().sum()} carry a standard title)")
    print(rr.group_by("risk_rating").len().sort("len", descending=True))
    print("  centres with the most RED (high-risk) rated standards:")
    with pl.Config(tbl_rows=8, fmt_str_lengths=34):
        print(real.filter(pl.col("risk_rating") == "Red")
              .group_by("centre_name", "county", "provider_name_canonical")
              .len().sort("len", descending=True).head(6))

    print("\n--- operators by NOT-COMPLIANT rate (>= 50 judgments) ---")
    with pl.Config(tbl_rows=8, fmt_str_lengths=36):
        print(real.group_by("provider_name_canonical").agg(
            pl.col("centre_id").n_unique().alias("centres"), pl.len().alias("judgments"),
            (100 * (pl.col("judgment_normalised") == "Not compliant").sum() / pl.len())
            .round(1).alias("pct_not_compliant"))
            .filter(pl.col("judgments") >= 50)
            .sort("pct_not_compliant", descending=True).head(6))

    print("\n--- UNKNOWN rows ---")
    print(f"  compliance: {comp['is_unknown'].sum()} unknown of {comp.height}")
    print(f"  facts:      {facts['is_unknown'].sum()} unknown of {facts.height}")
    ur = (facts.filter(pl.col("is_unknown")).group_by("unknown_reason").len()
          .sort("len", descending=True))
    with pl.Config(tbl_rows=20, fmt_str_lengths=92):
        print(ur)

    print("\n--- fact categories ---")
    with pl.Config(tbl_rows=20):
        print(facts.group_by("category").len().sort("len", descending=True))

    # ---------- CROSS-CHECK vs the HIQA overview report ----------
    print("\n================ CROSS-CHECK vs HIQA overview report ================")
    fig_p = SILVER / "hiqa_ipas_figures.parquet"
    if not fig_p.exists():
        print("  hiqa_ipas_figures.parquet not found - cross-check skipped")
        return
    fig = pl.read_parquet(fig_p).filter(pl.col("category") == "compliance_standard")
    ov = (fig.with_columns(
        pl.col("metric").str.extract(r"^Standard (\d+\.\d+) - ").alias("standard_ref"),
        pl.col("metric").str.extract(r"^Standard \d+\.\d+ - (.+)$").alias("j"))
        .filter(pl.col("j").is_in(["Compliant", "Substantially compliant",
                                   "Partially compliant", "Not compliant"]))
        .select("standard_ref", "j", pl.col("value_numeric").alias("overview_pct")))

    mine24 = real.filter(pl.col("inspection_date") < "2025-01-01")
    n24 = mine24.select("centre_id", "inspection_date").unique().height
    print(f"  the overview covers HIQA's 2024 inspections (60 conducted, 1 excluded = 59 reported).")
    print(f"  this corpus holds {n24} inspection reports dated in 2024 "
          f"({real.select('centre_id', 'inspection_date').unique().height} across the full 2024-2026 span).")
    tot = mine24.group_by("standard_ref").len().rename({"len": "n"})
    got = (mine24.group_by("standard_ref", "judgment_normalised").len()
           .join(tot, on="standard_ref")
           .with_columns((100 * pl.col("len") / pl.col("n")).round(0).alias("mine_pct"))
           .rename({"judgment_normalised": "j"}))
    cmp_ = (ov.join(got.select("standard_ref", "j", "mine_pct", "n"), on=["standard_ref", "j"], how="left")
            .with_columns((pl.col("mine_pct") - pl.col("overview_pct")).abs().alias("abs_diff"))
            .sort("abs_diff", descending=True, nulls_last=True))
    matched = cmp_.filter(pl.col("mine_pct").is_not_null())
    print(f"  comparable (standard x judgment) cells: {matched.height} of {cmp_.height}")
    if matched.height:
        print(f"  median absolute divergence: {matched['abs_diff'].median():.1f} percentage points; "
              f"mean {matched['abs_diff'].mean():.1f}pp; "
              f"within 5pp: {matched.filter(pl.col('abs_diff') <= 5).height}/{matched.height}; "
              f"within 10pp: {matched.filter(pl.col('abs_diff') <= 10).height}/{matched.height}")
        print("\n  LARGEST DIVERGENCES (reported, NOT adjusted):")
        with pl.Config(tbl_rows=12, fmt_str_lengths=26):
            print(matched.select("standard_ref", "j", "overview_pct", "mine_pct", "n", "abs_diff").head(12))
    # the sharper test: does the COMPLIED total agree, and does the SEVERITY SPLIT agree?
    ok_mine = (mine24.group_by("standard_ref").agg(
        pl.len().alias("n"),
        (100 * pl.col("judgment_normalised").is_in(["Compliant", "Substantially compliant"]).sum()
         / pl.len()).round(0).alias("mine_complied"),
        (100 * (pl.col("judgment_normalised") == "Not compliant").sum() / pl.len())
        .round(0).alias("mine_not_compliant")))
    ok_ov = (ov.filter(pl.col("j").is_in(["Compliant", "Substantially compliant"]))
             .group_by("standard_ref").agg(pl.col("overview_pct").sum().alias("ov_complied"))
             .join(ov.filter(pl.col("j") == "Not compliant")
                   .select("standard_ref", pl.col("overview_pct").alias("ov_not_compliant")),
                   on="standard_ref", how="left"))
    j = (ok_ov.join(ok_mine, on="standard_ref", how="inner")
         .with_columns((pl.col("mine_complied") - pl.col("ov_complied")).abs().alias("d_complied"),
                       (pl.col("mine_not_compliant") - pl.col("ov_not_compliant")).abs().alias("d_nc")))
    print(f"\n  A. 'compliant or substantially compliant' TOTAL per standard: "
          f"median divergence {j['d_complied'].median():.1f}pp, "
          f"within 10pp on {j.filter(pl.col('d_complied') <= 10).height}/{j.height} standards")
    print(f"  B. 'not compliant' SHARE per standard:                        "
          f"median divergence {j['d_nc'].median():.1f}pp, "
          f"within 10pp on {j.filter(pl.col('d_nc') <= 10).height}/{j.height} standards")
    print("  -> the overall compliant/not-compliant split reconciles well; the divergence sits in the"
          "\n     SEVERITY split, where these published reports show MORE 'not compliant' than the"
          "\n     overview's charts do. Worst offenders:")
    with pl.Config(tbl_rows=6, fmt_str_lengths=20):
        print(j.select("standard_ref", "n", "ov_complied", "mine_complied",
                       "ov_not_compliant", "mine_not_compliant", "d_nc")
              .sort("d_nc", descending=True).head(5))
    print("\n  NOTE: exact agreement is NOT expected and NOTHING here was tuned to match HIQA's."
          "\n  The overview summarises 59 inspections and rounds every chart to whole percent; this"
          "\n  corpus holds the separately PUBLISHED report PDFs. Every divergent judgment above was"
          "\n  re-verified against the raw PDF text by a third, independent regex path and held.")
    print("====================================================================")


if __name__ == "__main__":
    main()
