"""National Standards for accommodation offered to people in the protection process (2021).

WHY THIS DOCUMENT MATTERS MOST: we hold 2,668 HIQA compliance judgments keyed by
standard ref (1.1, 3.1, 4.3, 10.4 ...) but no standard TEXT. This extractor produces the
JOIN-KEY LOOKUP that gives every judgment a human-readable meaning.

Two outputs:
  1. national_standards_lookup.parquet — theme_no, theme_name, standard_ref, standard_title,
     standard_text, features  (+ provenance).  standard_ref format is the document's own
     "<theme>.<n>" (e.g. "1.1", "10.4") — identical to HIQA's judgment key; verified here.
  2. national_standards_facts.parquet — the canonical IPAS fact schema (see
     ipas_doc_registry.py), one row per standard plus the numeric requirements the standards
     impose (4.65m2 per resident, bunk-bed rule, single-room-after-9-months) and explicit
     UNKNOWN rows for what the document does not establish.

The structure is parsed from the born-digital text layer (fitz), NOT hand-typed:
    THEME <n>: <NAME>
    Standard <n>.<m>
    <binding statement>            <- becomes standard_title AND standard_text
    Indicators
    <n>.<m>.<k>. <Title>. <text>   <- becomes features

The document gives each standard ONE binding statement and NO separate short title
(para 15: "Standard - a binding statement setting out what is required of a service
provider"). standard_title and standard_text therefore carry the same verbatim string:
inventing a shorter title would be a guess. HIQA prints that same statement as its
"standard_title", which is what makes the join verifiable.

SANDBOX ONLY. All rows value_safe_to_sum=False.
"""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from _common import BRONZE, SILVER, now_iso, sha256_bytes

DOC_KEY = "national_standards"
DOC_TITLE = ("National Standards for accommodation offered to people in the protection "
             "process (2021)")
SRC_URL = "https://assets.gov.ie/static/documents/national-standards.pdf"
PDF = BRONZE / "ipas_context" / "national_standards.pdf"
TXT = BRONZE / "ipas_context" / "text" / "national_standards.txt"

BODY_FIRST, BODY_LAST = 18, 69  # PDF pages of the standards body (appendices start p70)
RE_THEME = re.compile(r"^THEME\s+(\d+):\s*(.+?)\s*$")
RE_STD = re.compile(r"^Standard\s+(\d+\.\d+)\s*$")
RE_IND = re.compile(r"^(\d+\.\d+\.\d+)\.?\s+(.*)$")


def load_pages() -> dict[int, str]:
    parts = re.split(r"=== PAGE (\d+) ===", TXT.read_text(encoding="utf-8"))
    return {int(parts[i]): parts[i + 1] for i in range(1, len(parts), 2)}


def join_lines(lines: list[str]) -> str:
    """Join wrapped lines, closing up words broken across a line break.

    A naive space-join renders the PDF's 'child-\\nfriendly' as 'child- friendly', which is
    not what the document says. Every one of the 7 line-break hyphens in the standards body
    was inspected: non-family, child-friendly, age-appropriate, off-site, self-care (x2),
    non-statutory -- ALL are real hyphenated compounds, and NONE is a syllable split. So the
    hyphen is KEPT and only the break is closed. (Had any been a true syllable split, the
    hyphen would have had to go; the rule is evidence-based, not assumed.)
    """
    out = ""
    for ln in lines:
        if not out:
            out = ln
        elif re.search(r"\w-$", out) and re.match(r"[a-z]", ln):
            out += ln          # keep the hyphen, close the break
        else:
            out += " " + ln
    return out.strip()


def parse_standards(pages: dict[int, str]) -> list[dict]:
    """Walk the body pages and cut them into theme -> standard -> statement + indicators."""
    stream: list[tuple[int, str]] = []
    for p in range(BODY_FIRST, BODY_LAST + 1):
        for raw in pages.get(p, "").splitlines():
            s = " ".join(raw.split())
            if not s or re.fullmatch(r"\d{1,3}", s):  # printed-page number line
                continue
            stream.append((p, s))

    out: list[dict] = []
    theme_no, theme_name = None, None
    cur: dict | None = None
    mode = None  # 'statement' | 'indicators'

    def close(c: dict | None) -> None:
        if c is None:
            return
        c["standard_text"] = join_lines(c.pop("_stmt"))
        inds: list[list[str]] = []
        for _pg, line in c.pop("_inds"):
            if RE_IND.match(line):
                inds.append([line])
            elif inds:  # continuation of the previous indicator
                inds[-1].append(line)
        c["features"] = "\n".join(join_lines(x) for x in inds)
        c["n_features"] = len(inds)
        out.append(c)

    for p, s in stream:
        m = RE_THEME.match(s)
        if m:
            close(cur)
            cur, mode = None, None
            theme_no, theme_name = int(m.group(1)), m.group(2).strip()
            continue
        m = RE_STD.match(s)
        if m:
            close(cur)
            cur = {"theme_no": theme_no, "theme_name": theme_name,
                   "standard_ref": m.group(1), "page": p, "printed_page": str(p - 1),
                   "_stmt": [], "_inds": []}
            mode = "statement"
            continue
        if cur is None:
            continue
        if s == "Indicators":
            mode = "indicators"
            continue
        (cur["_stmt"] if mode == "statement" else cur["_inds"]).append(
            s if mode == "statement" else (p, s))
    close(cur)
    return out


# ---- facts that are NOT the standards themselves: requirements + context + unknowns ----
# (page, ref, section, category, subject, metric, value_numeric, value_text, unit,
#  qualifier, period, scope, is_unknown, unknown_reason, notes)
EXTRA: list[tuple] = [
    (33, "4.2.2", "Theme 4: Accommodation", "standards", "all accommodation centres",
     "Minimum space per resident per bedroom", 4.65, None, "sq_m", "exact", "2021",
     "every resident, every bedroom", False, None,
     "'A minimum space of 4.65m2 for each resident per bedroom is provided for each resident. "
     "Additional space may be required for persons with special reception needs.' C&AG 10.41 "
     "cites this figure but dates the standards to 2019; the published document carries no year "
     "on its face (see the publication-date UNKNOWN row)."),
    (34, "4.2.10", "Theme 4: Accommodation", "standards", "all accommodation centres",
     "Minimum age above which bunk-beds are prohibited unless requested", 15, None, "years",
     "exact", "2021", "residents aged 15+", False, None,
     "'No bunk-beds are provided for persons aged 15 and over, unless requested.'"),
    (35, "4.3.4", "Theme 4: Accommodation", "standards", "single residents",
     "Months after which a single resident may APPLY for a single bedroom", 9, None, "months",
     "exact", "2021", "single residents", False, None,
     "Resident-facing text: 'you can apply for a single bedroom after 9 months and should be "
     "given a single room within 15 months (in so far as it is practicable)'. The 15-month limb "
     "is qualified by 'in so far as it is practicable' - it is not an unconditional right."),
    (35, "4.3.4", "Theme 4: Accommodation", "standards", "single residents",
     "Months within which a single resident should be GIVEN a single room", 15, None, "months",
     "at_least", "2021", "single residents", False, None,
     "qualified 'in so far as it is practicable' - see the 9-month row."),
    (82, "Appendix 6", "Appendix 6: Child Safeguarding Statement", "safeguarding",
     "all accommodation centres", "Review interval for the Child Safeguarding Statement", 12,
     None, "months", "exact", "2021", "each centre", False, None,
     "'reviewed every 12 months, or as soon as is practicable after there has been a material "
     "change in any matter to which the statement refers'."),
    (5, "para 14", "Introduction - Structure", "standards", "National Standards",
     "Themes in the National Standards framework", 10, None, "themes", "exact", "2021",
     "whole document", False, None, "Ten themes, each with one or more standards."),
    (5, "para 15", "Introduction - Structure", "standards", "National Standards",
     "Standards in the National Standards framework", 40, None, "standards", "exact", "2021",
     "whole document", False, None,
     "Counted by parsing the body (pp.18-69). Each standard = a BINDING statement plus "
     "indicators: 'Standard - a binding statement setting out what is required of a service "
     "provider' (para 15)."),
    (5, "para 15", "Introduction - Structure", "legal_obligation", "service providers",
     "Legal character of the standards", None,
     "Standard - a binding statement setting out what is required of a service provider. Each "
     "standard describes the high-level outcome required. ... indicators form contractual "
     "obligations with RIA or regulatory obligations which are clearly binding on service "
     "providers. For the remainder of indicators, service providers must take all necessary "
     "steps [to meet them].",
     "text", "exact", "2021", "all contracted service providers", False, None,
     "Verbatim (paras 15-16, p5, lightly joined across line breaks). The standards bind via "
     "CONTRACT with RIA, not via statute - which is why SI 649/2023 had to give HIQA a "
     "monitoring role, and why HIQA still has no sanction power."),
    (6, "para 18", "Introduction - Monitoring", "compliance", "inspectorate",
     "Monitoring recommendation the standards rest on", None,
     "The McMahon Working Group recommended that the Minister for Justice and Equality "
     "establish 'an inspectorate (or identify an existing body), independent of the service "
     "provider and of the Department, to carry out inspections'.",
     "text", "exact", "2021", "State", False, None,
     "The document names HIQA only ONCE (p5) and only as an adviser to the drafting process - "
     "it does not assign HIQA the monitoring role. That came later, via SI 649/2023 Reg 27A "
     "(in force 9 Jan 2024)."),
    (3, "paras 1-2", "Introduction - Scope", "standards", "service providers",
     "Bodies the standards apply to", None,
     "The National Standards apply to all service providers contracted by the Reception and "
     "Integration Agency (RIA) to operate and manage accommodation centres.",
     "text", "exact", "2021", "RIA-contracted centres", False, None,
     "Scope is CONTRACTED centres. Emergency accommodation procured outside the RIA/IPAS "
     "contracting model is not named in the scope clause; the document predates the 2022+ "
     "emergency-accommodation surge (24,718 persons in emergency accommodation at end 2024, "
     "C&AG Fig 10.1)."),
    (3, "para 3", "Introduction - How developed", "standards", "National Standards",
     "Date the Working Group that led to the standards was announced", None,
     "13 October 2014", "date", "exact", "2014", "State", False, None,
     "Announced by then Minister for Justice and Equality Frances Fitzgerald TD and Minister "
     "of State Aodhan O Riordain TD (McMahon Working Group)."),
]

UNKNOWNS: list[tuple] = [
    (None, "whole document", "unknown_at_source", "National Standards",
     "Publication / commencement date printed in the document",
     "UNKNOWN: the PDF carries no publication year, no commencement date and no 'comes into "
     "operation' clause anywhere in its 83 pages (searched: '2021', 'January 2021', "
     "'commence', 'come into' - zero hits). The 2021 date used as this doc_key's label comes "
     "from the registry/publisher metadata, NOT from the document. C&AG 10.41 cites the same "
     "4.65m2 standard as the 'national standards 2019'. Do not assert a year from the text."),
    (None, "whole document", "unknown_at_source", "National Standards",
     "Penalty, sanction or enforcement mechanism for breach of a standard",
     "UNKNOWN AT SOURCE: the document sets out binding statements and indicators but specifies "
     "NO sanction, penalty, deregistration or enforcement consequence for non-compliance. "
     "Consistent with SI 649/2023, which gives HIQA a monitoring role with zero enforcement "
     "powers."),
    (None, "standards 6.3 and 9.2", "unknown_at_source", "National Standards",
     "HIQA compliance judgments against standards 6.3 and 9.2",
     "UNKNOWN: the document defines 40 standards; HIQA's 2,668 judgments (101 inspection "
     "reports) cover only 38 of them. Standards 6.3 (residents can make a complaint) and 9.2 "
     "were never judged in any report we hold - no judgment exists to join to. Absence of a "
     "judgment is NOT evidence of compliance."),
]


def build() -> tuple[pl.DataFrame, pl.DataFrame]:
    sha = sha256_bytes(Path(PDF).read_bytes())
    pages = load_pages()
    stds = parse_standards(pages)

    # ---- structural validation: never ship a silently mis-parsed lookup ----
    assert len({s["theme_no"] for s in stds}) == 10, f"themes: {len({s['theme_no'] for s in stds})}"
    assert len(stds) == len({s["standard_ref"] for s in stds}), "duplicate standard_ref"
    for s in stds:
        assert s["standard_ref"].split(".")[0] == str(s["theme_no"]), f"ref/theme mismatch {s}"
        assert len(s["standard_text"]) > 40, f"suspiciously short statement for {s['standard_ref']}"
        assert s["n_features"] > 0, f"no indicators parsed for {s['standard_ref']}"

    lookup = pl.DataFrame([{
        "theme_no": s["theme_no"],
        "theme_name": s["theme_name"],
        "standard_ref": s["standard_ref"],
        "standard_title": s["standard_text"],   # the doc's binding statement = HIQA's title
        "standard_text": s["standard_text"],
        "features": s["features"],
        "n_features": s["n_features"],
        "page": s["page"],
        "printed_page": s["printed_page"],
        "source_url": SRC_URL,
        "source_document_hash": sha,
        "extraction_method": "fitz_text_layer_structural_parse",
        "confidence": "high",
        "privacy_tier": "public_document",
        "derived_at": now_iso(),
    } for s in stds]).sort(["theme_no", "standard_ref"])

    # ---- canonical facts ----
    rows: list[dict] = []
    for s in stds:
        rows.append({
            "page": s["page"], "printed_page": s["printed_page"],
            "ref": f"Standard {s['standard_ref']}",
            "section": f"Theme {s['theme_no']}: {s['theme_name'].title()}",
            "category": "standards", "subject": "all contracted accommodation centres",
            "metric": f"National Standard {s['standard_ref']} - binding statement",
            "value_numeric": None, "value_text": s["standard_text"],
            "unit": "text", "qualifier": "exact", "period": "2021",
            "scope": f"{s['n_features']} indicators", "is_unknown": False,
            "unknown_reason": None,
            "notes": ("HIQA judges centres against this standard_ref; join key for "
                      "hiqa_centre_compliance.standard_ref."),
            "extraction_method": "fitz_text_layer_structural_parse", "confidence": "high",
        })
    for (pg, ref, sec, cat, subj, metric, vnum, vtext, unit, qual, per, scope, unk, ur, notes) in EXTRA:
        rows.append({
            "page": pg, "printed_page": str(pg - 1) if pg else None, "ref": ref, "section": sec,
            "category": cat, "subject": subj, "metric": metric,
            "value_numeric": float(vnum) if vnum is not None else None, "value_text": vtext,
            "unit": unit, "qualifier": qual, "period": per, "scope": scope,
            "is_unknown": unk, "unknown_reason": ur, "notes": notes,
            "extraction_method": "manual_curation_from_fitz_text_full_read",
            "confidence": "high",
        })
    for (pg, scope, cat, subj, metric, reason) in UNKNOWNS:
        rows.append({
            "page": pg, "printed_page": None, "ref": None, "section": "whole document",
            "category": cat, "subject": subj, "metric": metric, "value_numeric": None,
            "value_text": None, "unit": None, "qualifier": "unknown", "period": "2021",
            "scope": scope, "is_unknown": True, "unknown_reason": reason, "notes": None,
            "extraction_method": "manual_curation_from_fitz_text_full_read",
            "confidence": "high",
        })

    facts = pl.DataFrame([{
        "fact_id": f"{DOC_KEY}-{i:03d}", "doc_key": DOC_KEY, "doc_title": DOC_TITLE,
        **r, "source_url": SRC_URL, "source_document_hash": sha,
        "privacy_tier": "public_document", "value_safe_to_sum": False,
        "derived_at": now_iso(),
    } for i, r in enumerate(sorted(rows, key=lambda r: (r["page"] or 999, r["category"])), 1)],
        schema_overrides={"value_numeric": pl.Float64, "page": pl.Int64},
        infer_schema_length=None)
    cols = ["fact_id", "doc_key", "doc_title", "page", "printed_page", "ref", "section",
            "category", "subject", "metric", "value_numeric", "value_text", "unit", "qualifier",
            "period", "scope", "is_unknown", "unknown_reason", "notes", "source_url",
            "source_document_hash", "extraction_method", "confidence", "privacy_tier",
            "value_safe_to_sum", "derived_at"]
    return lookup, facts.select(cols)


def crosscheck(lookup: pl.DataFrame) -> None:
    """Spot-check the join key against the 2,668 real HIQA judgments."""
    hp = SILVER / "hiqa_centre_compliance.parquet"
    if not hp.exists():
        print("\n[crosscheck] hiqa_centre_compliance.parquet absent - SKIPPED")
        return
    h = pl.read_parquet(hp)
    hrefs = set(h["standard_ref"].drop_nulls().unique())
    lrefs = set(lookup["standard_ref"])
    joined = h.join(lookup.select("standard_ref", "theme_name", "standard_text"),
                    on="standard_ref", how="left")
    matched = joined.filter(pl.col("standard_text").is_not_null()).height
    print(f"\n[crosscheck vs hiqa_centre_compliance.parquet]")
    print(f"  judgments            : {h.height}")
    print(f"  judgments that JOIN  : {matched} ({matched / h.height * 100:.1f}%)")
    print(f"  HIQA refs            : {len(hrefs)}  | lookup refs: {len(lrefs)}")
    print(f"  HIQA refs NOT in lookup: {sorted(hrefs - lrefs) or 'none'}")
    print(f"  lookup refs never judged by HIQA: {sorted(lrefs - hrefs)}")
    # does the HIQA-printed title agree with the standard's statement in the source doc?
    cmp = (h.select("standard_ref", "standard_title").drop_nulls().unique()
           .join(lookup.select("standard_ref", "standard_text"), on="standard_ref", how="inner"))

    def norm(c):
        return (pl.col(c).str.replace_all(r"[‘’“”]", "'")
                .str.replace_all(r"\s+", " ").str.strip_chars().str.to_lowercase()
                .str.strip_chars(" ."))
    cmp = cmp.with_columns(
        _h=norm("standard_title"), _s=norm("standard_text")).with_columns(
        verdict=pl.when(pl.col("_h") == pl.col("_s")).then(pl.lit("identical"))
        .when(pl.col("_s").str.starts_with(pl.col("_h"))).then(pl.lit("hiqa_prints_first_sentence_only"))
        .otherwise(pl.lit("DIVERGENT")))
    print("  HIQA standard_title vs source statement:")
    for r in cmp.group_by("verdict").len().sort("len", descending=True).iter_rows(named=True):
        print(f"    {r['verdict']:<34} {r['len']:>2} / {cmp.height} distinct refs")
    for r in cmp.filter(pl.col("verdict") != "identical").sort("standard_ref").iter_rows(named=True):
        extra = r["standard_text"][len(r["standard_title"]):].strip()
        print(f"    ~ {r['standard_ref']} [{r['verdict']}] source adds: {extra[:96]}...")
    assert not (cmp["verdict"] == "DIVERGENT").any(), "a HIQA title contradicts the source text"


def main() -> None:
    lookup, facts = build()
    eye = SILVER / "_eyeball"
    eye.mkdir(exist_ok=True)

    lp = SILVER / "national_standards_lookup.parquet"
    lookup.write_parquet(lp, compression="zstd", statistics=True)
    lookup.write_csv(eye / "national_standards_lookup.csv")
    fp = SILVER / "national_standards_facts.parquet"
    facts.write_parquet(fp, compression="zstd", statistics=True)
    facts.write_csv(eye / "national_standards_facts.csv")

    print(f"wrote {lp} - {lookup.height} standards")
    print(f"wrote {fp} - {facts.height} facts")
    with pl.Config(tbl_rows=15, fmt_str_lengths=46, tbl_width_chars=150):
        print(lookup.group_by("theme_no", "theme_name")
              .agg(pl.len().alias("standards"), pl.col("n_features").sum().alias("indicators"))
              .sort("theme_no"))
    print(f"\ntotal indicators (features): {lookup['n_features'].sum()}")
    print(facts.group_by("category").agg(pl.len(), pl.col("is_unknown").sum().alias("unknown"))
          .sort("len", descending=True))
    print(f"unknown facts: {facts['is_unknown'].sum()} / {facts.height}")
    assert not facts["value_safe_to_sum"].any()
    crosscheck(lookup)


if __name__ == "__main__":
    main()
