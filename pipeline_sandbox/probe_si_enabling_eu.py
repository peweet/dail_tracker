"""PROBE (throwaway): can we extract ENABLING POWER (section X of Act Y) and a
real EU reference (CELEX / Directive / Regulation number) from SI raw_text?

Tests review-plan items C2 (enabling-power) and C4 (EU/CELEX). Today the gold SI
table only has si_parent_legislation (Act-name regex) and si_eu_relationship (a
5-class heuristic) — neither carries a SECTION number or a specific EU instrument
ID. Source has raw_text, so we can measure the realistic ceiling.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_si_enabling_eu.py
Reads only; writes nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TAX = ROOT / "data/silver/iris_oifigiuil/iris_si_taxonomy.csv"

# C2: "in exercise of the powers conferred ... by section N (subsection) of the X Act YYYY"
ENABLING_SECTION_RE = re.compile(
    r"powers?\s+conferred\s+(?:on\s+(?:me|him|her|the\s+\w+)\s+)?by\s+"
    r"sections?\s+([0-9]+[A-Z]?(?:\s*\([0-9a-z]+\))*)"
    r"(?:[^.]{0,60}?)\bof\s+the\s+([A-Z][A-Za-z0-9&'\-, ]+?\sAct,?\s+\d{4})",
    re.I | re.S,
)
# looser: any "section N of the X Act YYYY"
SECTION_OF_ACT_RE = re.compile(r"sections?\s+([0-9]+[A-Z]?)\s+of\s+the\s+([A-Z][A-Za-z0-9&'\-, ]+?\sAct,?\s+\d{4})", re.I)

# C4: real EU instrument identifiers
CELEX_RE = re.compile(r"\b[0-9]{5}[A-Z][0-9]{4}\b")  # e.g. 32014L0102
DIRECTIVE_RE = re.compile(r"\bDirective\s+(?:\(EU\)\s+)?(?:No\.?\s*)?(\d{1,4}/\d{1,4})(?:/E[UC])?", re.I)
REGULATION_RE = re.compile(r"\bRegulation\s+\(E[UC]\)\s+(?:No\.?\s*)?(\d{1,4}/\d{1,4})", re.I)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    df = pl.read_csv(TAX, infer_schema_length=3000, ignore_errors=True)
    # SI rows only, not quarantined, with text
    sis = df.filter(
        pl.col("si_number").is_not_null()
        & pl.col("raw_text").is_not_null()
        & (pl.col("is_quarantined") != True)  # noqa: E712
    )
    hr("BASIS")
    print(f"SI rows with raw_text (not quarantined): {sis.height:,}")

    rows = sis.select(["si_year", "si_number", "raw_text", "si_parent_legislation", "si_eu_relationship"]).to_dicts()

    enab_strict = enab_loose = celex = direc = regn = 0
    eu_heur = 0
    samp_enab, samp_eu = [], []
    for r in rows:
        t = r["raw_text"] or ""
        ms = ENABLING_SECTION_RE.search(t)
        ml = SECTION_OF_ACT_RE.search(t)
        if ms:
            enab_strict += 1
            if len(samp_enab) < 12:
                samp_enab.append((f"{r['si_year']}/{r['si_number']}", ms.group(1).strip(), ms.group(2).strip()))
        if ml:
            enab_loose += 1
        cz = CELEX_RE.search(t)
        dz = DIRECTIVE_RE.search(t)
        rz = REGULATION_RE.search(t)
        if cz:
            celex += 1
        if dz:
            direc += 1
        if rz:
            regn += 1
        if (cz or dz or rz) and len(samp_eu) < 12:
            samp_eu.append((f"{r['si_year']}/{r['si_number']}",
                            cz.group(0) if cz else "",
                            dz.group(0) if dz else (rz.group(0) if rz else "")))
        if (r["si_eu_relationship"] or "none_detected") != "none_detected":
            eu_heur += 1

    n = sis.height
    hr("C2 — ENABLING POWER (section X of Act Y)")
    print(f"strict 'powers conferred by section N of the X Act YYYY': {enab_strict:,}  ({enab_strict / n:.1%})")
    print(f"loose  'section N of the X Act YYYY' anywhere           : {enab_loose:,}  ({enab_loose / n:.1%})")
    print("  (today's gold has NO section number at all — this is net-new)")
    hr("C2 SAMPLES (si -> section, enabling act)")
    for sid, sec, act in samp_enab:
        print(f"  {sid}: s.{sec} of {act}")

    hr("C4 — REAL EU INSTRUMENT IDs (vs today's 5-class heuristic)")
    print(f"existing si_eu_relationship != none_detected : {eu_heur:,}  ({eu_heur / n:.1%})  <- current heuristic")
    print(f"CELEX number (e.g. 32014L0102)               : {celex:,}  ({celex / n:.1%})")
    print(f"Directive NNNN/NN(/EU)                        : {direc:,}  ({direc / n:.1%})")
    print(f"Regulation (EU) NNNN/NN                       : {regn:,}  ({regn / n:.1%})")
    hr("C4 SAMPLES (si -> celex, directive/regulation)")
    for sid, cz, dz in samp_eu:
        print(f"  {sid}: celex={cz or '-'}  instrument={dz or '-'}")

    hr("VERDICT")
    print(f"  enabling-section feasible on ~{enab_strict / n:.0%}-{enab_loose / n:.0%} of SIs (strict..loose)")
    print(f"  specific EU instrument id recoverable on ~{max(celex, direc, regn) / n:.0%} of SIs")
    print("  both are net-new vs current gold; both must carry match_method + confidence + caveat")


if __name__ == "__main__":
    main()
