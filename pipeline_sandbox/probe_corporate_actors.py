"""PROBE (throwaway): can we extract APPOINTED ACTORS + ROLE + evidence span
from corporate-notice raw_text, auditably, without surfacing personal data?

Tests the D1 assumption from the review plan: that Irish corporate notices are
formulaic enough that "X was appointed liquidator/receiver/examiner" can be
extracted with an evidence span and a confidence proxy — replacing the
render-time firm-name regex in corporate.py:1362-1387.

PRIVACY GUARDRAILS (we agreed personal insolvency is excluded):
  1. Operate only on corporate_notices.parquet, which already drops personal
     insolvency at the ETL boundary. We RE-APPLY that filter defensively here.
  2. Emit only PROFESSIONAL appointee roles (receiver / liquidator / examiner /
     process_adviser). These are insolvency practitioners acting in a public
     professional capacity.
  3. petitioner / individual-subject mentions are QUARANTINED: we count them to
     understand exposure but never emit a private individual as a named actor.

Run:  .venv/Scripts/python.exe pipeline_sandbox/probe_corporate_actors.py
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

NOTICES = ROOT / "data/gold/parquet/corporate_notices.parquet"

PERSONAL_INSOLVENCY_RE = re.compile(
    r"A BANKRUPT|ADJUDICATED BANKRUPT|BANKRUPT IN MAIN PROCEEDINGS|PERSONAL INSOLVENCY|"
    r"DEBT SETTLEMENT ARRANGEMENT|DEBT RELIEF NOTICE|PROTECTIVE CERTIFICATE",
    re.I,
)

# Appointment-verb-anchored role patterns. The capture group before the verb is
# the actor span; we keep a wide evidence window for audit.
ROLE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("liquidator", re.compile(r"be appointed(?:\s+as)?(?:\s+the)?(?:\s+Official)?\s+Liquidator", re.I)),
    ("receiver", re.compile(r"APPOINTMENT OF (?:STATUTORY )?RECEIVER|be appointed(?:\s+as)?\s+Receiver", re.I)),
    ("examiner", re.compile(r"appointment of an Examiner|be appointed(?:\s+as)?\s+Examiner", re.I)),
    ("process_adviser", re.compile(r"Process Adviser|be appointed(?:\s+as)?\s+Process Adviser", re.I)),
]

# Actor-name extractor: "<Name>[, qualifier] of <firm/address> be appointed <role>"
# and "<Name> of <firm> be appointed as <role>". Best-effort, not exhaustive.
NAME_BEFORE_APPT = re.compile(
    r"(?:that|//|\d\.|hereby)\s+(?:Mr\.?\s+|Ms\.?\s+|Mrs\.?\s+)?"
    r"([A-Z][A-Za-zÁÉÍÓÚáéíóú'’\-]+(?:\s+[A-Z][A-Za-zÁÉÍÓÚáéíóú'’\-]+){1,3})"
    r"[, ].{0,80}?be appointed",
    re.I | re.S,
)

# Petitioner / on-behalf-of — QUARANTINE. Classify office/company vs individual.
PETITIONER_RE = re.compile(r"on the Petition of\s+(.{3,80}?)[,.]|on behalf of\s+(.{3,80}?)(?:,| the petitioner|having)", re.I | re.S)
OFFICE_OR_COMPANY_RE = re.compile(
    r"Collector[- ]General|Revenue|Minister|Bank|Limited|\bLtd\b|\bplc\b|\bDAC\b|Commission|Council|Authority|Company|Trustees|Department",
    re.I,
)

DISTRESS_SUBTYPES = {
    "receivership", "examinership", "scarp_process_adviser",
    "creditors_voluntary_liquidation", "members_voluntary_liquidation",
    "voluntary_liquidation_unspecified", "liquidation_unspecified", "court_winding_up",
}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def first_role(text: str) -> tuple[str | None, str | None]:
    """Return (role, evidence_span) for the first appointment verb found."""
    best = None
    for role, pat in ROLE_PATTERNS:
        m = pat.search(text)
        if m and (best is None or m.start() < best[1]):
            lo = max(0, m.start() - 90)
            hi = min(len(text), m.end() + 20)
            best = (role, m.start(), text[lo:hi].strip())
    if best:
        return best[0], best[2]
    return None, None


def actor_name(text: str) -> str | None:
    m = NAME_BEFORE_APPT.search(text)
    return m.group(1).strip() if m else None


def looks_like_person(name: str | None) -> bool:
    if not name:
        return False
    # crude: 2-4 capitalised tokens, no company suffix words
    if OFFICE_OR_COMPANY_RE.search(name):
        return False
    toks = name.split()
    return 2 <= len(toks) <= 4


def main() -> None:
    n = pl.read_parquet(NOTICES).with_row_index("probe_row_id")
    hr("INPUT + DEFENSIVE PERSONAL-INSOLVENCY RE-FILTER")
    leak = n.filter(pl.col("raw_text").map_elements(lambda s: bool(PERSONAL_INSOLVENCY_RE.search(s or "")), return_dtype=pl.Boolean))
    print(f"notices               : {n.height:,}")
    print(f"personal-insolvency leaks (must be 0): {leak.height}")
    n = n.filter(~pl.col("probe_row_id").is_in(leak["probe_row_id"]))

    # focus on subtypes where an appointment is expected
    work = n.filter(pl.col("notice_subtype").is_in(list(DISTRESS_SUBTYPES)))
    hr("SCOPE")
    print(f"distress/appointment-bearing notices: {work.height:,}")

    rows = work.select(["probe_row_id", "notice_subtype", "entity_name", "raw_text"]).to_dicts()
    out = []
    pet_individuals = 0
    pet_office = 0
    for r in rows:
        txt = r["raw_text"] or ""
        role, ev = first_role(txt)
        nm = actor_name(txt)
        # petitioner quarantine accounting
        pm = PETITIONER_RE.search(txt)
        if pm:
            pet = (pm.group(1) or pm.group(2) or "").strip()
            if OFFICE_OR_COMPANY_RE.search(pet):
                pet_office += 1
            elif looks_like_person(pet):
                pet_individuals += 1
        out.append({
            "notice_subtype": r["notice_subtype"],
            "role": role,
            "actor_name": nm,
            "actor_is_person": looks_like_person(nm),
            "has_evidence": ev is not None,
            "evidence_span": ev,
        })
    res = pl.DataFrame(out)

    hr("APPOINTMENT-ROLE EXTRACTION YIELD")
    got_role = res.filter(pl.col("role").is_not_null()).height
    got_name = res.filter(pl.col("actor_name").is_not_null()).height
    got_both = res.filter(pl.col("role").is_not_null() & pl.col("actor_name").is_not_null()).height
    print(f"role extracted        : {got_role:,}  ({got_role / res.height:.1%})")
    print(f"actor name extracted  : {got_name:,}  ({got_name / res.height:.1%})")
    print(f"role + name (strong)  : {got_both:,}  ({got_both / res.height:.1%})")

    hr("ROLE DISTRIBUTION")
    print(res.filter(pl.col("role").is_not_null()).group_by("role").len().sort("len", descending=True))

    hr("ROLE YIELD BY SUBTYPE")
    by = (
        res.group_by("notice_subtype")
        .agg(pl.len().alias("n"), pl.col("role").is_not_null().sum().alias("role_found"))
        .with_columns((pl.col("role_found") / pl.col("n")).alias("yield"))
        .sort("n", descending=True)
    )
    print(by)

    hr("PRIVACY: petitioner / on-behalf-of mentions (QUARANTINED — not emitted)")
    print(f"petitioner = office/company (safe to cite as actor): {pet_office:,}")
    print(f"petitioner = apparent individual (MUST quarantine): {pet_individuals:,}")
    print("note: extracted ACTOR names are insolvency practitioners (professional appointees).")
    print(f"  of strong extractions, actor looks like a person: {res.filter(pl.col('actor_is_person')).height:,}")
    print("  (practitioner personal names appear in the published notice by law — acceptable,")
    print("   but a display policy decision; petitioner-individuals are NOT.)")

    hr("SAMPLE: strong extractions (role + actor + evidence)")
    samp = res.filter(pl.col("role").is_not_null() & pl.col("actor_name").is_not_null()).head(12)
    for row in samp.iter_rows(named=True):
        print(f"\n[{row['notice_subtype']}] role={row['role']} actor={row['actor_name']!r} person={row['actor_is_person']}")
        print(f"   evidence: …{row['evidence_span']}…")

    hr("SAMPLE: role found but NO actor name (where extraction needs work)")
    miss = res.filter(pl.col("role").is_not_null() & pl.col("actor_name").is_null()).head(6)
    for row in miss.iter_rows(named=True):
        print(f"[{row['notice_subtype']}] role={row['role']}  evidence: …{row['evidence_span']}…")

    hr("EXPECTATION CHECKS")
    print(f"  [{'PASS' if leak.height == 0 else 'FAIL'}] no personal-insolvency leak")
    print(f"  [{'PASS' if got_role / res.height > 0.5 else 'CHECK'}] role extracted on >50% of distress notices")
    print(f"  [{'PASS' if got_both / res.height > 0.3 else 'CHECK'}] role+name (strong) on >30%")


if __name__ == "__main__":
    main()
