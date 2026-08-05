"""Extract DECISIONS and their steering from the clean council-minutes corpus.

EXPERIMENTAL sandbox. Reads meetings_clean.jsonl + corpus/<council>/*.txt and pulls, per
motion: proposer, seconder, outcome (AGREED/ADOPTED/DEFEATED/...), any numeric tally
("Result: 11 For, 7 Against"), roll-call flag, and the nearest preceding heading as item
context. "Steering" = who moves and seconds business, and what passes vs falls.

Outputs (all in this dir):
  decisions.jsonl   one row per detected motion/decision
  DECISIONS.md      summary: per-council counts, outcome mix, top proposers/seconders

Usage: python decisions_extract.py
"""

from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path

# Code lives in extractors/ (2026-08-01); the data workspace (corpus + jsonl in/out)
# stays in the sandbox where the harvest pipeline writes and the promote script reads.
HERE = Path(__file__).resolve().parents[1] / "pipeline_sandbox" / "council_minutes"

# name = up to ~45 chars after the honorific, cut at punctuation that ends a name
# GREEDY name match — the lazy form truncated every pattern-A SECONDER to its minimal
# 3 chars ("Bri" for "Brian Bbb"); nothing anchors the seconder's right edge, so the
# match must run long and _clean_name() splits the trailing sentence off (test-caught
# defect, 2026-08-01 — affected all extracted motions until the graduation tests)
_NAME = r"(?:Cllr\.?|Councillor|Comhairleoir)\s+([A-ZÁÉÍÓÚ][\w'’.\- ÁÉÍÓÚáéíóú]{2,45})"
_PROP_A = re.compile(
    r"(?:on the )?PROPOS(?:AL|ED)\s+(?:of|by)\s*" + _NAME + r"[,\s]+(?:and\s+)?SECONDED\s+by\s*" + _NAME,
    re.I,
)
_PROP_B = re.compile(r"Proposed by\s*" + _NAME + r"[,\s]+(?:and\s+)?[Ss]econded by\s*" + _NAME)
_OUTCOME = re.compile(r"\b(AGREED|ADOPTED|CARRIED|APPROVED|NOTED|RESOLVED|DEFEATED|LOST|DEFERRED|REJECTED|WITHDRAWN)\b")
_RESULT = re.compile(r"Result[:\s]+(\d+)\s*For[,\s]+(?:(\d+)\s*Against)?[,\s]*(?:(\d+)\s*Abstain\w*)?", re.I)
_ROLL = re.compile(r"roll[\s-]?call", re.I)
_DATE_FN = re.compile(r"(\d{1,2})[.\-\s_](\d{1,2})[.\-\s_](20\d{2})")
_DATE_ISO = re.compile(r"(20\d{2})[.\-\s_](\d{1,2})[.\-\s_](\d{1,2})")
_MONTH = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)"
    r"[a-z]*[.\-\s_]*(20\d{2})",
    re.I,
)

OUTCOME_PASS = {"AGREED", "ADOPTED", "CARRIED", "APPROVED", "RESOLVED"}
OUTCOME_FAIL = {"DEFEATED", "LOST", "REJECTED"}


def _clean_name(n: str) -> str:
    n = re.sub(r"\s+", " ", n).strip(" ,.;:-–")
    # cut trailing sentence fragments the lazy match sometimes drags in
    n = re.split(r"\s(?:that|the|to|and it|it was|this)\s", n, maxsplit=1, flags=re.I)[0]
    return n.strip(" ,.;:-–")


def _fname_date(fn: str) -> str:
    from urllib.parse import unquote

    fn = unquote(fn)
    if m := _DATE_ISO.search(fn):
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    if m := _DATE_FN.search(fn):
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    if m := _MONTH.search(fn):
        return f"{m.group(2)} {m.group(1).title()}"
    return ""


def _context(text: str, pos: int) -> str:
    """Nearest preceding short line that looks like an item heading."""
    for line in reversed(text[max(0, pos - 1200) : pos].splitlines()):
        ln = line.strip()
        if 8 <= len(ln) <= 120 and not _OUTCOME.search(ln) and not ln[0].islower():
            return ln
    return ""


def extract_doc(text: str, meta: dict) -> list[dict]:
    rows = []
    for rx in (_PROP_A, _PROP_B):
        for m in rx.finditer(text):
            after = text[m.end() : m.end() + 900]
            out = _OUTCOME.search(after)
            res = _RESULT.search(after)
            rows.append(
                {
                    "local_authority": meta["local_authority"],
                    "meeting": meta["meeting"],
                    "meeting_date": _fname_date(meta["meeting"]),
                    "source_url": meta.get("url", ""),
                    "item_context": _context(text, m.start()),
                    "proposer": _clean_name(m.group(1)),
                    "seconder": _clean_name(m.group(2)),
                    "motion_snippet": re.sub(r"\s+", " ", text[m.start() : m.start() + 260]),
                    "outcome": (out.group(1).upper() if out else ""),
                    "tally_for": int(res.group(1)) if res else None,
                    "tally_against": int(res.group(2)) if res and res.group(2) else None,
                    "tally_abstain": int(res.group(3)) if res and res.group(3) else None,
                    "rollcall": bool(_ROLL.search(after)),
                }
            )
    # dedupe pattern-A/B double hits on the same span
    seen, out_rows = set(), []
    for r in rows:
        k = (r["meeting"], r["proposer"], r["seconder"], r["motion_snippet"][:80])
        if k not in seen:
            seen.add(k)
            out_rows.append(r)
    return out_rows


def main() -> int:
    docs = [
        json.loads(line)
        for line in (HERE / "meetings_clean.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    all_rows, missing = [], 0
    for d in docs:
        tp = d.get("text_path")
        p = HERE / tp if tp else None
        if not p or not p.exists():
            missing += 1
            continue
        all_rows += extract_doc(p.read_text(encoding="utf-8"), d)

    (HERE / "decisions.jsonl").write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in all_rows), encoding="utf-8"
    )

    by_c = Counter(r["local_authority"] for r in all_rows)
    outcomes = Counter(r["outcome"] or "(none captured)" for r in all_rows)
    prop = Counter((r["local_authority"], r["proposer"]) for r in all_rows if r["proposer"])
    sec = Counter((r["local_authority"], r["seconder"]) for r in all_rows if r["seconder"])
    tallied = [r for r in all_rows if r["tally_for"] is not None]
    contested = [r for r in tallied if (r["tally_against"] or 0) > 0]

    L = ["# Council decisions & steering — extracted from the clean corpus\n"]
    L.append(
        f"Auto-generated by decisions_extract.py. **{len(all_rows)} motions** across "
        f"**{len(by_c)} councils** ({len(docs)} clean docs, {missing} missing text files skipped). "
        f"{len(tallied)} carry a numeric tally; {len(contested)} were contested (Against > 0).\n"
    )
    L.append("## Motions per council\n\n| council | motions |\n|---|---|")
    for c, n in by_c.most_common():
        L.append(f"| {c} | {n} |")
    L.append("\n## Outcome mix\n\n| outcome | n |\n|---|---|")
    for o, n in outcomes.most_common():
        L.append(f"| {o} | {n} |")
    L.append("\n## Top proposers (steering)\n\n| council | councillor | motions proposed |\n|---|---|---|")
    for (c, p_), n in prop.most_common(25):
        L.append(f"| {c} | {p_} | {n} |")
    L.append("\n## Top seconders\n\n| council | councillor | motions seconded |\n|---|---|---|")
    for (c, s_), n in sec.most_common(15):
        L.append(f"| {c} | {s_} | {n} |")
    if contested:
        L.append("\n## Contested votes (numeric tally, Against > 0)\n")
        L.append("| council | date | For | Against | outcome | context |\n|---|---|---|---|---|---|")
        for r in sorted(contested, key=lambda x: -(x["tally_against"] or 0))[:30]:
            L.append(
                f"| {r['local_authority']} | {r['meeting_date']} | {r['tally_for']} | "
                f"{r['tally_against']} | {r['outcome']} | {r['item_context'][:60]} |"
            )
    (HERE / "DECISIONS.md").write_text("\n".join(L), encoding="utf-8")
    print(
        f"decisions: {len(all_rows)} motions / {len(by_c)} councils / {len(tallied)} tallied "
        f"/ {len(contested)} contested -> decisions.jsonl + DECISIONS.md"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
