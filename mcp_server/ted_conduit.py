"""Live TED notice conduit + serve-layer reconciliation (MCP helper).

The dail-tracker gold layer stores a deliberately THIN slice of each TED award
(winner, buyer, CPV, a value-kind tag) — enough to rank suppliers, but stripped
of the meaning a human needs: what is actually being built, the real framework
ceiling, the award-criteria weighting, the competitive field. That richer record
lives in the authoritative source (TED, the EU Official Journal), one `notice_url`
hop away — and every gold row already carries that URL.

This module is the CONDUIT: given a publication-number it fetches the live notice
through the repo's own tested TED v3 client (services.ted_search) and parses the
fields the gold layer drops. It then RECONCILES source-of-truth against what the
pipeline ingested, emitting the field-level discrepancies — those discrepancies
are the feedback loop: each one is a concrete "gold is thin/stale/mis-parsed here"
signal a pipeline QA step can consume.

Nothing here is inference. Every value is the contracting authority's own
published figure or a plain source-vs-gold equality check.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from services.ted_search import fetch_ted_search

# Curated rich field set — the meaning the gold slice omits. (Field IDs verified
# against the TED v3 vocabulary; the API rejects unknown IDs, so this list is a
# tested contract.)
NOTICE_FIELDS = [
    "publication-number",
    "notice-title",
    "title-proc",
    "description-proc",
    "buyer-name",
    "procedure-type",
    "notice-type",
    "classification-cpv",
    "estimated-value-proc",
    "estimated-value-cur-proc",
    "result-framework-maximum-value-notice",
    "framework-maximum-value-lot",
    "award-criterion-type-lot",
    "award-criterion-number-lot",
    "award-criterion-number-weight-lot",
    "received-submissions-type-val",
    "winner-identifier",
    "winner-size",
    "dispatch-date",
]

# Where reconciliation discrepancies accumulate. Out-of-repo by default (this whole
# server is), but a single constant so a pipeline QA step can later point at it /
# promote it into the repo's source-health inputs.
RECON_LOG = Path(__file__).with_name("_reconciliation_log.jsonl")

_PN_RE = re.compile(r"(\d{4,8}-\d{4})")  # TED publication-number, e.g. 291090-2024


def normalise_pn(notice: str) -> str | None:
    """Pull a TED publication-number out of a raw id or a notice_url."""
    m = _PN_RE.search(notice or "")
    return m.group(1) if m else None


def _eng(v: Any) -> Any:
    """First English (or first available) value from TED's multilingual dict/list."""
    if isinstance(v, dict):
        for key in ("eng", "ENG", "en", *v.keys()):
            if v.get(key):
                val = v[key]
                return val[0] if isinstance(val, list) else val
        return None
    if isinstance(v, list):
        return v[0] if v else None
    return v


def _num(v: Any) -> float | None:
    try:
        return float(str(v).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


# Money figures embedded in the procurement title, e.g. "€500,000 - €5,000,000".
_MONEY_RE = re.compile(r"€\s?([\d,]+(?:\.\d+)?)")
# A seed-deliverable phrase, e.g. "12 semi-detached dwellings at Bun Daire, Kinnegad".
_DELIVERABLE_RE = re.compile(
    r"(\d+)\s+(?:no\.?\s+)?([\w\-/. ]*?(?:dwelling|home|hous|unit|apartment|flat|school|classroom)\w*)\b",
    re.I,
)


def _parse(n: dict) -> dict:
    """One raw TED notice dict -> the human-legible record the gold slice omits."""
    title = _eng(n.get("title-proc")) or _eng(n.get("notice-title"))
    desc = _eng(n.get("description-proc"))

    # Award criteria: TED returns parallel arrays (types vs numbers) that don't always
    # align 1:1. Surface BOTH raw — never fabricate a pairing (that would be inference).
    crit_types = n.get("award-criterion-type-lot")
    crit_nums = n.get("award-criterion-number-lot")

    # Per-project value band lives in the title text; the framework ceiling is a field.
    band = [m.replace(",", "") for m in _MONEY_RE.findall(title or "")]
    deliv = _DELIVERABLE_RE.search(desc or title or "")

    return {
        "publication_number": n.get("publication-number"),
        "title": title,
        "description": desc,
        "deliverable_seed": (f"{deliv.group(1)} {deliv.group(2).strip()}" if deliv else None),
        "buyer": _eng(n.get("buyer-name")),
        "procedure_type": n.get("procedure-type"),
        "notice_type": n.get("notice-type"),
        "cpv": n.get("classification-cpv"),
        "framework_maximum_value_eur": _num(
            n.get("result-framework-maximum-value-notice") or _eng(n.get("framework-maximum-value-lot"))
        ),
        "estimated_value_eur": _num(_eng(n.get("estimated-value-proc"))),
        "per_project_band_eur": [float(b) for b in band] if band else None,
        "award_criteria_raw": {"types": crit_types, "numbers": crit_nums},
        "tenders_received": _eng(n.get("received-submissions-type-val")),
        "winner_identifiers": n.get("winner-identifier"),
        "winner_sizes": n.get("winner-size"),
        "dispatch_date": n.get("dispatch-date"),
        "source_links": n.get("links") or {},
    }


def fetch_notice(pn: str) -> dict | None:
    """Fetch + parse ONE authoritative TED notice by publication-number. None if absent."""
    notices = fetch_ted_search(
        f"publication-number={pn}",
        NOTICE_FIELDS,
        label=f"conduit:{pn}",
        limit=5,
        max_pages=1,  # one notice; skips the bulk-completeness assertion
    )
    for n in notices:
        if n.get("publication-number") == pn:
            return _parse(n)
    return _parse(notices[0]) if notices else None


def reconcile(pn: str, gold_rows: list[dict], source: dict | None) -> list[dict]:
    """Compare the ingested gold slice against the authoritative source, field by field.

    Returns a list of discrepancy dicts (the feedback-loop payload). Each names the
    field, what gold holds, what the source says, and why it matters. Side-effect:
    appends the batch to RECON_LOG so a pipeline QA step can consume the stream.
    """
    out: list[dict] = []
    if source is None:
        out.append(
            {
                "pn": pn,
                "field": "notice",
                "gold": "present",
                "source": "unreachable",
                "issue": "authoritative notice could not be fetched — cannot verify gold",
            }
        )
        return _emit(out)

    g = gold_rows[0] if gold_rows else {}

    # 1. Tender count — gold routinely NULL where the source reports it.
    g_tend = g.get("n_tenders_received")
    if (g_tend is None or str(g_tend) in ("<NA>", "nan", "None")) and source.get("tenders_received"):
        out.append(
            {
                "pn": pn,
                "field": "n_tenders_received",
                "gold": None,
                "source": source["tenders_received"],
                "issue": "competition signal present at source but dropped in gold",
            }
        )

    # 2. Award-criteria weighting — gold keeps only an unweighted 'price+quality' tag.
    g_crit = g.get("award_criteria_kind")
    if source["award_criteria_raw"].get("numbers"):
        out.append(
            {
                "pn": pn,
                "field": "award_criteria_weights",
                "gold": g_crit,
                "source": {
                    "types": source["award_criteria_raw"]["types"],
                    "numbers": source["award_criteria_raw"]["numbers"],
                },
                "issue": "gold has the criteria KINDS but not the WEIGHTING (e.g. 70/30)",
            }
        )

    # 3. Framework ceiling — gold nulls the value (value_kind tag only) or holds a figure
    #    that disagrees with the source's framework maximum.
    src_max = source.get("framework_maximum_value_eur")
    g_val = g.get("award_value_eur")
    g_val = None if (g_val is None or str(g_val) in ("nan", "<NA>", "None")) else _num(g_val)
    if src_max is not None and (g_val is None or abs(g_val - src_max) > 1):
        out.append(
            {
                "pn": pn,
                "field": "framework_maximum_value_eur",
                "gold": g_val,
                "source": src_max,
                "issue": "framework ceiling missing or mismatched vs source (the €200m/€20m parse risk)",
            }
        )

    # 4. The deliverable / scope — gold has no human-legible 'what is built'.
    if source.get("deliverable_seed") or source.get("title"):
        out.append(
            {
                "pn": pn,
                "field": "scope_text",
                "gold": None,
                "source": {"title": source.get("title"), "deliverable": source.get("deliverable_seed")},
                "issue": "gold carries no title/deliverable — the record is unreadable without the source",
            }
        )

    # 5. Roster size — gold winner-row count vs the source's winner-identifier count.
    src_n = len(source.get("winner_identifiers") or [])
    if gold_rows and src_n and len(gold_rows) != src_n:
        out.append(
            {
                "pn": pn,
                "field": "winner_roster_size",
                "gold": len(gold_rows),
                "source": src_n,
                "issue": "winner count differs between gold and source",
            }
        )

    return _emit(out)


def _emit(discrepancies: list[dict]) -> list[dict]:
    """Append the batch to the reconciliation log (best-effort) and return it."""
    if discrepancies:
        try:
            with RECON_LOG.open("a", encoding="utf-8") as fh:
                for d in discrepancies:
                    fh.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
        except OSError:
            pass
    return discrepancies
