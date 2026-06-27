"""Ministerial diary entry classification (Build Plan Phase 5.1).

Deterministic, ordered keyword rules over the verbatim ``subject`` free text →
one ``entry_class`` per engagement. **Pipeline-owned, tested, never UI-side**
(logic firewall): the app filters on this column, it does not compute it.

Misclassification is cosmetic (it only drives filtering/grouping), never
factual — so the rule list itself is the contract and is published in
provenance. ``external_meeting`` is the residual-of-interest (the meetings that
matter for the org-match + lobbying-corroboration work); everything ahead of it
in the order peels off the procedural/known categories first.

Classes (Build Plan §7.1):
  govt_business   Cabinet / Government meetings / Cabinet Committees; inter-ministerial
                  (with the Taoiseach/Tánaiste, cabinet colleagues)
  oireachtas      Dáil/Seanad business: LQ, Topical Issues, stages, PQs, divisions, votes, PP
  party           party-political org business (Fine Gael/Green/FF/SF, CORE group, front
                  bench, selection conventions, árd fheis) — not govt, not outside-interest
  media           interviews, radio/TV (incl. named shows), photocalls, recordings, press
  internal_dept   officials' briefings, divisional/management-board, dept updates; the
                  minister's own office/team (advisers, private secretary, press office,
                  chief of staff, "diary meeting")
  travel          travel/flights to engagements
  constituency    constituency days/clinics
  external_meeting meetings/visits/launches/receptions with outside bodies (residual)
  other           nothing matched

Run (idempotent — re-derives entry_class in place on the sandbox parquet):
  .venv/Scripts/python.exe extractors/diary_entry_classify.py
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENTRIES = Path("data/sandbox/enrichment/ministerial_diary_entries.parquet")

# Ordered rules — FIRST match wins. Order is load-bearing: procedural/known
# categories are peeled off before the broad "external_meeting" keywords, and
# media is tested before internal_dept (a "press briefing" is media, not a
# departmental brief). Each pattern is matched case-insensitively against the
# raw subject. The human-readable keyword gloss in the trailing comment is what
# gets surfaced in provenance.
RULES: list[tuple[str, re.Pattern[str]]] = [
    (
        "govt_business",
        re.compile(
            r"\bcabinet\b|\bgovernment meeting\b|\bpre[-\s]?government\b|\bpre[-\s]?cabinet\b"
            r"|\bgovernment business\b|\bmemo to government\b|\bincorporeal\b|\bcabinet committee\b"
            r"|\bpre[-\s]?c\.?c\.?\b|\bcabinet cttee\b|\bpre[-\s]?cab\b|\bcab\.?\s*(?:prep|debrief)\b"
            # inter-ministerial coordination — meeting the Taoiseach/Tánaiste or cabinet
            # colleagues is government business, not an outside-interest engagement. (Uniquely
            # Irish titles, so no foreign-minister collision; an external org named alongside
            # still gets matched + flows to the overlap — govt_business is not excluded there.)
            r"|\bwith (?:the )?t[áa]naiste\b|\bwith (?:the )?taoiseach\b"
            r"|\b(?:ministerial|cabinet|government) colleagues?\b",
            re.IGNORECASE,
        ),
    ),  # Cabinet, Government Meeting, Cabinet Committee, pre-Cabinet/pre-CC, with Taoiseach/Tánaiste, colleagues
    (
        "oireachtas",
        re.compile(
            r"\bd[áa]il\b|\bseanad\b|\boireachtas\b|\bleaders?['’]?s?\W{0,3}questions\b|\btopical issues?\b"
            r"|\bcommittee stage\b|\breport stage\b|\b(?:second|third|fifth|committee|final) stage\b"
            r"|\bpromised legislation\b|\bparliamentary quest|\bpqs?\b|\bqpl\b|\bdivisions?\b"
            r"|\border of business\b|\bparliamentary party\b|\bpp\b|\bprivate members\b"
            r"|\badjournment\b|\bcommittee (?:meeting|hearing|appearance)\b|\b(?:joint|select) committee\b"
            r"|\bcommittee on\b|\bvotes?\b|\bvoting\b|\bvot(?:e)?able\b|\bcommencement matters?\b|\blqs?\b|\boob\b",
            re.IGNORECASE,
        ),
    ),  # Dáil/Seanad, Leader('s) Questions, Topical Issues, stages, PQs, divisions/votes/votable, committees, PP
    (
        # Party-political (organisational) business — NOT government and NOT an outside-interest
        # engagement. Placed AFTER oireachtas so parliamentary-party "PP" stays oireachtas; this
        # catches the party-org meetings (front bench, selection conventions, party executives).
        "party",
        re.compile(
            r"\bfine gael\b|\bgreen party\b|\bfianna f[áa]il\b|\bsinn f[ée]in\b|\bsocial democrats\b"
            r"|\bcore group\b|\bfront bench\b|\bindependent alliance\b|\bselection convention\b"
            r"|[áa]rd fheis\b|\bfg (?:trustees|councillors?|executive|members?)\b|\bparty meeting\b",
            re.IGNORECASE,
        ),
    ),  # Fine Gael/Green Party/FF/SF, CORE group, front bench, selection convention, árd fheis, FG trustees
    (
        "media",
        re.compile(
            r"\binterview\b|\bradio\b|\bnewstalk\b|\b\w*fm\b|\brt[ée]\b|\bpodcast\b"
            r"|\bpress (?:conference|briefing|event|release|call|cover)\b|\bphoto\s?call\b|\bphoto\s?shoot\b"
            r"|\bphoto op\b|\bdoorstep\b|\bpre[-\s]?rec\b|\brecording\b|\bbroadcast\b|\bwebinar\b"
            r"|\bnews at\b|\bmorning ireland\b|\bdrivetime\b|\bsix one\b|\bprime time\b"
            r"|\blate late\b|\bop[-\s]?ed\b|\bvideo\b"
            # named radio/TV programmes (distinctive only — NOT generic "this week"/"today with")
            r"|\bclaire byrne\b|\bpat kenny\b|\bthe last word\b|\btonight show\b|\bhard shoulder\b"
            r"|\btoday fm\b|\bireland am\b",
            re.IGNORECASE,
        ),
    ),  # interview, radio/TV (incl. local *fm + named shows), photocall/shoot, recording, press, video
    (
        "internal_dept",
        re.compile(
            r"\bbriefing\b|\bpre[-\s]?brief\b|\bbrief(?:ed|ing)? (?:by|with|on)\b|\bofficials\b"
            r"|\bdivisional\b|\b(?:management|departmental) board\b|\bdepartmental\b"
            r"|\bdept\.? (?:update|meeting)\b|\bweekly (?:update|meeting|division)"
            r"|\binternal meeting\b|\bsecretary general\b|\bsec gen\b|\bupdate with\b|\bcatch[-\s]?up\b"
            # internal counterparties — the minister's own office/team, not an outside body.
            # These dominated the "external_meeting" residual (advisers ×76, private sec ×41,
            # press office ×31) and inflated the org-match denominator; they name internal ROLES,
            # not activities, so they don't swallow genuine external prep.
            r"|\badvis[eo]rs?\b|\bspad\b|\bspecial advis[eo]r\b|\bprivate secretary\b|\bpriv\.? sec\b"
            r"|\bchief of staff\b|\bpress office\b|\bcomms team\b|\bassistant secretary\b|\bdiary meeting\b"
            r"|\ba/?sec\b|\bsecgen\b|\bsg meeting\b",  # role abbreviations (Assistant Sec / Sec Gen)
            re.IGNORECASE,
        ),
    ),  # briefing, officials, board, dept update, sec gen + advisers/private-sec/press-office/diary-meeting
    (
        "travel",
        re.compile(
            r"\btravel(?:ling)? to\b|\bflight\b|\bfly to\b|\btransfer to\b|\ben route\b"
            r"|\bdrive to\b|\btrain to\b|\bdeparting\b",
            re.IGNORECASE,
        ),
    ),  # travel to, flight, transfer to, en route, drive to
    (
        "constituency",
        re.compile(r"\bconstituency\b", re.IGNORECASE),
    ),  # constituency day (NOT bare "clinic" — in dept diaries that's an advisory event, not a TD clinic)
    (
        "external_meeting",
        re.compile(
            r"\bmeeting\b|\bmeet\b|\bvisit\b|\blaunch\b|\bopening\b|\breception\b|\broundtable\b"
            r"|\bround[-\s]?table\b|\bdinner\b|\blunch\b|\bbreakfast\b|\baddress\b|\bspeech\b"
            r"|\bconference\b|\bsummit\b|\bforum\b|\bevent\b|\bpresentation\b|\bsigning\b"
            r"|\bdelegation\b|\bdeputation\b|\bcourtesy call\b|\battend\b|\bengagement\b|\btour\b"
            r"|\bceremony\b|\bawards?\b|\bgala\b|\btrade mission\b|\bannouncement\b"
            r"|\bphone\s?call\b|\bcall with\b|\bvc with\b",
            re.IGNORECASE,
        ),
    ),  # meeting/visit/launch/reception/lunch/address/conference/attend/trade mission/announcement (residual of interest)
]


def classify(subject: str | None) -> str:
    """Return the entry_class for one diary subject (first matching rule wins)."""
    if not subject:
        return "other"
    for cls, pat in RULES:
        if pat.search(subject):
            return cls
    return "other"


def entry_class_expr(col: str = "subject") -> pl.Expr:
    """Vectorised polars equivalent of :func:`classify` for the pipeline write path.

    Folds the SAME ``RULES`` (order is load-bearing) into a first-match-wins
    ``when/then`` ladder, so we avoid a per-row Python callback over the whole
    frame. ``(?i)`` reproduces the ``re.IGNORECASE`` the scalar patterns compile
    with; a null/empty subject matches no branch and falls through to ``"other"`` —
    identical to ``classify``. ``test_diary_classify`` pins the two against each other.
    """
    subject = pl.col(col)
    expr: pl.Expr | None = None
    for cls, pat in RULES:
        cond = subject.str.contains("(?i)" + pat.pattern)
        expr = pl.when(cond).then(pl.lit(cls)) if expr is None else expr.when(cond).then(pl.lit(cls))
    assert expr is not None  # RULES is non-empty
    return expr.otherwise(pl.lit("other")).alias("entry_class")


def main() -> int:
    setup_standalone_logging("diary_entry_classify")
    if not ENTRIES.exists():
        log.error("entries parquet not found: %s — run ministerial_diaries_extract.py first", ENTRIES)
        return 1

    e = pl.read_parquet(ENTRIES)
    if "entry_class" in e.columns:  # idempotent re-derive
        e = e.drop("entry_class")
    e = e.with_columns(entry_class_expr())
    save_parquet(e, ENTRIES)
    e.write_csv(ENTRIES.with_suffix(".csv"))

    dist = e.group_by("entry_class").len().sort("len", descending=True)
    total = len(e)
    log.info("classified %d entries:", total)
    for row in dist.iter_rows(named=True):
        log.info("  %-16s %6d  (%4.1f%%)", row["entry_class"], row["len"], 100 * row["len"] / total)

    # eyeball precision: 3 sample subjects per class
    for cls in dist["entry_class"].to_list():
        samples = e.filter(pl.col("entry_class") == cls)["subject"].head(3).to_list()
        log.info("  e.g. [%s] %s", cls, " | ".join(repr(s[:48]) for s in samples))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
