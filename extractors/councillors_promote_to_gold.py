"""Promote the vetted Your-Councillors sandbox data → git-tracked gold CSVs in data/_meta/.

The sandbox build lives in pipeline_sandbox/council_minutes/. This copies the FINAL, vetted
datasets into data/_meta/ (kept by the `!data/_meta/*.csv` .gitignore negation, exactly like
la_chief_executives.csv) where registered v_la_councillors* views read them. Nested lists
(agenda items, order-of-business) are flattened to a ` | ` delimiter the page re-splits.

CAVEATS are carried in the data so the UI can be honest:
  - roster ~96% (some councils undercounted on Wikipedia)
  - standing orders parsed for only ~8/31 councils (rest: source not located)
  - named votes only where the council records roll-calls (Carlow)
  - Louth minutes are book-format scans → no agendas
Run:  python extractors/councillors_promote_to_gold.py
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SBX = ROOT / "pipeline_sandbox" / "council_minutes"
META = ROOT / "data" / "_meta"
SEP = " | "


def _jsonl(p: Path) -> list[dict]:
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()] if p.exists() else []


_MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


def _mdate(fn: str) -> str:
    """Best-effort meeting date from a minutes filename. Councils name files every way
    imaginable — ISO (2025_01_20), D-M-YYYY, compact DDMMYYYY (04122024), 'April 2026',
    'apr2026'. A filename that yields NO date returns '' (the page renders blank honestly)
    rather than a filename fragment, which is what put 'signed-minutes-council-m' into the
    gold CSV as a date."""
    from urllib.parse import unquote

    fn = unquote(str(fn)).rsplit("/", 1)[-1]
    m = re.search(r"(20\d{2})[.\-\s_](\d{1,2})[.\-\s_](\d{1,2})", fn)  # ISO-ish YYYY_MM_DD
    if m and 1 <= int(m.group(3)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(3)):02d}/{int(m.group(2)):02d}/{m.group(1)}"
    m = re.search(r"(\d{1,2})[.\-\s_](\d{1,2})[.\-\s_](20\d{2})", fn)  # D_M_YYYY
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/{m.group(3)}"
    m = re.search(r"(?<!\d)(\d{2})(\d{2})(20\d{2})(?!\d)", fn)  # compact DDMMYYYY
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{m.group(1)}/{m.group(2)}/{m.group(3)}"
    m = re.search(r"(?<!\d)(\d{1,2})[.\-\s_](\d{1,2})[.\-\s_](2\d)(?!\d)", fn)  # DD_MM_YY (Cork City)
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(1)):02d}/{int(m.group(2)):02d}/20{m.group(3)}"
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+(" + "|".join(_MONTHS) + r")\s+(20\d{2})",
        fn,
        re.I,
    )
    if m:
        return f"{m.group(1)} {m.group(2).title()} {m.group(3)}"
    m = re.search(
        r"(" + "|".join(mo[:3] for mo in _MONTHS) + r")[a-z]*[.\-\s_]?(20\d{2})", fn, re.I
    )  # April 2026 / apr2026
    if m:
        full = next(mo for mo in _MONTHS if mo.lower().startswith(m.group(1).lower()))
        return f"{full} {m.group(2)}"
    return ""


def _iso_to_display(iso: str) -> str:
    """'2026-02-09' → '09/02/2026'; anything non-ISO passes through _mdate or blanks."""
    m = re.fullmatch(r"(20\d{2})-(\d{2})-(\d{2})", str(iso or "").strip())
    return f"{m.group(3)}/{m.group(2)}/{m.group(1)}" if m else _mdate(iso)


def _agenda_date(raw: str, source_url: str) -> str:
    """A meeting_history date is kept only if it actually contains a date; otherwise fall
    back to parsing the source filename, else blank. Kills the 8 filename-fragment rows
    (Louth 'signed-minutes-council-m', Waterford '1_draft_plenary_minute')."""
    raw = str(raw or "").strip()
    if raw and re.search(r"20\d{2}", raw) and len(raw) <= 24:
        return raw
    return _mdate(raw) or _mdate(source_url)


# The local_authority value is the cross-source JOIN KEY and must match the CE roster / payments / AFS
# spelling EXACTLY (plain ASCII). The sandbox carries the Irish-accented DLR name; canonicalise it on the
# key column here so a re-promote never reintroduces the mismatch that orphans DLR from every other
# dataset. Display columns (source / agenda) keep their accents untouched.
# "Galway" (bare) appears on older meeting_history rows harvested before the city/county split was
# consistent; every other dataset spells the county authority "Galway County".
_CANON_LA = {"Dún Laoghaire-Rathdown": "Dun Laoghaire-Rathdown", "Galway": "Galway County"}


def _source_status_map() -> dict[str, str]:
    """{minutes filename -> text|ocr_winocr|html} from the harvest ledger.

    The ledger records HOW each document's text was obtained, and that is what sets the
    trust band of everything derived from it: 'ocr_winocr' rows are Extracted (OCR can
    mis-read a name or a tally), 'text'/'html' rows are born-digital. The promote used to
    discard this, which left the UI unable to badge an OCR-derived vote differently from a
    born-digital one. Keyed on BOTH the corpus text filename (corpus-parsed rows carry it
    as `meeting`) and the ledger's own `meeting` field (the network-fetched Carlow rows).
    """
    out: dict[str, str] = {}
    for m in _jsonl(SBX / "meetings_clean.jsonl"):
        status = str(m.get("status") or "")
        if not status:
            continue
        for key in (Path(str(m.get("text_path") or "")).name, str(m.get("meeting") or "")):
            if key:
                out[key] = status
    return out


def _published_document_types() -> dict[tuple[str, str], str]:
    """Return the defensive publication type for every available manifest document.

    The harvest ledger's historical labels include agenda PDFs as minutes and
    committee minutes as plenary. Reusing the corpus publication contract here
    prevents those rows from contaminating app-level decision and power counts.
    """
    from extractors.council_minutes_contract import classify_document

    out: dict[tuple[str, str], str] = {}
    for record in _jsonl(SBX / "meetings_clean.jsonl"):
        text_path = str(record.get("text_path") or "")
        path = SBX / text_path if text_path else None
        if path is None or not path.exists():
            continue
        local_authority = _CANON_LA.get(str(record.get("local_authority") or ""), record.get("local_authority") or "")
        out[(str(local_authority), str(record.get("meeting") or ""))] = classify_document(
            meeting=str(record.get("meeting") or ""),
            source_url=str(record.get("url") or ""),
            text=path.read_text(encoding="utf-8", errors="replace"),
            upstream_doc_type=str(record.get("doc_type") or ""),
        )
    return out


def _roster_folds() -> dict[str, set[str]]:
    """{council -> folded gold-roster names}, using the vote extractor's own fold.

    Imported rather than re-implemented on purpose: if this file had its own matcher it
    could disagree with the one that produced the rows, and a row would be labelled
    'resolved' here while the extractor had kept it as a printed form.
    """
    from extractors.council_votes_extract import _fold, _load_gold_roster

    las = {r["local_authority"] for r in _jsonl(SBX / "member_votes.jsonl")}
    return {la: {_fold(n) for n in _load_gold_roster(la)} for la in las}


def _write(name: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(META / name, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            if "local_authority" in r:
                r = {**r, "local_authority": _CANON_LA.get(str(r["local_authority"]), r["local_authority"])}
            w.writerow(r)
    print(f"  data/_meta/{name}: {len(rows)} rows")


def main() -> int:
    META.mkdir(parents=True, exist_ok=True)
    print("Promoting Your-Councillors sandbox -> data/_meta gold CSVs")
    published_types = _published_document_types()

    # 1. roster
    with open(SBX / "councillors_roster.csv", encoding="utf-8") as fh:
        roster = list(csv.DictReader(fh))
    _write(
        "la_councillors.csv",
        ["local_authority", "lea", "name", "party", "status", "source"],
        [{k: r.get(k, "") for k in ["local_authority", "lea", "name", "party", "status", "source"]} for r in roster],
    )

    # 2. coverage tiers — RECOUNTED at promote time, not copied.
    #
    # council_coverage.csv was written when the corpus held ~150 documents and its counts
    # are now stale-pessimistic in both directions: it says Galway City has 0 clean minutes
    # and no votes (it has 104 and 508), and Fingal 0 (it has 35). A page that renders
    # "not yet harvested" over data we hold is a false statement about our own coverage,
    # so the three FACTUAL columns are recomputed from the artifacts themselves.
    #
    # `tier` stays a judgement about how a council RECORDS decisions, so it is carried from
    # the CSV — with two fact-driven corrections that cannot be judgement calls:
    #   • a council with extracted named votes IS a roll_call council;
    #   • a council with no clean minutes is 'unseeded' (we hold nothing, so we know nothing
    #     about how it votes). 'unseeded' means not-yet-harvested, never "does not publish".
    with open(SBX / "council_coverage.csv", encoding="utf-8") as fh:
        cov = list(csv.DictReader(fh))
    clean = Counter(la for (la, _meeting), doc_type in published_types.items() if doc_type.endswith("_minutes"))
    vote_rows = Counter(v["local_authority"] for v in _jsonl(SBX / "member_votes.jsonl"))
    # A placeholder row is one where the Wikipedia parse lost the councillor's name and left
    # the LEA name in its place — those rows have an EMPTY lea. Testing the name prefix alone
    # would also drop a real councillor whose forename is her county ("Clare Colleran Molloy").
    roster_n = Counter(
        r["local_authority"]
        for r in roster
        if " " in r["name"].strip()
        and not (not r.get("lea", "").strip() and r["name"].strip().startswith(r["local_authority"]))
    )
    cov_rows = []
    for r in cov:
        la = r["local_authority"]
        n_clean = clean.get(la, 0)
        tier = r.get("tier", "")
        if vote_rows.get(la):
            tier = "roll_call"
        elif n_clean == 0:
            tier = "unseeded"
        cov_rows.append(
            {
                "local_authority": la,
                "tier": tier,
                "clean_minutes": n_clean,
                "roster_councillors": roster_n.get(la, 0),
                "has_votes": bool(vote_rows.get(la)),
            }
        )
    _write(
        "la_council_meeting_coverage.csv",
        ["local_authority", "tier", "clean_minutes", "roster_councillors", "has_votes"],
        cov_rows,
    )

    # 3. named votes (per councillor)
    #
    # Two provenance columns ship with every row so the page never has to guess:
    #   source_status  text|ocr_winocr|html — how the minutes' text was obtained.
    #   join_status    resolved|printed_form — whether `member` matches a gold roster name.
    # `printed_form` rows are NOT errors and are never dropped: the reconcile gate already
    # proved the division's names count to the printed tally, so removing them would break
    # that arithmetic. They are names the roster cannot resolve — mostly councillors of an
    # earlier term (every Galway City division predates the 2024 council) and Cork City
    # seats absent from the roster. The page joins councillor cards on `resolved` only and
    # reports the printed-form count on the Trust rail; a division's totals stay complete.
    votes = _jsonl(SBX / "member_votes.jsonl")
    status_by_file = _source_status_map()
    folds = _roster_folds()
    from extractors.council_votes_extract import _fold

    _write(
        "la_councillor_votes.csv",
        ["local_authority", "member", "meeting_date", "motion", "vote", "source_status", "join_status"],
        [
            {
                "local_authority": v["local_authority"],
                "member": v["member"],
                # filename-derived date first; else the extractor's own meeting_date (ISO,
                # read from the minutes' first page — e.g. ModernGov MId-named files)
                "meeting_date": _mdate(v.get("meeting", "")) or _iso_to_display(v.get("meeting_date", "")),
                "motion": (v.get("motion") or "")[:300],
                "vote": v["vote"],
                "source_status": v.get("source_status") or status_by_file.get(str(v.get("meeting") or ""), ""),
                "join_status": (
                    "resolved" if _fold(v["member"]) in folds.get(v["local_authority"], set()) else "printed_form"
                ),
            }
            for v in votes
        ],
    )

    # 4. meeting agendas (flatten agenda_items)
    mh = _jsonl(SBX / "meeting_history.jsonl")
    _write(
        "la_meeting_agendas.csv",
        ["local_authority", "meeting_date", "agenda", "source_url"],
        [
            {
                "local_authority": r["council"],
                "meeting_date": _agenda_date(r.get("date", ""), r.get("source_url", "")),
                "agenda": SEP.join(r.get("agenda_items", [])),
                "source_url": r.get("source_url", ""),
            }
            for r in mh
            if r.get("agenda_items")
        ],
    )

    # 5. standing orders (flatten order_of_business)
    so = _jsonl(SBX / "standing_orders.jsonl")
    _write(
        "la_standing_orders.csv",
        [
            "local_authority",
            "order_of_business",
            "notice_of_motion",
            "voting",
            "quorum",
            "records_named_votes",
            "source_url",
        ],
        [
            {
                "local_authority": r["local_authority"],
                "order_of_business": SEP.join(r.get("order_of_business", [])),
                "notice_of_motion": r.get("notice_of_motion", ""),
                "voting": r.get("voting", ""),
                "quorum": r.get("quorum", ""),
                "records_named_votes": r.get("records_named_votes", False),
                "source_url": r.get("source_url", ""),
            }
            for r in so
            if r.get("status") == "ok"
        ],
    )

    # 6. council decisions (proposer/seconder motion events parsed from minutes)
    #
    # BAND: Extracted — these are regex-anchored parses of minute prose, not a published
    # decisions register. What that means for the columns, measured 2026-08-01:
    #   • 5,874 of 6,497 rows have NO outcome word. The minutes record who proposed and
    #     seconded but not always what was resolved, so `outcome` is empty far more often
    #     than not. Empty means "not recorded in the minutes", never "no decision".
    #   • Only 5 rows carry a vote tally — the named-tally record is v_la_councillor_votes,
    #     not this. `rollcall` flags the 292 rows whose text mentions a roll-call.
    #   • meeting_date is present on 3,104 rows and is often month-grained ("2026 May")
    #     because that is all the source document names.
    # The sandbox writes Python repr strings ('None', 'True') into these fields; they are
    # normalised here so gold never ships the literal text "None" in a count column.
    dec = [
        row
        for row in _jsonl(SBX / "decisions.jsonl")
        if published_types.get(
            (
                _CANON_LA.get(str(row.get("local_authority") or ""), row.get("local_authority") or ""),
                str(row.get("meeting") or ""),
            )
        )
        in {"plenary_minutes", "md_minutes"}
    ]

    # Topic labels are the SAME events, not a second dataset: motion_topics.jsonl and
    # decisions.jsonl describe the identical 6,435 motion events (verified 2026-08-01 —
    # 100% key overlap on council+meeting+snippet, zero rows unique to either side).
    # Promoting them as two tables would put the same grain in two places, and the second
    # reader to find them would count motions twice. They join here instead.
    # Only ~23% of events carry a topic; an empty list means "the classifier matched no
    # topic", never "this motion was about nothing".
    topics_by_event = {}
    for r in _jsonl(SBX / "motion_topics.jsonl"):
        key = (r.get("local_authority"), r.get("meeting"), str(r.get("snippet") or "")[:80])
        topics_by_event[key] = r.get("topics") or []

    # Dedup on the full event identity. The sandbox emits 50 rows (Wexford 42, Galway City 8)
    # that repeat council + date + item_context + motion_snippet + proposer + seconder — the
    # same motion caught twice by overlapping parse windows. Two genuinely different motions
    # cannot collide here because they would differ in at least one of those six fields; two
    # that match on all six carry nothing to tell them apart, so keeping both only inflates
    # any count the page shows. The vote extractor already does this (_dedupe_motions);
    # decisions had no equivalent. Verified 2026-08-01: 6,497 -> 6,447 rows.
    seen: set[tuple] = set()
    deduped = []
    for r in dec:
        k = (
            r.get("local_authority"),
            r.get("meeting_date"),
            r.get("item_context"),
            r.get("motion_snippet"),
            r.get("proposer"),
            r.get("seconder"),
        )
        if k in seen:
            continue
        seen.add(k)
        deduped.append(r)
    if len(deduped) != len(dec):
        print(f"  (decisions: dropped {len(dec) - len(deduped)} duplicate motion events)")
    dec = deduped

    def _num(v: object) -> str:
        s = str(v or "").strip()
        return s if s.isdigit() else ""

    _write(
        "la_council_decisions.csv",
        [
            "local_authority",
            "meeting_date",
            "item_context",
            "motion_snippet",
            "proposer",
            "seconder",
            "outcome",
            "topics",
            "tally_for",
            "tally_against",
            "tally_abstain",
            "rollcall",
            "source_url",
            "source_status",
        ],
        [
            {
                "local_authority": r["local_authority"],
                "meeting_date": str(r.get("meeting_date") or "").strip(),
                "item_context": (r.get("item_context") or "")[:200],
                "motion_snippet": (r.get("motion_snippet") or "")[:300],
                "proposer": r.get("proposer") or "",
                "seconder": r.get("seconder") or "",
                "outcome": r.get("outcome") or "",
                "topics": SEP.join(
                    topics_by_event.get(
                        (r.get("local_authority"), r.get("meeting"), str(r.get("motion_snippet") or "")[:80]), []
                    )
                ),
                "tally_for": _num(r.get("tally_for")),
                "tally_against": _num(r.get("tally_against")),
                "tally_abstain": _num(r.get("tally_abstain")),
                "rollcall": str(r.get("rollcall")).strip().lower() == "true",
                "source_url": r.get("source_url") or "",
                "source_status": status_by_file.get(str(r.get("meeting") or ""), ""),
            }
            for r in dec
        ],
    )

    # 7. power events — where the ELECTED MEMBERS decide, versus where they only noted a
    # decision the executive had already taken. This is the distinction the corpus is best
    # placed to show and that nothing else in the app shows.
    #
    # power_type is a mapping of the extractor's OWN class vocabulary, not a new judgement:
    #   reserved_*    -> reserved   (the members' statutory decisions)
    #   requisition_* -> reserved   (a s.140 requisition is a members' power — they REQUIRE
    #                                the Chief Executive to act, Local Government Act 2001 s.140)
    #   exec_*        -> executive  (the members noted; the Chief Executive decided)
    # Any class that ever appears outside these three prefixes maps to '' rather than being
    # guessed into a bucket, and the view surfaces it as unclassified.
    #
    # BAND: Extracted. Rows are document-grain (a class and a hit count per document), so a
    # citation reaches the document, NOT the line — do not render these as quotes.
    # Reserved/executive powers belong to the full elected council. Municipal
    # districts and committees can make other decisions, but mixing their records
    # into this split changes the legal grain of the claim.
    pw = [
        row
        for row in _jsonl(SBX / "power_events.jsonl")
        if published_types.get(
            (
                _CANON_LA.get(str(row.get("local_authority") or ""), row.get("local_authority") or ""),
                str(row.get("meeting") or ""),
            )
        )
        == "plenary_minutes"
    ]

    def _power_type(cls: str) -> str:
        if cls.startswith(("reserved_", "requisition_")):
            return "reserved"
        return "executive" if cls.startswith("exec_") else ""

    _write(
        "la_council_power_events.csv",
        ["local_authority", "meeting", "doc_type", "power_class", "power_type", "n_hits", "source_status"],
        [
            {
                "local_authority": r["local_authority"],
                "meeting": r.get("meeting") or "",
                "doc_type": published_types.get(
                    (
                        _CANON_LA.get(str(r.get("local_authority") or ""), r.get("local_authority") or ""),
                        str(r.get("meeting") or ""),
                    ),
                    "",
                ),
                "power_class": r.get("power_class") or "",
                "power_type": _power_type(str(r.get("power_class") or "")),
                "n_hits": r.get("n_hits") or 0,
                "source_status": status_by_file.get(str(r.get("meeting") or ""), ""),
            }
            for r in pw
        ],
    )

    print("Done. These 7 CSVs are the gold source for the v_la_councillors* views.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
