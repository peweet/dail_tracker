"""Per-member councillor vote attribution from born-digital roll-call minutes.

Roll-call councils (e.g. Carlow, Laois) print a table:  Member | For | Against | Abstain | Absent
with a check mark (√ = U+221A, sometimes ✓/X) in the voted column. fitz `find_tables()` recovers
the cells incl. the mark, so we can attribute each NAMED councillor's vote — a true per-councillor
voting record, no OCR needed (these PDFs are born-digital). Two further born-digital formats are
parsed from the local corpus/ text dumps:

  • Cork City PROSE divisions:  `FOR: Comhairleoirí J. Maher, … (21)` / `AGAINST: … (9)` /
    `ABSTAIN: (0)`. Initial+surname names are resolved against the gold roster
    (data/_meta/la_councillors.csv); ambiguous initials are excluded from attribution.
  • Kilkenny PROSE roll-calls:  `Four (4) voted in favour: Cllrs. Maria Dollard, …`.
  • Laois ✓-GRID tables with split FIRST NAME / SURNAME header columns, reconciled against the
    printed result sentence ("The result of the Roll Call was 4 Members for, 9 against …").

RECONCILE GATE (AFS discipline): every prose/grid division must have its printed tally equal the
count of names parsed for that side; divisions failing the gate are dropped and counted, never
emitted. Default run is corpus-driven and PRESERVES the existing Carlow rows byte-identically
(pass --refresh-carlow to re-fetch Carlow from the network).

Output: member_votes.jsonl (one row per councillor-per-vote) + member_votes.csv.
Each row: local_authority, meeting (file), motion (nearest preceding context), member, vote
(+ meeting_date ISO for corpus-parsed rows).
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
HDRS = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/120 Safari/537.36")}

MARKS = {"√", "✓", "✔", "x", "X", "✗", "•", "Y", "y", "1"}
VOTE_COLS = ("for", "against", "abstain", "absent")


def is_mark(cell: str) -> bool:
    c = (cell or "").strip()
    return c in MARKS or (len(c) == 1 and not c.isalnum()) or "√" in c


def header_map(row: list[str]) -> dict[int, str] | None:
    """If this row is a vote-table header, return {col_index: 'for'|'against'|...}."""
    low = [(c or "").strip().lower() for c in row]
    m = {}
    for i, c in enumerate(low):
        for v in VOTE_COLS:
            if c == v or c.startswith(v):
                m[i] = v
    # need at least For + Against, and a name-ish first column
    if {"for", "against"} <= set(m.values()):
        return m
    return None


def parse_pdf(la: str, fname: str, pdf: bytes) -> list[dict]:
    import fitz  # noqa: PLC0415
    doc = fitz.open(stream=pdf, filetype="pdf")
    out = []
    last_motion = ""  # carry across page-breaks (motion often on the page before the table)
    for page in doc:
        try:
            tables = page.find_tables()
        except Exception:  # noqa: BLE001
            tables = None
        page_motion = _nearest_motion(page.get_text())
        if page_motion:
            last_motion = page_motion
        for tbl in (tables.tables if tables else []):
            rows = tbl.extract()
            if not rows:
                continue
            hmap = header_map(rows[0])
            if not hmap:
                continue
            y0 = tbl.bbox[1]
            above = "\n".join(b[4] for b in page.get_text("blocks") if b[1] < y0)
            motion = _nearest_motion(above) or last_motion
            name_col = min(set(range(len(rows[0]))) - set(hmap)) if set(hmap) else 0
            for r in rows[1:]:
                name = (r[name_col] or "").replace("\n", " ").strip()
                if not name or name.lower().startswith(("member", "total", "result")):
                    continue
                vote = next((hmap[i] for i in hmap if i < len(r) and is_mark(r[i])), None)
                if vote:
                    out.append({"local_authority": la, "meeting": fname,
                                "motion": motion[:240], "member": name, "vote": vote})
    return out


def normalise_members(rows: list[dict]) -> list[dict]:
    """Fold split/garbled names onto the council's real roster (high-frequency names)."""
    import difflib
    from collections import Counter
    by_la: dict[str, Counter] = {}
    for r in rows:
        by_la.setdefault(r["local_authority"], Counter())[r["member"]] += 1
    rosters = {la: [n for n, c in cnt.items() if c >= 3 and len(n) > 6 and " " in n]
               for la, cnt in by_la.items()}
    cleaned = []
    for r in rows:
        roster = rosters.get(r["local_authority"], [])
        if r["member"] in roster:
            cleaned.append(r)
            continue
        match = difflib.get_close_matches(r["member"], roster, n=1, cutoff=0.6)
        if match:
            r = {**r, "member": match[0]}
            cleaned.append(r)
        # else: drop unmatchable fragment
    return cleaned


_MOT = re.compile(r"(Resolution|Motion|Proposed by|That the|We the Members)[^\n]{0,220}", re.I)


def _nearest_motion(text_above: str) -> str:
    hits = _MOT.findall(text_above) if text_above else []
    # the closest preceding motion-ish line is the last match in reading order
    if hits:
        m = list(_MOT.finditer(text_above))[-1]
        return re.sub(r"\s+", " ", text_above[m.start():m.start() + 220]).strip()
    return ""


# Born-digital roll-call councils + a recent multi-meeting set to mine (extend as discovery grows).
SOURCES = {
    "Carlow": [
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20March%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20April%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-06/Minutes%20Carlow%20County%20Council%20May%202026.pdf",
        "https://carlow.ie/sites/default/files/2026-05/Minutes%20Carlow%20County%20Council%20February%202026_0.pdf",
    ],
}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Corpus-driven extension: Cork City (prose) · Kilkenny (prose) · Laois (✓-grid PDF).
# All divisions pass the RECONCILE GATE (printed tally == parsed name count) or are dropped.
# ══════════════════════════════════════════════════════════════════════════════════════════════

CORPUS = HERE / "corpus"
META = HERE.parents[1] / "data" / "_meta"

_MOJIBAKE_MARKERS = ("â€", "Ã", "Â", "\x92", "\x93", "\x94")


def _fix_mojibake(s: str) -> str:
    """Repair UTF-8-read-as-cp1252 artefacts (the O’Donoghue trap) without harming clean text."""
    if any(m in s for m in _MOJIBAKE_MARKERS):
        try:
            s = s.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return s.replace("\x92", "'").replace("\x91", "'").replace("\x93", '"').replace("\x94", '"')


def _fold(s: str) -> str:
    """Accent/apostrophe/case fold for name matching (NFKD, same family as shared/name_norm)."""
    import unicodedata
    s = unicodedata.normalize("NFKD", _fix_mojibake(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[’'`´.]", "", s)
    return re.sub(r"[\s\-]+", " ", s).strip().lower()


def _titlecase_name(s: str) -> str:
    """ALL-CAPS surname → display case, keeping Mc/Mac/O' prefixes ("MCDONALD" → "McDonald")."""
    def _word(w: str) -> str:
        wt = w.capitalize()
        if wt.startswith("Mc") and len(wt) > 2:
            wt = "Mc" + wt[2:].capitalize()
        m = re.match(r"^(O|D)['’](\w+)$", wt, re.I)
        if m:
            wt = f"{m.group(1).upper()}'{m.group(2).capitalize()}"
        return wt
    return " ".join(_word(w) for w in s.split())


def _corpus_meeting_date(fname: str) -> str:
    """ISO meeting date from a corpus filename (dd_mm_yy(yy) / yyyy_mm_dd / ddmmyyyy runs)."""
    for rx, order in (
        (r"(?<!\d)(20\d{2})_(\d{2})_(\d{2})(?!\d)", ("y", "m", "d")),
        (r"(?<!\d)(\d{1,2})_(\d{1,2})_(20\d{2})(?!\d)", ("d", "m", "y")),
        (r"(?<!\d)(\d{2})_(\d{2})_(\d{2})(?!\d)", ("d", "m", "yy")),
        (r"(?<!\d)(\d{2})(\d{2})(20\d{2})(?!\d)", ("d", "m", "y")),
    ):
        m = re.search(rx, fname)
        if m:
            parts = dict(zip(order, m.groups()))
            y = parts.get("y") or f"20{parts['yy']}"
            try:
                d, mo = int(parts["d"]), int(parts["m"])
                if 1 <= d <= 31 and 1 <= mo <= 12:
                    return f"{y}-{mo:02d}-{d:02d}"
            except ValueError:
                pass
    return ""


def _load_gold_roster(la: str) -> list[str]:
    """Real member names for one council from the gold roster (skips LEA-placeholder rows)."""
    p = META / "la_councillors.csv"
    if not p.exists():
        return []
    with open(p, encoding="utf-8") as fh:
        return [
            r["name"].strip()
            for r in csv.DictReader(fh)
            if r["local_authority"] == la and " " in r["name"].strip() and not r["name"].strip().startswith(la)
        ]


class RosterResolver:
    """Resolve printed name forms onto the gold roster.

    full(name)     — accent-folded full-name equality (Kilkenny/Laois print full names).
    initials(name) — 'J. Maher' → unique (first-initial, surname) roster candidate (Cork City).
                     Returns (resolved_or_None, status) where status ∈ ok|unmatched|ambiguous|title.
    """

    def __init__(self, la: str) -> None:
        self.names = _load_gold_roster(la)
        self.by_fold = {_fold(n): n for n in self.names}
        self.keyed = []  # (first_initial, folded_surname, full_name)
        for n in self.names:
            toks = _fold(n).split()
            if len(toks) >= 2:
                self.keyed.append((toks[0][0], toks[-1], n))

    def full(self, printed: str) -> str:
        return self.by_fold.get(_fold(printed), "")

    def initials(self, printed: str) -> tuple[str | None, str]:
        f = _fold(printed)
        if f.startswith(("an tardmheara", "an tard mheara", "lord mayor", "an cathaoirleach")):
            return None, "title"
        toks = f.split()
        if len(toks) < 2 or len(toks[0]) > 3:  # not an initial form (e.g. full first name) → try full
            hit = self.full(printed)
            return (hit, "ok") if hit else (None, "no_parse")
        initial, surname = toks[0][0], toks[-1]
        cands = [k for k in self.keyed if k[0] == initial and k[1] == surname]
        if not cands:  # spelling drift (Dinneen/Dineen) — fuzzy surname, same initial
            import difflib
            cands = [
                k for k in self.keyed
                if k[0] == initial and difflib.SequenceMatcher(None, k[1], surname).ratio() >= 0.86
            ]
        if len(cands) == 1:
            return cands[0][2], "ok"
        return (None, "ambiguous") if len(cands) > 1 else (None, "unmatched")


class Coverage:
    """Per-council extraction bookkeeping for the run report."""

    def __init__(self) -> None:
        self.divisions_found = 0
        self.divisions_kept = 0
        self.reconcile_drops = 0
        self.rows = 0
        self.ambiguous_excluded = 0
        self.title_excluded = 0
        self.unmatched_kept = 0

    def line(self) -> str:
        return (
            f"divisions {self.divisions_kept}/{self.divisions_found} kept "
            f"({self.reconcile_drops} reconcile-gate drops) · {self.rows} member-vote rows · "
            f"{self.ambiguous_excluded} ambiguous excluded · {self.title_excluded} title excluded · "
            f"{self.unmatched_kept} not-in-roster kept as printed"
        )


# ── Cork City prose ───────────────────────────────────────────────────────────────────────────
def _dedupe_motions(rows: list[dict]) -> list[dict]:
    """Distinct divisions in one meeting can share a motion string (e.g. four budget
    amendments each introduced only as 'A vote was taken where there appeared as
    follows:'). Suffix ' (vote N)' so the divisions stay distinguishable in gold — a
    reader/joiner must never collapse two real votes into one. Uses the transient
    '_div' ordinal each parser attaches; strips it on the way out."""
    by_motion: dict[tuple[str, str], set[int]] = {}
    for r in rows:
        by_motion.setdefault((r["meeting"], r["motion"]), set()).add(r["_div"])
    out = []
    for r in rows:
        divs = sorted(by_motion[(r["meeting"], r["motion"])])
        motion = r["motion"]
        if len(divs) > 1:
            motion = f"{motion} (vote {divs.index(r['_div']) + 1} of {len(divs)})".strip()
        out.append({k: v for k, v in {**r, "motion": motion}.items() if k != "_div"})
    return out


_CORK_MARKER = re.compile(r"\b(FOR|AGAINST|ABSTAIN)\s*:")
_CORK_MOTION = re.compile(
    r"(A vote was (?:taken|called)[^.]{0,220}|The following amendment[^.]{0,220}"
    r"|nominations? (?:to|of|for)[^.]{0,200}|It was proposed[^.]{0,220}|Proposed by[^.]{0,220}"
    r"|[Ee]lection of[^.]{0,200}|Resolution[^.]{0,220})",
    re.I,
)
_CORK_QUOTE = re.compile(r"[“‘][^”’“‘]{15,280}[”’]")
_CORK_CAND = re.compile(r"(Comhairleoir\w*\s+[A-Z][\w.’' -]{1,40}?)\s*$")
_VOTE_OF = {"FOR": "for", "AGAINST": "against", "ABSTAIN": "abstain"}


def _cork_side_names(seg: str) -> tuple[list[str] | None, int | None]:
    """Names + printed tally for one FOR/AGAINST/ABSTAIN segment, or (None, None) if no tally."""
    t = re.sub(r"\s+", " ", seg)
    m = re.search(r"\((\d+)\)", t)
    if not m:
        return None, None
    names_txt = t[: m.start()].strip()
    names_txt = re.sub(r"^Comhairleoir[íi]?\b[.:]?", "", names_txt).strip()
    names_txt = names_txt.strip(" .;:-")
    if not names_txt:
        return [], int(m.group(1))
    parts = [p.strip(" .;") for p in names_txt.split(",")]
    return [p for p in parts if re.search(r"[A-Za-zÀ-ÿ]{2,}", p)], int(m.group(1))


def parse_cork_prose(la: str, fname: str, text: str, cov: Coverage, resolver: RosterResolver) -> list[dict]:
    text = _fix_mojibake(text)
    mdate = _corpus_meeting_date(fname)
    marks = list(_CORK_MARKER.finditer(text))
    out: list[dict] = []
    # group consecutive markers into divisions starting at each FOR
    i = 0
    while i < len(marks):
        if marks[i].group(1) != "FOR":
            i += 1
            continue
        group = [marks[i]]
        j = i + 1
        while j < len(marks) and marks[j].group(1) != "FOR":
            group.append(marks[j])
            j += 1
        cov.divisions_found += 1
        sides: dict[str, tuple[list[str], int]] = {}
        ok = True
        for gi, gm in enumerate(group):
            seg_end = group[gi + 1].start() if gi + 1 < len(group) else min(gm.end() + 1200, len(text))
            names, tally = _cork_side_names(text[gm.end():seg_end])
            if names is None or tally is None or len(names) != tally:  # RECONCILE GATE
                ok = False
                break
            sides[_VOTE_OF[gm.group(1)]] = (names, tally)
        if not ok or "for" not in sides or "against" not in sides:
            cov.reconcile_drops += 1
            i = j
            continue
        cov.divisions_kept += 1
        # motion context: nearest preceding quote / motion sentence (+ nominee line for AGM votes)
        win = text[max(0, group[0].start() - 900): group[0].start()]
        flat = re.sub(r"\s+", " ", win)
        quotes = _CORK_QUOTE.findall(flat)
        sentences = _CORK_MOTION.findall(flat)
        cand = _CORK_CAND.search(win.rstrip())
        motion = (quotes[-1] if quotes else (sentences[-1] if sentences else "")).strip()
        if cand:
            motion = (motion + " — " if motion else "") + re.sub(r"\s+", " ", cand.group(1))
        for vote, (names, _tally) in sides.items():
            for printed in names:
                resolved, status = resolver.initials(printed)
                if status == "ambiguous":
                    cov.ambiguous_excluded += 1
                    continue
                if status == "title":
                    cov.title_excluded += 1
                    continue
                if resolved is None:
                    cov.unmatched_kept += 1
                    resolved = _fix_mojibake(printed)
                cov.rows += 1
                out.append({"local_authority": la, "meeting": fname, "meeting_date": mdate,
                            "motion": motion[:240], "member": resolved, "vote": vote})
        i = j
    return out


# ── Kilkenny prose ────────────────────────────────────────────────────────────────────────────
_KK_MARKER = re.compile(r"\((\d+)\)\s*(voted\s+in\s+favour|voted\s+against|absent)\s*:", re.I)
_KK_VOTE = {"favour": "for", "against": "against", "absent": "absent"}


def _kk_names(seg: str) -> list[str]:
    t = re.sub(r"\s+", " ", seg).strip()
    t = re.sub(r"^Cllrs?\.?\s*", "", t)
    t = re.sub(r"\s*(?:&|\band\b)\s*", ", ", t)
    return [p.strip(" .;") for p in t.split(",") if re.search(r"[A-Za-zÀ-ÿ]{2,}", p)]


def parse_kilkenny_prose(la: str, fname: str, text: str, cov: Coverage, resolver: RosterResolver) -> list[dict]:
    text = _fix_mojibake(text)
    mdate = _corpus_meeting_date(fname)
    marks = [m for m in _KK_MARKER.finditer(text) if "roll call" in text[max(0, m.start() - 700): m.start()].lower()]
    out: list[dict] = []
    i = 0
    while i < len(marks):
        if "favour" not in marks[i].group(2).lower():
            i += 1
            continue
        group = [marks[i]]
        j = i + 1
        while j < len(marks) and "favour" not in marks[j].group(2).lower():
            group.append(marks[j])
            j += 1
        cov.divisions_found += 1
        sides: list[tuple[str, list[str]]] = []
        ok = True
        for gi, gm in enumerate(group):
            seg_end = group[gi + 1].start() if gi + 1 < len(group) else min(gm.end() + 900, len(text))
            seg = text[gm.end():seg_end]
            seg = re.split(r"\bTherefore\b|\bA roll call vote\b|“", seg)[0]
            names = _kk_names(seg)
            # names segments end where the enumeration word of the NEXT side begins
            # (e.g. "…& Ger Frisby.  Seven (7) absent:") — the split above already bounds it.
            if len(names) != int(gm.group(1)):  # RECONCILE GATE
                ok = False
                break
            key = next(k for k in _KK_VOTE if k in gm.group(2).lower())
            sides.append((_KK_VOTE[key], names))
        if not ok or len(sides) < 2:
            cov.reconcile_drops += 1
            i = j
            continue
        cov.divisions_kept += 1
        win = re.sub(r"\s+", " ", text[max(0, group[0].start() - 900): group[0].start()])
        quotes = re.findall(r"“[^”]{15,300}”", win)
        sent = re.findall(r"A roll call vote was taken[^:]{0,220}", win)
        motion = (quotes[-1] if quotes else (sent[-1] if sent else "")).strip()
        for vote, names in sides:
            for printed in names:
                member = resolver.full(printed) or _titlecase_name(_fix_mojibake(printed))
                if not resolver.full(printed):
                    cov.unmatched_kept += 1
                cov.rows += 1
                out.append({"local_authority": la, "meeting": fname, "meeting_date": mdate,
                            "motion": motion[:240], "member": member, "vote": vote})
        i = j
    return out


# ── Laois ✓-grid (split FIRST NAME / SURNAME header) ─────────────────────────────────────────
_LAOIS_RESULT = re.compile(
    r"result of the Roll Call was\s+(\d+)\s+Members?\s+for[^.]*?(\d+)\s+Members?\s+against"
    r"[^.]*?(\d+)\s+Members?\s+absent",
    re.I | re.S,
)


def _grid_mark(cell: str, colname: str) -> bool:
    c = (cell or "").strip()
    return is_mark(c) or (c.upper().rstrip(" .") == colname.upper())


def _grid_rows(rows: list[list], hmap: dict[int, str], split_name: bool) -> list[tuple[str, str]]:
    """(member, vote) pairs from grid data rows; handles split first/surname columns."""
    out = []
    for r in rows:
        if split_name:
            first = re.sub(r"\s+", " ", (r[0] or "")).strip()
            sur = re.sub(r"\s+", " ", (r[1] or "")).strip()
            if not sur or _fold(first).startswith("first") or "surname" in _fold(sur):
                continue
            name = f"{first} {_titlecase_name(sur)}".strip()
        else:
            name = re.sub(r"\s+", " ", (r[0] or "")).strip()
            if not name or name.lower().startswith(("member", "councillor", "total", "result", "first")):
                continue
        vote = next((hmap[i] for i in hmap if i < len(r) and _grid_mark(r[i], hmap[i])), None)
        if vote:
            out.append((name, vote))
    return out


def parse_laois_grid(la: str, path: Path, cov: Coverage, resolver: RosterResolver) -> list[dict]:
    import fitz  # noqa: PLC0415
    doc = fitz.open(str(path))
    fname = path.name
    divisions: list[dict] = []   # {motion, pairs}
    open_div: dict | None = None
    last_motion = ""
    for page in doc:
        page_text = page.get_text()
        try:
            tables = page.find_tables()
        except Exception:  # noqa: BLE001
            tables = None
        for tbl in sorted((tables.tables if tables else []), key=lambda t: t.bbox[1]):
            rows = tbl.extract()
            if not rows:
                continue
            hmap = header_map(rows[0])
            if hmap:
                split_name = len(rows) > 1 and any("SURNAME" in ((c or "").upper()) for c in rows[1])
                above = "\n".join(b[4] for b in page.get_text("blocks") if b[1] < tbl.bbox[1])
                motion = _laois_motion(above) or last_motion
                open_div = {"motion": motion, "pairs": _grid_rows(rows[1:], hmap, split_name),
                            "hmap": hmap, "split": split_name}
                divisions.append(open_div)
            elif open_div is not None and rows and len(rows[0]) >= 3:
                # headerless continuation across a page break — same column signature
                open_div["pairs"] += _grid_rows(rows, open_div["hmap"], open_div["split"])
        pm = _laois_motion(page_text)
        if pm:
            last_motion = pm
        if _LAOIS_RESULT.search(page_text):
            open_div = None  # the printed result closes the running division
    # RECONCILE GATE: printed result sentences, in document order, must match counted marks
    full_text = "".join(p.get_text() for p in doc)
    results = _LAOIS_RESULT.findall(full_text)
    out: list[dict] = []
    mdate = _corpus_meeting_date(fname) or _doc_meeting_date(full_text)
    for idx, div in enumerate(divisions):
        cov.divisions_found += 1
        counts = {v: sum(1 for _n, vv in div["pairs"] if vv == v) for v in ("for", "against", "absent")}
        printed = results[idx] if idx < len(results) else None
        if not printed or (int(printed[0]), int(printed[1]), int(printed[2])) != (
            counts["for"], counts["against"], counts["absent"]
        ):
            cov.reconcile_drops += 1
            continue
        cov.divisions_kept += 1
        for name, vote in div["pairs"]:
            member = resolver.full(name) or name
            if not resolver.full(name):
                cov.unmatched_kept += 1
            cov.rows += 1
            out.append({"local_authority": la, "meeting": fname, "meeting_date": mdate,
                        "motion": div["motion"][:240], "member": member, "vote": vote})
    return out


_LAOIS_MOT = re.compile(
    r"(Notice of Motion No[^\n]{0,40}|“That[^”]{10,300}”|Resolution[^\n]{0,220}|Proposed by[^\n]{0,220})"
)


def _laois_motion(text_above: str) -> str:
    if not text_above:
        return ""
    flat = re.sub(r"\s+", " ", text_above)
    quotes = re.findall(r"“That[^”]{10,300}”", flat)
    if quotes:
        return quotes[-1]
    hits = _LAOIS_MOT.findall(flat)
    return hits[-1].strip() if hits else _nearest_motion(text_above)


def _doc_meeting_date(text: str) -> str:
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December),?\s+(20\d{2})",
        text[:2500],
        re.I,
    )
    if not m:
        return ""
    months = ["january", "february", "march", "april", "may", "june", "july", "august",
              "september", "october", "november", "december"]
    return f"{m.group(3)}-{months.index(m.group(2).lower()) + 1:02d}-{int(m.group(1)):02d}"


# ── corpus run + merge ───────────────────────────────────────────────────────────────────────
def run_corpus() -> list[dict]:
    """Parse the local corpus for the three new councils; return rows (Carlow untouched)."""
    jobs = [
        ("Cork City", "cork_city", "cork"),
        ("Kilkenny", "kilkenny", "kilkenny"),
        ("Laois", "laois", "laois_grid"),
    ]
    all_rows: list[dict] = []
    for la, sub, kind in jobs:
        cov = Coverage()
        resolver = RosterResolver(la)
        folder = CORPUS / sub
        if kind == "laois_grid":
            files = sorted(folder.glob("minutes_council_*.pdf"))
            for p in files:
                all_rows += parse_laois_grid(la, p, cov, resolver)
        else:
            files = sorted(p for p in folder.glob("*.txt") if "lcdc" not in p.name.lower())
            for p in files:
                text = p.read_text(encoding="utf-8", errors="replace")
                if kind == "cork":
                    all_rows += parse_cork_prose(la, p.name, text, cov, resolver)
                else:
                    all_rows += parse_kilkenny_prose(la, p.name, text, cov, resolver)
        print(f"{la:10} {cov.line()}")
    return all_rows


def refresh_carlow(extra: list[str]) -> list[dict]:
    """Original network path (Carlow roll-call PDFs) — only on --refresh-carlow."""
    rows = []
    work = list(SOURCES.items())
    if extra:
        work.append(("adhoc", extra))
    for la, urls in work:
        for u in urls:
            try:
                pdf = requests.get(u, headers=HDRS, timeout=70).content
                recs = parse_pdf(la, u.split("/")[-1], pdf)
                rows += recs
                print(f"{la:10} {u.split('/')[-1][:48]:48} -> {len(recs)} member-votes")
            except Exception as e:  # noqa: BLE001
                print(f"{la:10} ERR {type(e).__name__} {u.split('/')[-1][:48]}")
    return normalise_members(rows)


def main() -> int:
    args = sys.argv[1:]
    existing: list[dict] = []
    jl = HERE / "member_votes.jsonl"
    if jl.exists():
        existing = [json.loads(ln) for ln in jl.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if "--refresh-carlow" in args:
        carlow = refresh_carlow([a for a in args if not a.startswith("--")])
    else:
        # REGRESSION GATE: pass the existing Carlow rows through byte-identically
        carlow = [r for r in existing if r["local_authority"] == "Carlow"]
        print(f"Carlow     {len(carlow)} existing member-votes preserved (corpus mode)")
    rows = carlow + run_corpus()
    jl.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
    fields = ["local_authority", "meeting", "meeting_date", "motion", "member", "vote"]
    with open(HERE / "member_votes.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows([{k: r.get(k, "") for k in fields} for r in rows])
    print(f"\nTOTAL member-vote rows: {len(rows)}  -> member_votes.jsonl / .csv")
    from collections import Counter
    print("by council:", dict(Counter(r["local_authority"] for r in rows)))
    print("by vote:", dict(Counter(r["vote"] for r in rows)))
    print("distinct members:", len({(r['local_authority'], r['member']) for r in rows}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
