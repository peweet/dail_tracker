"""Parse Westmeath agenda PDF text into clean top-level agenda items."""
import re
import json

# Fada / mojibake repair: the gov site serves Latin-1-ish; fitz returns U+FFFD
# for fada vowels. We can't recover the exact vowel, so map common Irish words.
WORD_FIX = {
    "Miontuairisc�": "Miontuairiscí (Minutes)",
    "CL�R": "CLÁR (Agenda)",
    "Aon Gn� Eile": "Aon Ghnó Eile (Any Other Business)",
    "Aon Gn�": "Aon Ghnó Eile (Any Other Business)",
}


# Common OCR run-together joins on this council's agendas (same-case, so the
# lower->Upper heuristic can't split them). Conservative, dictionary-based.
JOIN_FIX = [
    ("Declarationof", "Declaration of"),
    ("Ms.Bernadette", "Ms. Bernadette"),
    ("Policyand", "Policy and"),
    ("Toconsiderthe", "To consider the"),
    ("Toconsider", "To consider"),
    ("Reportonthe", "Report on the"),
    ("Reporton", "Report on"),
    ("Planninginrespectofthe", "Planning in respect of the"),
    ("inrespectofthe", "in respect of the"),
    ("duplexunit", "duplex unit"),
    ("developmentofacommunityoffice", "development of a community office"),
    ("adjacentto", "adjacent to"),
    ("To considerrecommendation", "To consider recommendation"),
    ("Councilfortheyearended", "Council for the year ended"),
    ("yearended", "year ended"),
    ("receiveaprcsentationon", "receive a presentation on"),
    ("receiveapresentationon", "receive a presentation on"),
    ("To receivereports", "To receive reports"),
    ("To receivereport", "To receive report"),
    ("recommendations from SPc", "recommendations from SPC"),
    ("Recommendationfrom", "Recommendation from"),
    ("Chairof", "Chair of"),
]


def despace_ocr(s):
    """Insert spaces into OCR run-together text.

    First apply known same-case joins, then conservative boundary heuristics
    (lower->Upper, letter<->digit). Avoids splitting all-caps acronyms.
    """
    for bad, good in JOIN_FIX:
        s = s.replace(bad, good)
    # lower/Upper boundary: "ChiefExecutive" -> "Chief Executive"
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    # letter->digit: "December2024" -> "December 2024"
    s = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", s)
    # digit->letter: "31December" -> "31 December"
    s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean(s):
    s = re.sub(r"\s+", " ", s).strip()
    # fada-word repair: match ignoring the replacement char
    base = s.replace("�", "").replace("�", "")
    if base.startswith("Miontuairisc"):
        return "Miontuairiscí (Minutes)"
    if base.startswith("CL") and base.rstrip(" .R") in ("CL", "CLR"):
        return "CLÁR (Agenda)"
    if base.startswith("Aon Gn") or base.startswith("Aon Ghn"):
        return "Aon Ghnó Eile (Any Other Business)"
    for k, v in WORD_FIX.items():
        if s == k or s.startswith(k):
            return v
    s = s.replace("�", "").replace("�", "")
    s = despace_ocr(s)
    return s.strip(" .")


def parse_agenda(text):
    """Return list of top-level numbered agenda item titles."""
    lines = [ln.strip() for ln in text.splitlines()]
    # Find lines that are just a number+dot (e.g. "1.") -> the title is the
    # following non-empty line(s) up to the next number marker.
    items = []
    i = 0
    n = len(lines)
    num_re = re.compile(r"^(\d{1,2})\.$")
    while i < n:
        m = num_re.match(lines[i])
        if m:
            num = int(m.group(1))
            # collect following lines until next top-level number marker or a
            # roman/sub marker "(i)" which begins detail we treat as part of title break
            j = i + 1
            title_parts = []
            while j < n:
                if num_re.match(lines[j]):
                    break
                if re.match(r"^\((?:i|ii|iii|iv|v|vi|vii|viii|ix|x)\)$", lines[j]):
                    # sub-clause begins; stop collecting the heading
                    break
                if lines[j]:
                    title_parts.append(lines[j])
                else:
                    # blank line after we already have a title -> stop (heading is short)
                    if title_parts:
                        break
                j += 1
            title = clean(" ".join(title_parts))
            if title and len(items) < 30:
                items.append((num, title))
            i = j
        else:
            i += 1
    # keep order, ensure monotonic-ish numbering; dedupe
    seen = set()
    out = []
    for num, t in items:
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out[:15]


if __name__ == "__main__":
    import sys
    d = json.loads(open("c:/tmp/westmeath_agendas.json", encoding="utf-8", errors="replace").read())
    ocr = {}
    try:
        ocr = json.loads(open("c:/tmp/westmeath_ocr.json", encoding="utf-8", errors="replace").read())
    except Exception:
        pass
    result = []
    for o in d:
        date = o["date"]
        text = o.get("text", "")
        if (not text or len(text.strip()) < 40) and date in ocr:
            text = ocr[date]
            o["text"] = text
            o["scanned"] = True
        items = parse_agenda(text)
        # trim any over-long merged titles to first sentence/clause
        trimmed = []
        for it in items:
            if len(it) > 160:
                it = it[:157].rsplit(" ", 1)[0] + "..."
            trimmed.append(it)
        result.append({
            "date": date,
            "source_url": o["url"],
            "scanned": bool(o.get("scanned")),
            "agenda_items": trimmed,
        })
        print("====", date, "scanned=", o.get("scanned"), "n_items=", len(trimmed))
        for it in trimmed:
            try:
                print("  -", it)
            except UnicodeEncodeError:
                print("  -", it.encode("ascii", "replace").decode())
    open("c:/tmp/westmeath_final.json", "w", encoding="utf-8").write(
        json.dumps(result, ensure_ascii=False, indent=1))
