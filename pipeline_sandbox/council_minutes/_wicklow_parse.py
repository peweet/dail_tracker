import json, re, os

BASE = "pipeline_sandbox/council_minutes"
d = json.load(open(os.path.join(BASE, "_wicklow_ocr.json"), encoding="utf-8"))

# Lines to drop entirely (noise / attachments / cover letter)
DROP_EXACT = {"notincluded", "not included", "saop", "agenda", "nima", "meanma",
              "deanma", "heanma", "j193a", "t", "s"}
DROP_SUBSTR = ["seniorexecutiveofficer", "organisationaldevelopment", "helenpurcell",
               "helen prcei", "is mise le meas", "ismiselemeas", "yourattendance",
               "your attendance", "larrtar", "donchathaoirligh", "tionolfar",
               "chontae", "leanas", "cruinniu", "teams", "commencing", "hybrid meeting",
               "agenda attached", "is mise", "helen", "purcell"]

# An item starts when a line begins with one of these (after optional leading number)
START_RE = re.compile(
    r'^\s*(?:\d{1,2}\s*[\.\)]?\s*)?'
    r'(To confirm|To consider|To note|To receive|To agree|Election of|Presentation'
    r'|Correspondence|Notice of Motion|Notices of Motion|To approve|To adopt)\b',
    re.IGNORECASE)

# A bare line that is just an item number (OCR put number on its own line)
NUM_ONLY = re.compile(r'^\s*\d{1,2}\s*[\.\)]?\s*$')
# Attachment filename line
ATTACH = re.compile(r'(\.pdf|\.docx|^attachment\s*:)', re.IGNORECASE)

def is_noise(ln):
    low = ln.strip().lower()
    if not low:
        return True
    if low in DROP_EXACT:
        return True
    if any(s in low for s in DROP_SUBSTR):
        return True
    if ATTACH.search(ln):
        return True
    if NUM_ONLY.match(ln):
        return True
    return False

def fix_spacing(t):
    # Insert space between a lowercase/comma and uppercase where OCR fused words.
    # Conservative: only when no space and clearly two words.
    t = re.sub(r'([a-z,\.])([A-Z])', r'\1 \2', t)
    t = re.sub(r'\s+', ' ', t).strip()
    # common OCR fixes
    t = t.replace("Wickiow", "Wicklow").replace("Clir", "Cllr")
    return t

def parse_meeting(lines):
    # locate the agenda body: after the line that names the meeting ("... at 2:00pm")
    start_idx = 0
    for i, ln in enumerate(lines):
        if re.search(r'(Meeting|meeting) of Wicklow County Council', ln) and re.search(r'\d', ln):
            start_idx = i + 1
            break
    body = lines[start_idx:]

    # Cut off the Notices-of-Motion FULL TEXT block: keep the agenda item line
    # "Notice of Motion(s)" but stop collecting once we hit the verbatim motion text
    # (lines starting with quotes or "Notice of motion in the name of").
    items = []
    cur = None
    nom_seen = False
    for ln in body:
        raw = ln.rstrip()
        low = raw.strip().lower()
        # Stop the agenda list once the detailed NoM text begins
        if low.startswith("notice of motion in the name") or low.startswith("noticesofmotion"):
            nom_seen = True
        m = START_RE.match(raw)
        if m and not nom_seen:
            # new item
            if cur:
                items.append(cur)
            cur = raw.strip()
        else:
            if nom_seen:
                continue
            if is_noise(ln):
                continue
            if cur is not None:
                # continuation of current item
                cur += " " + raw.strip()
    if cur:
        items.append(cur)

    # clean
    out = []
    seen = set()
    for it in items:
        it = re.sub(r'^\s*\d{1,2}\s*[\.\)]\s*', '', it)  # strip leading number
        it = fix_spacing(it)
        # truncate overly long item text to ~280 chars at a word boundary
        if len(it) > 300:
            it = it[:297].rsplit(' ', 1)[0] + '...'
        key = it.lower()[:60]
        if key in seen:
            continue
        seen.add(key)
        if len(it) > 4:
            out.append(it)
    return out[:15]

result = {"council": "Wicklow", "meetings": []}
for k in sorted(d, key=lambda x: int(x)):
    m = d[k]
    items = parse_meeting(m["lines"])
    result["meetings"].append({
        "date": m["date"],
        "title": m["title"].replace(" File type .pdf", ""),
        "source_url": m["url"],
        "agenda_items": items,
    })
    print("="*70)
    print(m["date"], "|", m["title"], "| items:", len(items))
    for it in items:
        print("  -", it)

with open(os.path.join(BASE, "wicklow_meeting_history.json"), "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
print("\nWROTE wicklow_meeting_history.json")
