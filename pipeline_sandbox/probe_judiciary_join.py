"""THROWAWAY PROBE: test whether the Iris judicial-appointment spine joins to the
live Courts Service roster. Not pipeline code. Run: python pipeline_sandbox/probe_judiciary_join.py
Measures name-normalisation join rate and detects elevation (court differs Iris->roster).
"""

import unicodedata
import re
import polars as pl

# --- live roster (courts.ie/judges, fetched 2026-06-04), name -> court ---
ROSTER = {
    "Supreme Court": [
        "Donal O'Donnell", "Iseult O'Malley", "Seamus Woulfe", "Gerard Hogan",
        "Brian Murray", "Maurice Collins", "Aileen Donnelly", "Niamh Hyland",
        "Brian O'Moore", "Caroline Costello", "David Barniville",
    ],
    "Court of Appeal": [
        "Caroline Costello", "John A. Edwards", "Máire Whelan", "Patrick J. McCarthy",
        "Isobel Kennedy", "Mary Faherty", "Ann Power", "Donald Binchy",
        "Teresa Pilkington", "Senan Allen", "Nuala Butler", "Charles Meenan",
        "Tara Burns", "Michael MacGrath", "Denis McDonald", "Anthony Michael Collins",
        "Alexander Owens",
    ],
    "High Court": [
        "David Barniville", "Paul McDermott", "Anthony Barr", "David Keane",
        "Max Barrett", "Brian Cregan", "Tony Hunt", "Carmel Stewart", "Tony O'Connor",
        "Richard Humphreys", "Michael Twomey", "Miriam O'Regan", "Paul Coffey",
        "Leonie Reynolds", "Eileen Creedon", "Michael Quinn", "Garrett Simons",
        "John Jordan", "Mark Sanfey", "Mary Rose Gearty", "Mark Heslin", "Paul Burns",
        "Siobhan Stack", "David Holland", "Cian Ferriter", "Emily Egan",
        "Caroline Biggs", "Karen O'Connor", "Conor Dignam", "Siobhán Phelan",
        "Marguerite Bolger", "Kerida Naidoo", "Eileen Roberts", "Melanie Greally",
        "Mícheál P. O'Higgins", "Rory Mulcahy", "Emily Farrell", "Liam Kennedy",
        "Siobhan Lankford", "Barry O'Donnell", "Oisín Quinn", "Nuala Jackson",
        "Conleth Bradley", "Denise Brett", "David Nolan", "Patrick McGrath",
        "Nessa Cahill", "Sara Phelan", "Sean Gillane", "Mark Dunne", "Micheál O'Connell",
        "Patricia Ryan",
    ],
    "Circuit Court": [
        "Patricia Ryan", "James O'Donohoe", "Martin E. Nolan", "Sarah Berkeley",
        "Pauline Codd", "Keenan Johnson", "Eugene O'Kelly", "Francis Comerford",
        "Elma Sheahan", "John Francis Aylmer", "Sinéad Ní Chúlacháin",
        "Brian O'Callaghan", "Cormac Quinn", "Mary O'Malley Costello", "Patrick Quinn",
        "Martina Baxter", "Eoin Garavan", "Kathryn Hutton", "John O'Connor",
        "Helen Boyle", "Mary Morrissey", "Colin Daly", "Orla Crowe", "Dara Hayes",
        "Catherine Staines", "Kenneth Connolly", "John Martin", "Elva Duffy",
        "Elizabeth Maguire", "Simon McAleese", "Dermot Sheehan", "Sinead Behan",
        "Geoffrey Shannon", "Gerard Meehan", "Jennifer O'Brien", "Fiona O'Sullivan",
        "Jonathan Dunphy", "Christopher Callan", "Catherine White", "Ronan Munro",
        "Roderick Maguire", "Sinéad McMullan", "Deirdre Browne", "Alec Gabbett",
        "Paula Murphy", "Cephas Power", "Paul Kelly",
    ],
    "District Court": [
        "Paul Kelly", "Anne Watkin", "Anthony J. Halpin", "David McHugh",
        "Brendan Toale", "Gráinne Malone", "John Hughes", "Treasa Kelly",
        "John Campbell", "Máire Conneely", "Conor Fottrell", "Michèle Finan",
        "Catherine Ghent", "Shalom Binchy", "Karen Dowling", "Áine Shannon",
        "Michael Ramsey", "Emile Daly", "Éiteáin Cunningham", "Sandra Murphy",
        "James Faughnan", "Raymond Finnegan", "Nicola Jane Andrews", "Fiona Lydon",
        "Marie Keane", "Bernadette Owens", "Eirinn McKiernan", "Adrian Harris",
        "Carol Anne Coolican", "Patricia Harney", "Andrew Cody", "David Kennedy",
        "David Waters", "Joanne Carroll", "Philip O'Leary", "William Aylmer",
        "Colm Roberts", "John O'Leary", "Geraldine Carthy", "John Cheatle",
        "Kevin Staunton", "Desmond Zaidan", "Gerard Furlong", "Alan Mitchell",
        "Deirdre Gearty", "John King", "Miriam A. Walsh", "John F. Brennan",
        "Brendan O'Reilly", "Patricia Cronin", "Vincent Deane", "Ciaran Liddy",
        "Susan Fay", "Monika Leech", "Stephanie Coggans", "Fiona Brennan",
        "Michael Connellan", "Catherine Hayden", "Mark O'Connell", "Peter White",
        "Áine Clancy", "Catherine Ryan", "Valerie Corcoran", "Elizabeth Healy",
        "Derek Cooney", "Edward Carroll", "Paula Cullinane", "Andrew Gubbins",
        "Tom MacSharry", "Mary McAveety", "Darach McCarthy",
    ],
}

HONORIFICS = {"mr", "mrs", "ms", "dr", "judge", "justice", "the", "hon", "honourable"}
JUNK_TOKENS = {"as", "proxy", "by", "the", "to", "of", "limited", "ltd", "llp",
               "company", "box", "po", "scheme", "name", "type", "abhcoide", "sinsearach"}


def norm_tokens(name: str) -> frozenset:
    """lowercase, strip diacritics, drop honorifics/punct -> token set."""
    if not name:
        return frozenset()
    n = unicodedata.normalize("NFD", name)
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")  # strip accents
    n = n.lower().replace("'", " ").replace(".", " ").replace(",", " ")
    toks = [t for t in re.split(r"\s+", n) if len(t) > 1 and t not in HONORIFICS]
    return frozenset(toks)


# build roster token-sets
roster_norm = {}  # frozenset(name tokens) -> (display, court)
for court, names in ROSTER.items():
    for nm in names:
        roster_norm.setdefault(norm_tokens(nm), (nm, court))

# roster name lookup that also allows subset matching (Iris "Sean Gillane" vs roster "Sean Gillane")
roster_list = [(norm_tokens(nm), nm, court) for court, names in ROSTER.items() for nm in names]


def match_roster(tokset):
    """exact, else best subset/superset overlap (>=2 shared surname-ish tokens)."""
    if tokset in roster_norm:
        return roster_norm[tokset]
    best = None
    for rset, rnm, rcourt in roster_list:
        shared = tokset & rset
        if len(shared) >= 2 and (tokset <= rset or rset <= tokset):
            best = (rnm, rcourt)
            break
    return best


df = pl.read_parquet("data/gold/parquet/public_appointments.parquet")
jud = df.filter(pl.col("appointment_type") == "judicial")

REAL_COURTS = ["High Court", "District Court", "Circuit Court", "Supreme Court", "Court of Appeal"]

# explode multi-name rows
rows = []
for appointee, court, date in jud.select(["appointee", "body", "issue_date"]).iter_rows():
    if appointee is None:
        continue
    for piece in appointee.split(";"):
        piece = piece.strip()
        toks = norm_tokens(piece)
        if not toks or len(toks & JUNK_TOKENS) >= max(1, len(toks) - 1):
            continue  # junk fragment
        rows.append((piece, court, date, toks))

print(f"Iris judicial notices: {len(jud)}  | exploded clean name-fragments: {len(rows)}")
real = [r for r in rows if r[1] in REAL_COURTS]
junk = [r for r in rows if r[1] not in REAL_COURTS]
print(f"  on a REAL court: {len(real)}   | in 'Courts' junk bucket: {len(junk)}")

matched, elevated, unmatched = [], [], []
for piece, court, date, toks in real:
    m = match_roster(toks)
    if m is None:
        unmatched.append((piece, court, date))
    else:
        rnm, rcourt = m
        if rcourt != court:
            elevated.append((piece, court, rcourt, date))
        else:
            matched.append((piece, court, date))

total = len(real)
hit = len(matched) + len(elevated)
print(f"\n=== JOIN to live roster (Iris real-court appointments, n={total}) ===")
print(f"  matched same court : {len(matched)}")
print(f"  matched, ELEVATED  : {len(elevated)}  (appointed to X, now sits higher)")
print(f"  no roster match    : {len(unmatched)}  (pre-2016 N/A, retired, or norm-fail)")
print(f"  >>> JOIN HIT RATE  : {hit}/{total} = {hit/total:.0%}")

print("\n=== ELEVATIONS detected (the killer feature) ===")
for piece, ic, rc, date in elevated:
    print(f"  {piece:28} {ic:15} ({date}) ->  now {rc}")

print("\n=== sample UNMATCHED (expected: retired / appointed pre-2016 / since-departed) ===")
for piece, court, date in unmatched[:15]:
    print(f"  {piece:30} {court:15} {date}")
