import sys, re, csv
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/?A\b.*$", "", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return " ".join(t for t in s.split() if t and t not in DROP)

# Scan the disclosed CSV for any Supplier whose norm == 'TONE'
import collections
path = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
matches = collections.Counter()
amounts = collections.defaultdict(float)
descs = collections.defaultdict(set)
ents = collections.defaultdict(set)
raw_suppliers = collections.Counter()
with open(path, encoding="utf-8", errors="replace", newline="") as f:
    r = csv.DictReader(f)
    print("CSV cols:", r.fieldnames)
    for row in r:
        sup = row.get("Supplier") or row.get("supplier") or ""
        n = norm(sup)
        if n == "TONE":
            matches[sup] += 1
            raw_suppliers[sup]+=1
            try: amounts[sup]+= float((row.get("Total") or "0").replace(",",""))
            except: pass
            descs[sup].add((row.get("Description") or "")[:80])
            ents[sup].add(row.get("entity") or "")

print("\n=== Disclosed-file suppliers normalising to 'TONE' ===")
for sup,c in matches.most_common():
    print(f"\nSupplier raw: {sup!r}  rows={c}  total~={amounts[sup]:,.0f}")
    print("  entities:", sorted(e for e in ents[sup] if e)[:10])
    print("  desc sample:", list(descs[sup])[:6])

if not matches:
    print("NONE found that normalise exactly to 'TONE'.")
    # show suppliers containing token TONE
    print("\n=== Suppliers containing whole token TONE (norm) ===")
    seen=collections.Counter()
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            sup = row.get("Supplier") or ""
            n = norm(sup)
            if re.search(r"\bTONE\b", n):
                seen[(sup,n)]+=1
    for (sup,n),c in seen.most_common(40):
        print(f"  {sup!r} -> {n!r}  rows={c}")
