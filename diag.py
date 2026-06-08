import json, collections
raw=json.load(open(r"C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/ted/ted_ie_awards_raw.json",encoding="utf-8"))
print(f"notices in cache: {len(raw):,}")
# date span
dates=[(n.get('dispatch-date') or '')[:10] for n in raw if n.get('dispatch-date')]
print(f"dispatch-date span: {min(dates)} .. {max(dates)}")
yrs=collections.Counter(d[:4] for d in dates)
print("by year:", dict(sorted(yrs.items())))
# fill rates of key fields
def fill(f): return sum(1 for n in raw if n.get(f))
for f in ["buyer-name","organisation-name-tenderer","tendering-party-name","winner-identifier","tender-value","classification-cpv","procedure-type","received-submissions-type-val"]:
    print(f"  fill {f:32}: {fill(f):6,} / {len(raw):,}  ({fill(f)/len(raw):.0%})")
# fill by era: 2016-2019 vs 2024+
def fill_era(f, lo, hi):
    rows=[n for n in raw if lo <= (n.get('dispatch-date') or '0000')[:4] <= hi]
    if not rows: return "n/a"
    return f"{sum(1 for n in rows if n.get(f))/len(rows):.0%} of {len(rows)}"
print("\nwinner field fill by era:")
for f in ["organisation-name-tenderer","tendering-party-name","winner-identifier","tender-value"]:
    print(f"  {f:32} 2016-2019={fill_era(f,'2016','2019')}   2024+={fill_era(f,'2024','2026')}")
