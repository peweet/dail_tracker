import sys, re, csv
sys.stdout.reconfigure(encoding="utf-8")
DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s=str(s).upper(); s=re.sub(r"\bT/A\b.*$","",s); s=s.replace("&"," "); s=re.sub(r"[^A-Z0-9]+"," ",s)
    return " ".join(t for t in s.split() if t and t not in DROP)
agg={}
with open("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        n=norm(row.get("Supplier",""))
        if n in ("KEYSTONE PROCUREMENT","ROADSTONE"):
            try: t=float(row.get("Total","0") or 0)
            except: t=0.0
            d=agg.setdefault(n,{"lines":0,"total":0.0,"ent":{},"desc":set()})
            d["lines"]+=1; d["total"]+=t
            d["ent"][row.get("entity","")]=d["ent"].get(row.get("entity",""),0)+t
            if row.get("Description"): d["desc"].add(row["Description"][:70])
for n,d in agg.items():
    print(f"\n{n}: lines={d['lines']} total=EUR{d['total']:,.0f}")
    for e,t in sorted(d["ent"].items(),key=lambda x:-x[1])[:6]:
        print(f"   {e}: EUR{t:,.0f}")
    print("   sample descs:", list(d["desc"])[:6])
