import requests
URL="https://api.ted.europa.eu/v3/notices/search"
H={"User-Agent":"dail-tracker research probe","Accept":"application/json"}
# candidate legacy/award field names for winner + value
cands=["winner-name","contractor","contractor-name","organisation-name","organisation-name-buyer",
       "value","total-value","awarded-value","contract-value","result-value","notice-value",
       "organisation-identifier","winner-size","contract-conclusion-date","contract-title","title"]
q="buyer-country=IRL AND notice-type=can-standard AND publication-date>=20180101 AND publication-date<=20181231"
ok=[]
for f in cands:
    body={"query":q,"fields":[f],"limit":5,"page":1,"paginationMode":"PAGE_NUMBER"}
    r=requests.post(URL,json=body,headers=H,timeout=60)
    if r.status_code==200:
        notices=r.json().get("notices",[])
        filled=sum(1 for n in notices if n.get(f))
        ok.append(f); print(f"  OK   {f:28} filled {filled}/{len(notices)}  sample={str([n.get(f) for n in notices if n.get(f)][:1])[:80]}")
    else:
        print(f"  400  {f:28} {r.text[:70]}")
