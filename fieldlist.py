import requests, json, time
URL="https://api.ted.europa.eu/v3/notices/search"
H={"User-Agent":"dail-tracker research probe","Accept":"application/json"}
time.sleep(3)
body={"query":"buyer-country=IRL","fields":["__bad__"],"limit":1,"page":1,"paginationMode":"PAGE_NUMBER"}
r=requests.post(URL,json=body,headers=H,timeout=60)
msg=r.json().get("message","") if r.headers.get("content-type","").startswith("application/json") else r.text
# the supported list is in the message; extract field-like tokens mentioning winner/value/org/contractor
import re
toks=re.findall(r"[a-z][a-z0-9-]{2,}", msg)
interesting=sorted(set(t for t in toks if any(k in t for k in["winn","valu","contract","organ","tender","award","result","name","ident","cont"])))
print("candidate winner/value/org fields in supported list:")
for t in interesting: print("  ",t)
print("\n(total supported tokens:",len(set(toks)),")")
