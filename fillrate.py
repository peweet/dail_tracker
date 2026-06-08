import requests, time
URL="https://api.ted.europa.eu/v3/notices/search"
H={"User-Agent":"dail-tracker research probe","Accept":"application/json"}
FIELDS=["winner-name","winner-identifier","total-value","result-value-notice","result-value-lot",
        "organisation-name-tenderer","tender-value","buyer-name"]
def sample(year):
    q=f"buyer-country=IRL AND notice-type=can-standard AND publication-date>={year}0101 AND publication-date<={year}1231"
    body={"query":q,"fields":FIELDS,"limit":250,"page":1,"paginationMode":"PAGE_NUMBER"}
    for attempt in range(4):
        r=requests.post(URL,json=body,headers=H,timeout=120)
        if r.status_code==200: break
        time.sleep(5)
    notices=r.json().get("notices",[])
    print(f"\n{year}: sampled {len(notices)} notices")
    for f in FIELDS:
        filled=sum(1 for n in notices if n.get(f))
        ex=next((n.get(f) for n in notices if n.get(f)),None)
        print(f"   {f:30} {filled/len(notices):4.0%}  e.g. {str(ex)[:55]}")
    time.sleep(3)
for y in [2018,2021,2024]:
    sample(y)
