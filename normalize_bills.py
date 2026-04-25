import json

import pandas as pd
from flatten_json import flatten

from config import LEGISLATION_DIR, SILVER_DIR
from utility.select_drop_rename_cols_mappings import bill_cols_to_drop, bill_rename

drop=['bill.longTitleGa','bill.act.longTitleGa','bill.act.shortTitleGa', 'billSort.billShortTitleGaSort','bill.act.uri','bill.methodURI','bill.uri', 'bill.mostRecentStage.event.chamber.uri', 
      'bill.mostRecentStage.event.house.uri','bill.sourceURI', 'bill.shortTitleGa'
      'bill.mostRecentStage.event.stageURI','billSort.actShortTitleGaSort', 'bill.statusURI','bill.mostRecentStage.event.uri', 'bill.originHouse.uri']

with open(LEGISLATION_DIR / "all_bills_by_td.json") as f:
    data = json.load(f)
# Extract individual bill records from each TD's response
bills = [result for td_response in data for result in td_response.get("results", [])]
bills= bills.explode("bill.debates").reset_index(drop=True)

#TODO: CANDIDATES TO UNNEST BELOW, merge logic into legislation and remove this file
#stages, debates, sponsors, events, versions, relatedDocs
#stages;debates;sponsors already done
# bill.debates
# [
#     {
#         "chamber": {"showAs": "DÃ¡il Ã‰ireann", "uri": "https://data.oireachtas.ie/ie/oireachtas/def/house/dail"},
#         "date": "2024-10-08",
#         "debateSectionId": "dbsect_13",
#         "showAs": "Sale of Tickets (Cultural, Entertainment, Recreational and Sporting Events) (Amendment) Bill 2024: First Stage",
#         "uri": "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2024-10-08/debate/main",
#     }
# ]
# bill.events
# [
#     {
#         "event": {
#             "chamber": {
#                 "chamberCode": "dail",
#                 "showAs": "DÃ¡il Ã‰ireann",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/def/house/dail",
#             },
#             "dates": [{"date": "2024-09-13"}, {"date": "2024-09-13"}, {"date": "2024-10-08"}, {"date": "2025-05-27"}],
#             "eventURI": "https://data.oireachtas.ie/ie/oireachtas/def/bill-event/approved-for-initiation",
#             "showAs": "Approved for Initiation",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/approved-for-initiation",
#         }
#     },
#     {
#         "event": {
#             "chamber": {
#                 "chamberCode": "dail",
#                 "showAs": "DÃ¡il Ã‰ireann",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/def/house/dail",
#             },
#             "dates": [{"date": "2024-11-08"}],
#             "eventURI": "https://data.oireachtas.ie/ie/oireachtas/def/bill-event/bill-lapsed",
#             "showAs": "Bill Lapsed",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/bill-lapsed",
#         }
#     },
# ]


#bill.mostRecentStage.event.dates
# [{'date': '2024-10-08'}, {'date': '2024-10-08'}]

#bill.relatedDocs
# [
#     {
#         "relatedDoc": {
#             "date": "2024-10-08",
#             "docType": "memo",
#             "formats": {
#                 "pdf": {"uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/eng/memo/b8324d-memo.pdf"},
#                 "xml": None,
#             },
#             "lang": "eng",
#             "showAs": "Explanatory Memorandum",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/eng/memo",
#         }
#     }
# ]

#bill.sponsors
# [
#     {
#         "sponsor": {
#             "as": {"showAs": None, "uri": None},
#             "by": {
#                 "showAs": "Jim O'Callaghan",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/member/id/Jim-O'Callaghan.D.2016-10-03",
#             },
#             "isPrimary": True,
#         }
#     },
#     {
#         "sponsor": {
#             "as": {"showAs": None, "uri": None},
#             "by": {
#                 "showAs": "Niamh Smyth",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/member/id/Niamh-Smyth.D.2016-10-03",
#             },
#             "isPrimary": False,
#         }
#     },
# ]


#bill.stages
# [
#     {
#         "event": {
#             "chamber": {
#                 "chamberCode": "dail",
#                 "showAs": "DÃ¡il Ã‰ireann",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/def/house/dail",
#             },
#             "dates": [{"date": "2018-02-22"}],
#             "house": {
#                 "chamberCode": "dail",
#                 "chamberType": "house",
#                 "houseCode": "dail",
#                 "houseNo": "32",
#                 "showAs": "32nd DÃ¡il",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/house/dail/32",
#             },
#             "progressStage": 1,
#             "showAs": "First Stage",
#             "stageCompleted": True,
#             "stageOutcome": None,
#             "stageURI": "https://data.oireachtas.ie/ie/oireachtas/def/bill-stage/1",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2018/23/stage/dail/1",
#         }
#     },
#     {
#         "event": {
#             "chamber": {
#                 "chamberCode": "dail",
#                 "showAs": "DÃ¡il Ã‰ireann",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/def/house/dail",
#             },
#             "dates": [{"date": "2018-02-22"}, {"date": "2018-02-22"}],
#             "house": {
#                 "chamberCode": "dail",
#                 "chamberType": "house",
#                 "houseCode": "dail",
#                 "houseNo": "32",
#                 "showAs": "32nd DÃ¡il",
#                 "uri": "https://data.oireachtas.ie/ie/oireachtas/house/dail/32",
#             },
#             "progressStage": 2,
#             "showAs": "Second Stage",
#             "stageCompleted": True,
#             "stageOutcome": None,
#             "stageURI": "https://data.oireachtas.ie/ie/oireachtas/def/bill-stage/2",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2018/23/stage/dail/2",
#         }
#     },
# ]


# bill.versions
# [
#     {
#         "version": {
#             "date": "2024-10-08",
#             "docType": "bill",
#             "formats": {
#                 "pdf": {"uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/eng/initiated/b8324d.pdf"},
#                 "xml": None,
#             },
#             "lang": "eng",
#             "showAs": "As Initiated",
#             "uri": "https://data.oireachtas.ie/ie/oireachtas/bill/2024/83/eng/initiated",
#         }
#     }
# ]

output_path = SILVER_DIR / "new_flattened_bills.csv"
df.to_csv(output_path)
df1 = pd.read_csv(SILVER_DIR / "new_flattened_bills.csv")
df1 = (
    df1.drop(bill_cols_to_drop, axis=1, errors="ignore")
    .rename(columns=bill_rename)
)
df1.to_csv(SILVER_DIR / "drop_cols_flattened_bills.csv")
df1.to_parquet(SILVER_DIR / "parquet" / "drop_cols_flattened_bills.parquet", index=False)
# 
if __name__ == "__main__":
    print("Bills JSON flattening complete. Output saved to new_flattened_bills.csv and drop_cols_flattened_bills.csv.")
