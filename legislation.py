import pandas as pd

# legislation_urls = construct_urls_for_api(api_scenario="legislation")
# fetch_data = fetch_all(urls=legislation_urls)
# save_members_json(fetch_data, scenario="legislation")

# flatten the top-level results array — one item per bill
bills = []
for page in pd.read_json("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\data\\bronze\\legislation_results.json")['results']:
    bills.extend(page)

# shared bill-level meta carried into every fact table
BILL_META = [
    ['billSort', 'billShortTitleEnSort'],
    ['billSort', 'billYearSort'],
    ['bill', 'billNo'],
    ['bill', 'billYear'],
    ['bill', 'billType'],
    ['bill', 'shortTitleEn'],
    ['bill', 'longTitleEn'],
    ['bill', 'lastUpdated'],
    ['bill', 'status'],
    ['bill', 'source'],
    ['bill', 'method'],
    ['bill', 'mostRecentStage', 'event', 'showAs'],
    ['bill', 'mostRecentStage', 'event', 'progressStage'],
    ['bill', 'mostRecentStage', 'event', 'stageCompleted'],
    ['bill', 'mostRecentStage', 'event', 'house', 'showAs'],
    'contextDate',
]

# one row per sponsor-bill — primary join to members data via by.uri
sponsors_df = pd.json_normalize(
    bills,
    record_path=['bill', 'sponsors'],
    meta=BILL_META,
    errors='ignore'
)
# .rename({"sponsor.as.uri": "sponsor_uri"})
# one row per stage-bill — legislative progress timeline
stages_df = pd.json_normalize(
    bills,
    record_path=['bill', 'stages'],
    meta=BILL_META,
    errors='ignore'
)

# one row per debate-bill — debate history per bill
debates_df = pd.json_normalize(
    bills,
    record_path=['bill', 'debates'],
    meta=BILL_META,
    errors='ignore'
)

sponsors_df= sponsors_df.dropna(axis=0, subset=["sponsor.by.showAs"], how="all")
sponsors_df= sponsors_df.dropna(axis=1, how="all")
sponsors_df = sponsors_df.replace(r'[\r\n]+', ' ', regex=True
                        ).replace(r'\s{2,}', ' ', regex=True)

sponsors_df.to_csv('sponsors.csv')

stages_df.dropna(axis=0, how="all").to_csv('stages.csv')

# debates_df['date'] = pd.to_datetime(debates_df['date'], format="%Y-%m-%d")
debates_df = debates_df.sort_values(by="date",axis=0,  ascending=True)
debates_df.dropna(axis=0, how="all").to_csv('debates.csv')