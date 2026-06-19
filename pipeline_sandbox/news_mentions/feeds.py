"""Curated, group-aware RSS feed registry for the media-mentions sandbox.

Built from the Phase-0 canvass (2026-06-19). Only feeds confirmed LIVE
(HTTP 200 + >=3 real items) are listed. Tier classifies editorial risk /
coverage role; `areas` lists the Dail constituencies a local title primarily
serves (national/specialist left empty).

Publisher-group patterns learned:
  - Iconic Newspapers  -> root /rss (NOT /section/<id>/rss, which is empty)
  - Reach plc          -> ?service=rss  (Brotli-compressed: request gzip only)
  - Independent WP     -> /feed/ or /rss
  - Celtic Media       -> feeds DISABLED (omitted)
  - Mediahuis regionals-> RSS removed + Cloudflare 403 (omitted)
  - No-RSS-but-publishes (Galway Bay FM, Echo Cork, Southern Star) -> scrape-only (omitted)
"""

# tier: national | specialist | local_paper | local_radio | partisan
FEEDS = [
    # ---- national politics / breaking ----
    {"name": "RTE Politics",        "url": "https://www.rte.ie/feeds/rss/?index=/news/politics/", "tier": "national",   "areas": []},
    {"name": "RTE News",            "url": "https://www.rte.ie/rss/news.xml",                      "tier": "national",   "areas": []},
    {"name": "Extra.ie",            "url": "https://extra.ie/feed/",                               "tier": "national",   "areas": []},
    # ---- specialist (sector politics) ----
    {"name": "Agriland",            "url": "https://www.agriland.ie/feed/",                        "tier": "specialist", "areas": []},
    # ---- partisan (flagged separately) ----
    {"name": "Gript",               "url": "https://gript.ie/feed/",                               "tier": "partisan",   "areas": []},
    # ---- Munster ----
    {"name": "CorkBeo",             "url": "https://www.corkbeo.ie/?service=rss",                  "tier": "local_paper","areas": ["Cork East","Cork North-Central","Cork North-West","Cork South-Central","Cork South-West"]},
    {"name": "The Avondhu",         "url": "https://avondhupress.ie/feed/",                        "tier": "local_paper","areas": ["Cork East","Cork North-West"]},
    {"name": "Clare Champion",      "url": "https://clarechampion.ie/feed/",                       "tier": "local_paper","areas": ["Clare"]},
    {"name": "Clare FM",            "url": "https://www.clare.fm/news/feed/",                      "tier": "local_radio","areas": ["Clare"]},
    {"name": "Radio Kerry",         "url": "https://www.radiokerry.ie/news/feed/",                 "tier": "local_radio","areas": ["Kerry"]},
    {"name": "Limerick Live",       "url": "https://www.limerickleader.ie/rss",                    "tier": "local_paper","areas": ["Limerick City","Limerick County"]},
    {"name": "Limerick Post",       "url": "https://www.limerickpost.ie/feed/",                    "tier": "local_paper","areas": ["Limerick City","Limerick County"]},
    {"name": "Tipperary Live",      "url": "https://www.tipperarylive.ie/rss",                     "tier": "local_paper","areas": ["Tipperary North","Tipperary South"]},
    {"name": "Tipp FM",             "url": "https://tippfm.com/news/feed/",                        "tier": "local_radio","areas": ["Tipperary North","Tipperary South"]},
    {"name": "Munster Express",     "url": "https://www.munster-express.ie/feed/",                 "tier": "local_paper","areas": ["Waterford"]},
    {"name": "Waterford Live",      "url": "https://www.waterfordlive.ie/rss",                     "tier": "local_paper","areas": ["Waterford"]},
    {"name": "WLR FM",              "url": "https://wlrfm.com/feed/",                              "tier": "local_radio","areas": ["Waterford"]},
    {"name": "Beat 102-103",        "url": "https://www.beat102103.com/feed/",                     "tier": "local_radio","areas": ["Wexford","Carlow-Kilkenny","Waterford","Tipperary South"]},
    # ---- Connacht / Ulster ----
    {"name": "Connacht Tribune",    "url": "https://www.connachttribune.ie/rss",                   "tier": "local_paper","areas": ["Galway East","Galway West"]},
    {"name": "Mayo News",           "url": "https://www.mayonews.ie/rss",                          "tier": "local_paper","areas": ["Mayo"]},
    {"name": "Midwest Radio",       "url": "https://www.midwestradio.ie/feed/",                    "tier": "local_radio","areas": ["Mayo"]},
    {"name": "Roscommon People",    "url": "https://roscommonpeople.ie/feed/",                     "tier": "local_paper","areas": ["Roscommon-Galway"]},
    {"name": "Sligo Weekender",     "url": "https://sligoweekender.ie/feed/",                      "tier": "local_paper","areas": ["Sligo-Leitrim"]},
    {"name": "Shannonside",         "url": "https://www.shannonside.ie/feed/",                     "tier": "local_radio","areas": ["Sligo-Leitrim","Roscommon-Galway"]},
    {"name": "Donegal News",        "url": "https://donegalnews.com/feed/",                        "tier": "local_paper","areas": ["Donegal"]},
    {"name": "Donegal Daily",       "url": "https://www.donegaldaily.com/feed/",                   "tier": "local_paper","areas": ["Donegal"]},
    {"name": "Highland Radio",      "url": "https://highlandradio.com/feed/",                      "tier": "local_radio","areas": ["Donegal"]},
    {"name": "Northern Standard",   "url": "https://northernstandard.ie/feed/",                    "tier": "local_paper","areas": ["Cavan-Monaghan"]},
    {"name": "Northern Sound",      "url": "https://www.northernsound.ie/feed/",                   "tier": "local_radio","areas": ["Cavan-Monaghan"]},
    # ---- Leinster (ex-Dublin): Iconic root /rss ----
    {"name": "Kilkenny People",     "url": "https://www.kilkennypeople.ie/rss",                    "tier": "local_paper","areas": ["Carlow-Kilkenny"]},
    {"name": "KCLR 96FM",           "url": "https://kclr96fm.com/feed/",                           "tier": "local_radio","areas": ["Carlow-Kilkenny"]},
    {"name": "Leinster Leader",     "url": "https://www.leinsterleader.ie/rss",                    "tier": "local_paper","areas": ["Kildare North","Kildare South"]},
    {"name": "Leinster Express",    "url": "https://www.leinsterexpress.ie/rss",                   "tier": "local_paper","areas": ["Laois"]},
    {"name": "Offaly Express",      "url": "https://www.offalyexpress.ie/rss",                     "tier": "local_paper","areas": ["Offaly"]},
    {"name": "Longford Leader",     "url": "https://www.longfordleader.ie/rss",                    "tier": "local_paper","areas": ["Longford-Westmeath"]},
    {"name": "Dundalk Democrat",    "url": "https://www.dundalkdemocrat.ie/rss",                   "tier": "local_paper","areas": ["Louth"]},
    # ---- Dublin ----
    {"name": "Dublin Live",         "url": "https://www.dublinlive.ie/?service=rss",               "tier": "local_paper","areas": ["Dublin Bay North","Dublin Bay South","Dublin Central","Dublin Mid-West","Dublin North-West","Dublin South-Central","Dublin South-West","Dublin West"]},
    {"name": "The Echo (Dublin)",   "url": "https://www.echo.ie/feed/",                            "tier": "local_paper","areas": ["Dublin Mid-West","Dublin South-West","Dublin South-Central"]},
    {"name": "Dublin Inquirer",     "url": "https://www.dublininquirer.com/latest/feed",           "tier": "local_paper","areas": ["Dublin Bay North","Dublin Bay South","Dublin Central"]},
    {"name": "Dublin Gazette",      "url": "https://dublingazette.com/feed/",                      "tier": "local_paper","areas": ["Dublin West","Dublin Fingal East","Dublin Fingal West","Dublin Rathdown"]},
    {"name": "Dublin City FM",      "url": "https://dublincityfm.ie/feed/",                        "tier": "local_radio","areas": ["Dublin Central","Dublin Bay North"]},
]
