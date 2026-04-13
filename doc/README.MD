This is a civic project to ingest open source data published by the Oireachtas and to collate it 
into a dataset for civic purposes. 

Unfortunately parts of this project are inherently brittle as many of the crucial Irish information is in PDFs and it makes extraction quite tricky and complicated. To over come this there is a lot of manual download and loading of PDFs to allow the project to work. Also some direct extract of CSVs from lobbying.ie is required. There is a plan to automate this via Playwright but it is quite a lot of work for a process than can take 5 mins manually.

I will provide a list of steps to follow if you want to recreate this project from scratch. If there are any glaring errors it is a genuine mistake on my part. 

There is some convertion using OCR libraries of scanned SIPO General Election Expense PDFs and a basic attempt of loading into a table but for now this is just an expermimental feature!

API link: https://api.oireachtas.ie/
SIPO publications: https://www.sipo.ie/en/collection/9f7db-publications/
Register of Lobbying: https://www.lobbying.ie/

Data sourcing:
Parliamentary data (members, legislation, questions, debates) is sourced from
the Houses of the Oireachtas Open Data API (https://api.oireachtas.ie/) and is used under
the Oireachtas (Open Data) PSI Licence (CC BY 4.0).
© Houses of the Oireachtas.

## ⚠️ Data Disclaimer

This project aggregates publicly available data from official sources related to Dáil Éireann. It uses the Dail API as well as various 
PDF documents published by governmental bodies such as SIPO (Standards In Public Office Commission) or the Dail Eireann themselves. 

- This project is not affiliated with any government body
- There is no political party or any bias intended in this project. If you spot any it is purely accidental
- Data accuracy is not guaranteed
- Users should verify critical information with official sources

Use at your own risk.

## 🛡️ Liability

This software is provided "as is", without warranty of any kind.

The authors are not responsible for:
- inaccuracies in data
- misuse of the software
- any damages arising from its use

Some notes on the data pipeline. Data extracted from the PDF files is inherently brittle 
and may contain data gaps. The data is extracted from 2024-2025 periods and some TD's are simply missing from the PDFs for reasons I can not surmise. It can occur in the future the links may dissapear or become inactive or be deactivated. I will provide an archived folder of all the PDFs used.
 
I will provide screenshots of the sources and how to create it.

You must do some of your own investigation prior to blindly trust in the data. I have made 
my very best effort to create a high quality data piece of work however some gaps may be present.

Feel free to fork it and see if I missed anything.

Data Glossary:
For interst mapping
interest_mapping = 
    "1": "Occupations",
    "2": "Shares",
    "3": "Directorships",
    "4": "Land (including property)",
    "5": "Gifts",
    "6": "Property supplied or lent or a Service supplied",
    "7": "Travel Facilities",
    "8": "Remunerated Position",
    "9": "Contracts"

MIT License

Copyright (c) 2026 Peweet

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, subject to the following conditions:

1. The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

2. Attribution is required. Users must provide appropriate credit to the original author.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.

IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

Additional Terms for Data Usage:

This project may include or process publicly available data from third-party
sources (e.g., government or public registers such as lobbying.ie).

- This project is not affiliated with, endorsed by, or officially connected to any government body.
- All data remains the responsibility of the original source.
- No guarantees are made regarding the accuracy, completeness, or timeliness of data.
- Users must verify data with official sources before relying on it.

Use of third-party data may be subject to additional terms imposed by the original provider.
