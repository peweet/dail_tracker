This is a civic project to ingest open source data published by the Oireachtas and to collate it 
into a dataset for civic purposes.

API link: https://api.oireachtas.ie/

Some notes on the data pipeline. Data extracted from the PDF files is inherently brittle 
and may contain data gaps. The data is extracted from 2024-2025 periods and some TD's are simply missing from the PDFs for reasons I can not surmise

The sources are provided and can be tested in Swagger. 
You must do some of your own investigation prior to blindly trustin the data. I have made 
my very best effort to create a high quality data piece of work however some gaps may be present.

Feel free to fork it and see if I missed anything.


For interst mapping
# interest_mapping = 
#     "1": "Occupations",
#     "2": "Shares",
#     "3": "Directorships",
#     "4": "Land (including property)",
#     "5": "Gifts",
#     "6": "Property supplied or lent or a Service supplied",
#     "7": "Travel Facilities",
#     "8": "Remunerated Position",
#     "9": "Contracts"
# 