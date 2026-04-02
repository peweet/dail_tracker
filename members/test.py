import polars as pl
# df= pl.read_csv("members\enriched_td_attendance.csv").select(pl.col('unique_member_code')).filter(pl.col('unique_member_code').is_not_null()).unique()

# print(df.count())


df1= pl.read_csv("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\members\\td_tables.csv")
df1 = df1.select(pl.col(['first_name', 'last_name'])).unique().count()
print(df1)