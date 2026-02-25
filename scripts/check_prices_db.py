import sqlite3

c = sqlite3.connect("data/prices.db")

rows = c.execute("select count(*) from underlying_prices").fetchone()[0]
tickers = c.execute("select count(distinct ticker) from underlying_prices").fetchone()[0]
sample = c.execute(
    "select dt, close, rv_20 from underlying_prices where ticker='AAPL' order by dt desc limit 5"
).fetchall()

print("rows:", rows)
print("tickers:", tickers)
print("sample AAPL:", sample)

c.close()