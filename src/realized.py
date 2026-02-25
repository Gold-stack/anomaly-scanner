import math
from datetime import datetime
import requests

def realized_vol_annualized_from_closes(closes: list[float], window: int = 30) -> float | None:
    if len(closes) < window + 1:
        return None
    # log returns
    rets = []
    for i in range(1, len(closes)):
        c0, c1 = closes[i-1], closes[i]
        if c0 is None or c1 is None or c0 <= 0 or c1 <= 0:
            return None
        rets.append(math.log(c1 / c0))
    # take last window
    w = rets[-window:]
    if len(w) < window:
        return None
    mean = sum(w) / len(w)
    var = sum((x - mean) ** 2 for x in w) / (len(w) - 1)
    # annualize with 252
    return math.sqrt(var * 252.0)

def fetch_daily_closes_marketdata(ticker: str, api_key: str, lookback_days: int = 260) -> tuple[list[str], list[float]]:
    # MarketData candles endpoint (daily). Du hast MarketData Trader.
    # URL-Pattern je nach deinem bestehenden Backend: ggf. hast du schon eine helper function.
    url = f"https://api.marketdata.app/v1/stocks/candles/D/{ticker}/"
    params = {
        "countback": lookback_days,
        "token": api_key
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("s") != "ok":
        return [], []

    # MarketData gibt typischerweise arrays: t (unix), c (close)
    ts = j.get("t") or []
    closes = j.get("c") or []
    dates = []
    for t in ts:
        # seconds epoch
        dates.append(datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d"))
    return dates, [float(x) for x in closes]
