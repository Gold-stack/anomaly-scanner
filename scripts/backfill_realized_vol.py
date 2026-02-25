import os
import time
import math
import sqlite3
from datetime import date, timedelta

import requests
import pandas as pd


API_BASE = "https://api.marketdata.app/v1"
API_KEY = os.environ.get("MARKETDATA_API_KEY", "").strip()

# Anpassbar:
TICKERS_CSV = os.environ.get("TICKERS_CSV", "tickers.csv")  # Pfad zu deiner Ticker-CSV
DB_PATH = os.environ.get("PRICES_DB", "data/prices.db")
FROM_DATE = os.environ.get("FROM_DATE", (date.today() - timedelta(days=365 * 2)).isoformat())
TO_DATE = os.environ.get("TO_DATE", (date.today() - timedelta(days=1)).isoformat())  # bis gestern
RV_WINDOWS = [20, 60]  # RV_20 und RV_60


def read_tickers(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path, header=None)

    # Falls nur eine Spalte vorhanden ist
    if df.shape[1] == 1:
        tickers = df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
        tickers = [t for t in tickers if t and t.upper() != "NAN"]
        return sorted(list(dict.fromkeys([t.upper() for t in tickers])))

    # Falls mehrere Spalten vorhanden sind
    for col in ["ticker", "symbol", "Ticker", "Symbol"]:
        if col in df.columns:
            tickers = df[col].dropna().astype(str).str.strip().tolist()
            tickers = [t for t in tickers if t and t.upper() != "NAN"]
            return sorted(list(dict.fromkeys([t.upper() for t in tickers])))

    raise ValueError(f"Ticker-Spalte nicht gefunden in {csv_path}.")


def md_get_candles_daily(symbol: str, from_date: str, to_date: str, session: requests.Session) -> dict:
    # D = daily resolution (laut Doku) :contentReference[oaicite:2]{index=2}
    url = f"{API_BASE}/stocks/candles/D/{symbol}/"
    params = {
        "from": from_date,
        "to": to_date,
    }
    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    # simple retries
    last_err = None
    for attempt in range(1, 6):
        try:
            r = session.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                # rate limit -> backoff
                time.sleep(1.5 * attempt)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1.0 * attempt)
    raise RuntimeError(f"Failed candles for {symbol}: {last_err}")


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS underlying_prices (
        ticker TEXT NOT NULL,
        dt TEXT NOT NULL,                 -- YYYY-MM-DD
        close REAL NOT NULL,
        log_return REAL,                  -- may be NULL on first row
        rv_20 REAL,
        rv_60 REAL,
        PRIMARY KEY (ticker, dt)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_underlying_prices_ticker_dt ON underlying_prices(ticker, dt);")
    conn.commit()


def candles_json_to_df(symbol: str, j: dict) -> pd.DataFrame:
    status = j.get("s")
    if status != "ok":
        return pd.DataFrame(columns=["ticker", "dt", "close"])

    closes = j.get("c", [])
    times = j.get("t", [])
    if not closes or not times or len(closes) != len(times):
        return pd.DataFrame(columns=["ticker", "dt", "close"])

    # t ist unix timestamp (UTC). Daily returned without times in UI, aber API liefert t. :contentReference[oaicite:3]{index=3}
    dt = pd.to_datetime(pd.Series(times, dtype="int64"), unit="s", utc=True).dt.date.astype(str)
    df = pd.DataFrame({
        "ticker": symbol,
        "dt": dt,
        "close": pd.Series(closes, dtype="float64"),
    })
    df = df.sort_values("dt").reset_index(drop=True)
    return df


def add_rv_features(df: pd.DataFrame) -> pd.DataFrame:
    # log_return = ln(C_t / C_{t-1})
    df["log_return"] = (df["close"] / df["close"].shift(1)).apply(lambda x: math.log(x) if pd.notna(x) and x > 0 else None)

    # annualize: sqrt(252) * std(returns_window)
    ann = math.sqrt(252.0)
    for w in RV_WINDOWS:
        col = f"rv_{w}"
        df[col] = df["log_return"].rolling(window=w, min_periods=w).std(ddof=0) * ann

    return df


def upsert_prices(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    rows = df[["ticker", "dt", "close", "log_return", "rv_20", "rv_60"]].to_records(index=False)
    conn.executemany("""
        INSERT INTO underlying_prices (ticker, dt, close, log_return, rv_20, rv_60)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, dt) DO UPDATE SET
            close=excluded.close,
            log_return=excluded.log_return,
            rv_20=excluded.rv_20,
            rv_60=excluded.rv_60;
    """, list(rows))
    conn.commit()


def main():
    if not API_KEY:
        print("WARN: MARKETDATA_API_KEY ist leer. Falls du Auth brauchst, setze die ENV-Variable.")
        # Viele Endpoints funktionieren ohne Key nur eingeschränkt; besser Key setzen.

    tickers = read_tickers(TICKERS_CSV)
    print(f"Tickers loaded: {len(tickers)}")
    print(f"Backfill range: {FROM_DATE} -> {TO_DATE}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    ensure_db(conn)

    session = requests.Session()

    ok = 0
    no_data = 0
    failed = 0

    for i, t in enumerate(tickers, start=1):
        try:
            j = md_get_candles_daily(t, FROM_DATE, TO_DATE, session=session)
            df = candles_json_to_df(t, j)
            if df.empty:
                no_data += 1
                print(f"[{i}/{len(tickers)}] {t}: no_data")
                continue

            df = add_rv_features(df)
            upsert_prices(conn, df)
            ok += 1

            if i % 25 == 0:
                print(f"Progress: {i}/{len(tickers)} | ok={ok}, no_data={no_data}, failed={failed}")

        except Exception as e:
            failed += 1
            print(f"[{i}/{len(tickers)}] {t}: FAILED -> {e}")

    conn.close()
    print(f"DONE | ok={ok}, no_data={no_data}, failed={failed}")
    print(f"DB: {DB_PATH}")


if __name__ == "__main__":
    main()