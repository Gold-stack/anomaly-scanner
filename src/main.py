from __future__ import annotations

import os
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .db import init_db, get_conn
from .universe import fetch_sp500_tickers  # kann wegen Wikipedia 403 failen -> fallback unten
from .realized import fetch_daily_closes_marketdata, realized_vol_annualized_from_closes
from .scan_sp500 import get_atm_iv_for_ticker, score_iv_gap, compute_iv_rv_score

app = FastAPI(title="Options Anomaly Scanner (MVP)")

# --- CORS (für lokale Tests + später Railway) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # für MVP ok; später auf deine Domain einschränken
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


# -----------------------
# Helpers
# -----------------------
def _first_float(arr) -> Optional[float]:
    if isinstance(arr, list) and arr:
        try:
            v = arr[0]
            if v is None:
                return None
            return float(v)
        except Exception:
            return None
    return None


def _read_sp500_from_csv() -> List[str]:
    """
    Fallback: liest S&P 500 Ticker aus lokaler CSV.
    Erwartet:
      - Env SP500_CSV (Dateiname) oder default 'sp500.csv'
      - File im Projekt-Root (eine Ebene über backend/)
    Unterstützte Formate:
      - Spalte 'Symbol' oder 'ticker' oder 'Ticker'
      - oder eine Spalte ohne Header (eine Zeile pro Ticker)
    """
    backend_dir = Path(__file__).resolve().parent
    project_root = backend_dir.parent

    csv_name = os.getenv("SP500_CSV", "sp500.csv")
    csv_path = project_root / csv_name

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found: {csv_path}. Lege '{csv_name}' im Projekt-Root ab "
            f"(neben static/ und backend/). Oder setze SP500_CSV."
        )

    tickers: List[str] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)

        # Heuristik: hat Header?
        has_header = csv.Sniffer().has_header(sample) if sample.strip() else False

        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        return []

    if has_header:
        # nochmal mit DictReader
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f2:
            dr = csv.DictReader(f2)
            for row in dr:
                sym = (
                    (row.get("Symbol") or row.get("symbol") or "").strip()
                    or (row.get("Ticker") or row.get("ticker") or "").strip()
                )
                if sym:
                    tickers.append(sym.upper())
    else:
        # erste Spalte pro Zeile
        for r in rows:
            if not r:
                continue
            sym = (r[0] or "").strip()
            if sym and sym.lower() not in ("symbol", "ticker"):
                tickers.append(sym.upper())

    # cleanup (Dots etc.)
    clean = []
    for t in tickers:
        t = t.replace(".", "-").strip().upper()
        if t and t not in clean:
            clean.append(t)

    return clean


def _get_sp500_tickers_safe() -> Tuple[List[str], str]:
    """
    Versucht zuerst universe.fetch_sp500_tickers() (kann 403 geben),
    sonst fallback auf lokale CSV.
    """
    try:
        t = fetch_sp500_tickers()
        t = [x.replace(".", "-").strip().upper() for x in t if x]
        return t, "universe.fetch_sp500_tickers()"
    except Exception as e:
        # Fallback CSV
        t = _read_sp500_from_csv()
        return t, f"csv_fallback ({type(e).__name__}: {e})"


# -----------------------
# API
# -----------------------
@app.get("/api/health")
def health():
    return {"s": "ok"}


@app.get("/api/stocks/price")
def stocks_price(ticker: str):
    api_key = os.getenv("MARKETDATA_API_KEY")
    if not api_key:
        return {"s": "error", "msg": "MARKETDATA_API_KEY missing"}

    t = ticker.strip().upper()

    url = f"https://api.marketdata.app/v1/stocks/quotes/{t}/"
    r = requests.get(url, params={"token": api_key}, timeout=20)
    r.raise_for_status()
    j = r.json()

    if j.get("s") != "ok":
        return {"s": "error", "msg": j.get("errmsg") or "quote not ok"}

    mid = _first_float(j.get("mid"))
    bid = _first_float(j.get("bid"))
    ask = _first_float(j.get("ask"))
    last = _first_float(j.get("last"))

    if mid is None:
        if bid is not None and ask is not None:
            mid = (bid + ask) / 2.0
        else:
            mid = last

    if mid is None:
        return {"s": "error", "msg": "no price fields found"}

    return {"s": "ok", "ticker": t, "mid": mid, "bid": bid, "ask": ask, "last": last}


@app.post("/api/universe/sp500/refresh")
def refresh_sp500():
    """
    Lädt Ticker und schreibt in DB.
    Falls Wikipedia blockiert: nimmt lokale CSV (sp500.csv im Projekt-Root oder SP500_CSV env).
    """
    tickers, source = _get_sp500_tickers_safe()

    with get_conn() as conn:
        conn.execute("DELETE FROM universe_sp500")
        conn.executemany("INSERT OR REPLACE INTO universe_sp500 (ticker) VALUES (?)", [(t,) for t in tickers])
        conn.commit()

    return {"s": "ok", "count": len(tickers), "source": source}


@app.get("/api/universe/sp500")
def get_sp500(limit: int = 0):
    with get_conn() as conn:
        rows = conn.execute("SELECT ticker FROM universe_sp500 ORDER BY ticker").fetchall()
    tickers = [r["ticker"] for r in rows]
    if limit and limit > 0:
        tickers = tickers[:limit]
    return {"s": "ok", "tickers": tickers, "count": len(tickers)}


@app.post("/api/history/backfill_realized")
def backfill_realized(window: int = 30, limit: int = 0, lookback_days: int = 260):
    api_key = os.getenv("MARKETDATA_API_KEY")
    if not api_key:
        return {"s": "error", "msg": "MARKETDATA_API_KEY missing"}

    with get_conn() as conn:
        rows = conn.execute("SELECT ticker FROM universe_sp500 ORDER BY ticker").fetchall()
    tickers = [r["ticker"] for r in rows]
    if limit and limit > 0:
        tickers = tickers[:limit]

    asof = datetime.utcnow().strftime("%Y-%m-%d")
    done = 0
    failed = []

    for t in tickers:
        try:
            dates, closes = fetch_daily_closes_marketdata(t, api_key=api_key, lookback_days=lookback_days)
            if len(dates) != len(closes) or len(closes) < window + 1:
                failed.append({"ticker": t, "reason": "not_enough_data"})
                continue

            rv = realized_vol_annualized_from_closes(closes, window=window)
            if rv is None:
                failed.append({"ticker": t, "reason": "rv_none"})
                continue

            with get_conn() as conn:
                conn.executemany(
                    "INSERT OR REPLACE INTO spot_close (ticker, date, close) VALUES (?, ?, ?)",
                    [(t, d, float(c)) for d, c in zip(dates, closes)],
                )
                conn.execute(
                    "INSERT OR REPLACE INTO realized_vol (ticker, window, asof_date, rv) VALUES (?, ?, ?, ?)",
                    (t, window, asof, float(rv)),
                )
                conn.commit()

            done += 1
        except Exception as e:
            failed.append({"ticker": t, "reason": str(e)})

    return {"s": "ok", "window": window, "asof_date": asof, "done": done, "failed": failed, "total": len(tickers)}


@app.get("/api/scan/sp500")
def scan_sp500(window: int = 30, top: int = 50, limit: int = 0, base_url: str = "http://127.0.0.1:8000"):
    """
    base_url: dein eigener Backend-Host (für interne Calls auf /api/stocks/price etc.)
    Für Railway später: base_url auf deine public URL setzen.
    """
    with get_conn() as conn:
        rows = conn.execute("SELECT ticker FROM universe_sp500 ORDER BY ticker").fetchall()
    tickers = [r["ticker"] for r in rows]
    if limit and limit > 0:
        tickers = tickers[:limit]

    asof = datetime.utcnow().strftime("%Y-%m-%d")

    with get_conn() as conn:
        rv_rows = conn.execute(
            "SELECT ticker, rv FROM realized_vol WHERE window=? AND asof_date=?",
            (window, asof),
        ).fetchall()
    rv_map = {r["ticker"]: float(r["rv"]) for r in rv_rows}

    results = []
    for t in tickers:
        rv = rv_map.get(t)

        # ATM IV aus Options-Chain + Quotes (scan_sp500.py)
        atm = get_atm_iv_for_ticker(base_url, t, max_quotes=80)
        iv = atm.get("iv")
        gap, score = score_iv_gap(iv, rv)

        results.append(
            {
                "ticker": t,
                "spot": atm.get("spot"),
                "expiry": atm.get("expiry"),
                "atm_strike": atm.get("atm_strike"),
                "iv": iv,
                "rv": rv,
                "iv_gap": gap,
                "score": score,
                "reason": atm.get("reason")
                or (None if (iv is not None and rv is not None) else "missing_rv_or_iv"),
            }
        )

    # sort: highest score first; missing scores go bottom
    def _sort_key(x):
        return (-1 if x["score"] is None else x["score"])

    results.sort(key=_sort_key, reverse=True)
    ranked = [r for r in results if r["score"] is not None]
    missing = [r for r in results if r["score"] is None]
    out = ranked[:top] + missing[:10]

    return {"s": "ok", "asof_date": asof, "window": window, "top": top, "count": len(results), "ranked": out}


@app.get("/api/options/chain")
def options_chain(ticker: str):
    api_key = os.getenv("MARKETDATA_API_KEY")
    if not api_key:
        return {"s": "error", "msg": "MARKETDATA_API_KEY missing"}

    t = ticker.strip().upper()
    url = f"https://api.marketdata.app/v1/options/chain/{t}/"

    r = requests.get(url, params={"token": api_key}, timeout=30)
    r.raise_for_status()
    j = r.json()

    if j.get("s") != "ok":
        return {"s": "error", "msg": j.get("errmsg") or "chain not ok"}

    return {"s": "ok", "optionSymbols": j.get("optionSymbol", [])}


@app.get("/api/options/quotes_batch")
def options_quotes_batch(symbols: str, limit: int = 50):
    api_key = os.getenv("MARKETDATA_API_KEY")
    if not api_key:
        return {"s": "error", "msg": "MARKETDATA_API_KEY missing"}

    sym_list = [s.strip() for s in symbols.split(",") if s.strip()][:limit]
    if not sym_list:
        return {"s": "error", "msg": "no symbols provided"}

    quotes = {}
    failed = []

    for sym in sym_list:
        try:
            url = f"https://api.marketdata.app/v1/options/quotes/{sym}/"
            r = requests.get(url, params={"token": api_key}, timeout=15)

            # 200 und 203 akzeptieren
            if r.status_code not in (200, 203):
                failed.append({
                    "symbol": sym,
                    "http": r.status_code,
                    "text": r.text[:200]
                })
                continue

            j = r.json()

            if j.get("s") != "ok":
                failed.append({
                    "symbol": sym,
                    "msg": j
                })
                continue

            quotes[sym] = j

        except Exception as e:
            failed.append({
                "symbol": sym,
                "err": str(e)
            })

    return {
        "s": "ok",
        "quotes": quotes,
        "failed": failed,
        "count": len(quotes)
    }

@app.get("/api/scan")
def scan():
    # Robust: falls Wikipedia blockiert, nimm CSV fallback
    tickers, _source = _get_sp500_tickers_safe()
    results = []

    for t in tickers:
        data = compute_iv_rv_score(t)
        if data:
            results.append(data)

    # nach IV/RV Ratio sortieren (hoch = teuer)
    results = sorted(results, key=lambda x: x["iv_rv_ratio"], reverse=True)

    return {"count": len(results), "top10": results[:10]}


# -----------------------
# Static Frontend Mount
# -----------------------
# Static liegt bei dir in backend/static (nicht im Projekt-Root)
BACKEND_DIR = Path(__file__).resolve().parent
STATIC_DIR = BACKEND_DIR / "static"

app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")