from __future__ import annotations

import os
import math
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests


# ----------------------------
# Helpers
# ----------------------------
def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _first(v) -> Any:
    # unsere /api/options/quotes_batch liefert Felder als Listen (bid:[..], iv:[..] etc.)
    if isinstance(v, list) and v:
        return v[0]
    return v


# ----------------------------
# Realized vol (aus data/prices.db)
# ----------------------------
def get_latest_rv20(ticker: str, db_path: str = "data/prices.db") -> Optional[float]:
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT rv_20
            FROM underlying_prices
            WHERE ticker = ?
            ORDER BY dt DESC
            LIMIT 1
            """,
            (ticker,),
        ).fetchone()
        conn.close()
        if not row:
            return None
        return _safe_float(row[0])
    except Exception:
        return None


# ----------------------------
# MarketData via lokale API Endpoints
# ----------------------------
def fetch_chain_symbols(base_url: str, ticker: str) -> List[str]:
    r = requests.get(f"{base_url}/api/options/chain", params={"ticker": ticker}, timeout=30)
    r.raise_for_status()
    j = r.json()
    if j.get("s") != "ok":
        return []
    syms = j.get("optionSymbols") or []
    return [s for s in syms if isinstance(s, str) and s.strip()]


def fetch_spot(base_url: str, ticker: str) -> Optional[float]:
    r = requests.get(f"{base_url}/api/stocks/price", params={"ticker": ticker}, timeout=20)
    r.raise_for_status()
    j = r.json()
    if j.get("s") != "ok":
        return None

    mid = _safe_float(j.get("mid"))
    bid = _safe_float(j.get("bid"))
    ask = _safe_float(j.get("ask"))
    last = _safe_float(j.get("last"))

    if mid is not None:
        return mid
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return last


def fetch_quotes_batch_chunked(
    base_url: str,
    symbols: List[str],
    chunk_size: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """
    WICHTIG: Chunking verhindert zu lange URLs / Provider Limits.
    Gibt dict: {symbol: {"iv":[...], "delta":[...], ...}}
    """
    out: Dict[str, Dict[str, Any]] = {}

    # dedupe + cap (Sicherheit)
    symbols = [s.strip() for s in symbols if s and isinstance(s, str)]
    seen = set()
    uniq = []
    for s in symbols:
        if s not in seen:
            uniq.append(s)
            seen.add(s)

    for i in range(0, len(uniq), chunk_size):
        chunk = uniq[i : i + chunk_size]
        try:
            r = requests.get(
                f"{base_url}/api/options/quotes_batch",
                params={"symbols": ",".join(chunk), "limit": len(chunk)},
                timeout=45,
            )
            r.raise_for_status()
            j = r.json()
            if j.get("s") != "ok":
                continue
            quotes = j.get("quotes") or {}
            if isinstance(quotes, dict):
                out.update(quotes)
        except Exception:
            # chunk überspringen (kein crash)
            continue

    return out


def pick_atm_from_quotes(
    spot: float,
    quotes: Dict[str, Dict[str, Any]],
) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """
    Simple MVP:
    - nimmt Option mit delta am nächsten zu 0.50 (Call-ATM Proxy)
    - gibt (symbol, iv, delta)
    """
    best_sym = None
    best_iv = None
    best_delta = None
    best_dist = None

    for sym, q in quotes.items():
        iv = _safe_float(_first(q.get("iv")))
        delta = _safe_float(_first(q.get("delta")))
        if iv is None or delta is None:
            continue

        dist = abs(delta - 0.5)
        if best_dist is None or dist < best_dist:
            best_dist = dist
            best_sym = sym
            best_iv = iv
            best_delta = delta

    return best_sym, best_iv, best_delta


# ----------------------------
# Public funcs used by main.py
# ----------------------------
def get_atm_iv_for_ticker(base_url: str, ticker: str, max_quotes: int = 80) -> Dict[str, Any]:
    """
    Returns dict with keys: spot, iv, delta, option_symbol, reason
    """
    t = ticker.strip().upper()
    try:
        spot = fetch_spot(base_url, t)
        if spot is None:
            return {"ticker": t, "spot": None, "iv": None, "delta": None, "option_symbol": None, "reason": "no_spot"}

        chain = fetch_chain_symbols(base_url, t)
        if not chain:
            return {"ticker": t, "spot": spot, "iv": None, "delta": None, "option_symbol": None, "reason": "no_chain"}

        # cap fürs MVP
        chain = chain[: max_quotes]

        quotes = fetch_quotes_batch_chunked(base_url, chain, chunk_size=20)
        if not quotes:
            return {"ticker": t, "spot": spot, "iv": None, "delta": None, "option_symbol": None, "reason": "no_quotes"}

        sym, iv, delta = pick_atm_from_quotes(spot, quotes)
        if iv is None:
            return {"ticker": t, "spot": spot, "iv": None, "delta": None, "option_symbol": None, "reason": "no_iv"}

        return {"ticker": t, "spot": spot, "iv": iv, "delta": delta, "option_symbol": sym, "reason": None}

    except Exception as e:
        return {"ticker": t, "spot": None, "iv": None, "delta": None, "option_symbol": None, "reason": f"err:{e}"}


def score_iv_gap(iv: Optional[float], rv: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """
    gap = iv - rv
    score = (iv/rv) - 1
    """
    if iv is None or rv is None or rv == 0:
        return None, None
    gap = iv - rv
    score = (iv / rv) - 1.0
    return gap, score


def compute_iv_rv_score(ticker: str, base_url: str = "http://127.0.0.1:8000") -> Optional[Dict[str, Any]]:
    """
    Minimal: iv/rv Ratio Ranking
    """
    t = ticker.strip().upper()

    atm = get_atm_iv_for_ticker(base_url, t, max_quotes=80)
    iv = _safe_float(atm.get("iv"))
    rv = get_latest_rv20(t)

    if iv is None or rv is None or rv == 0:
        return None

    return {
        "ticker": t,
        "spot": atm.get("spot"),
        "iv": iv,
        "rv20": rv,
        "iv_rv_ratio": iv / rv,
        "delta": atm.get("delta"),
        "option_symbol": atm.get("option_symbol"),
    }