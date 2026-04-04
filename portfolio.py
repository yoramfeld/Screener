"""
Portfolio tracker — stores open positions and computes trailing stop levels.

Data is stored in Upstash Redis (Vercel KV) via the REST API so it persists
across Vercel function invocations and GitHub Actions runs.

Keys:
  positions  — JSON dict  {ticker: {buy_price, quantity, buy_date}}
  trades     — JSON list  [{ticker, buy_price, quantity, buy_date,
                            sell_price, sell_date, pct_pnl, dollar_pnl}]
              (newest first)

Stop level = SMA150 * (1 - STOP_BELOW_SMA) — trails upward as SMA150 rises.
"""

import json
import logging
import os
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import requests
import yfinance as yf

log = logging.getLogger(__name__)

STOP_BELOW_SMA = 0.02  # stop placed 2% below SMA150

Position = Dict  # {ticker, buy_price, quantity, buy_date, current, pct_change, sma150, stop}
Trade    = Dict  # {ticker, buy_price, quantity, buy_date, sell_price, sell_date, pct_pnl, dollar_pnl}


# ---------------------------------------------------------------------------
# KV helpers
# ---------------------------------------------------------------------------

def _kv_get(key: str):
    """Return parsed JSON value for key, or None if key is missing."""
    url   = os.environ.get("KV_REST_API_URL", "")
    token = os.environ.get("KV_REST_API_TOKEN", "")
    if not url or not token:
        log.error("KV credentials not set (KV_REST_API_URL / KV_REST_API_TOKEN)")
        return None
    try:
        resp   = requests.post(url, headers={"Authorization": f"Bearer {token}"},
                               json=["GET", key], timeout=5)
        result = resp.json().get("result")
        return json.loads(result) if result is not None else None
    except Exception as exc:
        log.error("KV GET %s failed: %s", key, exc)
        return None


def _kv_set(key: str, value) -> None:
    """Serialize value to JSON and store at key."""
    url   = os.environ.get("KV_REST_API_URL", "")
    token = os.environ.get("KV_REST_API_TOKEN", "")
    if not url or not token:
        log.error("KV credentials not set")
        return
    try:
        requests.post(url, headers={"Authorization": f"Bearer {token}"},
                      json=["SET", key, json.dumps(value)], timeout=5)
    except Exception as exc:
        log.error("KV SET %s failed: %s", key, exc)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def _load_positions() -> dict:
    """Return {ticker: {buy_price, quantity, buy_date}} from KV."""
    return _kv_get("positions") or {}


def _save_positions(positions: dict) -> None:
    _kv_set("positions", positions)


def _load_trades() -> list:
    """Return list of trade dicts from KV (newest first)."""
    return _kv_get("trades") or []


def _save_trades(trades: list) -> None:
    _kv_set("trades", trades)


def delete_position(ticker: str) -> bool:
    """Remove a position without recording a trade. Returns True if it existed."""
    ticker    = ticker.upper()
    positions = _load_positions()
    if ticker not in positions:
        return False
    del positions[ticker]
    _save_positions(positions)
    log.info("Position deleted: %s", ticker)
    return True


def add_position(ticker: str, buy_price: float, quantity: float) -> None:
    ticker    = ticker.upper()
    positions = _load_positions()
    positions[ticker] = {
        "buy_price": buy_price,
        "quantity":  quantity,
        "buy_date":  date.today().isoformat(),
    }
    _save_positions(positions)
    log.info("Position added: %s %g shares @ $%.2f", ticker, quantity, buy_price)


def close_position(ticker: str, sell_price: float, quantity: Optional[float] = None) -> Optional[Trade]:
    """
    Record a sell. If quantity is None, sells all shares.
    For partial sells, reduces the position and keeps the remainder open.
    Returns the trade dict or None if position not found.
    """
    ticker    = ticker.upper()
    positions = _load_positions()
    if ticker not in positions:
        return None

    pos       = positions[ticker]
    buy_price = pos["buy_price"]
    held_qty  = pos["quantity"]
    buy_date  = pos["buy_date"]

    sell_qty   = quantity if quantity is not None else held_qty
    sell_qty   = min(sell_qty, held_qty)
    pct_pnl    = (sell_price - buy_price) / buy_price * 100
    dollar_pnl = (sell_price - buy_price) * sell_qty
    sell_date  = date.today().isoformat()
    remaining  = held_qty - sell_qty

    trade = {
        "ticker":     ticker,
        "buy_price":  buy_price,
        "quantity":   sell_qty,
        "buy_date":   buy_date,
        "sell_price": sell_price,
        "sell_date":  sell_date,
        "pct_pnl":    round(pct_pnl, 2),
        "dollar_pnl": round(dollar_pnl, 2),
        "remaining":  remaining if remaining > 0 else 0,
    }

    trades = _load_trades()
    trades.insert(0, {k: v for k, v in trade.items() if k != "remaining"})
    _save_trades(trades)

    if remaining > 0:
        positions[ticker]["quantity"] = remaining
    else:
        del positions[ticker]
    _save_positions(positions)

    log.info("Sold %g %s @ $%.2f (%.2f%% / $%.2f), %g remaining",
             sell_qty, ticker, sell_price, pct_pnl, dollar_pnl, remaining)
    return trade


def get_trades() -> List[Trade]:
    return _load_trades()


def get_positions() -> List[dict]:
    positions = _load_positions()
    return [
        {"ticker": ticker, **data}
        for ticker, data in sorted(positions.items(), key=lambda x: x[1]["buy_date"])
    ]


# ---------------------------------------------------------------------------
# Live enrichment
# ---------------------------------------------------------------------------

def enrich_positions() -> List[Position]:
    """Fetch current price and SMA150 for all open positions."""
    positions = get_positions()
    if not positions:
        return []

    tickers = [p["ticker"] for p in positions]
    try:
        raw = yf.download(
            tickers,
            period="1y",
            interval="1d",
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            threads=True,
        )
    except Exception as exc:
        log.error("Portfolio data fetch failed: %s", exc)
        return []

    enriched = []
    for pos in positions:
        try:
            ticker = pos["ticker"]
            if isinstance(raw.columns, pd.MultiIndex):
                if len(tickers) == 1:
                    df = raw[ticker].copy()   # single ticker: ticker is top level
                else:
                    df = raw.xs(ticker, axis=1, level=1).copy()  # multi: field=0, ticker=1
            else:
                df = raw.copy()

            df = df.dropna(subset=["Close"])
            if len(df) < 150:
                continue

            df["sma150"] = df["Close"].rolling(150).mean()
            current    = float(df["Close"].iloc[-1])
            sma150     = float(df["sma150"].iloc[-1])
            stop       = sma150 * (1 - STOP_BELOW_SMA)
            pct_chg    = (current - pos["buy_price"]) / pos["buy_price"] * 100
            dollar_chg = (current - pos["buy_price"]) * pos["quantity"]

            enriched.append({
                **pos,
                "current":       round(current, 2),
                "pct_change":    round(pct_chg, 2),
                "dollar_change": round(dollar_chg, 2),
                "sma150":        round(sma150, 2),
                "stop":          round(stop, 2),
                "stop_hit":      current < stop,
            })
        except Exception as exc:
            log.warning("Could not enrich %s: %s", pos["ticker"], exc)

    return enriched
