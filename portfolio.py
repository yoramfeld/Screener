"""
Portfolio tracker — stores open positions and computes trailing stop levels.

Positions table:
  ticker    — stock symbol
  buy_price — entry price
  quantity  — number of shares
  buy_date  — date of entry (YYYY-MM-DD)

Stop level = SMA150 * (1 - STOP_BELOW_SMA) — trails upward as SMA150 rises.
Total P&L uses dollar-weighted return: sum(dollar_pnl) / sum(cost_basis).
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

import config

log = logging.getLogger(__name__)

STOP_BELOW_SMA = 0.02  # stop placed 2% below SMA150

_CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    ticker     TEXT PRIMARY KEY,
    buy_price  REAL NOT NULL,
    quantity   REAL NOT NULL DEFAULT 0,
    buy_date   TEXT NOT NULL
);
"""

_CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT    NOT NULL,
    buy_price   REAL    NOT NULL,
    quantity    REAL    NOT NULL DEFAULT 0,
    buy_date    TEXT    NOT NULL,
    sell_price  REAL    NOT NULL,
    sell_date   TEXT    NOT NULL,
    pct_pnl     REAL    NOT NULL,
    dollar_pnl  REAL    NOT NULL DEFAULT 0
);
"""

Position = Dict  # {ticker, buy_price, quantity, buy_date, current, pct_change, sma150, stop}
Trade    = Dict  # {ticker, buy_price, quantity, buy_date, sell_price, sell_date, pct_pnl, dollar_pnl}


@contextmanager
def _conn():
    con = sqlite3.connect(config.DB_PATH)
    try:
        con.execute(_CREATE_POSITIONS)
        con.execute(_CREATE_TRADES)
        # Migrate existing tables that predate quantity column
        for table in ("positions", "trades"):
            cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
            if "quantity" not in cols:
                con.execute(f"ALTER TABLE {table} ADD COLUMN quantity REAL NOT NULL DEFAULT 0")
        if "dollar_pnl" not in [r[1] for r in con.execute("PRAGMA table_info(trades)").fetchall()]:
            con.execute("ALTER TABLE trades ADD COLUMN dollar_pnl REAL NOT NULL DEFAULT 0")
        con.commit()
        yield con
    finally:
        con.close()


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------

def add_position(ticker: str, buy_price: float, quantity: float) -> None:
    ticker = ticker.upper()
    with _conn() as con:
        con.execute(
            "INSERT OR REPLACE INTO positions (ticker, buy_price, quantity, buy_date) VALUES (?, ?, ?, ?)",
            (ticker, buy_price, quantity, date.today().isoformat()),
        )
        con.commit()
    log.info("Position added: %s %g shares @ $%.2f", ticker, quantity, buy_price)


def close_position(ticker: str, sell_price: float) -> Optional[Trade]:
    """Record the sell, compute P&L, remove from positions. Returns the trade or None if not found."""
    ticker = ticker.upper()
    with _conn() as con:
        row = con.execute(
            "SELECT buy_price, quantity, buy_date FROM positions WHERE ticker=?", (ticker,)
        ).fetchone()
        if not row:
            return None
        buy_price, quantity, buy_date = row
        pct_pnl    = (sell_price - buy_price) / buy_price * 100
        dollar_pnl = (sell_price - buy_price) * quantity
        sell_date  = date.today().isoformat()
        con.execute(
            "INSERT INTO trades (ticker, buy_price, quantity, buy_date, sell_price, sell_date, pct_pnl, dollar_pnl) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ticker, buy_price, quantity, buy_date, sell_price, sell_date,
             round(pct_pnl, 2), round(dollar_pnl, 2)),
        )
        con.execute("DELETE FROM positions WHERE ticker=?", (ticker,))
        con.commit()
    log.info("Position closed: %s @ $%.2f (%.2f%% / $%.2f)", ticker, sell_price, pct_pnl, dollar_pnl)
    return {
        "ticker":     ticker,
        "buy_price":  buy_price,
        "quantity":   quantity,
        "buy_date":   buy_date,
        "sell_price": sell_price,
        "sell_date":  sell_date,
        "pct_pnl":    round(pct_pnl, 2),
        "dollar_pnl": round(dollar_pnl, 2),
    }


def get_trades() -> List[Trade]:
    with _conn() as con:
        rows = con.execute(
            "SELECT ticker, buy_price, quantity, buy_date, sell_price, sell_date, pct_pnl, dollar_pnl "
            "FROM trades ORDER BY sell_date DESC"
        ).fetchall()
    return [
        {"ticker": r[0], "buy_price": r[1], "quantity": r[2], "buy_date": r[3],
         "sell_price": r[4], "sell_date": r[5], "pct_pnl": r[6], "dollar_pnl": r[7]}
        for r in rows
    ]


def get_positions() -> List[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT ticker, buy_price, quantity, buy_date FROM positions ORDER BY buy_date"
        ).fetchall()
    return [{"ticker": r[0], "buy_price": r[1], "quantity": r[2], "buy_date": r[3]} for r in rows]


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
            period="200d",
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
            if len(tickers) == 1:
                df = raw.copy()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
            else:
                df = raw.xs(ticker, axis=1, level=1).copy()

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
                "current":     round(current, 2),
                "pct_change":  round(pct_chg, 2),
                "dollar_change": round(dollar_chg, 2),
                "sma150":      round(sma150, 2),
                "stop":        round(stop, 2),
                "stop_hit":    current < stop,
            })
        except Exception as exc:
            log.warning("Could not enrich %s: %s", pos["ticker"], exc)

    return enriched
