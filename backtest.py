"""
Backtester — replays the screener signals across 3 years of history and
measures forward returns at 5, 10, and 20 trading days.

For each ticker × date where a signal would have fired:
  - Entry price  = Close on signal day
  - Forward return = (Close[D+N] - Close[D]) / Close[D] * 100
  - Dedup: same signal on same ticker suppressed for 20 days after firing

Results are aggregated per signal type and sent via Telegram.
"""

import logging
from typing import Dict, List

import pandas as pd
import yfinance as yf

import config
from screener import _extract_ticker, _calc_rsi

log = logging.getLogger(__name__)

HOLD_DAYS    = [5, 10, 20]
SIGNAL_TYPES = ["bounce", "golden_cross", "death_cross", "rsi_oversold", "rsi_overbought", "sma_alignment", "high_pullback"]
DEDUP_DAYS   = 20   # suppress repeat signal on same ticker for this many bars


def run(tickers: List[str], years: int = 3) -> Dict:
    """Download history and return aggregated backtest stats."""
    log.info("Backtesting %d tickers over %d years...", len(tickers), years)
    raw = yf.download(
        tickers,
        period=f"{years}y",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    all_trades: Dict[str, List[dict]] = {st: [] for st in SIGNAL_TYPES}

    for ticker in tickers:
        try:
            df = _extract_ticker(raw, ticker, len(tickers))
            if df is None or len(df) < 225:   # 205 history + 20 forward
                continue
            df = df.dropna(subset=["Close", "Open", "Low", "Volume"]).reset_index(drop=True)
            if len(df) < 225:
                continue
            _scan_ticker(df, ticker, all_trades)
        except Exception as exc:
            log.debug("Backtest error %s: %s", ticker, exc)

    log.info("Backtest complete — aggregating results")
    return _aggregate(all_trades)


def _scan_ticker(df: pd.DataFrame, ticker: str, all_trades: Dict[str, List[dict]]) -> None:
    # Pre-compute indicators
    close  = df["Close"]
    df["sma150"] = close.rolling(150).mean()
    df["sma50"]  = close.rolling(50).mean()
    df["sma200"] = close.rolling(200).mean()
    df["rsi"]    = _calc_rsi_series(close, config.RSI_PERIOD)
    df["vol20"]  = df["Volume"].rolling(20).mean()

    last_fired: Dict[str, int] = {}  # signal_type → last bar index

    # Start at 205 so all indicators are valid; stop 20 bars from end for forward returns
    for i in range(205, len(df) - DEDUP_DAYS):
        for signal_type in SIGNAL_TYPES:
            # Dedup check
            if i - last_fired.get(signal_type, -999) < DEDUP_DAYS:
                continue

            fired = False
            if signal_type == "bounce":
                fired = _check_bounce(df, i)
            elif signal_type in ("golden_cross", "death_cross"):
                fired = _check_cross(df, i, signal_type)
            elif signal_type == "rsi_oversold":
                fired = _check_rsi(df, i, "oversold")
            elif signal_type == "rsi_overbought":
                fired = _check_rsi(df, i, "overbought")
            elif signal_type == "sma_alignment":
                fired = _check_sma_alignment(df, i)
            elif signal_type == "high_pullback":
                fired = _check_high_pullback(df, i)

            if not fired:
                continue

            entry = float(df["Close"].iloc[i])
            trade = {"ticker": ticker, "entry": entry}
            for h in HOLD_DAYS:
                idx = min(i + h, len(df) - 1)
                exit_price = float(df["Close"].iloc[idx])
                trade[f"ret_{h}d"] = round((exit_price - entry) / entry * 100, 2)

            all_trades[signal_type].append(trade)
            last_fired[signal_type] = i


def _check_bounce(df: pd.DataFrame, i: int) -> bool:
    sma   = df["sma150"].iloc[i]
    sma5  = df["sma150"].iloc[i - 5]
    if pd.isna(sma) or sma <= sma5:          # slope must be rising
        return False
    close = df["Close"].iloc[i]
    open_ = df["Open"].iloc[i]
    low   = df["Low"].iloc[i]
    if low >= sma * (1 + config.PROXIMITY_CAP):
        return False
    if not (close > open_ and close > sma):  # bullish close above SMA
        return False
    if (close - sma) / sma > 0.05:           # not too extended
        return False
    vol20 = df["vol20"].iloc[i]
    if vol20 > 0 and df["Volume"].iloc[i] / vol20 < config.VOLUME_MIN_RATIO:
        return False
    return True


def _check_cross(df: pd.DataFrame, i: int, signal_type: str) -> bool:
    sma50_cur  = df["sma50"].iloc[i]
    sma50_prv  = df["sma50"].iloc[i - 1]
    sma200_cur = df["sma200"].iloc[i]
    sma200_prv = df["sma200"].iloc[i - 1]
    if pd.isna(sma50_cur) or pd.isna(sma200_cur):
        return False
    if signal_type == "golden_cross":
        return sma50_prv <= sma200_prv and sma50_cur > sma200_cur
    else:
        return sma50_prv >= sma200_prv and sma50_cur < sma200_cur


def _check_sma_alignment(df: pd.DataFrame, i: int) -> bool:
    sma50  = df["sma50"].iloc[i]
    sma150 = df["sma150"].iloc[i]
    sma200 = df["sma200"].iloc[i]
    if pd.isna(sma50) or pd.isna(sma150) or pd.isna(sma200):
        return False
    return sma50 > sma150 > sma200


def _check_high_pullback(df: pd.DataFrame, i: int) -> bool:
    close  = df["Close"].iloc[i]
    open_  = df["Open"].iloc[i]
    start  = max(0, i - 252)
    high52 = df["High"].iloc[start:i + 1].max()
    if pd.isna(high52) or high52 == 0:
        return False
    pct_below = (high52 - close) / high52 * 100
    return pct_below >= 20 and close > open_


def _check_rsi(df: pd.DataFrame, i: int, direction: str) -> bool:
    rsi = df["rsi"].iloc[i]
    if pd.isna(rsi):
        return False
    if direction == "oversold":
        return rsi < config.RSI_OVERSOLD
    return rsi > config.RSI_OVERBOUGHT


def _calc_rsi_series(close: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI as a full Series (not just the last value)."""
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _aggregate(all_trades: Dict[str, List[dict]]) -> Dict:
    stats = {}
    for signal_type, trades in all_trades.items():
        if not trades:
            stats[signal_type] = {"count": 0}
            continue
        entry = {}
        for h in HOLD_DAYS:
            rets = [t[f"ret_{h}d"] for t in trades]
            wins = sum(1 for r in rets if r > 0)
            entry[f"win_rate_{h}d"] = round(wins / len(rets) * 100, 1)
            entry[f"avg_ret_{h}d"]  = round(sum(rets) / len(rets), 2)
        all_rets = [t[f"ret_{HOLD_DAYS[1]}d"] for t in trades]  # rank by 10d
        best  = max(trades, key=lambda t: t[f"ret_{HOLD_DAYS[1]}d"])
        worst = min(trades, key=lambda t: t[f"ret_{HOLD_DAYS[1]}d"])
        stats[signal_type] = {
            "count": len(trades),
            "best":  {"ticker": best["ticker"],  "ret": best[f"ret_{HOLD_DAYS[1]}d"]},
            "worst": {"ticker": worst["ticker"], "ret": worst[f"ret_{HOLD_DAYS[1]}d"]},
            **entry,
        }
    return stats
