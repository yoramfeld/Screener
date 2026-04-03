"""
Core screening engine.

Pipeline (applied to every ticker in the universe):
  1. Fetch 200 days of OHLCV data (batched single yfinance call).
  2. SMA150 slope — today's SMA150 must be higher than 5 trading days ago.
  3. Bounce trigger — today's Low touched below SMA150 AND Close > Open (bullish candle).
  4. Proximity cap — Close must be within PROXIMITY_CAP (default 3%) above SMA150.
     Avoids alerting on stocks that are already extended.
  5. Volume filter — today's volume >= VOLUME_MIN_RATIO of the 20-day average.
  6. Earnings guard — flag ticker if an earnings date falls within the next 48 hours.

Market context check (runs before scanning the universe):
  - If SPY's intraday return < SPY_DROP_THRESHOLD (default -2%), abort the entire run.

Returns a list of Signal dicts — one per qualifying ticker.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd
import yfinance as yf

import config

log = logging.getLogger(__name__)

Signal = Dict  # {ticker, close, sma150, pct_from_sma, volume_ratio, earnings_flag}


# ---------------------------------------------------------------------------
# Market context
# ---------------------------------------------------------------------------

def market_is_healthy() -> bool:
    """Return False if SPY is down more than SPY_DROP_THRESHOLD intraday."""
    try:
        spy = yf.download("SPY", period="2d", interval="1d", auto_adjust=True, progress=False)
        if len(spy) < 2:
            log.warning("Not enough SPY data — assuming market is healthy")
            return True
        prev_close = float(spy["Close"].iloc[-2])
        today_close = float(spy["Close"].iloc[-1])
        intraday_return = (today_close - prev_close) / prev_close
        log.info("SPY intraday return: %.2f%%", intraday_return * 100)
        if intraday_return < config.SPY_DROP_THRESHOLD:
            log.warning(
                "Market context check FAILED: SPY %.2f%% < threshold %.2f%%",
                intraday_return * 100,
                config.SPY_DROP_THRESHOLD * 100,
            )
            return False
        return True
    except Exception as exc:
        log.warning("SPY check error: %s — assuming healthy", exc)
        return True


# ---------------------------------------------------------------------------
# Earnings guard
# ---------------------------------------------------------------------------

def _has_earnings_soon(ticker: str) -> bool:
    """Return True if earnings are scheduled within the next 48 hours."""
    try:
        info = yf.Ticker(ticker).calendar
        if info is None or info.empty:
            return False
        # calendar index contains 'Earnings Date' as a column or row
        if "Earnings Date" in info.columns:
            dates = info["Earnings Date"].dropna()
        elif "Earnings Date" in info.index:
            dates = pd.Series([info.loc["Earnings Date"]])
        else:
            return False
        now = datetime.now(tz=timezone.utc)
        cutoff = now + timedelta(hours=48)
        for d in dates:
            try:
                d_aware = pd.Timestamp(d).tz_localize("UTC") if pd.Timestamp(d).tzinfo is None else pd.Timestamp(d).tz_convert("UTC")
                if now <= d_aware <= cutoff:
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------

def screen(tickers: List[str]) -> List[Signal]:
    """
    Download OHLCV for all tickers in one batch call, then apply filters.
    Returns qualifying signals sorted by pct_from_sma ascending (closest to SMA first).
    """
    if not tickers:
        return []

    log.info("Downloading data for %d tickers …", len(tickers))
    raw = yf.download(
        tickers,
        period="200d",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    signals: List[Signal] = []

    for ticker in tickers:
        try:
            df = _extract_ticker(raw, ticker, len(tickers))
            if df is None or len(df) < 155:
                # Need at least 150 bars for SMA + 5 for slope
                continue

            sig = _evaluate(ticker, df)
            if sig:
                signals.append(sig)
        except Exception as exc:
            log.debug("Error processing %s: %s", ticker, exc)

    signals.sort(key=lambda s: s["pct_from_sma"])
    log.info("Screen complete — %d signal(s) found", len(signals))
    return signals


def _extract_ticker(raw: pd.DataFrame, ticker: str, total: int) -> Optional[pd.DataFrame]:
    """Extract a single-ticker OHLCV DataFrame from a potentially multi-ticker download."""
    try:
        if total == 1:
            # Single ticker — raw IS the df
            return raw.copy()
        # Multi-ticker — raw has a MultiIndex on columns: (field, ticker)
        df = raw.xs(ticker, axis=1, level=1).copy()
        return df if not df.empty else None
    except KeyError:
        return None


def _evaluate(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """Apply all filters to a single ticker's DataFrame. Return Signal or None."""
    df = df.dropna(subset=["Close", "Open", "Low", "Volume"])

    # --- SMA150 ---
    df["sma150"] = df["Close"].rolling(150).mean()
    if df["sma150"].isna().iloc[-1]:
        return None

    sma_today = float(df["sma150"].iloc[-1])
    sma_5ago = float(df["sma150"].iloc[-6])  # 5 trading days back

    # 1. Slope check — SMA must be rising
    if sma_today <= sma_5ago:
        return None

    close = float(df["Close"].iloc[-1])
    open_ = float(df["Open"].iloc[-1])
    low = float(df["Low"].iloc[-1])

    # 2. Bounce trigger — low tagged SMA, candle closed bullish
    if not (low < sma_today and close > open_):
        return None

    # 3. Proximity cap — close not more than PROXIMITY_CAP above SMA
    pct_from_sma = (close - sma_today) / sma_today
    if pct_from_sma > config.PROXIMITY_CAP or pct_from_sma < 0:
        # Negative means closed below SMA — we want touches that recovered above
        return None

    # 4. Volume filter — today vs 20-day average
    avg_vol = float(df["Volume"].iloc[-21:-1].mean())  # prior 20 days
    today_vol = float(df["Volume"].iloc[-1])
    volume_ratio = today_vol / avg_vol if avg_vol > 0 else 0.0
    if volume_ratio < config.VOLUME_MIN_RATIO:
        return None

    # 5. Earnings guard (individual API call — only for qualifying tickers)
    earnings_flag = _has_earnings_soon(ticker)

    return {
        "ticker": ticker,
        "close": round(close, 2),
        "sma150": round(sma_today, 2),
        "pct_from_sma": round(pct_from_sma * 100, 2),  # stored as %
        "volume_ratio": round(volume_ratio * 100, 1),   # stored as %
        "earnings_flag": earnings_flag,
    }
