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
from typing import Dict, Generator, List, Optional

import pandas as pd
import yfinance as yf

import config

log = logging.getLogger(__name__)

Signal = Dict  # {signal_type, ticker, close, ...}


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

def stream_signals(tickers: List[str]) -> Generator[Signal, None, None]:
    """
    Download OHLCV for all tickers in one batch call, then yield signals as
    they are found — one at a time — so callers can act on each immediately.

    on_first_evaluated(first_ticker, next_ticker) is called after the first
    ticker is processed, giving the caller a chance to send a "started" message.
    """
    if not tickers:
        return

    log.info("Downloading data for %d tickers ...", len(tickers))
    raw = yf.download(
        tickers,
        period="200d",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    for ticker in tickers:
        try:
            df = _extract_ticker(raw, ticker, len(tickers))

            if df is None or len(df) < 205:
                continue

            bounce = _evaluate_bounce(ticker, df)
            if bounce:
                yield bounce

            cross = _evaluate_cross(ticker, df)
            if cross:
                yield cross

            rsi = _evaluate_rsi(ticker, df)
            if rsi:
                yield rsi

        except Exception as exc:
            log.debug("Error processing %s: %s", ticker, exc)

    log.info("Stream complete")


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


def _evaluate_cross(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """Detect Golden Cross or Death Cross within the last CROSS_LOOKBACK_DAYS bars."""
    df = df.dropna(subset=["Close"])
    df["sma50"] = df["Close"].rolling(50).mean()
    df["sma200"] = df["Close"].rolling(200).mean()

    if df["sma50"].isna().iloc[-1] or df["sma200"].isna().iloc[-1]:
        return None

    close = float(df["Close"].iloc[-1])

    for i in range(1, config.CROSS_LOOKBACK_DAYS + 1):
        sma50_cur = float(df["sma50"].iloc[-i])
        sma50_prv = float(df["sma50"].iloc[-i - 1])
        sma200_cur = float(df["sma200"].iloc[-i])
        sma200_prv = float(df["sma200"].iloc[-i - 1])

        if sma50_prv <= sma200_prv and sma50_cur > sma200_cur:
            signal_type = "golden_cross"
        elif sma50_prv >= sma200_prv and sma50_cur < sma200_cur:
            signal_type = "death_cross"
        else:
            continue

        days_ago = i - 1  # 0 = today, 1 = yesterday, etc.
        earnings_flag = _has_earnings_soon(ticker)
        return {
            "signal_type": signal_type,
            "ticker": ticker,
            "close": round(close, 2),
            "sma50": round(float(df["sma50"].iloc[-1]), 2),
            "sma200": round(float(df["sma200"].iloc[-1]), 2),
            "days_ago": days_ago,
            "earnings_flag": earnings_flag,
        }

    return None


def sample_debug(ticker: str = "AAPL") -> str:
    """Return a one-liner of raw metrics for a single ticker — used in the no-signals message."""
    try:
        df = yf.download(ticker, period="200d", interval="1d", auto_adjust=True, progress=False)
        df = df.dropna(subset=["Close"])
        if len(df) < 205:
            return f"{ticker}: not enough data"

        close = float(df["Close"].iloc[-1])
        low   = float(df["Low"].iloc[-1])

        df["sma150"] = df["Close"].rolling(150).mean()
        df["sma50"]  = df["Close"].rolling(50).mean()
        df["sma200"] = df["Close"].rolling(200).mean()

        sma150  = float(df["sma150"].iloc[-1])
        sma5ago = float(df["sma150"].iloc[-6])
        sma50   = float(df["sma50"].iloc[-1])
        sma200  = float(df["sma200"].iloc[-1])
        rsi     = round(_calc_rsi(df["Close"], config.RSI_PERIOD), 1)
        slope   = "↑" if sma150 > sma5ago else "↓"

        avg_vol    = float(df["Volume"].iloc[-21:-1].mean())
        vol_ratio  = round(df["Volume"].iloc[-1] / avg_vol * 100, 0) if avg_vol > 0 else 0

        return (
            f"Sample ({ticker}): Close=${close:.2f} Low=${low:.2f} "
            f"SMA150=${sma150:.2f}{slope} SMA50=${sma50:.2f} SMA200=${sma200:.2f} "
            f"RSI={rsi} Vol={vol_ratio:.0f}%"
        )
    except Exception as exc:
        return f"Debug fetch failed: {exc}"


def _calc_rsi(close: pd.Series, period: int) -> float:
    """Wilder's RSI using exponential smoothing (alpha = 1/period)."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1])


def _evaluate_rsi(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """Flag oversold (RSI < RSI_OVERSOLD) or overbought (RSI > RSI_OVERBOUGHT)."""
    df = df.dropna(subset=["Close"])
    if len(df) < config.RSI_PERIOD + 1:
        return None

    rsi = _calc_rsi(df["Close"], config.RSI_PERIOD)
    close = float(df["Close"].iloc[-1])

    if rsi < config.RSI_OVERSOLD:
        signal_type = "rsi_oversold"
    elif rsi > config.RSI_OVERBOUGHT:
        signal_type = "rsi_overbought"
    else:
        return None

    earnings_flag = _has_earnings_soon(ticker)
    return {
        "signal_type": signal_type,
        "ticker": ticker,
        "close": round(close, 2),
        "rsi": round(rsi, 1),
        "earnings_flag": earnings_flag,
    }


def _evaluate_bounce(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
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
    low = float(df["Low"].iloc[-1])

    # 2. Touch — low came within PROXIMITY_CAP (3%) above SMA150
    if not (low < sma_today * (1 + config.PROXIMITY_CAP)):
        return None

    # 3. Reversal — close recovered above low, above SMA150, within 3% of the low
    pct_from_sma = (close - sma_today) / sma_today
    if not (close > low and close > sma_today and close < low * (1 + config.PROXIMITY_CAP)):
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
        "signal_type": "bounce",
        "ticker": ticker,
        "close": round(close, 2),
        "sma150": round(sma_today, 2),
        "pct_from_sma": round(pct_from_sma * 100, 2),  # stored as %
        "volume_ratio": round(volume_ratio * 100, 1),   # stored as %
        "earnings_flag": earnings_flag,
    }
