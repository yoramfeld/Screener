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

def _get_earnings_info(ticker: str, cache: Optional[dict] = None) -> dict:
    """Return {"days_away": int|None, "date_str": str|None} for nearest earnings.

    days_away > 0  — earnings are N calendar days in the future
    days_away <= 0 — earnings were N calendar days ago (or today)
    None           — no earnings data found

    Pass cache={} to share results across calls within the same scan run.
    """
    if cache is not None and ticker in cache:
        return cache[ticker]

    result: dict = {"days_away": None, "date_str": None}
    try:
        info = yf.Ticker(ticker).calendar
        if info is not None and not info.empty:
            if "Earnings Date" in info.columns:
                dates = info["Earnings Date"].dropna()
            elif "Earnings Date" in info.index:
                dates = pd.Series([info.loc["Earnings Date"]])
            else:
                dates = pd.Series([])

            now = datetime.now(tz=timezone.utc)
            closest_days: Optional[int] = None
            closest_ts = None
            for d in dates:
                try:
                    ts = pd.Timestamp(d)
                    ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
                    days = (ts - now).days
                    if closest_days is None or abs(days) < abs(closest_days):
                        closest_days = days
                        closest_ts   = ts
                except Exception:
                    continue

            if closest_ts is not None:
                result = {
                    "days_away": closest_days,
                    "date_str":  closest_ts.strftime("%b %d"),
                }
    except Exception:
        pass

    if cache is not None:
        cache[ticker] = result
    return result


def _has_earnings_soon(ticker: str) -> bool:
    """Return True if earnings are scheduled within the next 48 hours."""
    info = _get_earnings_info(ticker)
    days = info.get("days_away")
    return days is not None and 0 <= days <= 2


# ---------------------------------------------------------------------------
# Analyst recommendations
# ---------------------------------------------------------------------------

def _get_analyst_rec(ticker: str) -> dict:
    """Return {buy, hold, sell, target} or {} if data unavailable."""
    try:
        t = yf.Ticker(ticker)
        summary = t.recommendations_summary
        if summary is not None and not summary.empty and "0m" in summary["period"].values:
            row  = summary[summary["period"] == "0m"].iloc[0]
            buy  = int(row.get("strongBuy", 0)) + int(row.get("buy", 0))
            hold = int(row.get("hold", 0))
            sell = int(row.get("sell", 0)) + int(row.get("strongSell", 0))
        else:
            buy = hold = sell = 0
        targets = t.analyst_price_targets or {}
        target  = round(float(targets.get("mean", 0)), 0) or None
        if buy == 0 and hold == 0 and sell == 0:
            return {}
        return {"buy": buy, "hold": hold, "sell": sell, "target": target}
    except Exception:
        return {}


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
        period="1y",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    earnings_cache: dict = {}  # shared across tickers for the channel scanner

    for ticker in tickers:
        try:
            df = _extract_ticker(raw, ticker, len(tickers))

            if df is None or len(df) < 205:
                continue

            bounce = _evaluate_bounce(ticker, df)
            if bounce:
                yield bounce

            crossover = _evaluate_sma150_crossover(ticker, df)
            if crossover:
                yield crossover

            cross = _evaluate_cross(ticker, df)
            if cross:
                yield cross

            rsi = _evaluate_rsi(ticker, df)
            if rsi:
                yield rsi

            alignment = _evaluate_sma_alignment(ticker, df)
            if alignment:
                yield alignment

            pullback = _evaluate_high_pullback(ticker, df)
            if pullback:
                yield pullback

            atr = _evaluate_atr_trailing(ticker, df)
            if atr:
                yield atr

            channel = _evaluate_channel(ticker, df, earnings_cache)
            if channel:
                yield channel

        except Exception as exc:
            log.debug("Error processing %s: %s", ticker, exc)

    log.info("Stream complete")


def _extract_ticker(raw: pd.DataFrame, ticker: str, total: int) -> Optional[pd.DataFrame]:
    """Extract a single-ticker OHLCV DataFrame from a potentially multi-ticker download."""
    try:
        if total == 1:
            return raw.copy()
        if not isinstance(raw.columns, pd.MultiIndex):
            return raw.copy()
        if ticker in raw.columns.get_level_values(0):
            df = raw[ticker].copy()
        else:
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

        days_ago      = i - 1  # 0 = today, 1 = yesterday, etc.
        earnings_flag = _has_earnings_soon(ticker)
        analyst_rec   = _get_analyst_rec(ticker)
        return {
            "signal_type": signal_type,
            "ticker":      ticker,
            "close":       round(close, 2),
            "sma50":       round(float(df["sma50"].iloc[-1]), 2),
            "sma200":      round(float(df["sma200"].iloc[-1]), 2),
            "days_ago":    days_ago,
            "earnings_flag": earnings_flag,
            "analyst_rec": analyst_rec,
        }

    return None


def scan_above(tickers: List[str], top_n: int = 20) -> List[Signal]:
    """
    Return up to top_n stocks where Close > SMA150 and SMA150 is rising,
    sorted by % above SMA150 ascending (closest to the line first).
    """
    if not tickers:
        return []

    log.info("Downloading data for %d tickers (above scan)...", len(tickers))
    raw = yf.download(
        tickers,
        period="1y",
        interval="1d",
        auto_adjust=True,
        progress=False,
        group_by="ticker",
        threads=True,
    )

    results = []
    n_extracted = n_rising = n_above = 0
    for ticker in tickers:
        try:
            df = _extract_ticker(raw, ticker, len(tickers))
            if df is None or len(df) < 155:
                continue
            n_extracted += 1

            df = df.dropna(subset=["Close"])
            df["sma150"] = df["Close"].rolling(150).mean()
            if df["sma150"].isna().iloc[-1]:
                continue

            sma_today = float(df["sma150"].iloc[-1])
            sma_5ago  = float(df["sma150"].iloc[-6])
            close     = float(df["Close"].iloc[-1])

            if sma_today <= sma_5ago:
                continue
            n_rising += 1

            if close <= sma_today:
                continue
            n_above += 1

            pct_from_sma = (close - sma_today) / sma_today * 100
            results.append({
                "signal_type": "above",
                "ticker":       ticker,
                "close":        round(close, 2),
                "sma150":       round(sma_today, 2),
                "pct_from_sma": round(pct_from_sma, 2),
                "earnings_flag": False,
            })
        except Exception as exc:
            log.warning("Above scan error %s: %s", ticker, exc)

    log.info("Above scan: %d extracted, %d rising SMA, %d above SMA → %d results",
             n_extracted, n_rising, n_above, len(results))
    results.sort(key=lambda x: x["pct_from_sma"])
    return results[:top_n]


def sample_debug(ticker: str = "AAPL") -> str:
    """Return a one-liner of raw metrics for a single ticker — used in the no-signals message."""
    try:
        df = yf.download(ticker, period="1y", interval="1d", auto_adjust=True, progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df.dropna(subset=["Close"])
        if len(df) < 50:
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


def _calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range using Wilder's EMA."""
    prev_close = df["Close"].shift(1)
    tr = pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"]  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def _calc_atr_stop(close: pd.Series, atr: pd.Series, multiplier: float = 2.0) -> pd.Series:
    """
    Trailing stop = max(prev_stop, close - multiplier*ATR).
    Stop only moves up — lets winners ride while cutting losses.
    """
    stop = pd.Series(float("nan"), index=close.index)
    for i in range(len(close)):
        if pd.isna(atr.iloc[i]):
            continue
        candidate = float(close.iloc[i]) - multiplier * float(atr.iloc[i])
        if pd.isna(stop.iloc[i - 1]) if i > 0 else True:
            stop.iloc[i] = candidate
        else:
            stop.iloc[i] = max(float(stop.iloc[i - 1]), candidate)
    return stop


def _evaluate_atr_trailing(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """
    ATR Trailing Stop strategy:
      BUY  — price crosses above SMA20 today.
      STOP — price closes below the ATR trailing stop today.
    """
    df = df.dropna(subset=["Close", "High", "Low"])
    if len(df) < 25:
        return None

    df["sma20"] = df["Close"].rolling(20).mean()
    atr         = _calc_atr(df)
    df["atr"]   = atr
    df["stop"]  = _calc_atr_stop(df["Close"], atr)

    if df["sma20"].isna().iloc[-1] or df["stop"].isna().iloc[-1]:
        return None

    close      = float(df["Close"].iloc[-1])
    close_prev = float(df["Close"].iloc[-2])
    sma20      = float(df["sma20"].iloc[-1])
    sma20_prev = float(df["sma20"].iloc[-2])
    stop       = float(df["stop"].iloc[-1])
    atr_val    = float(df["atr"].iloc[-1])
    pct_from_stop = (close - stop) / close * 100

    # BUY: price crosses above SMA20 today
    if close_prev <= sma20_prev and close > sma20:
        earnings_flag = _has_earnings_soon(ticker)
        analyst_rec   = _get_analyst_rec(ticker)
        return {
            "signal_type":    "atr_buy",
            "ticker":         ticker,
            "close":          round(close, 2),
            "sma20":          round(sma20, 2),
            "atr":            round(atr_val, 2),
            "atr_stop":       round(stop, 2),
            "pct_from_stop":  round(pct_from_stop, 2),
            "earnings_flag":  earnings_flag,
            "analyst_rec":    analyst_rec,
        }

    # STOP HIT: price closes below trailing stop
    if close < stop:
        earnings_flag = _has_earnings_soon(ticker)
        analyst_rec   = _get_analyst_rec(ticker)
        return {
            "signal_type":    "atr_stop",
            "ticker":         ticker,
            "close":          round(close, 2),
            "sma20":          round(sma20, 2),
            "atr":            round(atr_val, 2),
            "atr_stop":       round(stop, 2),
            "pct_from_stop":  round(pct_from_stop, 2),
            "earnings_flag":  earnings_flag,
            "analyst_rec":    analyst_rec,
        }

    return None


def _calc_rsi_series(close: pd.Series, period: int) -> pd.Series:
    """Wilder's RSI as a full Series — needed for crossover detection."""
    delta    = close.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _calc_adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average Directional Index (ADX) — measures trend strength (not direction).
    ADX < 25 indicates a ranging / channel market; > 25 indicates a trend.
    """
    high  = df["High"]
    low   = df["Low"]
    close = df["Close"]

    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    up   = high - prev_high
    down = prev_low - low

    plus_dm  = pd.Series(0.0, index=df.index)
    minus_dm = pd.Series(0.0, index=df.index)
    plus_dm[  (up > down) & (up > 0)]   = up[  (up > down) & (up > 0)]
    minus_dm[(down > up)  & (down > 0)] = down[(down > up)  & (down > 0)]

    alpha = 1 / period
    s_tr  = tr.ewm(      alpha=alpha, min_periods=period, adjust=False).mean()
    s_pdm = plus_dm.ewm( alpha=alpha, min_periods=period, adjust=False).mean()
    s_ndm = minus_dm.ewm(alpha=alpha, min_periods=period, adjust=False).mean()

    plus_di  = 100 * s_pdm / s_tr
    minus_di = 100 * s_ndm / s_tr
    dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    return dx.ewm(alpha=alpha, min_periods=period, adjust=False).mean()


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
    analyst_rec   = _get_analyst_rec(ticker)
    return {
        "signal_type": signal_type,
        "ticker":      ticker,
        "close":       round(close, 2),
        "rsi":         round(rsi, 1),
        "earnings_flag": earnings_flag,
        "analyst_rec": analyst_rec,
    }


def _evaluate_sma_alignment(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """Bullish SMA alignment: SMA50 > SMA150 > SMA200 (Stage 2 uptrend)."""
    df = df.dropna(subset=["Close"])
    df["sma50"]  = df["Close"].rolling(50).mean()
    df["sma150"] = df["Close"].rolling(150).mean()
    df["sma200"] = df["Close"].rolling(200).mean()

    sma50  = float(df["sma50"].iloc[-1])
    sma150 = float(df["sma150"].iloc[-1])
    sma200 = float(df["sma200"].iloc[-1])

    if pd.isna(sma50) or pd.isna(sma150) or pd.isna(sma200):
        return None
    if not (sma50 > sma150 > sma200):
        return None

    close = float(df["Close"].iloc[-1])
    earnings_flag = _has_earnings_soon(ticker)
    analyst_rec   = _get_analyst_rec(ticker)
    return {
        "signal_type":  "sma_alignment",
        "ticker":       ticker,
        "close":        round(close, 2),
        "sma50":        round(sma50, 2),
        "sma150":       round(sma150, 2),
        "sma200":       round(sma200, 2),
        "earnings_flag": earnings_flag,
        "analyst_rec":  analyst_rec,
    }


def _evaluate_high_pullback(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """20%+ below 52-week high with a positive close today (beaten-down bounce)."""
    df = df.dropna(subset=["Close", "Open"])

    close  = float(df["Close"].iloc[-1])
    open_  = float(df["Open"].iloc[-1])
    high52 = float(df["High"].iloc[-252:].max()) if len(df) >= 252 else float(df["High"].max())

    pct_below = (high52 - close) / high52 * 100
    if pct_below < 20:
        return None
    if close <= open_:   # must close up on the day
        return None

    earnings_flag = _has_earnings_soon(ticker)
    analyst_rec   = _get_analyst_rec(ticker)
    return {
        "signal_type":  "high_pullback",
        "ticker":       ticker,
        "close":        round(close, 2),
        "high52":       round(high52, 2),
        "pct_below":    round(pct_below, 2),
        "earnings_flag": earnings_flag,
        "analyst_rec":  analyst_rec,
    }


def _evaluate_channel(ticker: str, df: pd.DataFrame, earnings_cache: Optional[dict] = None) -> Optional[Signal]:
    """
    Swing Channel Scanner.

    A stock qualifies as a channel stock if ALL of:
      - 20-day H/L ratio between 1.08 and 1.20 (meaningful but bounded range)
      - ADX(14) < 25 (no strong directional trend)
      - RSI(14) has oscillated between 35 and 65 over the last 20 bars

    BUY signal (all required):
      - Close within 3% above 20-day low (near channel floor)
      - RSI crossed above 35 today (was < 35 yesterday)
      - Volume > 1.5× 20-day average

    SELL signal (either condition):
      - Close within 3% below 20-day high (near channel ceiling)
      - RSI crossed below 65 today (was > 65 yesterday)

    Earnings blackout: signal suppressed within 5 days before / 3 days after earnings.
    Earnings warning:  flag shown if earnings are 6–10 days away.
    """
    df = df.dropna(subset=["Close", "High", "Low", "Volume"])
    if len(df) < 30:
        return None

    rsi_s = _calc_rsi_series(df["Close"], config.RSI_PERIOD)
    adx_s = _calc_adx(df)

    if rsi_s.isna().iloc[-1] or adx_s.isna().iloc[-1]:
        return None

    close    = float(df["Close"].iloc[-1])
    high20   = float(df["High"].iloc[-20:].max())
    low20    = float(df["Low"].iloc[-20:].min())
    rsi_cur  = float(rsi_s.iloc[-1])
    rsi_prev = float(rsi_s.iloc[-2])
    adx      = float(adx_s.iloc[-1])

    # --- Channel detection ---
    hl_ratio = high20 / low20 if low20 > 0 else 0
    if not (1.08 <= hl_ratio <= 1.20):
        return None
    if adx >= 25:
        return None
    rsi_20 = rsi_s.iloc[-20:]
    if rsi_20.max() < 35 or rsi_20.min() > 65:
        return None  # RSI not oscillating through channel range

    # --- Earnings blackout ---
    e_info   = _get_earnings_info(ticker, earnings_cache)
    days_out = e_info.get("days_away")
    if days_out is not None and -3 <= days_out <= 5:
        return None  # blackout window

    earnings_flag = days_out is not None and 5 < days_out <= 10  # warning: 6–10 days away

    avg_vol   = float(df["Volume"].iloc[-21:-1].mean())
    vol_ratio = float(df["Volume"].iloc[-1]) / avg_vol if avg_vol > 0 else 0

    pct_from_low  = (close - low20) / low20 * 100
    pct_from_high = (high20 - close) / high20 * 100
    hard_stop     = round(low20 * (1 - 0.025), 2)

    # --- BUY ---
    if pct_from_low <= 3 and rsi_prev < 35 and rsi_cur >= 35 and vol_ratio >= 1.5:
        analyst_rec = _get_analyst_rec(ticker)
        return {
            "signal_type":   "channel_buy",
            "ticker":        ticker,
            "close":         round(close, 2),
            "channel_low":   round(low20, 2),
            "channel_high":  round(high20, 2),
            "rsi":           round(rsi_cur, 1),
            "adx":           round(adx, 1),
            "pct_from_low":  round(pct_from_low, 2),
            "pct_from_high": round(pct_from_high, 2),
            "hard_stop":     hard_stop,
            "vol_ratio":     round(vol_ratio * 100, 1),
            "earnings_flag": earnings_flag,
            "earnings_days": days_out,
            "earnings_date": e_info.get("date_str", ""),
            "analyst_rec":   analyst_rec,
        }

    # --- SELL ---
    near_high  = pct_from_high <= 3
    rsi_x_down = rsi_prev > 65 and rsi_cur <= 65
    if near_high or rsi_x_down:
        analyst_rec = _get_analyst_rec(ticker)
        return {
            "signal_type":   "channel_sell",
            "ticker":        ticker,
            "close":         round(close, 2),
            "channel_low":   round(low20, 2),
            "channel_high":  round(high20, 2),
            "rsi":           round(rsi_cur, 1),
            "adx":           round(adx, 1),
            "pct_from_low":  round(pct_from_low, 2),
            "pct_from_high": round(pct_from_high, 2),
            "hard_stop":     hard_stop,
            "reason":        "near_high" if near_high else "rsi_cross",
            "earnings_flag": earnings_flag,
            "earnings_days": days_out,
            "earnings_date": e_info.get("date_str", ""),
            "analyst_rec":   analyst_rec,
        }

    return None


def scan_earnings_week(tickers: List[str]) -> List[dict]:
    """Return watchlist tickers with earnings in the next 7 days, sorted by date."""
    cache: dict = {}
    results = []
    for ticker in tickers:
        info = _get_earnings_info(ticker, cache)
        days = info.get("days_away")
        if days is not None and 0 <= days <= 7:
            results.append({
                "ticker":    ticker,
                "days_away": days,
                "date_str":  info["date_str"],
            })
    results.sort(key=lambda x: x["days_away"])
    return results


def scan_top_recommendations(tickers: List[str], top_n: int = 10, min_analysts: int = 5) -> List[dict]:
    """Return the top_n S&P 500 stocks by analyst buy consensus.

    Score = buy / (buy + hold + sell).  Only tickers with >= min_analysts
    total ratings are considered, to filter out noise from thin coverage.
    Results are sorted by score descending, then by raw buy count descending.
    """
    results = []
    for ticker in tickers:
        rec = _get_analyst_rec(ticker)
        if not rec:
            continue
        b, h, s = rec.get("buy", 0), rec.get("hold", 0), rec.get("sell", 0)
        total = b + h + s
        if total < min_analysts:
            continue
        score = b / total
        results.append({
            "ticker":  ticker,
            "buy":     b,
            "hold":    h,
            "sell":    s,
            "total":   total,
            "score":   round(score * 100, 1),   # % buy
            "target":  rec.get("target"),
        })

    results.sort(key=lambda x: (x["score"], x["buy"]), reverse=True)
    return results[:top_n]


def _evaluate_sma150_crossover(ticker: str, df: pd.DataFrame) -> Optional[Signal]:
    """Price crosses above SMA150 today (was below yesterday) with rising SMA and volume."""
    df = df.dropna(subset=["Close", "Volume"])

    df["sma150"] = df["Close"].rolling(150).mean()
    if df["sma150"].isna().iloc[-1] or df["sma150"].isna().iloc[-2]:
        return None

    sma_today = float(df["sma150"].iloc[-1])
    sma_5ago  = float(df["sma150"].iloc[-6])

    # SMA150 must be rising
    if sma_today <= sma_5ago:
        return None

    close_today = float(df["Close"].iloc[-1])
    close_prev  = float(df["Close"].iloc[-2])

    # Price crossed above SMA150 today
    if not (close_prev < sma_today and close_today > sma_today):
        return None

    # Not extended — close within 5% above SMA150
    pct_from_sma = (close_today - sma_today) / sma_today
    if pct_from_sma > 0.05:
        return None

    # Volume confirmation
    avg_vol   = float(df["Volume"].iloc[-21:-1].mean())
    today_vol = float(df["Volume"].iloc[-1])
    volume_ratio = today_vol / avg_vol if avg_vol > 0 else 0.0
    if volume_ratio < config.VOLUME_MIN_RATIO:
        return None

    earnings_flag = _has_earnings_soon(ticker)
    analyst_rec   = _get_analyst_rec(ticker)

    return {
        "signal_type":  "sma150_crossover",
        "ticker":       ticker,
        "close":        round(close_today, 2),
        "sma150":       round(sma_today, 2),
        "pct_from_sma": round(pct_from_sma * 100, 2),
        "volume_ratio": round(volume_ratio * 100, 1),
        "earnings_flag": earnings_flag,
        "analyst_rec":  analyst_rec,
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
    open_ = float(df["Open"].iloc[-1])
    low   = float(df["Low"].iloc[-1])

    # 2. Touch — low pulled back within 3% above SMA150
    if not (low < sma_today * (1 + config.PROXIMITY_CAP)):
        return None

    # 3. Bounce — bullish candle that closed above SMA150
    if not (close > open_ and close > sma_today):
        return None

    # 4. Not extended — close within 5% above SMA150
    pct_from_sma = (close - sma_today) / sma_today
    if pct_from_sma > 0.05:
        return None

    # 4. Volume filter — today vs 20-day average
    avg_vol = float(df["Volume"].iloc[-21:-1].mean())  # prior 20 days
    today_vol = float(df["Volume"].iloc[-1])
    volume_ratio = today_vol / avg_vol if avg_vol > 0 else 0.0
    if volume_ratio < config.VOLUME_MIN_RATIO:
        return None

    # 5. Earnings guard + analyst rec (individual API calls — only for qualifying tickers)
    earnings_flag = _has_earnings_soon(ticker)
    analyst_rec   = _get_analyst_rec(ticker)

    return {
        "signal_type":  "bounce",
        "ticker":       ticker,
        "close":        round(close, 2),
        "sma150":       round(sma_today, 2),
        "pct_from_sma": round(pct_from_sma * 100, 2),
        "volume_ratio": round(volume_ratio * 100, 1),
        "earnings_flag": earnings_flag,
        "analyst_rec":  analyst_rec,
    }
