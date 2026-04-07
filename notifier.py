"""
Telegram notification dispatcher.

Formats all signals into a single message and sends it via the Bot API.
A "no signals" summary is sent when the screener finds nothing, so you
always know the job ran successfully.
"""

import logging
from typing import List

import requests

import config
from screener import Signal
from portfolio import Position

log = logging.getLogger(__name__)

_TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Exchange prefix map for TradingView deep links
_TV_EXCHANGE = {
    # Extend as needed — defaults to NASDAQ for unknowns
}


def _tradingview_url(ticker: str) -> str:
    exchange = _TV_EXCHANGE.get(ticker, "NASDAQ")
    return f"https://www.tradingview.com/chart/?symbol={exchange}:{ticker}"


def _analyst_line(sig: Signal) -> str:
    rec = sig.get("analyst_rec", {})
    if not rec:
        return ""
    parts = []
    if rec.get("buy"):  parts.append(f"{rec['buy']} Buy")
    if rec.get("hold"): parts.append(f"{rec['hold']} Hold")
    if rec.get("sell"): parts.append(f"{rec['sell']} Sell")
    target_str = f"  |  Target: ${rec['target']:.0f}" if rec.get("target") else ""
    return f"  📊 {' · '.join(parts)}{target_str}\n"


def _format_signal(sig: Signal) -> str:
    earnings_line = "  ⚠️ EARNINGS within 48h — HIGH RISK\n" if sig["earnings_flag"] else ""
    analyst_line  = _analyst_line(sig)
    chart = f"  [Chart]({_tradingview_url(sig['ticker'])})"

    if sig["signal_type"] in ("golden_cross", "death_cross"):
        days_ago = sig.get("days_ago", 0)
        when = "today" if days_ago == 0 else f"{days_ago}d ago"
        emoji, label = ("🟡", f"Golden Cross (BUY) — {when}") if sig["signal_type"] == "golden_cross" else ("💀", f"Death Cross (SELL) — {when}")
        return (
            f"{emoji} *{sig['ticker']}* — {label}\n"
            f"  Price: ${sig['close']}  |  SMA50: ${sig['sma50']}  |  SMA200: ${sig['sma200']}\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "rsi_oversold":
        return (
            f"📉 *{sig['ticker']}* — RSI Oversold (BUY)\n"
            f"  Price: ${sig['close']}  |  RSI: {sig['rsi']} (< {config.RSI_OVERSOLD})\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "rsi_overbought":
        return (
            f"🔴 *{sig['ticker']}* — RSI Overbought (SELL)\n"
            f"  Price: ${sig['close']}  |  RSI: {sig['rsi']} (> {config.RSI_OVERBOUGHT})\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "atr_buy":
        return (
            f"🟩 *{sig['ticker']}* — ATR Trailing Stop: BUY\n"
            f"  Price: ${sig['close']}  crossed above SMA20: ${sig['sma20']}\n"
            f"  ATR: ${sig['atr']}  |  Stop: ${sig['atr_stop']} ({sig['pct_from_stop']:.1f}% below)\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "atr_stop":
        return (
            f"🟥 *{sig['ticker']}* — ATR Trailing Stop: EXIT\n"
            f"  Price: ${sig['close']}  below stop: ${sig['atr_stop']}\n"
            f"  ATR: ${sig['atr']}  |  SMA20: ${sig['sma20']}\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "sma_alignment":
        return (
            f"🔼 *{sig['ticker']}* — Bullish SMA Alignment\n"
            f"  Price: ${sig['close']}  |  SMA50: ${sig['sma50']}  >  SMA150: ${sig['sma150']}  >  SMA200: ${sig['sma200']}\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "high_pullback":
        return (
            f"🎯 *{sig['ticker']}* — {sig['pct_below']}% Below 52-Week High\n"
            f"  Price: ${sig['close']}  |  52w High: ${sig['high52']}  |  Positive close today\n"
            f"{earnings_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "channel_buy":
        days = sig.get("earnings_days")
        e_line = (
            f"  ⚠️ Earnings in {days}d ({sig.get('earnings_date', '')}) — avoid if short-term\n"
            if sig.get("earnings_flag") and days else earnings_line
        )
        return (
            f"🟦 *{sig['ticker']}* — Swing Channel: BUY\n"
            f"  Price: ${sig['close']}  |  Channel: ${sig['channel_low']} – ${sig['channel_high']}\n"
            f"  RSI: {sig['rsi']} (crossed ↑35)  |  ADX: {sig['adx']}  |  Vol: {sig['vol_ratio']}% of avg\n"
            f"  Entry: ~${sig['close']}  |  Stop: ${sig['hard_stop']}  |  Target: ${sig['channel_high']}\n"
            f"  {sig['pct_from_low']:.1f}% from floor  |  {sig['pct_from_high']:.1f}% from ceiling\n"
            f"{e_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "channel_sell":
        reason = "near channel high" if sig.get("reason") == "near_high" else "RSI crossed ↓65"
        days = sig.get("earnings_days")
        e_line = (
            f"  ⚠️ Earnings in {days}d ({sig.get('earnings_date', '')}) — avoid if short-term\n"
            if sig.get("earnings_flag") and days else earnings_line
        )
        return (
            f"🟫 *{sig['ticker']}* — Swing Channel: SELL ({reason})\n"
            f"  Price: ${sig['close']}  |  Channel: ${sig['channel_low']} – ${sig['channel_high']}\n"
            f"  RSI: {sig['rsi']}  |  ADX: {sig['adx']}\n"
            f"  {sig['pct_from_low']:.1f}% from floor  |  {sig['pct_from_high']:.1f}% from ceiling\n"
            f"{e_line}"
            f"{analyst_line}"
            f"{chart}"
        )
    # bounce
    return (
        f"📈 *{sig['ticker']}* — SMA150 Bounce\n"
        f"  Price: ${sig['close']}  |  SMA150: ${sig['sma150']}↑  "
        f"(+{sig['pct_from_sma']}%)\n"
        f"  Volume: {sig['volume_ratio']}% of avg\n"
        f"{earnings_line}"
        f"{analyst_line}"
        f"{chart}"
    )


def send_started(total: int) -> None:
    """Send the 'downloading' message before the batch data fetch begins."""
    _post(f"⏳ Downloading stocks data for {total} stocks... it takes a minute.")


def send_signal(sig: Signal) -> None:
    """Send a single signal immediately as it's found."""
    _post(_format_signal(sig))


def _post(text: str) -> None:
    """Raw Telegram send."""
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        log.error("Telegram credentials not configured")
        return
    url = _TELEGRAM_URL.format(token=config.TELEGRAM_BOT_TOKEN)
    try:
        resp = requests.post(
            url,
            json={"chat_id": config.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown", "disable_web_page_preview": False},
            timeout=10,
        )
        if not resp.ok:
            log.error("Telegram send failed: %s — %s", resp.status_code, resp.text)
    except Exception as exc:
        log.error("Telegram send error: %s", exc)


def _build_message(
    signals: List[Signal],
    aborted: bool = False,
    total_screened: int = 0,
    sample_tickers: List[str] = [],
    debug: str = "",
) -> str:
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    sample = ", ".join(sample_tickers) if sample_tickers else "n/a"
    screened_line = f"_Screened {total_screened} stocks (e.g. {sample}, ...)_\n"

    if aborted:
        return (
            f"🛑 *Swing Screener* — {now}\n"
            f"{screened_line}"
            f"Run aborted: SPY is in a sharp intraday sell-off. No alerts sent."
        )

    if not signals:
        debug_line = f"\n_{debug}_" if debug else ""
        return (
            f"✅ *Swing Screener* — {now}\n"
            f"{screened_line}"
            f"No setups found (bounce, cross or RSI)."
            f"{debug_line}"
        )

    header = f"🔔 *Swing Screener* — {now}\n{screened_line}{len(signals)} setup(s) found:\n\n"
    body = "\n\n".join(_format_signal(s) for s in signals)
    return header + body


def send_summary(signals: List[Signal], aborted: bool = False, total_screened: int = 0, sample_tickers: List[str] = [], debug: str = "") -> None:
    """Send the end-of-run summary (abort notice or 'no signals found')."""
    _post(_build_message(signals, aborted=aborted, total_screened=total_screened, sample_tickers=sample_tickers, debug=debug))


def send_pnl(trades: list) -> None:
    """Send closed trade history with per-trade and overall P&L."""
    if not trades:
        _post("📒 *P&L History* — no closed trades yet.")
        return

    lines = []
    for t in trades:
        emoji       = "🟢" if t["pct_pnl"] >= 0 else "🔴"
        sign        = "+" if t["pct_pnl"] >= 0 else ""
        dollar_sign = "+" if t["dollar_pnl"] >= 0 else ""
        qty_str     = f"{t['quantity']:g} shares  " if t.get("quantity") else ""
        lines.append(
            f"{emoji} *{t['ticker']}*  {qty_str}\n"
            f"  Buy: ${t['buy_price']} ({t['buy_date']})  →  "
            f"Sell: ${t['sell_price']} ({t['sell_date']})\n"
            f"  Profit: {sign}{t['pct_pnl']}%  ({dollar_sign}${t['dollar_pnl']:,.2f})"
        )

    total_dollars  = sum(t["dollar_pnl"] for t in trades)
    total_cost     = sum(t["buy_price"] * t["quantity"] for t in trades if t.get("quantity"))
    weighted_pct   = (total_dollars / total_cost * 100) if total_cost else 0
    winners        = sum(1 for t in trades if t["pct_pnl"] >= 0)
    sign           = "+" if total_dollars >= 0 else ""
    pct_sign       = "+" if weighted_pct >= 0 else ""

    summary = (
        f"\n📊 *Total: {sign}${total_dollars:,.2f} ({pct_sign}{weighted_pct:.1f}%)* "
        f"across {len(trades)} trade(s) ({winners}W / {len(trades) - winners}L)"
    )

    _post("📒 *P&L History*\n\n" + "\n\n".join(lines) + summary)


def send_portfolio(positions: List[Position]) -> None:
    """Send current stop levels for all open positions."""
    if not positions:
        _post("📋 *Portfolio* — no open positions.")
        return

    lines = []
    for p in sorted(positions, key=lambda x: x["pct_change"], reverse=True):
        arrow      = "🟢" if p["pct_change"] >= 0 else "🔴"
        sign       = "+" if p["pct_change"] >= 0 else ""
        stop_pnl   = (p["stop"] - p["buy_price"]) / p["buy_price"] * 100
        stop_sign  = "+" if stop_pnl >= 0 else ""
        warning    = "  ⚠️ STOP HIT" if p["stop_hit"] else ""
        sma_arrow  = "↑" if p.get("sma150_rising") else "↓"
        lines.append(
            f"{arrow} *{p['ticker']}* — entry ${p['buy_price']}  ({p['buy_date']})\n"
            f"  Now: ${p['current']} ({sign}{p['pct_change']}%)  "
            f"|  SMA150: ${p['sma150']}{sma_arrow}  "
            f"|  Stop: ${p['stop']} ({stop_sign}{stop_pnl:.1f}%){warning}"
        )

    total_value    = sum(p["current"] * p["quantity"] for p in positions)
    total_cost     = sum(p["buy_price"] * p["quantity"] for p in positions)
    total_dollar   = total_value - total_cost
    total_pct      = (total_dollar / total_cost * 100) if total_cost else 0
    total_sign     = "+" if total_dollar >= 0 else ""
    summary = (
        f"\n\n💼 *Total:* ${total_value:,.0f}  |  "
        f"{total_sign}${total_dollar:,.0f} ({total_sign}{total_pct:.1f}%)"
    )

    _post("📋 *Portfolio — Stop Levels*\n\n" + "\n\n".join(lines) + summary)


def send_above(matches: list) -> None:
    """Send stocks trading above their rising SMA150, sorted closest-first."""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    if not matches:
        _post(f"📶 *Above SMA150* — {now}\nNo stocks found above a rising SMA150.")
        return

    lines = []
    for m in matches:
        sign  = "+" if m["pct_from_sma"] >= 0 else ""
        chart = f"[Chart]({_tradingview_url(m['ticker'])})"
        lines.append(
            f"📶 *{m['ticker']}* — ${m['close']}  |  SMA150: ${m['sma150']}↑ ({sign}{m['pct_from_sma']}%)  {chart}"
        )

    header = f"📶 *Above SMA150* — {now}\n_{len(matches)} stocks, sorted by proximity_\n\n"
    _post(header + "\n".join(lines))


def send_earnings_week(matches: list) -> None:
    """Send Sunday earnings calendar — tickers with earnings in the next 7 days."""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    if not matches:
        _post(f"📅 *Earnings This Week* — {now}\nNo earnings in the next 7 days for watchlist stocks.")
        return

    # Group by date
    by_day: dict = {}
    for m in matches:
        key = (m["days_away"], m["date_str"])
        by_day.setdefault(key, []).append(m["ticker"])

    lines = []
    for (days, date_str), tickers in sorted(by_day.items()):
        lines.append(f"*{date_str}* (+{days}d): {', '.join(tickers)}")

    _post(f"📅 *Earnings This Week* — {now}\n\n" + "\n".join(lines))


def send_backtest(stats: dict, years: int = 3) -> None:
    """Send backtest results summary."""
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    _LABELS = {
        "bounce":        ("📈", "SMA150 Bounce"),
        "golden_cross":  ("🟡", "Golden Cross"),
        "death_cross":   ("💀", "Death Cross"),
        "rsi_oversold":  ("📉", "RSI Oversold"),
        "rsi_overbought":("🔴", "RSI Overbought"),
        "sma_alignment": ("🔼", "Bullish SMA Alignment"),
        "high_pullback": ("🎯", "52-Week High Pullback"),
        "atr_buy":       ("🟩", "ATR Trailing Stop — Buy"),
        "atr_stop":      ("🟥", "ATR Trailing Stop — Exit"),
    }

    lines = []
    for signal_type, (emoji, label) in _LABELS.items():
        s = stats.get(signal_type, {})
        if not s or s.get("count", 0) == 0:
            lines.append(f"{emoji} *{label}* — no signals found")
            continue
        sign = lambda r: "+" if r >= 0 else ""
        lines.append(
            f"{emoji} *{label}* — {s['count']} signals\n"
            f"  Win rate:  5d {s['win_rate_5d']}%  ·  10d {s['win_rate_10d']}%  ·  20d {s['win_rate_20d']}%\n"
            f"  Avg return: 5d {sign(s['avg_ret_5d'])}{s['avg_ret_5d']}%  ·  "
            f"10d {sign(s['avg_ret_10d'])}{s['avg_ret_10d']}%  ·  "
            f"20d {sign(s['avg_ret_20d'])}{s['avg_ret_20d']}%\n"
            f"  Best: *{s['best']['ticker']}* {sign(s['best']['ret'])}{s['best']['ret']}%  "
            f"|  Worst: *{s['worst']['ticker']}* {sign(s['worst']['ret'])}{s['worst']['ret']}%"
        )

    # --- Conclusion: rank by composite score = win_rate_20d × avg_ret_20d ---
    scored = []
    for signal_type, (emoji, label) in _LABELS.items():
        s = stats.get(signal_type, {})
        if s and s.get("count", 0) >= 50:   # ignore signals with too few samples
            score = s["win_rate_20d"] * s["avg_ret_20d"]
            scored.append((score, emoji, label, s))
    scored.sort(reverse=True)

    medals = ["🥇", "🥈", "🥉"]
    conclusion_lines = []
    for idx, (score, emoji, label, s) in enumerate(scored[:3]):
        sign = lambda r: "+" if r >= 0 else ""
        conclusion_lines.append(
            f"{medals[idx]} *{label}*\n"
            f"  Win rate 20d: {s['win_rate_20d']}%  |  Avg return 20d: {sign(s['avg_ret_20d'])}{s['avg_ret_20d']}%  |  Score: {score:.1f}"
        )

    conclusion = "\n\n🏆 *Top 3 Signals*\n\n" + "\n\n".join(conclusion_lines) if conclusion_lines else ""

    header = f"📊 *Backtest — {years} Years (as of {now})*\n\n"
    _post(header + "\n\n".join(lines) + conclusion)
