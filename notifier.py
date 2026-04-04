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


def _format_signal(sig: Signal) -> str:
    earnings_line = "  ⚠️ EARNINGS within 48h — HIGH RISK\n" if sig["earnings_flag"] else ""
    chart = f"  [Chart]({_tradingview_url(sig['ticker'])})"

    if sig["signal_type"] in ("golden_cross", "death_cross"):
        days_ago = sig.get("days_ago", 0)
        when = "today" if days_ago == 0 else f"{days_ago}d ago"
        emoji, label = ("🟡", f"Golden Cross (BUY) — {when}") if sig["signal_type"] == "golden_cross" else ("💀", f"Death Cross (SELL) — {when}")
        return (
            f"{emoji} *{sig['ticker']}* — {label}\n"
            f"  Price: ${sig['close']}  |  SMA50: ${sig['sma50']}  |  SMA200: ${sig['sma200']}\n"
            f"{earnings_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "rsi_oversold":
        return (
            f"📉 *{sig['ticker']}* — RSI Oversold (BUY)\n"
            f"  Price: ${sig['close']}  |  RSI: {sig['rsi']} (< {config.RSI_OVERSOLD})\n"
            f"{earnings_line}"
            f"{chart}"
        )
    if sig["signal_type"] == "rsi_overbought":
        return (
            f"🔴 *{sig['ticker']}* — RSI Overbought (SELL)\n"
            f"  Price: ${sig['close']}  |  RSI: {sig['rsi']} (> {config.RSI_OVERBOUGHT})\n"
            f"{earnings_line}"
            f"{chart}"
        )
    # bounce
    return (
        f"📈 *{sig['ticker']}* — SMA150 Bounce\n"
        f"  Price: ${sig['close']}  |  SMA150: ${sig['sma150']}  "
        f"(+{sig['pct_from_sma']}%)\n"
        f"  Volume: {sig['volume_ratio']}% of avg\n"
        f"{earnings_line}"
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
    for p in positions:
        arrow      = "🟢" if p["pct_change"] >= 0 else "🔴"
        sign       = "+" if p["pct_change"] >= 0 else ""
        stop_pnl   = (p["stop"] - p["buy_price"]) / p["buy_price"] * 100
        stop_sign  = "+" if stop_pnl >= 0 else ""
        warning    = "  ⚠️ STOP HIT" if p["stop_hit"] else ""
        lines.append(
            f"{arrow} *{p['ticker']}* — entry ${p['buy_price']}  ({p['buy_date']})\n"
            f"  Now: ${p['current']} ({sign}{p['pct_change']}%)  "
            f"|  SMA150: ${p['sma150']}  "
            f"|  Stop: ${p['stop']} ({stop_sign}{stop_pnl:.1f}%){warning}"
        )

    _post("📋 *Portfolio — Stop Levels*\n\n" + "\n\n".join(lines))
