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
    return (
        f"📈 *{sig['ticker']}* — SMA150 Bounce\n"
        f"  Price: ${sig['close']}  |  SMA150: ${sig['sma150']}  "
        f"(+{sig['pct_from_sma']}%)\n"
        f"  Volume: {sig['volume_ratio']}% of avg\n"
        f"{earnings_line}"
        f"  [Chart]({_tradingview_url(sig['ticker'])})"
    )


def _build_message(
    signals: List[Signal],
    aborted: bool = False,
    total_screened: int = 0,
    sample_tickers: List[str] = [],
) -> str:
    from datetime import datetime, timezone
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    screened_line = ""
    if total_screened:
        sample = ", ".join(sample_tickers) if sample_tickers else ""
        screened_line = f"_Screened {total_screened} stocks (e.g. {sample}, …)_\n"

    if aborted:
        return (
            f"🛑 *Swing Screener* — {now}\n"
            f"{screened_line}"
            f"Run aborted: SPY is in a sharp intraday sell-off. No alerts sent."
        )

    if not signals:
        return (
            f"✅ *Swing Screener* — {now}\n"
            f"{screened_line}"
            f"No SMA150 bounce setups found today."
        )

    header = f"🔔 *Swing Screener* — {now}\n{screened_line}{len(signals)} setup(s) found:\n\n"
    body = "\n\n".join(_format_signal(s) for s in signals)
    return header + body


def send(signals: List[Signal], aborted: bool = False, total_screened: int = 0, sample_tickers: List[str] = []) -> bool:
    """Send a Telegram message. Returns True on success."""
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        log.error("Telegram credentials not configured — skipping notification")
        return False

    message = _build_message(signals, aborted=aborted, total_screened=total_screened, sample_tickers=sample_tickers)
    url = _TELEGRAM_URL.format(token=config.TELEGRAM_BOT_TOKEN)
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.ok:
            log.info("Telegram message sent (%d signal(s))", len(signals))
            return True
        log.error("Telegram send failed: %s — %s", resp.status_code, resp.text)
        return False
    except Exception as exc:
        log.error("Telegram send error: %s", exc)
        return False
