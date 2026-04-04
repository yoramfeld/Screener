"""
Vercel serverless webhook — receives Telegram bot commands via Flask.

Commands:
  /run               — trigger screener
  /buy AAPL 182.40 50 — record a buy
  /sell AAPL 185.20  — sell all shares
  /sell AAPL 185.20 30 — partial sell
  /portfolio         — open positions & stop levels
  /pnl               — closed trades & total profit
  /market            — is the market open?
  /help or /?        — command list

Heavy work (screener, portfolio, pnl) is delegated to GitHub Actions.
Vercel only handles fast command routing.
"""

import os
import re
import sys
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
from flask import Flask, abort, request

import config


def _check_buy(parts: list) -> str:
    """Return an error string if /buy args are invalid, else empty string."""
    if len(parts) != 4:
        return "Usage: `/buy AAPL 182.40 50`"
    _, ticker, price_str, qty_str = parts
    if not re.fullmatch(r"[A-Za-z]{1,5}", ticker):
        return f"❌ `{ticker}` doesn't look like a valid ticker (1–5 letters)"
    try:
        price = float(price_str)
    except ValueError:
        return f"❌ `{price_str}` is not a valid price"
    try:
        qty = float(qty_str)
    except ValueError:
        return f"❌ `{qty_str}` is not a valid quantity"
    if price <= 0:
        return "❌ Price must be greater than 0"
    if price > 100_000:
        return f"❌ Price ${price:,.2f} looks too high — double-check"
    if qty <= 0:
        return "❌ Quantity must be greater than 0"
    if qty > 1_000_000:
        return f"❌ Quantity {qty:,} looks too high — double-check"
    return ""


def _check_sell(parts: list, held_qty: float = None) -> str:
    """Return an error string if /sell args are invalid, else empty string."""
    if len(parts) < 3 or len(parts) > 4:
        return "Usage: `/sell AAPL 185.20` or `/sell AAPL 185.20 30`"
    _, ticker, price_str = parts[:3]
    if not re.fullmatch(r"[A-Za-z]{1,5}", ticker):
        return f"❌ `{ticker}` doesn't look like a valid ticker (1–5 letters)"
    try:
        price = float(price_str)
    except ValueError:
        return f"❌ `{price_str}` is not a valid price"
    if price <= 0:
        return "❌ Price must be greater than 0"
    if price > 100_000:
        return f"❌ Price ${price:,.2f} looks too high — double-check"
    if len(parts) == 4:
        try:
            qty = float(parts[3])
        except ValueError:
            return f"❌ `{parts[3]}` is not a valid quantity"
        if qty <= 0:
            return "❌ Quantity must be greater than 0"
        if held_qty is not None and qty > held_qty:
            return f"❌ You only hold {held_qty:g} shares — can't sell {qty:g}"
    return ""

app = Flask(__name__)

_GITHUB_DISPATCH_URL = (
    "https://api.github.com/repos/yoramfeld/Screener/actions/workflows/screener.yml/dispatches"
)
_TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"

_ET = ZoneInfo("America/New_York")

# NYSE holidays 2026
_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
}


def _send_message(text: str) -> None:
    requests.post(
        _TELEGRAM_URL.format(token=config.TELEGRAM_BOT_TOKEN),
        json={"chat_id": config.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=5,
    )


def _trigger(run_type: str = "screen") -> bool:
    pat = os.environ.get("GITHUB_PAT", "")
    if not pat:
        return False
    resp = requests.post(
        _GITHUB_DISPATCH_URL,
        headers={"Authorization": f"Bearer {pat}", "Accept": "application/vnd.github+json"},
        json={"ref": "main", "inputs": {"run_type": run_type}},
        timeout=10,
    )
    return resp.status_code == 204


def _market_status() -> str:
    from datetime import datetime, time
    now = datetime.now(tz=_ET)
    date_str = now.strftime("%Y-%m-%d")
    if now.weekday() >= 5:
        return "🔴 Market is *CLOSED* (weekend)"
    if date_str in _HOLIDAYS_2026:
        return "🔴 Market is *CLOSED* (holiday)"
    t = now.time()
    if t < time(9, 30):
        return f"🌅 Market opens at 9:30 AM ET (now {now.strftime('%H:%M')} ET)"
    if t <= time(16, 0):
        return f"🟢 Market is *OPEN* — closes at 4:00 PM ET (now {now.strftime('%H:%M')} ET)"
    return f"🌆 Market is *CLOSED* — after hours (now {now.strftime('%H:%M')} ET)"


@app.route("/api/index", methods=["POST"])
def webhook():
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if secret and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != secret:
        abort(403)

    body    = request.get_json(silent=True) or {}
    message = body.get("message", {})
    chat_id = str(message.get("chat", {}).get("id", ""))

    # Reject anyone who isn't the authorized user — silent drop, no reply
    if chat_id != config.TELEGRAM_CHAT_ID:
        return "OK", 200

    text  = message.get("text", "").strip()
    parts = text.split()
    cmd   = parts[0].lower() if parts else ""

    if cmd == "/run":
        if _trigger("screen"):
            _send_message("⏳ Downloading stocks data... it takes a minute.")
        else:
            _send_message("Failed to trigger. Check GITHUB_PAT in Vercel env vars.")

    elif cmd == "/buy":
        err = _check_buy(parts)
        if err:
            _send_message(err)
        else:
            import portfolio
            ticker   = parts[1].upper()
            price    = float(parts[2])
            quantity = float(parts[3])
            portfolio.add_position(ticker, price, quantity)
            _send_message(
                f"✅ Recorded: *{ticker}* {quantity:g} shares @ ${price:.2f} "
                f"(cost ${price * quantity:,.2f})\nStop tracked with each screener run."
            )

    elif cmd == "/sell":
        import portfolio
        # Look up held quantity for validation before committing
        ticker = parts[1].upper() if len(parts) >= 2 else ""
        held   = next((p["quantity"] for p in portfolio.get_positions() if p["ticker"] == ticker), None)
        if held is None and ticker:
            _send_message(f"❌ *{ticker}* not found in your portfolio.")
        else:
            err = _check_sell(parts, held_qty=held)
            if err:
                _send_message(err)
            else:
                sell_price = float(parts[2])
                quantity   = float(parts[3]) if len(parts) == 4 else None
                trade = portfolio.close_position(ticker, sell_price, quantity)
                if trade:
                    emoji       = "🟢" if trade["pct_pnl"] >= 0 else "🔴"
                    sign        = "+" if trade["pct_pnl"] >= 0 else ""
                    dollar_sign = "+" if trade["dollar_pnl"] >= 0 else ""
                    remaining   = trade.get("remaining", 0)
                    remain_line = f"\n  Remaining: {remaining:g} shares still open" if remaining > 0 else ""
                    _send_message(
                        f"{emoji} *{ticker}* — sold {trade['quantity']:g} shares\n"
                        f"  Buy: ${trade['buy_price']} ({trade['buy_date']})\n"
                        f"  Sell: ${trade['sell_price']} ({trade['sell_date']})\n"
                        f"  P&L: {sign}{trade['pct_pnl']}% ({dollar_sign}${trade['dollar_pnl']:,.2f})"
                        f"{remain_line}"
                    )

    elif cmd == "/portfolio":
        if _trigger("portfolio"):
            _send_message("📋 Fetching portfolio... one moment.")
        else:
            _send_message("Failed to trigger. Check GITHUB_PAT in Vercel env vars.")

    elif cmd == "/pnl":
        if _trigger("pnl"):
            _send_message("📒 Fetching P&L history... one moment.")
        else:
            _send_message("Failed to trigger. Check GITHUB_PAT in Vercel env vars.")

    elif cmd == "/market":
        _send_message(_market_status())

    elif cmd in ("/start", "/help", "/?"):
        _send_message(
            "📖 *Commands*\n"
            "`/run` — scan all stocks now\n"
            "`/buy AAPL 182.40 50` — record a buy\n"
            "`/sell AAPL 185.20` — sell all shares\n"
            "`/sell AAPL 185.20 30` — partial sell\n"
            "`/portfolio` — open positions & stop levels\n"
            "`/pnl` — closed trades & total profit\n"
            "`/market` — is the market open?\n"
            "`/help` or `/?` — show this list"
        )

    else:
        _send_message(
            "Unknown command. Try `/help` for the full list."
        )

    return "OK", 200
