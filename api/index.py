"""
Vercel serverless webhook — receives Telegram bot commands via Flask.

Supported commands:
  /run               — triggers the GitHub Actions screener workflow immediately
  /buy AAPL 182.40   — record a position at entry price
  /sell AAPL         — remove a position
  /portfolio         — show all open positions with current stop levels

SETUP (one-time):
  1. Deploy to Vercel:
       vercel deploy --prod
     Note the URL, e.g. https://screener-xyz.vercel.app

  2. Add these in Vercel dashboard → Project → Settings → Environment Variables:
       TELEGRAM_BOT_TOKEN  — your bot token
       TELEGRAM_CHAT_ID    — your chat id
       GITHUB_PAT          — GitHub PAT with 'actions' scope
       WEBHOOK_SECRET      — any random string you choose

  3. Register the webhook with Telegram (open in browser):
       https://api.telegram.org/bot<TOKEN>/setWebhook
         ?url=https://screener-xyz.vercel.app/api/index
         &secret_token=<WEBHOOK_SECRET>
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
from flask import Flask, abort, request

import config
import notifier
import portfolio

app = Flask(__name__)

_GITHUB_DISPATCH_URL = (
    "https://api.github.com/repos/yoramfeld/Screener/actions/workflows/screener.yml/dispatches"
)
_TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"


def _send_message(text: str) -> None:
    requests.post(
        _TELEGRAM_URL.format(token=config.TELEGRAM_BOT_TOKEN),
        json={"chat_id": config.TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=5,
    )


def _trigger_screener() -> bool:
    pat = os.environ.get("GITHUB_PAT", "")
    if not pat:
        return False
    resp = requests.post(
        _GITHUB_DISPATCH_URL,
        headers={
            "Authorization": f"Bearer {pat}",
            "Accept": "application/vnd.github+json",
        },
        json={"ref": "main"},
        timeout=10,
    )
    return resp.status_code == 204


@app.route("/api/index", methods=["POST"])
def webhook():
    # Verify request came from Telegram
    secret = os.environ.get("WEBHOOK_SECRET", "")
    if secret and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != secret:
        abort(403)

    body = request.get_json(silent=True) or {}
    text = body.get("message", {}).get("text", "").strip()
    parts = text.split()
    cmd = parts[0].lower() if parts else ""

    if cmd == "/run":
        if _trigger_screener():
            _send_message("⏳ Downloading stocks data... it takes a minute.")
        else:
            _send_message("Failed to trigger the screener. Check GITHUB_PAT in Vercel env vars.")

    elif cmd == "/buy":
        if len(parts) != 4:
            _send_message("Usage: `/buy AAPL 182.40 50`")
        else:
            try:
                ticker   = parts[1].upper()
                price    = float(parts[2])
                quantity = float(parts[3])
                portfolio.add_position(ticker, price, quantity)
                cost = price * quantity
                _send_message(
                    f"✅ Recorded: *{ticker}* {quantity:g} shares @ ${price:.2f} "
                    f"(cost ${cost:,.2f})\nStop will be tracked with each screener run."
                )
            except ValueError:
                _send_message("Invalid input. Usage: `/buy AAPL 182.40 50`")

    elif cmd == "/sell":
        if len(parts) != 3:
            _send_message("Usage: `/sell AAPL 185.20`")
        else:
            try:
                ticker     = parts[1].upper()
                sell_price = float(parts[2])
                trade = portfolio.close_position(ticker, sell_price)
                if trade:
                    emoji     = "🟢" if trade["pct_pnl"] >= 0 else "🔴"
                    sign      = "+" if trade["pct_pnl"] >= 0 else ""
                    dollar_sign = "+" if trade["dollar_pnl"] >= 0 else ""
                    _send_message(
                        f"{emoji} *{ticker}* closed\n"
                        f"  Buy: ${trade['buy_price']} × {trade['quantity']:g} shares ({trade['buy_date']})\n"
                        f"  Sell: ${trade['sell_price']} ({trade['sell_date']})\n"
                        f"  P&L: {sign}{trade['pct_pnl']}% ({dollar_sign}${trade['dollar_pnl']:,.2f})"
                    )
                else:
                    _send_message(f"*{ticker}* not found in portfolio.")
            except ValueError:
                _send_message("Invalid price. Usage: `/sell AAPL 185.20`")

    elif cmd == "/pnl":
        notifier.send_pnl(portfolio.get_trades())

    elif cmd == "/portfolio":
        positions = portfolio.enrich_positions()
        notifier.send_portfolio(positions)

    elif cmd == "/start":
        _send_message(
            "Bot is active. Commands:\n"
            "`/run` — scan now\n"
            "`/buy AAPL 182.40 50` — record a buy (ticker, price, shares)\n"
            "`/sell AAPL 185.20` — record a sell + P&L\n"
            "`/portfolio` — open positions & stop levels\n"
            "`/pnl` — closed trades & total profit"
        )

    else:
        _send_message(
            "Unknown command. Available:\n"
            "`/run`  `/buy TICKER PRICE`  `/sell TICKER PRICE`  `/portfolio`  `/pnl`"
        )

    return "OK", 200
