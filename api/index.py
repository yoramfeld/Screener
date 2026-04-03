"""
Vercel serverless webhook — receives Telegram bot commands via Flask.

Supported commands:
  /run  — triggers the GitHub Actions screener workflow immediately

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

app = Flask(__name__)

_GITHUB_DISPATCH_URL = (
    "https://api.github.com/repos/yoramfeld/Screener/actions/workflows/screener.yml/dispatches"
)
_TELEGRAM_URL = "https://api.telegram.org/bot{token}/sendMessage"


def _send_message(text: str) -> None:
    requests.post(
        _TELEGRAM_URL.format(token=config.TELEGRAM_BOT_TOKEN),
        json={"chat_id": config.TELEGRAM_CHAT_ID, "text": text},
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
    text = body.get("message", {}).get("text", "").strip().lower()

    if text == "/run":
        _send_message("Running screener... you'll get the results in a moment.")
        if not _trigger_screener():
            _send_message("Failed to trigger the screener. Check GITHUB_PAT in Vercel env vars.")
    elif text == "/start":
        _send_message("Bot is active. Send /run to trigger the screener.")
    else:
        _send_message(f"Unknown command: {text}\nAvailable: /run")

    return "OK", 200
