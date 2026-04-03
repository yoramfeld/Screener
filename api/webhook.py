"""
Vercel serverless webhook — receives Telegram bot commands.

Supported commands:
  /run  — triggers the GitHub Actions screener workflow immediately

SETUP (one-time):
  1. Deploy this to Vercel:
       vercel deploy --prod
     Note the deployment URL, e.g. https://screener-xyz.vercel.app

  2. Register the webhook with Telegram (run once in a browser or curl):
       https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook
         ?url=https://screener-xyz.vercel.app/api/webhook
         &secret_token=<WEBHOOK_SECRET>

  3. Add these to Vercel environment variables:
       TELEGRAM_BOT_TOKEN  — your bot token
       TELEGRAM_CHAT_ID    — your chat id
       GITHUB_PAT          — GitHub personal access token (needs 'actions' scope)
       WEBHOOK_SECRET      — any random string you choose (used to verify requests)

  4. Add GITHUB_PAT to GitHub repo secrets too (for workflow_dispatch auth).
"""

import json
import logging
import os
import sys
from http.server import BaseHTTPRequestHandler

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import config

log = logging.getLogger(__name__)

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
    """Call GitHub Actions workflow_dispatch API."""
    pat = os.environ.get("GITHUB_PAT", "")
    if not pat:
        log.error("GITHUB_PAT not set")
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
    return resp.status_code == 204  # GitHub returns 204 No Content on success


def _handle_update(body: dict) -> None:
    message = body.get("message", {})
    text = message.get("text", "").strip().lower()

    if text == "/run":
        _send_message("Running screener... you'll get the results in a moment.")
        success = _trigger_screener()
        if not success:
            _send_message("Failed to trigger the screener. Check GITHUB_PAT in Vercel env vars.")
    elif text == "/start":
        _send_message("Bot is active. Send /run to trigger the screener.")
    else:
        _send_message(f"Unknown command: {text}\nAvailable: /run")


class handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        # Verify the request came from Telegram using the secret token header
        secret = os.environ.get("WEBHOOK_SECRET", "")
        if secret and self.headers.get("X-Telegram-Bot-Api-Secret-Token") != secret:
            self.send_response(403)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")

        try:
            _handle_update(body)
        except Exception as exc:
            log.exception("Webhook handler error: %s", exc)

        # Always return 200 so Telegram doesn't retry
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)
