"""
Configuration — all values read from environment variables.

TELEGRAM SETUP (one-time, ~2 minutes):
  1. Open Telegram and message @BotFather → send /newbot → follow prompts.
     You'll receive a token like:  123456789:ABCDefgh...
     Set this as TELEGRAM_BOT_TOKEN.

  2. Message your new bot at least once (just send "hi").
     Then open in a browser:
       https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
     Find  result[0].message.chat.id  — that number is your TELEGRAM_CHAT_ID.

  3. In your GitHub repo → Settings → Secrets and variables → Actions, add:
       TELEGRAM_BOT_TOKEN   = <token from step 1>
       TELEGRAM_CHAT_ID     = <chat id from step 2>

OPTIONAL OVERRIDES (set as GitHub Actions env vars or locally in .env):
  DB_PATH               — path to SQLite file (default: alerts.db)
  ALERT_COOLDOWN_DAYS   — days before the same ticker can be re-alerted (default 1)
  SPY_DROP_THRESHOLD    — abort if SPY intraday return is below this (default -0.02)
  VOLUME_MIN_RATIO      — minimum volume vs 20-day avg to qualify (default 0.80)
  PROXIMITY_CAP         — max % above SMA150 for close to still qualify (default 0.03)
"""

import os

# --- Telegram ---
TELEGRAM_BOT_TOKEN: str = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.environ.get("TELEGRAM_CHAT_ID", "")

# --- SQLite dedup ---
DB_PATH: str = os.environ.get("DB_PATH", "alerts.db")

# --- Screener knobs ---
ALERT_COOLDOWN_DAYS: int = int(os.environ.get("ALERT_COOLDOWN_DAYS", "1"))
SPY_DROP_THRESHOLD: float = float(os.environ.get("SPY_DROP_THRESHOLD", "-0.02"))
VOLUME_MIN_RATIO: float = float(os.environ.get("VOLUME_MIN_RATIO", "0.80"))
PROXIMITY_CAP: float = float(os.environ.get("PROXIMITY_CAP", "0.03"))
