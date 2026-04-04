"""
Entry point — run the full screening pipeline.

Usage:
  python run.py

Exits with code 0 on success (including "no signals"), code 1 on error.
"""

import logging
import os
import sys
from datetime import datetime, timezone

import database
import notifier
import screener
import universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# Trading hours in UTC: 9:30 AM–4:00 PM EST = 14:30–21:00 UTC
_MARKET_OPEN_UTC  = (14, 30)
_MARKET_CLOSE_UTC = (21,  0)


def _within_trading_hours() -> bool:
    now = datetime.now(tz=timezone.utc)
    t = (now.hour, now.minute)
    return _MARKET_OPEN_UTC <= t <= _MARKET_CLOSE_UTC


def main() -> None:
    # 0. Trading hours gate — skip for manual workflow_dispatch triggers
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    if not is_manual and not _within_trading_hours():
        log.info("Outside trading hours — exiting")
        return

    # 1. Market context — abort early if SPY is in a sharp sell-off
    if not screener.market_is_healthy():
        notifier.send_summary([], aborted=True)
        log.warning("Run aborted: SPY below threshold")
        return

    # 2. Fetch universe (~600 tickers)
    tickers = universe.get_universe()

    # 3. Send started message immediately — before the slow batch download
    notifier.send_started(tickers[0], tickers[1] if len(tickers) > 1 else "", len(tickers))

    # 4. Stream signals — send each match immediately as it's found
    signals_sent = 0

    for signal in screener.stream_signals(tickers):
        if database.was_alerted(signal["ticker"], signal["signal_type"]):
            continue
        database.mark_alerted(signal["ticker"], signal["signal_type"])
        notifier.send_signal(signal)
        signals_sent += 1

    # 4. End summary — only needed when nothing was found
    if signals_sent == 0:
        notifier.send_summary([], total_screened=len(tickers), sample_tickers=tickers[:3])

    log.info("Done — %d signal(s) sent", signals_sent)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
