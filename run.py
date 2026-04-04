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
        notifier.send([], aborted=True)
        log.warning("Run aborted: SPY below threshold")
        return

    # 2. Fetch universe (~600 tickers)
    tickers = universe.get_universe()

    # 3. Screen — batch download + apply all filters
    all_signals = screener.screen(tickers)

    # 4. Deduplicate — each (ticker, signal_type) pair tracked independently
    new_signals = [
        s for s in all_signals
        if not database.was_alerted(s["ticker"], s["signal_type"])
    ]
    skipped = len(all_signals) - len(new_signals)
    if skipped:
        log.info("Skipped %d already-alerted signal(s)", skipped)

    # 5. Persist before sending
    for s in new_signals:
        database.mark_alerted(s["ticker"], s["signal_type"])

    # 6. Dispatch
    notifier.send(new_signals, total_screened=len(tickers), sample_tickers=tickers[:3])
    log.info("Done — %d new signal(s) sent", len(new_signals))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
