"""
Entry point — run the full screening pipeline.

RUN_TYPE env var controls behaviour (set via GitHub Actions input):
  screen    (default) — scan universe and send signals
  portfolio           — send current portfolio stop levels
  pnl                 — send closed trade history

Exits with code 0 on success, code 1 on error.
"""

import logging
import os
import sys
from datetime import datetime, timezone

import database
import notifier
import portfolio
import screener
import universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

_MARKET_OPEN_UTC  = (14, 30)
_MARKET_CLOSE_UTC = (21,  0)


def _within_trading_hours() -> bool:
    now = datetime.now(tz=timezone.utc)
    t = (now.hour, now.minute)
    return _MARKET_OPEN_UTC <= t <= _MARKET_CLOSE_UTC


def run_screen() -> None:
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    if not is_manual and not _within_trading_hours():
        log.info("Outside trading hours — exiting")
        return

    if not screener.market_is_healthy():
        notifier.send_summary([], aborted=True)
        log.warning("Run aborted: SPY below threshold")
        return

    tickers = universe.get_universe()
    signals_sent = 0

    for signal in screener.stream_signals(tickers):
        if database.was_alerted(signal["ticker"], signal["signal_type"]):
            continue
        database.mark_alerted(signal["ticker"], signal["signal_type"])
        notifier.send_signal(signal)
        signals_sent += 1

    if signals_sent == 0:
        debug = screener.sample_debug(tickers[0] if tickers else "AAPL")
        notifier.send_summary([], total_screened=len(tickers), sample_tickers=tickers[:3], debug=debug)

    log.info("Done — %d signal(s) sent", signals_sent)

    positions = portfolio.enrich_positions()
    if positions:
        notifier.send_portfolio(positions)


def run_portfolio() -> None:
    positions = portfolio.enrich_positions()
    notifier.send_portfolio(positions)


def run_pnl() -> None:
    trades = portfolio.get_trades()
    notifier.send_pnl(trades)


if __name__ == "__main__":
    run_type = os.environ.get("RUN_TYPE", "screen")
    dispatch = {"screen": run_screen, "portfolio": run_portfolio, "pnl": run_pnl}
    fn = dispatch.get(run_type, run_screen)
    try:
        fn()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
