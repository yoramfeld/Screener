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

import backtest
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

_HOLIDAYS = {
    # NYSE holidays 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25",
    # NYSE holidays 2027
    "2027-01-01", "2027-01-18", "2027-02-15", "2027-03-26",
    "2027-05-31", "2027-07-05", "2027-09-06", "2027-11-25", "2027-12-24",
}


def _market_is_open() -> bool:
    now      = datetime.now(tz=timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    if date_str in _HOLIDAYS:
        return False
    t = (now.hour, now.minute)
    return _MARKET_OPEN_UTC <= t <= _MARKET_CLOSE_UTC


def run_screen() -> None:
    is_manual = os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
    if not is_manual and not _market_is_open():
        log.info("Market closed or holiday — exiting")
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


def run_above() -> None:
    tickers = universe.get_universe()
    matches = screener.scan_above(tickers)
    notifier.send_above(matches)


def run_backtest() -> None:
    tickers = universe.get_universe()
    stats   = backtest.run(tickers, years=3)
    notifier.send_backtest(stats, years=3)


def run_pnl() -> None:
    trades = portfolio.get_trades()
    notifier.send_pnl(trades)


def run_earnings() -> None:
    tickers = universe.get_universe()
    matches = screener.scan_earnings_week(tickers)
    notifier.send_earnings_week(matches)


if __name__ == "__main__":
    run_type = os.environ.get("RUN_TYPE", "screen")
    dispatch = {
        "screen":    run_screen,
        "portfolio": run_portfolio,
        "pnl":       run_pnl,
        "above":     run_above,
        "backtest":  run_backtest,
        "earnings":  run_earnings,
    }
    fn = dispatch.get(run_type, run_screen)
    try:
        fn()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
