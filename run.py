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
    fg      = screener.fetch_fear_greed()

    # Collect all new signals first
    new_signals = []
    for signal in screener.stream_signals(tickers):
        if database.was_alerted(signal["ticker"], signal["signal_type"]):
            continue
        new_signals.append(signal)

    # Rank by analyst buy ratio (desc), tiebreak by SMA150 proximity (asc)
    def _buy_ratio(sig):
        rec   = sig.get("analyst_rec") or {}
        total = (rec.get("buy") or 0) + (rec.get("hold") or 0) + (rec.get("sell") or 0)
        return (rec.get("buy") or 0) / total if total else 0

    def _score(sig):
        sma150   = sig.get("sma150")
        close    = sig.get("close") or 0
        sma_prox = (close - sma150) / sma150 if sma150 else 1.0
        return (-_buy_ratio(sig), sma_prox)

    # Deduplicate: one signal per ticker (keep best-scored)
    best_per_ticker: dict = {}
    for sig in new_signals:
        t = sig["ticker"]
        if t not in best_per_ticker or _score(sig) < _score(best_per_ticker[t]):
            best_per_ticker[t] = sig
    unique_signals = list(best_per_ticker.values())

    top_signals = sorted(unique_signals, key=_score)[:5]

    # Extreme Fear: suppress unless the signal has very strong analyst backing
    if fg["rating"] == "Extreme Fear":
        top_signals = [s for s in top_signals if _buy_ratio(s) >= 0.75]

    # Mark all new signals as alerted (whether sent or not)
    for signal in new_signals:
        database.mark_alerted(signal["ticker"], signal["signal_type"])

    if not top_signals:
        notifier.send_summary([], total_screened=len(tickers), fear_greed=fg)
    else:
        notifier.send_scan_results(top_signals, fear_greed=fg)

    log.info("Done — %d signal(s) sent", len(top_signals))


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


def run_rec() -> None:
    tickers = universe.get_universe()
    results = screener.scan_top_recommendations(tickers)
    notifier.send_top_recommendations(results)


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
        "rec":       run_rec,
    }
    fn = dispatch.get(run_type, run_screen)
    try:
        fn()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
