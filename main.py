"""
Entry point — run the full screening pipeline.

Usage:
  python main.py

The script exits with code 0 on success (including "no signals found"),
and code 1 on an unexpected error, so GitHub Actions marks the run failed.
"""

import logging
import sys

import database
import notifier
import screener
import universe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)


def main() -> None:
    # 1. Market context — abort early if SPY is in a sharp sell-off
    if not screener.market_is_healthy():
        notifier.send([], aborted=True)
        log.warning("Run aborted: SPY below threshold")
        return

    # 2. Fetch universe (~600 tickers)
    tickers = universe.get_universe()

    # 3. Screen — batch download + apply all filters
    all_signals = screener.screen(tickers)

    # 4. Deduplicate — drop tickers already alerted within cooldown window
    new_signals = [s for s in all_signals if not database.was_alerted(s["ticker"])]
    skipped = len(all_signals) - len(new_signals)
    if skipped:
        log.info("Skipped %d already-alerted ticker(s)", skipped)

    # 5. Persist before sending (so a Telegram failure doesn't cause double-alerts on retry)
    for s in new_signals:
        database.mark_alerted(s["ticker"])

    # 6. Dispatch
    notifier.send(new_signals)
    log.info("Done — %d new signal(s) sent", len(new_signals))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Unhandled error in pipeline")
        sys.exit(1)
