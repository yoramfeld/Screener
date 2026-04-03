"""
Fetches the stock universe: S&P 500 + Nasdaq 100.

Sources are Wikipedia tables — stable, freely available, no API key required.
Tickers are normalised for yfinance (dots replaced with dashes, e.g. BRK.B → BRK-B).
"""

import logging
from typing import List

import pandas as pd

log = logging.getLogger(__name__)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_NDX100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _normalise(ticker: str) -> str:
    return ticker.strip().replace(".", "-").upper()


def _fetch_sp500() -> List[str]:
    try:
        tables = pd.read_html(_SP500_URL, attrs={"id": "constituents"})
        return [_normalise(t) for t in tables[0]["Symbol"].tolist()]
    except Exception as exc:
        log.warning("S&P 500 fetch failed: %s", exc)
        return []


def _fetch_ndx100() -> List[str]:
    try:
        # The Nasdaq-100 page has multiple tables; the constituents table
        # contains a 'Ticker' column.
        tables = pd.read_html(_NDX100_URL)
        for tbl in tables:
            if "Ticker" in tbl.columns:
                return [_normalise(t) for t in tbl["Ticker"].dropna().tolist()]
        log.warning("Nasdaq-100 table not found on Wikipedia page")
        return []
    except Exception as exc:
        log.warning("Nasdaq-100 fetch failed: %s", exc)
        return []


def get_universe() -> List[str]:
    """Return a deduplicated, sorted list of tickers from S&P 500 + Nasdaq 100."""
    sp500 = _fetch_sp500()
    ndx100 = _fetch_ndx100()
    combined = sorted(set(sp500 + ndx100))
    log.info("Universe: %d tickers (%d S&P500, %d NDX100)", len(combined), len(sp500), len(ndx100))
    return combined
