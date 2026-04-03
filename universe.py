"""
Fetches the stock universe: S&P 500 + Nasdaq 100.

Sources are Wikipedia tables — stable, freely available, no API key required.
Tickers are normalised for yfinance (dots replaced with dashes, e.g. BRK.B → BRK-B).
"""

import io
import logging
from typing import List

import pandas as pd
import requests

log = logging.getLogger(__name__)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_NDX100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"

# Mimic a browser so Wikipedia doesn't block the request
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def _get_html(url: str) -> str:
    resp = requests.get(url, headers=_HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def _normalise(ticker: str) -> str:
    return ticker.strip().replace(".", "-").upper()


def _fetch_sp500() -> List[str]:
    try:
        html = _get_html(_SP500_URL)
        tables = pd.read_html(io.StringIO(html), attrs={"id": "constituents"})
        return [_normalise(t) for t in tables[0]["Symbol"].tolist()]
    except Exception as exc:
        log.warning("S&P 500 fetch failed: %s", exc)
        return []


def _fetch_ndx100() -> List[str]:
    try:
        html = _get_html(_NDX100_URL)
        tables = pd.read_html(io.StringIO(html))
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
