"""
Fetches the stock universe: S&P 100.

Source is the Wikipedia S&P 100 table — stable, freely available, no API key required.
Tickers are normalised for yfinance (dots replaced with dashes, e.g. BRK.B → BRK-B).
"""

import io
import logging
from typing import List

import pandas as pd
import requests

log = logging.getLogger(__name__)

_SP100_URL = "https://en.wikipedia.org/wiki/S%26P_100"

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


def _fetch_sp100() -> List[str]:
    try:
        html = _get_html(_SP100_URL)
        tables = pd.read_html(io.StringIO(html))
        for tbl in tables:
            if "Symbol" in tbl.columns:
                return [_normalise(t) for t in tbl["Symbol"].dropna().tolist()]
        log.warning("S&P 100 table not found on Wikipedia page")
        return []
    except Exception as exc:
        log.warning("S&P 100 fetch failed: %s", exc)
        return []


def get_universe() -> List[str]:
    """Return a sorted list of S&P 100 tickers."""
    tickers = _fetch_sp100()
    log.info("Universe: %d tickers (S&P 100)", len(tickers))
    return tickers
