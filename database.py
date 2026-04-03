"""
Alert deduplication via SQLite.

Each alert is stored as (ticker, alert_date). The same ticker won't be
re-alerted for ALERT_COOLDOWN_DAYS days.

The DB file path defaults to alerts.db in the project root and is persisted
between GitHub Actions runs via actions/cache (keyed by date).
"""

import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, timedelta

import config

log = logging.getLogger(__name__)

_CREATE = """
CREATE TABLE IF NOT EXISTS alerts (
    ticker     TEXT NOT NULL,
    alert_date TEXT NOT NULL,
    PRIMARY KEY (ticker, alert_date)
);
"""


@contextmanager
def _conn():
    con = sqlite3.connect(config.DB_PATH)
    try:
        con.execute(_CREATE)
        con.commit()
        yield con
    finally:
        con.close()


def was_alerted(ticker: str) -> bool:
    """Return True if this ticker was alerted within the cooldown window."""
    today = date.today()
    dates = [
        (today - timedelta(days=i)).isoformat()
        for i in range(config.ALERT_COOLDOWN_DAYS)
    ]
    placeholders = ",".join("?" * len(dates))
    with _conn() as con:
        row = con.execute(
            f"SELECT 1 FROM alerts WHERE ticker=? AND alert_date IN ({placeholders})",
            [ticker, *dates],
        ).fetchone()
    return row is not None


def mark_alerted(ticker: str) -> None:
    """Record that an alert was sent for this ticker today."""
    with _conn() as con:
        con.execute(
            "INSERT OR IGNORE INTO alerts (ticker, alert_date) VALUES (?, ?)",
            (ticker, date.today().isoformat()),
        )
        con.commit()
    log.debug("Marked alerted: %s", ticker)
