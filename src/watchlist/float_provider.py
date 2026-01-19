from __future__ import annotations

import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Optional, Tuple

import requests
import sqlite3

from . import db


@dataclass(frozen=True)
class FloatResult:
    symbol: str
    float_shares: int
    outstanding_shares: Optional[int]
    source: str
    fetched_utc: str


class FmpFloatProvider:
    """Fetch floatShares using Financial Modeling Prep.

    Endpoint:
      GET https://financialmodelingprep.com/stable/shares-float?symbol=...&apikey=...
    """

    def __init__(self, api_key: str, *, base_url: str = "https://financialmodelingprep.com/stable", timeout_s: float = 10.0):
        if not api_key:
            raise ValueError("FMP API key required")
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._sess = requests.Session()

    def prefetch(
        self,
        conn: sqlite3.Connection,
        symbols: Iterable[str],
        asof_date_ny: str,
        *,
        allow_stale_days: int = 14,
        max_retries: int = 4,
        min_delay_s: float = 0.15,
        backoff_base_s: float = 0.5,
    ) -> Dict[str, int]:
        """Return map symbol->floatShares. Updates DB snapshots."""
        out: Dict[str, int] = {}
        for sym in symbols:
            sym = sym.upper().strip()
            fs = self.get_float_shares(
                conn,
                sym,
                asof_date_ny,
                allow_stale_days=allow_stale_days,
                max_retries=max_retries,
                min_delay_s=min_delay_s,
                backoff_base_s=backoff_base_s,
            )
            if fs is not None:
                out[sym] = fs
        return out

    def get_float_shares(
        self,
        conn: sqlite3.Connection,
        symbol: str,
        asof_date_ny: str,
        *,
        allow_stale_days: int = 14,
        max_retries: int = 4,
        min_delay_s: float = 0.15,
        backoff_base_s: float = 0.5,
    ) -> Optional[int]:
        symbol = symbol.upper().strip()

        # 1) cache today
        cur = conn.execute(
            "SELECT float_shares FROM float_snapshots WHERE symbol=? AND asof_date=?",
            (symbol, asof_date_ny),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])

        # 2) stale cache
        if allow_stale_days > 0:
            min_date = (datetime.fromisoformat(asof_date_ny).date() - timedelta(days=allow_stale_days)).isoformat()
            cur = conn.execute(
                """
                SELECT float_shares
                FROM float_snapshots
                WHERE symbol=? AND asof_date BETWEEN ? AND ?
                ORDER BY asof_date DESC
                LIMIT 1
                """,
                (symbol, min_date, asof_date_ny),
            )
            row = cur.fetchone()
            if row:
                return int(row[0])

        # 3) fetch
        result = self._fetch(symbol, max_retries=max_retries, min_delay_s=min_delay_s, backoff_base_s=backoff_base_s)
        if result is None:
            return None

        db.upsert_float(conn, symbol, asof_date_ny, result.float_shares, result.source)
        return result.float_shares

    def _fetch(
        self,
        symbol: str,
        *,
        max_retries: int,
        min_delay_s: float,
        backoff_base_s: float,
    ) -> Optional[FloatResult]:
        url = f"{self._base_url}/shares-float"
        params = {"symbol": symbol, "apikey": self._api_key}

        for attempt in range(1, max_retries + 1):
            time.sleep(min_delay_s)
            try:
                r = self._sess.get(url, params=params, timeout=self._timeout_s)

                if r.status_code == 429:
                    self._backoff(attempt, backoff_base_s, hard=True)
                    continue

                r.raise_for_status()
                data = r.json()

                if not isinstance(data, list) or not data or not isinstance(data[0], dict):
                    self._backoff(attempt, backoff_base_s)
                    continue

                payload = data[0]
                fs = self._to_int(payload.get("floatShares"))
                os_ = self._to_int(payload.get("outstandingShares"))

                if fs is None or fs <= 0:
                    return None

                # sanity check, but don't drop it
                if os_ is not None and os_ > 0 and fs > os_:
                    pass

                return FloatResult(
                    symbol=symbol,
                    float_shares=fs,
                    outstanding_shares=os_,
                    source="fmp:shares-float",
                    fetched_utc=datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                )

            except (requests.Timeout, requests.ConnectionError):
                self._backoff(attempt, backoff_base_s)
                continue
            except requests.HTTPError:
                # key bad / symbol not found / etc.
                return None
            except ValueError:
                self._backoff(attempt, backoff_base_s)
                continue

        return None

    def _backoff(self, attempt: int, base: float, *, hard: bool = False) -> None:
        delay = base * (2 ** (attempt - 1))
        jitter = random.random() * (0.75 if hard else 0.25)
        time.sleep(delay + jitter)

    def _to_int(self, v) -> Optional[int]:
        if v is None:
            return None
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return None
