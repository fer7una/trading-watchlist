from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


def connect(db_path: str) -> sqlite3.Connection:
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def init_schema(conn: sqlite3.Connection, schema_file: str) -> None:
    with open(schema_file, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()


def load_float_snapshots(conn: sqlite3.Connection, asof_date_ny: str) -> Dict[str, int]:
    cur = conn.execute(
        "SELECT symbol, float_shares FROM float_snapshots WHERE asof_date = ?",
        (asof_date_ny,),
    )
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def upsert_symbol(conn: sqlite3.Connection, symbol: str, con_id: Optional[int], primary_exchange: Optional[str]) -> None:
    conn.execute(
        """
        INSERT INTO symbols(symbol, con_id, primary_exchange, last_seen_utc)
        VALUES(?, ?, ?, datetime('now'))
        ON CONFLICT(symbol) DO UPDATE SET
          con_id=COALESCE(excluded.con_id, symbols.con_id),
          primary_exchange=COALESCE(excluded.primary_exchange, symbols.primary_exchange),
          last_seen_utc=excluded.last_seen_utc
        """,
        (symbol, con_id, primary_exchange),
    )
    conn.commit()


def upsert_float(conn: sqlite3.Connection, symbol: str, asof_date_ny: str, float_shares: int, source: str) -> None:
    conn.execute(
        """
        INSERT INTO float_snapshots(symbol, asof_date, float_shares, source, created_utc)
        VALUES(?, ?, ?, ?, datetime('now'))
        ON CONFLICT(symbol, asof_date) DO UPDATE SET
          float_shares=excluded.float_shares,
          source=excluded.source,
          created_utc=excluded.created_utc
        """,
        (symbol, asof_date_ny, int(float_shares), source),
    )
    conn.commit()


def cache_minute_bars(conn: sqlite3.Connection, symbol: str, rows: List[Tuple[str, float, float, float, float, int]]) -> None:
    """rows: list of (ts_utc_iso, open, high, low, close, volume)"""
    conn.executemany(
        """
        INSERT OR REPLACE INTO minute_bars(symbol, ts_utc, open, high, low, close, volume)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        [(symbol, *r) for r in rows],
    )
    conn.commit()


def load_minute_volumes_since(conn: sqlite3.Connection, symbol: str, since_utc_iso: str) -> List[Tuple[str, int]]:
    cur = conn.execute(
        """
        SELECT ts_utc, volume
        FROM minute_bars
        WHERE symbol = ? AND ts_utc >= ?
        ORDER BY ts_utc ASC
        """,
        (symbol, since_utc_iso),
    )
    return [(row[0], int(row[1] or 0)) for row in cur.fetchall()]


def load_baseline_curve(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    session: str,
    bar_size: str,
    lookback_days: int,
    method: str,
    trim_pct: float,
) -> Optional[Dict]:
    cur = conn.execute(
        """
        SELECT symbol, session, bar_size, lookback_days, method, trim_pct, updated_utc, history_days_used, baseline_json, notes
        FROM baseline_curves
        WHERE symbol = ?
          AND session = ?
          AND bar_size = ?
          AND lookback_days = ?
          AND method = ?
          AND trim_pct = ?
        """,
        (symbol, session, bar_size, int(lookback_days), method, float(trim_pct)),
    )
    row = cur.fetchone()
    if not row:
        return None
    baseline_json = row[8]
    baseline = json.loads(baseline_json) if baseline_json else []
    return {
        "symbol": row[0],
        "session": row[1],
        "bar_size": row[2],
        "lookback_days": int(row[3]),
        "method": row[4],
        "trim_pct": float(row[5]),
        "updated_utc": row[6],
        "history_days_used": int(row[7] or 0),
        "baseline_cumvol": baseline,
        "notes": row[9],
    }


def upsert_baseline_curve(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    session: str,
    bar_size: str,
    lookback_days: int,
    method: str,
    trim_pct: float,
    updated_utc: str,
    history_days_used: int,
    baseline_cumvol: List[float],
    notes: Optional[str],
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO baseline_curves(
          symbol, session, bar_size, lookback_days, method, trim_pct,
          updated_utc, history_days_used, baseline_json, notes
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            symbol,
            session,
            bar_size,
            int(lookback_days),
            method,
            float(trim_pct),
            updated_utc,
            int(history_days_used),
            json.dumps(baseline_cumvol),
            notes,
        ),
    )
    conn.commit()
