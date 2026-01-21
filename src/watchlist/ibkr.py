from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import math
from typing import List, Optional, Tuple

from ib_insync import IB, Stock, ScannerSubscription


@dataclass(frozen=True)
class IbContractInfo:
    symbol: str
    con_id: int
    primary_exchange: Optional[str]


def connect(host: str, port: int, client_id: int, timeout_s: float, market_data_type: int) -> IB:
    ib = IB()

    def on_error(reqId: int, errorCode: int, errorString: str, contract) -> None:
        if errorCode in (2104, 2106, 2158, 162, 10167):
            return
        print(f"IB ERROR reqId={reqId} code={errorCode} msg={errorString}")

    ib.errorEvent += on_error

    if os.getenv("IB_CLIENT_ID_AUTO", "0") in ("1", "true", "True"):
        # evita colisiones con scans “zombie”
        client_id = client_id + (os.getpid() % 1000)
        
    ib.connect(host, port, clientId=client_id, readonly=True, timeout=timeout_s)
    # 1=live, 2=frozen, 3=delayed, 4=delayed-frozen
    ib.reqMarketDataType(int(market_data_type))
    return ib


def scan_top_perc_gainers(
    ib: IB,
    *,
    price_min: float,
    price_max: float,
    volume_min: int,
    max_rows: int
) -> List[IbContractInfo]:
    sub = ScannerSubscription()
    sub.instrument = "STK"
    sub.locationCode = "STK.NASDAQ"   # o el que estés probando con fallback
    sub.scanCode = "TOP_PERC_GAIN"
    sub.numberOfRows = max_rows

    rows = ib.reqScannerData(sub, [], [])
    print(f"Scanner RAW rows={len(rows)}")

    out: List[IbContractInfo] = []
    dropped = 0

    for idx, r in enumerate(rows or []):
        cd = getattr(r, "contractDetails", None)
        if cd is None:
            dropped += 1
            if idx < 3:
                print("DROP: missing contractDetails", r)
            continue

        c = getattr(cd, "contract", None)
        if c is None:
            dropped += 1
            if idx < 3:
                print("DROP: missing contract", cd)
            continue

        symbol = getattr(c, "symbol", None)
        con_id = getattr(c, "conId", None)
        primary_exchange = getattr(c, "primaryExchange", None) or getattr(cd, "primaryExchange", None)

        if not symbol or not con_id:
            dropped += 1
            if idx < 3:
                print("DROP: missing symbol/conId", symbol, con_id, primary_exchange)
            continue

        out.append(IbContractInfo(
            symbol=symbol,
            con_id=int(con_id),
            primary_exchange=primary_exchange if primary_exchange else None
        ))

    # dedup preserving order
    seen = set()
    uniq: List[IbContractInfo] = []
    for i in out:
        if i.symbol not in seen:
            seen.add(i.symbol)
            uniq.append(i)

    print(f"Scanner PARSED={len(uniq)} dropped={dropped}")
    if uniq[:5]:
        print("Scanner sample:", [(x.symbol, x.primary_exchange) for x in uniq[:5]])

    return uniq

def _to_float(x):
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None

def _to_int(x):
    v = _to_float(x)
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def snapshot_metrics(ib: IB, symbol: str) -> Tuple[Optional[float], Optional[float], Optional[int], Optional[float], Optional[float]]:
    """Return last, prevClose, volumeToday, bid, ask."""
    c = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(c)

    t = ib.reqMktData(c, "", False, False)
    ib.sleep(2.0)

    last = getattr(t, "last", None) or getattr(t, "marketPrice", None)
    prev_close = getattr(t, "prevClose", None) or getattr(t, "close", None)
    volume = getattr(t, "volume", None)
    bid = getattr(t, "bid", None)
    ask = getattr(t, "ask", None)

    return (
        _to_float(last),
        _to_float(prev_close),
        _to_int(volume),
        _to_float(bid),
        _to_float(ask),
    )



def historical_bars_intraday(
    ib: IB,
    symbol: str,
    *,
    duration_days: int,
    use_rth: bool,
    bar_size: str,
) -> List:
    c = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(c)

    return ib.reqHistoricalData(
        c,
        endDateTime="",
        durationStr=f"{duration_days} D",
        barSizeSetting=bar_size,
        whatToShow="TRADES",
        useRTH=1 if use_rth else 0,
        formatDate=1,
    )
