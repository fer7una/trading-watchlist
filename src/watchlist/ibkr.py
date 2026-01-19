from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from ib_insync import IB, Stock, ScannerSubscription


@dataclass(frozen=True)
class IbContractInfo:
    symbol: str
    con_id: int
    primary_exchange: Optional[str]


def connect(host: str, port: int, client_id: int, timeout_s: float = 10.0) -> IB:
    ib = IB()
    ib.connect(host, port, clientId=client_id, readonly=True, timeout=timeout_s)
    return ib


def scan_top_perc_gainers(ib: IB, *, price_min: float, price_max: float, volume_min: int, max_rows: int) -> List[IbContractInfo]:
    sub = ScannerSubscription()
    sub.instrument = "STK"
    sub.locationCode = "STK.US.MAJOR"
    sub.scanCode = "TOP_PERC_GAIN"
    sub.abovePrice = price_min
    sub.belowPrice = price_max
    sub.aboveVolume = volume_min
    sub.numberOfRows = max_rows

    rows = ib.reqScannerSubscription(sub, [], [])
    out: List[IbContractInfo] = []
    for r in rows:
        try:
            cd = r.contractDetails
            c = cd.contract
            out.append(IbContractInfo(symbol=c.symbol, con_id=c.conId, primary_exchange=cd.primaryExchange))
        except Exception:
            continue
    # de-dup preserving order
    seen = set()
    uniq: List[IbContractInfo] = []
    for i in out:
        if i.symbol not in seen:
            seen.add(i.symbol)
            uniq.append(i)
    return uniq


def snapshot_metrics(ib: IB, symbol: str) -> Tuple[Optional[float], Optional[float], Optional[int], Optional[float], Optional[float]]:
    """Return last, prevClose, volumeToday, bid, ask."""
    c = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(c)

    t = ib.reqMktData(c, "", False, False)
    ib.sleep(1.2)

    last = getattr(t, "last", None) or getattr(t, "marketPrice", None)
    prev_close = getattr(t, "prevClose", None) or getattr(t, "close", None)
    volume = getattr(t, "volume", None)
    bid = getattr(t, "bid", None)
    ask = getattr(t, "ask", None)

    return (
        float(last) if last is not None else None,
        float(prev_close) if prev_close is not None else None,
        int(volume) if volume is not None else None,
        float(bid) if bid is not None else None,
        float(ask) if ask is not None else None,
    )


def historical_bars_1m(ib: IB, symbol: str, *, duration_days: int, use_rth: bool) -> List:
    c = Stock(symbol, "SMART", "USD")
    ib.qualifyContracts(c)

    return ib.reqHistoricalData(
        c,
        endDateTime="",
        durationStr=f"{duration_days} D",
        barSizeSetting="1 min",
        whatToShow="TRADES",
        useRTH=1 if use_rth else 0,
        formatDate=1,
    )
