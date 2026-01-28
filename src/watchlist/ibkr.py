from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import os
import math
from typing import List, Optional, Tuple

from ib_insync import IB, Stock, ScannerSubscription

SCAN_INSTRUMENT = "STK"
SCAN_LOCATION_CODE = os.getenv("SCAN_LOCATION_CODE", "STK.US.MAJOR")
SCAN_CODE = "TOP_PERC_GAIN"

MARKET_DATA_TYPE_LABELS = {
    1: "live",
    2: "frozen",
    3: "delayed",
    4: "delayed-frozen",
}


def market_data_type_label(market_data_type: int) -> str:
    return MARKET_DATA_TYPE_LABELS.get(int(market_data_type), "unknown")


class IbkrConnectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class IbContractInfo:
    symbol: str
    con_id: int
    primary_exchange: Optional[str]


def connect(host: str, port: int, client_id: int, timeout_s: float, market_data_type: int) -> IB:
    ib = IB()

    def on_error(reqId: int, errorCode: int, errorString: str, contract) -> None:
        contract_summary = None
        if contract is not None:
            symbol = getattr(contract, "symbol", None)
            con_id = getattr(contract, "conId", None)
            if symbol or con_id:
                contract_summary = f"{symbol or '?'}({con_id or '?'})"
        if contract_summary:
            print(f"IBKR_ERROR code={errorCode} reason={errorString} reqId={reqId} contract={contract_summary}")
        else:
            print(f"IBKR_ERROR code={errorCode} reason={errorString} reqId={reqId}")

    ib.errorEvent += on_error

    if os.getenv("IB_CLIENT_ID_AUTO", "0") in ("1", "true", "True"):
        # evita colisiones con scans “zombie”
        client_id = client_id + (os.getpid() % 1000)
        
    try:
        ib.connect(host, port, clientId=client_id, readonly=True, timeout=timeout_s)
    except (TimeoutError, OSError) as exc:
        raise IbkrConnectionError(
            f"IBKR connection failed (host={host} port={port}). "
            "Verify TWS/Gateway is running and IB_HOST/IB_PORT are correct."
        ) from exc
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
    sub.instrument = SCAN_INSTRUMENT
    sub.locationCode = SCAN_LOCATION_CODE   # o el que estés probando con fallback
    sub.scanCode = SCAN_CODE
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
        exchange = getattr(c, "exchange", None)
        if not primary_exchange and exchange and str(exchange).upper() == "SMART":
            primary_exchange = "SMART"

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


def is_otc_pink(primary_exchange: Optional[str]) -> bool:
    if not primary_exchange:
        return False
    pe = primary_exchange.strip().upper()
    return "OTC" in pe or "PINK" in pe or "OTCBB" in pe

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

    def _normalize_bar_size(value: str) -> str:
        v = (value or "").strip().lower()
        if v.endswith("m") and v[:-1].isdigit():
            return f"{int(v[:-1])} min"
        if v.endswith("min") and v[:-3].isdigit():
            return f"{int(v[:-3])} min"
        return value

    return ib.reqHistoricalData(
        c,
        endDateTime="",
        durationStr=f"{duration_days} D",
        barSizeSetting=_normalize_bar_size(bar_size),
        whatToShow="TRADES",
        useRTH=1 if use_rth else 0,
        formatDate=1,
    )
