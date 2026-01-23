from dotenv import load_dotenv
load_dotenv()

from ib_insync import IB, Stock
import os, time

HOST = os.getenv("IB_HOST", "172.19.64.1")
PORT = int(os.getenv("IB_PORT", "7496"))
CID  = int(os.getenv("IB_CLIENT_ID", "8"))
MDT  = int(os.getenv("IB_MARKET_DATA_TYPE", "1"))  # 1 live, 3 delayed

ib = IB()
ib.connect(HOST, PORT, clientId=CID, readonly=True, timeout=15)
ib.reqMarketDataType(MDT)

c = Stock("AAPL", "SMART", "USD")
ib.qualifyContracts(c)

print("Contract:", c)

t = ib.reqMktData(c, "", False, False)
ib.sleep(2)

print("MarketDataType:", getattr(t, "marketDataType", None))
print("bid/ask/last:", t.bid, t.ask, t.last)
print("prevClose/close:", getattr(t, "prevClose", None), getattr(t, "close", None))
print("volume:", getattr(t, "volume", None))

bars = ib.reqHistoricalData(
    c,
    endDateTime="",
    durationStr="2 D",
    barSizeSetting="1 min",
    whatToShow="TRADES",
    useRTH=0,
    formatDate=1
)

print("bars:", len(bars))
if bars:
    print("last bar:", bars[-1].date, bars[-1].close, bars[-1].volume)

ib.disconnect()
