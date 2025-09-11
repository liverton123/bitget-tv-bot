import os
from fastapi import FastAPI, Header, HTTPException
from app.bitget import BitgetClient
from app.models import TVSignal

app = FastAPI(title="TV→Bitget AutoTrader")

API_KEY = os.environ["BITGET_API_KEY"]
API_SECRET = os.environ["BITGET_API_SECRET"]
API_PASSPHRASE = os.environ["BITGET_API_PASSPHRASE"]
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
PRODUCT_TYPE = os.environ.get("PRODUCT_TYPE", "USDT-FUTURES")
MARGIN_COIN = os.environ.get("MARGIN_COIN", "USDT")

client = BitgetClient(API_KEY, API_SECRET, API_PASSPHRASE)

def map_symbol(tv_symbol: str) -> str:
    core = tv_symbol.split(":")[-1]
    core = core.replace(".P","").replace(".perp","").replace(".PERP","")
    return core  # e.g. "BTCUSDT"

def to_size(x: float) -> str:
    return f"{x:.6f}"

def get_available_and_leverage() -> tuple[float, float]:
    acc = client.get_single_account(marginCoin=MARGIN_COIN, productType=PRODUCT_TYPE)
    data = acc.get("data", {}) if isinstance(acc, dict) else {}
    available = float(data.get("available", data.get("availableBalance", data.get("availableEquity", "0"))))
    lev = 1.0
    if isinstance(data.get("crossedMarginLeverage", None), (int, float, str)) and str(data["crossedMarginLeverage"]).strip():
        lev = float(data["crossedMarginLeverage"])
    elif isinstance(data.get("isolatedLongLever", None), (int, float, str)) and str(data["isolatedLongLever"]).strip():
        lev = float(data["isolatedLongLever"])
    return available, max(lev, 1.0)

def calc_qty(available_usdt: float, leverage: float, last_price: float) -> float:
    notional = available_usdt * 0.70 * leverage
    return notional / max(last_price, 1e-9)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/tv")
def tv_webhook(signal: TVSignal, x_token: str = Header(default="")):
    if WEBHOOK_TOKEN and x_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

    if signal.side.lower() != "long":
        raise HTTPException(status_code=400, detail="Only long side is allowed")

    symbol = map_symbol(signal.symbol)
    last_price = float(signal.price)

    if signal.action == "open":
        available, exch_lev = get_available_and_leverage()
        qty = calc_qty(available, exch_lev, last_price)
        size = to_size(qty)
        res = client.place_order(
            symbol=symbol,
            side="buy",
            tradeSide="open",
            size=size,
            productType=PRODUCT_TYPE,
            marginCoin=MARGIN_COIN,
            orderType="market",
        )
        return {"ok": True, "did": "open-long", "symbol": symbol, "size": size, "res": res}

    if signal.action == "close":
        size = to_size(999999.0)  # intent: close all; exchange will reject if no position
        res = client.place_order(
            symbol=symbol,
            side="sell",
            tradeSide="close",
            size=size,
            productType=PRODUCT_TYPE,
            marginCoin=MARGIN_COIN,
            orderType="market",
        )
        return {"ok": True, "did": "close-long", "symbol": symbol, "res": res}

    raise HTTPException(status_code=400, detail=f"Unknown action: {signal.action}")
