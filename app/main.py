import os
from fastapi import FastAPI, Header, HTTPException
from app.bitget import BitgetClient
from app.models import TVSignal

app = FastAPI(title="TV→Bitget AutoTrader")

# ----- 환경변수
API_KEY = os.environ["BITGET_API_KEY"]
API_SECRET = os.environ["BITGET_API_SECRET"]
API_PASSPHRASE = os.environ["BITGET_API_PASSPHRASE"]
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")
LEVERAGE = float(os.environ.get("LEVERAGE", "5"))               # 계정 레버리지가 우선이지만, 계산식에 사용
PRODUCT_TYPE = os.environ.get("PRODUCT_TYPE", "USDT-FUTURES")
MARGIN_COIN = os.environ.get("MARGIN_COIN", "USDT")

client = BitgetClient(API_KEY, API_SECRET, API_PASSPHRASE)

def map_symbol(tv_symbol: str) -> str:
    """
    TradingView 심볼 → Bitget 심볼 간단 매핑
    예) "BINANCE:BTCUSDT.P" → "BTCUSDT"
    """
    core = tv_symbol.split(":")[-1]
    core = core.replace(".P","").replace(".perp","").replace(".PERP","")
    return core

def round_size(size: float) -> str:
    # 심볼별 최소수량/소수점 자릿수는 실제 스펙 조회해 맞추는 게 가장 안전
    return f"{size:.6f}"

def get_available_usdt() -> float:
    acc = client.get_single_account(marginCoin=MARGIN_COIN, productType=PRODUCT_TYPE)
    data = acc.get("data", {})
    return float(data.get("available", data.get("availableBalance", data.get("availableEquity", "0"))))

def calc_qty(available_usdt: float, last_price: float) -> float:
    notional = available_usdt * 0.70 * LEVERAGE
    return notional / max(last_price, 1e-9)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/tv")
def tv_webhook(signal: TVSignal, x_token: str = Header(default="")):
    # 보안 토큰 검사
    if WEBHOOK_TOKEN and x_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

    # 롱만 허용
    if signal.side.lower() != "long":
        raise HTTPException(status_code=400, detail="Only long side is allowed")

    symbol = map_symbol(signal.symbol)
    last_price = float(signal.price)

    if signal.action == "open":
        available = get_available_usdt()
        qty = calc_qty(available, last_price)
        size = round_size(qty)
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

    elif signal.action == "close":
        # MVP: 전량 청산 의도. (실전에서는 포지션 조회 후 보유수량만큼 정확히 청산하는 로직 권장)
        size = round_size(999999.0)
        res = client.place_order(
            symbol=symbol,
            side="sell",
            tradeSide="close",
            size=size,
            productType=PRODUCT_TYPE,
            marginCoin=MARGIN_COIN,
            orderType="market",
        )
        return {"ok": True, "did":
