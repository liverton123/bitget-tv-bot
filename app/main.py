import os, math
from fastapi import FastAPI, Header, HTTPException, Request
from app.bitget import BitgetClient
from app.models import TVSignal

app = FastAPI(title="TV→Bitget AutoTrader")

# ----- 환경변수
API_KEY = os.environ["BITGET_API_KEY"]
API_SECRET = os.environ["BITGET_API_SECRET"]
API_PASSPHRASE = os.environ["BITGET_API_PASSPHRASE"]
# 레버리지는 "계정 기준"을 따르므로, 여기서는 단순 기준값으로만 사용 (거래소 설정이 우선)
LEVERAGE = float(os.environ.get("LEVERAGE", "5"))
PRODUCT_TYPE = os.environ.get("PRODUCT_TYPE", "USDT-FUTURES")
MARGIN_COIN = os.environ.get("MARGIN_COIN", "USDT")
WEBHOOK_TOKEN = os.environ.get("WEBHOOK_TOKEN", "")  # 보안용

client = BitgetClient(API_KEY, API_SECRET, API_PASSPHRASE)

def map_symbol(tv_symbol: str) -> str:
    """
    TradingView 심볼 → Bitget 심볼 간단 매핑
    예) "BINANCE:BTCUSDT.P" 또는 "CRYPTO:BTCUSDT" 등에서 'BTCUSDT'만 추출
    """
    core = tv_symbol.split(":")[-1]
    core = core.replace(".P", "").replace(".perp", "").replace(".PERP", "")
    return core  # Bitget도 "BTCUSDT" 형식

def round_size(symbol: str, size: float) -> str:
    """
    간단 반올림: 거래쌍 최소수량/틱단위는 실제 심볼 스펙을 조회해 맞추는 게 안전.
    MVP에선 6자리 소수로 제한.
    """
    return f"{size:.6f}"

def calc_order_qty(available_usdt: float, last_price: float) -> float:
    """
    목표 명목가치 = available * 0.70 * leverage
    수량 = 명목가치 / 가격
    """
    notional = available_usdt * 0.70 * LEVERAGE
    qty = notional / max(last_price, 1e-9)
    return qty

@app.post("/tv")
async def tv_webhook(signal: TVSignal, request: Request, x_token: str = Header(default="")):
    # 보안 토큰 검사
    if WEBHOOK_TOKEN and x_token != WEBHOOK_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

    symbol = map_symbol(signal.symbol)

    # 1) 선물 계정 잔고 조회
    # - all-account-balance 또는 선물 단일계정 둘 중 하나 사용 (둘 다 예시로 지원)
    acc = client.get_single_account(marginCoin=MARGIN_COIN, productType=PRODUCT_TYPE)
    # 가용자금 추출(필드명은 API 응답 기준. 'available' 또는 'availableBalance' 등)
    data = acc.get("data", {})
    # 필드 유연 처리
    available = float(
        data.get("available", data.get("availableBalance", data.get("availableEquity", "0")))
    )

    # 2) 가격은 TradingView에서 넘어온 값을 사용(시장가 기준). 안전하게 float 변환
    last_price = float(signal.price)

    if signal.action == "open":
        # 롱만 가정 → buy/open
        qty = calc_order_qty(available, last_price)
        size = round_size(symbol, qty)
        res = client.place_order(
            symbol=symbol,
            side="buy",
            tradeSide="open",
            size=size,
            productType=PRODUCT_TYPE,
            marginCoin=MARGIN_COIN,
            orderType="market"
        )
        return {"ok": True, "order": res}

    elif signal.action == "close":
        # 보유분 전량 청산 가정 → sell/close, 수량을 크게 줘도 reduce-only가 아니라서
        # 실제 포지션 수량을 조회해 정확히 청산하는 로직 권장 (MVP에선 큰 값으로 처리 X)
        # 안전하게 시장가 'close' 수량을 충분히 크게 주지 말고, 운영 시 포지션 조회 후 전달 권장
        size = round_size(symbol, 999999.0)  # 단순하게 "사이즈 크게"는 비추천. 실전선 포지션 조회 권장.
        res = client.place_order(
            symbol=symbol,
            side="sell",
            tradeSide="close",
            size=size,
            productType=PRODUCT_TYPE,
            marginCoin=MARGIN_COIN,
            orderType="market"
        )
        return {"ok": True, "order": res}

    else:
        raise HTTPException(status_code=400, detail=f"Unknown action: {signal.action}")
