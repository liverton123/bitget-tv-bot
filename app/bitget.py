import time, hmac, base64, json, hashlib
import requests
from typing import Dict, Any, Optional

BITGET_API = "https://api.bitget.com"

class BitgetClient:
    def __init__(self, api_key: str, api_secret: str, passphrase: str):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.passphrase = passphrase

    def _ts(self) -> str:
        # Bitget는 밀리초 타임스탬프 문자열 사용
        return str(int(time.time() * 1000))

    def _sign(self, ts: str, method: str, path: str, query: str = "", body: str = "") -> str:
        msg = f"{ts}{method.upper()}{path}"
        if query:
            msg += f"?{query}"
        if body:
            msg += body
        digest = hmac.new(self.api_secret, msg.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def _headers(self, ts: str, sign: str) -> Dict[str, str]:
        return {
            "ACCESS-KEY": self.api_key,
            "ACCESS-SIGN": sign,
            "ACCESS-PASSPHRASE": self.passphrase,
            "ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
            "locale": "en-US",
        }

    # ----- 잔고 조회 (모든 계정 자산)
    def all_account_balance(self) -> Dict[str, Any]:
        path = "/api/v2/account/all-account-balance"
        ts = self._ts()
        sign = self._sign(ts, "GET", path)
        r = requests.get(BITGET_API + path, headers=self._headers(ts, sign), timeout=10)
        r.raise_for_status()
        return r.json()

    # ----- 선물 단일 계정 조회 (USDT-M)
    def get_single_account(self, marginCoin: str = "USDT", productType: str = "USDT-FUTURES") -> Dict[str, Any]:
        path = "/api/v2/mix/account/account"
        query = f"productType={productType}&marginCoin={marginCoin}"
        ts = self._ts()
        sign = self._sign(ts, "GET", path, query)
        r = requests.get(f"{BITGET_API}{path}?{query}", headers=self._headers(ts, sign), timeout=10)
        r.raise_for_status()
        return r.json()

    # ----- 주문
    def place_order(
        self,
        symbol: str,
        side: str,       # "buy" / "sell"
        tradeSide: str,  # "open" / "close"
        size: str,       # 수량(문자열)
        productType: str = "USDT-FUTURES",
        marginCoin: str = "USDT",
        orderType: str = "market"
    ) -> Dict[str, Any]:
        path = "/api/v2/mix/order/place-order"
        body_obj = {
            "symbol": symbol,          # 예: "BTCUSDT"
            "productType": productType,
            "marginCoin": marginCoin,
            "side": side,              # buy/sell
            "tradeSide": tradeSide,    # open/close
            "orderType": orderType,    # market/limit
            "size": size               # 수량(거래소 틱규칙에 맞춰 소수 자리 처리)
        }
        body = json.dumps(body_obj, separators=(",", ":"))
        ts = self._ts()
        sign = self._sign(ts, "POST", path, "", body)
        r = requests.post(BITGET_API + path, headers=self._headers(ts, sign), data=body, timeout=10)
        r.raise_for_status()
        return r.json()
