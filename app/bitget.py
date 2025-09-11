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

    def _get(self, path: str, query: Optional[str] = "") -> Dict[str, Any]:
        ts = self._ts()
        sign = self._sign(ts, "GET", path, query or "")
        url = f"{BITGET_API}{path}" + (f"?{query}" if query else "")
        r = requests.get(url, headers=self._headers(ts, sign), timeout=10)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body_obj: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(body_obj, separators=(",", ":"))
        ts = self._ts()
        sign = self._sign(ts, "POST", path, "", body)
        r = requests.post(BITGET_API + path, headers=self._headers(ts, sign), data=body, timeout=10)
        r.raise_for_status()
        return r.json()

    # ----- 선물 계정(USDT-M) 조회
    def get_single_account(self, marginCoin: str = "USDT", productType: str = "USDT-FUTURES") -> Dict[str, Any]:
        path = "/api/v2/mix/account/account"
        query = f"productType={productType}&marginCoin={marginCoin}"
        return self._get(path, query)

    # ----- 모든 계정 잔고(총자산/가용)
    def all_account_balance(self) -> Dict[str, Any]:
        path = "/api/v2/account/all-account-balance"
        return self._get(path)

    # ----- (선택) 포지션 조회 - 심볼 단건
    # Bitget의 단건 포지션 엔드포인트 명칭/쿼리는 버전 따라 다를 수 있어, 필요 시 문서 확인 후 맞추세요.
    def get_position_single(self, symbol: str, marginCoin: str = "USDT", productType: str = "USDT-FUTURES") -> Optional[Dict[str, Any]]:
        try:
            path = "/api/v2/mix/position/single-position"
            query = f"productType={productType}&marginCoin={marginCoin}&symbol={symbol}"
            res = self._get(path, query)
            return res
        except Exception:
            return None

    # ----- 주문 (V2)
    def place_order(
        self,
        symbol: str,
        side: str,       # "buy" or "sell"
        tradeSide: str,  # "open" or "close"
        size: str,       # 수량(문자열)
        productType: str = "USDT-FUTURES",
        marginCoin: str = "USDT",
        orderType: str = "market"
    ) -> Dict[str, Any]:
        path = "/api/v2/mix/order/place-order"
        body_obj = {
            "symbol": symbol,
            "productType": productType,
            "marginCoin": marginCoin,
            "side": side,              # buy/sell
            "tradeSide": tradeSide,    # open/close
            "orderType": orderType,    # market/limit
            "size": size
        }
        return self._post(path, body_obj)
