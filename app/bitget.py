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

    def get_single_account(self, marginCoin: str = "USDT", productType: str = "USDT-FUTURES") -> Dict[str, Any]:
        path = "/api/v2/mix/account/account"
        query = f"productType={productType}&marginCoin={marginCoin}"
        return self._get(path, query)

    def place_order(
        self,
        symbol: str,
        side: str,
        tradeSide: str,
        size: str,
        productType: str = "USDT-FUTURES",
        marginCoin: str = "USDT",
        orderType: str = "market",
    ) -> Dict[str, Any]:
        path = "/api/v2/mix/order/place-order"
        body_obj = {
            "symbol": symbol,
            "productType": productType,
            "marginCoin": marginCoin,
            "side": side,           # "buy" or "sell"
            "tradeSide": tradeSide, # "open" or "close"
            "orderType": orderType, # "market" or "limit"
            "size": size
        }
        return self._post(path, body_obj)
