# app/main.py
# TV_WEBHOOK_V2 + Bitget wiring (long-only, 70% equity)
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Tuple
import os, hmac, hashlib, base64, json, time, logging, math, requests

app = FastAPI(title="tv-bot", version="TVv2")
log = logging.getLogger("uvicorn.error")

API_KEY       = os.getenv("BITGET_API_KEY", "")
API_SECRET    = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE= os.getenv("BITGET_API_PASSPHRASE", "")
BASE          = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
PRODUCT_TYPE  = "USDT-FUTURES"
MARGIN_COIN   = "USDT"

HTTP_TIMEOUT  = 15
IDEMP_TTL     = 180
_seen: Dict[str, float] = {}

def _now_ms() -> str:
    # Bitget v2 requires MILLISECOND timestamp (13 digits)
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path_q: str, body: str) -> str:
    # prehash = timestamp + method + requestPath(+query) + body
    msg = f"{ts}{method.upper()}{path_q}{body}"
    sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _req(method: str, path: str, *, params: Dict[str, Any] = None, body: Dict[str, Any] = None) -> Tuple[int, Any]:
    url = BASE + path
    q = ""
    if params:
        # Bitget includes the query string in the signature
        from urllib.parse import urlencode
        q = "?" + urlencode(params)
        url = url + q
    body_str = json.dumps(body) if body else ""
    ts = _now_ms()
    sign = _sign(ts, method, path + (q or ""), body_str)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }
    resp = requests.request(method, url, data=body_str if body else None, headers=headers, timeout=HTTP_TIMEOUT)
    try:
        return resp.status_code, resp.json()
    except Exception:
        return resp.status_code, {"raw": resp.text}

def _norm_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {(k.lower() if isinstance(k, str) else k): v for k, v in d.items()}

def _to_items(raw: str) -> List[Dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("batch"), list):
            return [_norm_keys(x) for x in data["batch"] if isinstance(x, dict)]
        if isinstance(data, dict):
            return [_norm_keys(data)]
    except Exception:
        pass
    # newline-separated fallback
    items: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            items.append(_norm_keys(json.loads(s)))
    return items

def _prune(now: float) -> None:
    cutoff = now - IDEMP_TTL
    for k, t in list(_seen.items()):
        if t < cutoff:
            _seen.pop(k, None)

def _dup(key: str, now: float) -> bool:
    _prune(now)
    if key in _seen:
        return True
    _seen[key] = now
    return False

# ---------- Bitget helpers ----------
def tv_symbol_to_umcbl(tv_symbol: str) -> str:
    # "SOONUSDT.P" -> "SOONUSDT_UMCBL"
    base = tv_symbol.replace(".P", "")
    return f"{base}_UMCBL"

def get_account_available_usdt() -> float:
    code, js = _req("GET", "/api/v2/mix/account/account",
                    params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if code != 200:
        log.warning("Bitget account GET failed: %s %s", code, js)
        return 0.0
    data = js.get("data") or {}
    if isinstance(data, list) and data:
        data = data[0]
    avail = float(data.get("available", 0) or 0)
    return max(0.0, avail)

def get_contract_info(symbol_umcbl: str) -> Dict[str, Any]:
    code, js = _req("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE})
    if code == 200 and isinstance(js.get("data"), list):
        for it in js["data"]:
            if it.get("symbol") == symbol_umcbl:
                return it
    return {}

def round_down(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(x / step) * step

def get_last_price(symbol_umcbl: str) -> float:
    code, js = _req("GET", "/api/v2/mix/market/ticker", params={"symbol": symbol_umcbl})
    if code == 200 and isinstance(js.get("data"), dict):
        p = float(js["data"].get("last", 0) or 0)
        return p
    return 0.0

def get_open_positions() -> List[Dict[str, Any]]:
    code, js = _req("GET", "/api/v2/mix/position/all-position", params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if code != 200:
        log.warning("Bitget positions GET failed: %s %s", code, js)
        return []
    return js.get("data") or []

def get_symbol_position_size(symbol_umcbl: str) -> float:
    pos = get_open_positions()
    size = 0.0
    for p in pos:
        if p.get("symbol") == symbol_umcbl:
            sz = float(p.get("total", 0) or 0)
            size = max(size, sz)
    return size

def there_is_any_long() -> Tuple[bool, str]:
    pos = get_open_positions()
    for p in pos:
        sz = float(p.get("total", 0) or 0)
        if sz > 0:
            return True, p.get("symbol", "")
    return False, ""

def place_market_buy(symbol_umcbl: str) -> Dict[str, Any]:
    px = get_last_price(symbol_umcbl)
    if px <= 0:
        return {"ok": False, "reason": "no price"}
    avail = get_account_available_usdt()
    if avail <= 0:
        return {"ok": False, "reason": "no balance"}
    contract = get_contract_info(symbol_umcbl)
    size_step = float(contract.get("sizeStep") or contract.get("minTradeNum") or 0.0001)
    qty = (avail * 0.70) / px
    qty = round_down(qty, size_step)
    if qty <= 0:
        return {"ok": False, "reason": "qty<=0", "calc": {"avail": avail, "price": px, "step": size_step}}

    body = {
        "symbol": symbol_umcbl,
        "marginCoin": MARGIN_COIN,
        "size": str(qty),
        "side": "buy",
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": "false"
    }
    log.info("Bitget BUY %s qty=%s (avail=%.4f, px=%.6f)", symbol_umcbl, body["size"], avail, px)
    code, js = _req("POST", "/api/v2/mix/order/place-order", body=body)
    log.info("Bitget BUY resp %s %s", code, js)
    return {"ok": code == 200, "resp": js, "status": code}

def place_market_sell_reduce(symbol_umcbl: str) -> Dict[str, Any]:
    qty = get_symbol_position_size(symbol_umcbl)
    if qty <= 0:
        return {"ok": True, "skipped": "no position"}
    body = {
        "symbol": symbol_umcbl,
        "marginCoin": MARGIN_COIN,
        "size": str(qty),
        "side": "sell",
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": "true"
    }
    log.info("Bitget CLOSE %s qty=%s", symbol_umcbl, body["size"])
    code, js = _req("POST", "/api/v2/mix/order/place-order", body=body)
    log.info("Bitget CLOSE resp %s %s", code, js)
    return {"ok": code == 200, "resp": js, "status": code}

# ---------- routing (매매법은 그대로; 여기서만 호출) ----------
def route_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    action = str(sig.get("action", "")).lower()
    symbol_tv = sig.get("symbol") or ""
    if not action or not symbol_tv:
        return {"ok": False, "reason": "missing action/symbol"}

    symbol_umcbl = tv_symbol_to_umcbl(symbol_tv)
    any_long, long_sym = there_is_any_long()

    if action == "open":
        if any_long:
            return {"ok": True, "skipped": f"position exists on {long_sym}"}
        return place_market_buy(symbol_umcbl)

    if action == "close":
        return place_market_sell_reduce(symbol_umcbl)

    return {"ok": False, "reason": "unknown action"}

# ---------- health ----------
@app.get("/healthz")
async def healthz():
    return {"ok": True, "v": "TVv2-bitget"}

# ---------- TradingView webhook ----------
@app.post("/tv")
async def tv(req: Request):
    try:
        raw = (await req.body()).decode("utf-8", "ignore")
        ctype = req.headers.get("content-type", "")
        log.info("TVv2 recv len=%d ctype=%r", len(raw), ctype)

        try:
            items = _to_items(raw)
        except Exception:
            log.exception("parse error")
            return JSONResponse({"ok": True, "accepted": 0, "items": 0, "err": "parse"}, status_code=200)

        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now = time.time()

        for obj in items:
            log.info("TVv2 ROUTE -> %r", obj)
            act = obj.get("action"); sym = obj.get("symbol")
            if not (isinstance(act, str) and isinstance(sym, str)):
                skipped += 1
                results.append({"ok": False, "reason": "missing"})
                continue

            key = f"{sym}|{act}|{str(obj.get('time',''))}"
            if _dup(key, now):
                results.append({"ok": True, "skipped": "duplicate"})
                continue

            try:
                res = route_signal(obj)
                if res.get("ok"):
                    accepted += 1
                else:
                    skipped += 1
                results.append(res)
            except Exception as e:
                log.exception("route fail")
                results.append({"ok": False, "error": str(e)})

        return JSONResponse({"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "results": results, "v": "TVv2-bitget"}, status_code=200)

    except Exception:
        log.exception("unhandled /tv")
        return JSONResponse({"ok": True, "accepted": 0, "items": 0, "err": "unhandled", "v": "TVv2-bitget"}, status_code=200)
