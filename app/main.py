# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Tuple
import os, time, json, hmac, hashlib, base64, math, logging, requests
from urllib.parse import urlencode, quote

app = FastAPI(title="tv-bot", version="TVv2-bitget")
log = logging.getLogger("uvicorn.error")

# ========= Bitget settings =========
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
BASE           = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

PRODUCT_TYPE   = "USDT-FUTURES"
MARGIN_COIN    = "USDT"
HTTP_TIMEOUT   = 15

# One-position lock (only one long across all symbols)
USE_GLOBAL_LONG_LOCK = True
# Use 70% of available balance for entry
USE_BALANCE_RATIO = 0.70

# Idempotency (deduplicate same (symbol, action, time) within TTL)
IDEMP_TTL_SEC = 180
_seen: Dict[str, float] = {}

# ========= Time & signing =========
def now_ms() -> str:
    # Bitget v2 requires 13-digit millisecond timestamp
    return str(int(time.time() * 1000))

def qs_canonical(params: Dict[str, Any] | None) -> str:
    """Bitget v2 canonical query: key ASC, RFC3986 encoding, no leading '?'."""
    if not params:
        return ""
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    return urlencode(items, quote_via=quote, safe="")

def sign_v2(ts: str, method: str, path: str, qs: str, body: str) -> str:
    """
    prehash = timestamp + method + requestPath + queryString + body
              (queryString is concatenated WITHOUT '?')
    """
    prehash = f"{ts}{method.upper()}{path}{qs}{body}"
    sig = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def req(method: str, path: str, *, params: Dict[str, Any] | None = None, body: Dict[str, Any] | None = None) -> Tuple[int, Dict[str, Any]]:
    qs = qs_canonical(params)
    url = BASE + path + (("?" + qs) if qs else "")
    body_str = json.dumps(body) if body else ""
    ts = now_ms()
    sig = sign_v2(ts, method, path, qs, body_str)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sig,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json",
    }
    r = requests.request(method, url, data=body_str if body else None, headers=headers, timeout=HTTP_TIMEOUT)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text}

# ========= Utils =========
def norm_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {(k.lower() if isinstance(k, str) else k): v for k, v in d.items()}

def parse_tv_payload(raw: str) -> List[Dict[str, Any]]:
    """
    Accept:
      {"batch":[{...},{...}]}  or  single object  or  newline separated objects
    Returns a list of dicts with lower-cased keys.
    """
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("batch"), list):
            return [norm_keys(x) for x in data["batch"] if isinstance(x, dict)]
        if isinstance(data, dict):
            return [norm_keys(data)]
    except Exception:
        pass
    items: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            items.append(norm_keys(json.loads(s)))
    return items

def is_dup(key: str, now_t: float) -> bool:
    cutoff = now_t - IDEMP_TTL_SEC
    for k, t in list(_seen.items()):
        if t < cutoff:
            _seen.pop(k, None)
    if key in _seen:
        return True
    _seen[key] = now_t
    return False

def tv_symbol_to_umcbl(tv_symbol: str) -> str:
    # TradingView synthetic: e.g., BANANAUSDT.P  -> BANANAUSDT_UMCBL (Bitget USDT-M perp)
    return tv_symbol.replace(".P", "") + "_UMCBL"

def round_down(x: float, step: float) -> float:
    return math.floor(x / step) * step if step > 0 else x

# ========= Bitget helpers =========
def get_account_available() -> float:
    c, j = req("GET", "/api/v2/mix/account/account", params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if c != 200:
        log.warning("Bitget account GET failed: %s %s", c, j)
        return 0.0
    data = j.get("data")
    if isinstance(data, list) and data:
        data = data[0]
    if isinstance(data, dict):
        try:
            return float(data.get("available", 0) or 0)
        except Exception:
            return 0.0
    return 0.0

def get_last_price(symbol: str) -> float:
    c, j = req("GET", "/api/v2/mix/market/ticker", params={"symbol": symbol})
    if c == 200 and isinstance(j.get("data"), dict):
        try:
            return float(j["data"].get("last", 0) or 0)
        except Exception:
            return 0.0
    return 0.0

def get_contract(symbol: str) -> Dict[str, Any]:
    c, j = req("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE})
    if c == 200:
        for it in j.get("data", []):
            if it.get("symbol") == symbol:
                return it
    return {}

def get_positions() -> List[Dict[str, Any]]:
    c, j = req("GET", "/api/v2/mix/position/all-position", params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if c != 200:
        log.warning("Bitget positions GET failed: %s %s", c, j)
        return []
    return j.get("data") or []

def get_pos_size(symbol: str) -> float:
    for p in get_positions():
        if p.get("symbol") == symbol:
            try:
                return float(p.get("total", 0) or 0)
            except Exception:
                return 0.0
    return 0.0

def any_long_open() -> Tuple[bool, str]:
    if not USE_GLOBAL_LONG_LOCK:
        return False, ""
    for p in get_positions():
        try:
            tot = float(p.get("total", 0) or 0)
        except Exception:
            tot = 0.0
        if tot > 0:
            return True, p.get("symbol", "")
    return False, ""

# ========= Order placement =========
def place_buy(symbol: str) -> Dict[str, Any]:
    px = get_last_price(symbol)
    if px <= 0:
        return {"ok": False, "reason": "no price"}
    avail = get_account_available()
    if avail <= 0:
        return {"ok": False, "reason": "no balance"}

    cinfo = get_contract(symbol)
    step_raw = cinfo.get("sizeStep") or cinfo.get("minTradeNum") or "0.001"
    try:
        step = float(step_raw)
    except Exception:
        step = 0.001

    use = avail * USE_BALANCE_RATIO
    qty = round_down(use / px, step)
    if qty <= 0:
        return {"ok": False, "reason": "qty<=0", "calc": {"avail": avail, "use": use, "px": px, "step": step}}

    body = {
        "symbol": symbol,
        "marginCoin": MARGIN_COIN,
        "size": str(qty),
        "side": "buy",
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": "false",
    }
    log.info("BUY %s %s", symbol, body)
    c, j = req("POST", "/api/v2/mix/order/place-order", body=body)
    log.info("BUY resp %s %s", c, j)
    return {"ok": c == 200, "resp": j, "qty": qty, "price": px}

def place_close(symbol: str) -> Dict[str, Any]:
    qty = get_pos_size(symbol)
    if qty <= 0:
        return {"ok": True, "skipped": "no position"}
    body = {
        "symbol": symbol,
        "marginCoin": MARGIN_COIN,
        "size": str(qty),
        "side": "sell",
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": "true",
    }
    log.info("CLOSE %s %s", symbol, body)
    c, j = req("POST", "/api/v2/mix/order/place-order", body=body)
    log.info("CLOSE resp %s %s", c, j)
    return {"ok": c == 200, "resp": j, "qty": qty}

# ========= Routing =========
def route_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    act = str(sig.get("action", "")).lower()
    sym_tv = sig.get("symbol") or ""
    if not act or not sym_tv:
        return {"ok": False, "reason": "missing fields"}
    symbol = tv_symbol_to_umcbl(sym_tv)

    if act == "open":
        locked, locked_sym = any_long_open()
        if locked:
            return {"ok": True, "skipped": f"already long {locked_sym}"}
        return place_buy(symbol)

    if act == "close":
        return place_close(symbol)

    return {"ok": False, "reason": f"unknown action {act}"}

# ========= Health =========
@app.get("/healthz")
async def healthz():
    return {"ok": True, "v": "TVv2-bitget"}

# ========= TradingView webhook =========
@app.post("/tv")
async def tv(req: Request):
    try:
        raw = (await req.body()).decode("utf-8", "ignore")
        ctype = req.headers.get("content-type", "")
        log.info("TVv2 recv len=%d ctype=%r", len(raw), ctype)

        items = parse_tv_payload(raw)  # keeps batch order
        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now_t = time.time()

        for obj in items:
            log.info("TVv2 ROUTE -> %r", obj)
            act = obj.get("action")
            sym = obj.get("symbol")
            if not isinstance(act, str) or not isinstance(sym, str):
                skipped += 1
                results.append({"ok": False, "reason": "invalid object"})
                continue

            # idempotency key: symbol|action|time (string)
            key = f"{sym}|{act}|{str(obj.get('time',''))}"
            if is_dup(key, now_t):
                results.append({"ok": True, "skipped": "duplicate"})
                continue

            res = route_signal(obj)
            if res.get("ok"):
                accepted += 1
            else:
                skipped += 1
            results.append(res)

        # Always 200 back to TradingView to avoid alert suppression
        return JSONResponse(
            {"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "results": results, "v": "TVv2-bitget"},
            status_code=200,
        )
    except Exception:
        log.exception("unhandled /tv")
        return JSONResponse({"ok": True, "accepted": 0, "items": 0, "err": "unhandled", "v": "TVv2-bitget"}, status_code=200)
