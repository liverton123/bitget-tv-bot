from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Tuple, Optional
import os, time, json, hmac, hashlib, base64, math, logging, requests
from urllib.parse import urlencode, quote

app = FastAPI(title="tv-bot", version="TVv7-bitget")
log = logging.getLogger("uvicorn.error")

# ========= Bitget settings =========
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
BASE           = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

PRODUCT_TYPE   = "USDT-FUTURES"
MARGIN_COIN    = "USDT"
HTTP_TIMEOUT   = 15

# ========= Bot behavior (기존 유지) =========
USE_GLOBAL_LONG_LOCK = False
USE_BALANCE_RATIO    = 0.70
LEVERAGE             = float(os.getenv("LEVERAGE", "1"))
IDEMP_TTL_SEC        = 10

_seen: Dict[str, float] = {}

# ========= HTTP helpers =========
def now_ms() -> str:
    return str(int(time.time() * 1000))  # 13-digit ms

def qs_canonical(params: Dict[str, Any] | None) -> str:
    if not params:
        return ""
    items = sorted((k, "" if v is None else str(v)) for k, v in params.items())
    return urlencode(items, quote_via=quote, safe="")

def sign_v2(ts: str, method: str, path: str, qs: str, body: str) -> str:
    query_part = ("?" + qs) if qs else ""
    prehash = f"{ts}{method.upper()}{path}{query_part}{body}"
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

# ========= Symbol index (핵심) =========
# Bitget 선물 목록을 인덱싱: "AAVEUSDT" -> "AAVEUSDT_UMCBL"
_contracts_index: Dict[str, str] = {}
_contracts_last_load: float = 0.0
CONTRACTS_TTL_SEC = 600  # 10분마다 갱신

def build_contracts_index(force: bool = False) -> None:
    global _contracts_index, _contracts_last_load
    now = time.time()
    if not force and _contracts_index and (now - _contracts_last_load < CONTRACTS_TTL_SEC):
        return
    c, j = req("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE})
    new_index: Dict[str, str] = {}
    if c == 200 and isinstance(j.get("data"), list):
        for it in j["data"]:
            sym = str(it.get("symbol", "")).upper()            # e.g., "AAVEUSDT_UMCBL"
            if "_" in sym:
                base = sym.split("_", 1)[0]                    # "AAVEUSDT"
                # UMCBL(USDT-M) 선호, 그 외 suffix도 수용
                # 이미 값이 있으면 UMCBL을 우선으로 교체
                if base not in new_index or sym.endswith("_UMCBL"):
                    new_index[base] = sym
    else:
        log.warning("contracts fetch failed: %s %s", c, j)

    if new_index:
        _contracts_index = new_index
        _contracts_last_load = now
        log.info("contracts indexed: %d bases (e.g. AAVEUSDT->%s)", len(_contracts_index),
                 _contracts_index.get("AAVEUSDT", "N/A"))

@app.on_event("startup")
async def _warmup():
    try:
        build_contracts_index(force=True)
    except Exception:
        log.exception("contracts warmup failed")

def tv_to_base_symbol(tv_symbol: str) -> str:
    """ 'BINANCE:AAVEUSDT.P' -> 'AAVEUSDT' """
    s = str(tv_symbol).upper().split(":")[-1].strip()
    for suf in (".P", ".PERP", "-PERP"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s

def resolve_tv_symbol(tv_symbol: str) -> Optional[str]:
    """TV 심볼을 Bitget 실제 심볼로. 인덱스에서 O(1) 조회."""
    build_contracts_index()
    base = tv_to_base_symbol(tv_symbol)
    real = _contracts_index.get(base)
    if real:
        return real
    log.warning("symbol resolve failed for %s (base=%s, indexed=%d)", tv_symbol, base, len(_contracts_index))
    return None

# ========= Utils =========
def norm_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {(k.lower() if isinstance(k, str) else k): v for k, v in d.items()}

def parse_tv_payload(raw: str) -> List[Dict[str, Any]]:
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

def round_down(x: float, step: float) -> float:
    return math.floor(x / step) * step if step > 0 else x

# ========= Bitget helpers =========
def get_account_available() -> float:
    c, j = req("GET", "/api/v2/mix/account/account", params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if c != 200:
        log.warning("account GET failed: %s %s", c, j)
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
            if str(it.get("symbol", "")).upper() == symbol.upper():
                return it
    return {}

def get_positions() -> List[Dict[str, Any]]:
    c, j = req("GET", "/api/v2/mix/position/all-position", params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN})
    if c != 200:
        log.warning("positions GET failed: %s %s", c, j)
        return []
    return j.get("data") or []

def get_pos_size(symbol: str) -> float:
    for p in get_positions():
        if str(p.get("symbol", "")).upper() == symbol.upper():
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
        log.warning("BUY skipped: no price for %s", symbol)
        return {"ok": False, "reason": "no price"}

    avail = get_account_available()
    if avail <= 0:
        log.warning("BUY skipped: no balance (avail=%s)", avail)
        return {"ok": False, "reason": "no balance"}

    cinfo = get_contract(symbol)
    step_raw = cinfo.get("sizeStep") or cinfo.get("minTradeNum") or "0.001"
    try:
        step = float(step_raw)
    except Exception:
        step = 0.001

    use = avail * USE_BALANCE_RATIO
    notional = use * max(1.0, LEVERAGE)
    qty = round_down(notional / px, step)
    if qty <= 0:
        calc = {"avail": avail, "use": use, "lev": LEVERAGE, "px": px, "step": step}
        log.warning("BUY skipped: qty<=0 calc=%r", calc)
        return {"ok": False, "reason": "qty<=0", "calc": calc}

    body = {
        "symbol": symbol,                # 반드시 실제 심볼(e.g., AAVEUSDT_UMCBL)
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
        log.info("CLOSE skipped: no position for %s", symbol)
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
    tvsym = sig.get("symbol") or ""
    if not act or not tvsym:
        return {"ok": False, "reason": "missing fields"}

    real_symbol = resolve_tv_symbol(tvsym)
    if not real_symbol:
        return {"ok": False, "reason": f"symbol resolve failed for {tvsym}"}

    if act == "open":
        locked, locked_sym = any_long_open()
        if locked:
            return {"ok": True, "skipped": f"already long {locked_sym}"}
        return place_buy(real_symbol)

    if act == "close":
        return place_close(real_symbol)

    return {"ok": False, "reason": f"unknown action {act}"}

# ========= Health =========
@app.get("/healthz")
async def healthz():
    return {"ok": True, "v": "TVv7-bitget"}

# ========= TradingView webhook =========
@app.post("/tv")
async def tv(req: Request):
    try:
        raw = (await req.body()).decode("utf-8", "ignore")
        ctype = req.headers.get("content-type", "")
        log.info("TVv7 recv len=%d ctype=%r", len(raw), ctype)

        items = parse_tv_payload(raw)
        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now_t = time.time()

        for obj in items:
            log.info("TVv7 ROUTE -> %r", obj)
            act = obj.get("action"); sym = obj.get("symbol")
            if not isinstance(act, str) or not isinstance(sym, str):
                skipped += 1; results.append({"ok": False, "reason": "invalid object"}); continue

            key = f"{sym}|{act}|{str(obj.get('time',''))}"
            if is_dup(key, now_t):
                res = {"ok": True, "skipped": "duplicate"}
                log.info("TVv7 RESULT -> %r", res)
                results.append(res); continue

            res = route_signal(obj)
            log.info("TVv7 RESULT -> %r", res)
            if res.get("ok"): accepted += 1
            else: skipped += 1
            results.append(res)

        return JSONResponse({"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "results": results, "v": "TVv7-bitget"})
    except Exception:
        log.exception("unhandled /tv")
        return JSONResponse({"ok": True, "accepted": 0, "items": 0, "err": "unhandled", "v": "TVv7-bitget"})
