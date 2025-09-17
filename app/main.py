from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Tuple, Optional
import os, time, json, hmac, hashlib, base64, math, logging, requests
from urllib.parse import urlencode, quote

app = FastAPI(title="tv-bot", version="TVv5.1-bitget")
log = logging.getLogger("uvicorn.error")

# ========= Bitget settings =========
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
BASE           = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

PRODUCT_TYPE   = "USDT-FUTURES"
MARGIN_COIN    = "USDT"
HTTP_TIMEOUT   = 15

# ========= Bot behavior =========
USE_GLOBAL_LONG_LOCK = False            # 초기엔 OFF (원하면 True)
USE_BALANCE_RATIO    = 0.70
LEVERAGE             = float(os.getenv("LEVERAGE", "1"))  # Render env에 LEVERAGE=3 등
IDEMP_TTL_SEC        = 10
ALLOW_SPOT_FALLBACK  = False            # 속도 우선
CONTRACTS_TTL_SEC    = 600              # 선물 심볼 캐시 주기

_seen: Dict[str, float] = {}

# ========= Custom overrides (필요 종목만 예외 매핑) =========
# TV 심볼 → Bitget 심볼
CUSTOM_OVERRIDES: Dict[str, str] = {
    "PENDLEUSDT.P": "PENDLEUSDT_UMCBL",   # 필요시 정확 심볼명을 넣어 사용
    # "BIOUSDT.P": "BIOUSDT_UMCBL",
    # "DOGEUSDT.P": "DOGEUSDT_UMCBL",
}

# ========= Contracts cache (빠른 해석) =========
_contracts_set: set[str] = set()    # e.g. {"BTCUSDT_UMCBL", ...}
_contracts_last_load: float = 0.0

# ========= Basic HTTP helpers =========
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

# ========= Contracts cache loader =========
def load_contracts_if_stale(force: bool = False) -> None:
    global _contracts_set, _contracts_last_load
    now = time.time()
    if not force and _contracts_set and (now - _contracts_last_load < CONTRACTS_TTL_SEC):
        return
    c, j = req("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE})
    fresh: set[str] = set()
    if c == 200 and isinstance(j.get("data"), list):
        for it in j["data"]:
            s = it.get("symbol")
            if isinstance(s, str):
                fresh.add(s.upper())
    else:
        log.warning("contracts fetch failed: %s %s", c, j)
    if fresh:
        _contracts_set = fresh
        _contracts_last_load = now
        log.info("contracts loaded: %d symbols", len(_contracts_set))

@app.on_event("startup")
async def _warmup():
    try:
        load_contracts_if_stale(force=True)
    except Exception:
        log.exception("contracts warmup failed")

# ========= Symbol resolver (강화 로그) =========
def resolve_tv_symbol(tv_symbol: str) -> Optional[str]:
    """
    TV 심볼 -> Bitget 실제 심볼 (선물 우선).
    순서:
      0) CUSTOM_OVERRIDES
      1) 캐시된 선물셋에서 _UMCBL/_DMCBL/_CMCBL 조회
      2) 같은 prefix로 시작하는 임의 선물 심볼
      3) (옵션) 현물 _SPBL 조회
    실패 시, 어떤 후보를 시도했고 캐시 상태가 어떤지 로그를 남긴다.
    """
    load_contracts_if_stale()

    key = str(tv_symbol).upper()
    if key in CUSTOM_OVERRIDES:
        res = CUSTOM_OVERRIDES[key]
        log.info("resolver override %s -> %s", key, res)
        return res

    sym = key.split(":")[-1]      # "BINANCE:DOGEUSDT.P" -> "DOGEUSDT.P"
    if sym.endswith(".P"):
        sym = sym[:-2]            # "DOGEUSDT"
    base = sym

    tried = []
    for suf in ("_UMCBL", "_DMCBL", "_CMCBL"):
        cand = base + suf
        tried.append(cand)
        if cand in _contracts_set:
            return cand

    # 같은 prefix로 시작하는 선물 심볼 찾기 (드문 케이스)
    matches = [c for c in _contracts_set if c.startswith(base + "_")]
    if matches:
        log.warning("resolver fallback matches for %s -> %s", tv_symbol, matches[:3])
        return matches[0]

    # 현물 폴백(기본 비활성)
    if ALLOW_SPOT_FALLBACK:
        spot = base + "_SPBL"
        c, j = req("GET", "/api/v2/mix/market/ticker", params={"symbol": spot})
        if c == 200 and isinstance(j.get("data"), dict):
            try:
                if float(j["data"].get("last", 0) or 0) > 0:
                    log.warning("resolver spot fallback %s -> %s", tv_symbol, spot)
                    return spot
            except Exception:
                pass

    log.warning("symbol resolve failed for %s (tried=%s, contracts_loaded=%d)",
                tv_symbol, tried, len(_contracts_set))
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
    sym_tv = sig.get("symbol") or ""
    if not act or not sym_tv:
        return {"ok": False, "reason": "missing fields"}

    symbol = resolve_tv_symbol(sym_tv)
    if not symbol:
        msg = f"symbol resolve failed for {sym_tv}"
        log.warning(msg)
        return {"ok": False, "reason": msg}

    if act == "open":
        locked, locked_sym = any_long_open()
        if locked:
            msg = f"already long {locked_sym}"
            log.info("OPEN skipped: %s", msg)
            return {"ok": True, "skipped": msg}
        return place_buy(symbol)

    if act == "close":
        return place_close(symbol)

    return {"ok": False, "reason": f"unknown action {act}"}

# ========= Health =========
@app.get("/healthz")
async def healthz():
    return {"ok": True, "v": "TVv5.1-bitget"}

# ========= TradingView webhook =========
@app.post("/tv")
async def tv(req: Request):
    try:
        raw = (await req.body()).decode("utf-8", "ignore")
        ctype = req.headers.get("content-type", "")
        log.info("TVv5.1 recv len=%d ctype=%r", len(raw), ctype)

        items = parse_tv_payload(raw)
        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now_t = time.time()

        for obj in items:
            log.info("TVv5.1 ROUTE -> %r", obj)
            act = obj.get("action")
            sym = obj.get("symbol")
            if not isinstance(act, str) or not isinstance(sym, str):
                skipped += 1
                results.append({"ok": False, "reason": "invalid object"})
                continue

            key = f"{sym}|{act}|{str(obj.get('time',''))}"
            if is_dup(key, now_t):
                res = {"ok": True, "skipped": "duplicate"}
                log.info("TVv5.1 RESULT -> %r", res)
                results.append(res)
                continue

            res = route_signal(obj)
            log.info("TVv5.1 RESULT -> %r", res)
            if res.get("ok"):
                accepted += 1
            else:
                skipped += 1
            results.append(res)

        return JSONResponse(
            {"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "results": results, "v": "TVv5.1-bitget"},
            status_code=200,
        )
    except Exception:
        log.exception("unhandled /tv")
        return JSONResponse({"ok": True, "accepted": 0, "items": 0, "err": "unhandled", "v": "TVv5.1-bitget"}, status_code=200)
