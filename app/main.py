# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List, Tuple
import os, hmac, hashlib, base64, json, time, logging, math, requests

app = FastAPI(title="tv-bot", version="TVv2-bitget")
log = logging.getLogger("uvicorn.error")

# ==== Bitget settings ====
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
BASE           = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
PRODUCT_TYPE   = "USDT-FUTURES"
MARGIN_COIN    = "USDT"
HTTP_TIMEOUT   = 15

# idempotency (중복 방지)
IDEMP_TTL = 180
_seen: Dict[str, float] = {}

# ---- time/sign ----
def _now_ms() -> str:
    # Bitget v2: milliseconds timestamp required (13 digits)
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path_q: str, body: str) -> str:
    msg = f"{ts}{method.upper()}{path_q}{body}"
    sig = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def _req(method: str, path: str, *, params: Dict[str, Any] = None, body: Dict[str, Any] = None) -> Tuple[int, Any]:
    url = BASE + path
    q = ""
    if params:
        from urllib.parse import urlencode
        q = "?" + urlencode(params)
        url += q
    body_str = json.dumps(body) if body else ""
    ts = _now_ms()
    sign = _sign(ts, method, path + (q or ""), body_str)
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,           # ms
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }
    r = requests.request(method, url, data=body_str if body else None, headers=headers, timeout=HTTP_TIMEOUT)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text}

# ---- utils ----
def _norm(d: Dict[str, Any]) -> Dict[str, Any]:
    return { (k.lower() if isinstance(k, str) else k): v for k,v in d.items() }

def _to_items(raw: str) -> List[Dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
        # batch 배열 또는 단건 모두 지원
        if isinstance(data, dict) and isinstance(data.get("batch"), list):
            return [_norm(x) for x in data["batch"] if isinstance(x, dict)]
        if isinstance(data, dict):
            return [_norm(data)]
    except Exception:
        pass
    items = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            items.append(_norm(json.loads(s)))
    return items

def _dup(key: str, now_t: float) -> bool:
    cutoff = now_t - IDEMP_TTL
    for k, t in list(_seen.items()):
        if t < cutoff:
            _seen.pop(k, None)
    if key in _seen:
        return True
    _seen[key] = now_t
    return False

def tv_symbol_to_umcbl(tv_symbol: str) -> str:
    return tv_symbol.replace(".P", "") + "_UMCBL"

# ---- Bitget helpers ----
def get_account_available() -> float:
    c,j = _req("GET", "/api/v2/mix/account/account", params={"productType": PRODUCT_TYPE,"marginCoin": MARGIN_COIN})
    if c != 200:
        log.warning("Bitget account GET failed: %s %s", c, j)
        return 0.0
    data = j.get("data") or {}
    if isinstance(data, list) and data:
        data = data[0]
    return float(data.get("available", 0) or 0)

def get_last_price(symbol: str) -> float:
    c,j = _req("GET", "/api/v2/mix/market/ticker", params={"symbol": symbol})
    if c==200 and isinstance(j.get("data"), dict):
        return float(j["data"].get("last",0) or 0)
    return 0.0

def get_contract(symbol: str) -> Dict[str,Any]:
    c,j = _req("GET","/api/v2/mix/market/contracts",params={"productType":PRODUCT_TYPE})
    if c==200:
        for it in j.get("data",[]):
            if it.get("symbol")==symbol:
                return it
    return {}

def round_down(x: float, step: float) -> float:
    return math.floor(x/step)*step if step>0 else x

def get_positions() -> List[Dict[str,Any]]:
    c,j = _req("GET","/api/v2/mix/position/all-position",params={"productType":PRODUCT_TYPE,"marginCoin":MARGIN_COIN})
    if c!=200:
        log.warning("Bitget positions GET failed: %s %s", c, j)
        return []
    return j.get("data") or []

def get_pos_size(symbol: str) -> float:
    for p in get_positions():
        if p.get("symbol")==symbol:
            return float(p.get("total",0) or 0)
    return 0.0

def any_long() -> Tuple[bool,str]:
    for p in get_positions():
        if float(p.get("total",0) or 0)>0:
            return True,p.get("symbol","")
    return False,""

def place_buy(symbol: str) -> Dict[str,Any]:
    px = get_last_price(symbol)
    avail = get_account_available()
    if px<=0:  return {"ok":False,"reason":"no price"}
    if avail<=0:return {"ok":False,"reason":"no balance"}
    cinfo = get_contract(symbol)
    step  = float(cinfo.get("sizeStep") or cinfo.get("minTradeNum") or 0.001)
    qty   = round_down((avail*0.7)/px, step)
    if qty<=0: return {"ok":False,"reason":"qty<=0","calc":{"avail":avail,"px":px,"step":step}}
    body = {"symbol":symbol,"marginCoin":MARGIN_COIN,"size":str(qty),"side":"buy","orderType":"market","timeInForceValue":"normal","reduceOnly":"false"}
    log.info("BUY %s %s",symbol,body)
    c,j = _req("POST","/api/v2/mix/order/place-order",body=body)
    log.info("BUY resp %s %s",c,j)
    return {"ok":c==200,"resp":j}

def place_close(symbol: str) -> Dict[str,Any]:
    qty = get_pos_size(symbol)
    if qty<=0:
        return {"ok":True,"skipped":"no position"}
    body = {"symbol":symbol,"marginCoin":MARGIN_COIN,"size":str(qty),"side":"sell","orderType":"market","timeInForceValue":"normal","reduceOnly":"true"}
    log.info("CLOSE %s %s",symbol,body)
    c,j = _req("POST","/api/v2/mix/order/place-order",body=body)
    log.info("CLOSE resp %s %s",c,j)
    return {"ok":c==200,"resp":j}

# ---- routing ----
def route_signal(sig: Dict[str,Any]) -> Dict[str,Any]:
    act = str(sig.get("action","")).lower()
    sym_tv = sig.get("symbol") or ""
    if not act or not sym_tv:
        return {"ok":False,"reason":"missing"}
    sym = tv_symbol_to_umcbl(sym_tv)
    long_on, long_sym = any_long()
    if act=="open":
        if long_on:
            return {"ok":True,"skipped":f"already long {long_sym}"}
        return place_buy(sym)
    if act=="close":
        return place_close(sym)
    return {"ok":False,"reason":"unknown action"}

# ---- health ----
@app.get("/healthz")
async def healthz():
    return {"ok":True,"v":"TVv2-bitget"}

# ---- TradingView webhook ----
@app.post("/tv")
async def tv(req: Request):
    try:
        raw=(await req.body()).decode("utf-8","ignore")
        log.info("TVv2 recv len=%d ctype=%r",len(raw),req.headers.get("content-type",""))
        items=_to_items(raw)              # {"batch":[...]} 또는 단건 모두 파싱
        accepted,skipped=0,0
        results=[]
        now=time.time()
        for obj in items:                  # 들어온 순서대로 순차 처리 (동일 봉 다중 이벤트 보장)
            log.info("TVv2 ROUTE -> %r",obj)
            act=obj.get("action"); sym=obj.get("symbol")
            if not (isinstance(act,str) and isinstance(sym,str)):
                skipped+=1; results.append({"ok":False,"reason":"missing"}); continue
            key=f"{sym}|{act}|{str(obj.get('time',''))}"
            if _dup(key,now):
                results.append({"ok":True,"skipped":"duplicate"}); continue
            res=route_signal(obj)
            if res.get("ok"): accepted+=1
            else: skipped+=1
            results.append(res)
        return JSONResponse({"ok":True,"accepted":accepted,"skipped":skipped,"items":len(items),"results":results,"v":"TVv2-bitget"},status_code=200)
    except Exception:
        log.exception("unhandled /tv")
        return JSONResponse({"ok":True,"accepted":0,"items":0,"err":"unhandled","v":"TVv2-bitget"},status_code=200)
