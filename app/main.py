# app/main.py
# TV_WEBHOOK_V2  (배포 확인용 표식)
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List
import json, logging, time

app = FastAPI(title="tv-bot", version="TVv2")
log = logging.getLogger("uvicorn.error")

# --- idempotency: 중복 알림 180초 차단 ---
IDEMP_TTL = 180
_seen: Dict[str, float] = {}

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

def _norm(d: Dict[str, Any]) -> Dict[str, Any]:
    # 키만 소문자화 (값은 보존)
    return { (k.lower() if isinstance(k, str) else k): v for k, v in d.items() }

def _to_items(raw: str) -> List[Dict[str, Any]]:
    """배치/단일/개행분리 포맷 모두 허용"""
    raw = raw.strip()
    if not raw:
        return []
    # 1) JSON 파싱 시도
    try:
        data = json.loads(raw)
        if isinstance(data, dict) and isinstance(data.get("batch"), list):
            return [_norm(x) for x in data["batch"] if isinstance(x, dict)]
        if isinstance(data, dict):
            return [_norm(data)]
    except Exception:
        pass
    # 2) 개행 분리 포맷 허용
    items: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        items.append(_norm(json.loads(s)))
    return items

# --- 여기서 기존 매매 로직 호출만 한다. 매매법 수정 금지 ---
def route_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    """
    예시:
        action = sig["action"]   # "open" | "close"
        symbol = sig["symbol"]
        price  = sig.get("price")
        if action == "open":
            return trader.open_long(symbol, price=price)   # 기존 함수 호출
        else:
            return trader.close_long(symbol, price=price)
    """
    log.info(f"TVv2 ROUTE -> {sig}")  # 배포 확인용
    return {"ok": True}

# --- 배포 확인용 헬스체크 ---
@app.get("/healthz")
async def healthz():
    return {"ok": True, "v": "TVv2"}

# --- 트레이딩뷰 웹훅 (배치/단일/개행 모두 허용, 항상 200 OK) ---
@app.post("/tv")
async def tv(req: Request):
    try:
        raw = (await req.body()).decode("utf-8", "ignore")
        ctype = req.headers.get("content-type", "")
        log.info("TVv2 recv len=%d ctype=%r", len(raw), ctype)

        try:
            items = _to_items(raw)
        except Exception:
            log.exception("TVv2 parse error")
            return JSONResponse({"ok": True, "accepted": 0, "skipped": 0, "items": 0, "err": "parse"}, status_code=200)

        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now = time.time()

        for obj in items:
            act = obj.get("action")
            sym = obj.get("symbol")
            if not (isinstance(act, str) and isinstance(sym, str)):
                skipped += 1
                results.append({"ok": False, "reason": "missing action/symbol"})
                continue

            key = f"{sym}|{act}|{str(obj.get('time',''))}"
            if _dup(key, now):
                results.append({"ok": True, "skipped": "duplicate"})
                continue

            try:
                res = route_signal(obj)  # ← 여기서 기존 주문 함수만 호출
                accepted += 1
                results.append({"ok": True, "result": res})
            except Exception as e:
                log.exception("TVv2 route failed")
                results.append({"ok": False, "error": str(e)})

        return JSONResponse({"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "v": "TVv2", "results": results}, status_code=200)

    except Exception:
        log.exception("TVv2 unhandled")
        return JSONResponse({"ok": True, "accepted": 0, "items": 0, "v": "TVv2", "err": "unhandled"}, status_code=200)
