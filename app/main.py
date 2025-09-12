# main.py
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from typing import Any, Dict, List
import json, logging, time

app = FastAPI()
log = logging.getLogger("uvicorn.error")

# ---- idempotency: 180s window to drop duplicates
IDEMP_TTL = 180
_seen: Dict[str, float] = {}

def _prune_now(now: float) -> None:
    drop = [k for k, t in _seen.items() if now - t > IDEMP_TTL]
    for k in drop:
        _seen.pop(k, None)

def _is_dup(key: str, now: float) -> bool:
    _prune_now(now)
    if key in _seen:
        return True
    _seen[key] = now
    return False

def _to_dict(s: str) -> Dict[str, Any]:
    """best-effort JSON parse from string"""
    s = s.strip()
    return json.loads(s)

def _split_multiline(body: str) -> List[str]:
    # tolerate newline-separated JSON objects
    parts = [p.strip() for p in body.splitlines() if p.strip()]
    return parts if len(parts) > 1 else [body.strip()]

def _normalize_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    # lower-case keys; keep original values
    return { (k.lower() if isinstance(k, str) else k): v for k, v in d.items() }

# ---- hook: connect to your existing trading executor here
def route_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    """
    DO NOT change your trading logic. Just call the existing functions here.
    For example:
        if sig["action"] == "open":
            return trader.open_long(sig["symbol"], price=sig.get("price"))
        elif sig["action"] == "close":
            return trader.close_long(sig["symbol"], price=sig.get("price"))
    """
    # placeholder: only log
    log.info(f"ROUTED: {sig}")
    return {"ok": True}

@app.post("/tv")
async def tv(request: Request):
    """
    Accepts:
      - {"action": "...", ...}
      - {"batch":[ {...}, {...} ]}
      - newline-separated {...}\n{...}\n...
    Always returns 200 with per-item results (never 4xx to TV).
    """
    try:
        raw = (await request.body()).decode("utf-8", errors="ignore")
        ctype = request.headers.get("content-type", "").lower()
        log.info(f"TV webhook received. ctype={ctype!r}, size={len(raw)}")

        items: List[Dict[str, Any]] = []

        # 1) try JSON first (works for both object and batch)
        parsed: Any = None
        try:
            parsed = json.loads(raw)
        except Exception:
            # 2) maybe newline-separated JSONs
            try:
                parts = _split_multiline(raw)
                items = [_normalize_keys(_to_dict(p)) for p in parts]
            except Exception:
                log.exception("TV parse failed (multiline fallback)")
                return JSONResponse({"ok": True, "accepted": 0, "error": "parse"}, status_code=200)

        if parsed is not None:
            if isinstance(parsed, dict) and "batch" in parsed and isinstance(parsed["batch"], list):
                items = [_normalize_keys(i) for i in parsed["batch"] if isinstance(i, dict)]
            elif isinstance(parsed, dict):
                items = [_normalize_keys(parsed)]
            else:
                log.error("TV parse: unexpected root type: %s", type(parsed))
                return JSONResponse({"ok": True, "accepted": 0, "error": "schema"}, status_code=200)

        # 3) route each item
        accepted, skipped = 0, 0
        results: List[Dict[str, Any]] = []
        now = time.time()

        for obj in items:
            # minimal schema check
            act = obj.get("action")
            side = obj.get("side")
            sym  = obj.get("symbol")
            if not (isinstance(act, str) and isinstance(sym, str)):
                skipped += 1
                results.append({"ok": False, "reason": "missing action/symbol"})
                continue

            # idempotency key (TV often retries within seconds)
            when = str(obj.get("time", ""))  # may be epoch or iso string
            key  = f"{sym}|{act}|{when}"
            if _is_dup(key, now):
                results.append({"ok": True, "skipped": "duplicate"})
                continue

            try:
                res = route_signal(obj)  # <--- your existing trading code is called here
                accepted += 1
                results.append({"ok": True, "result": res})
            except Exception as e:
                # never propagate 4xx/5xx back to TV
                log.exception("route_signal failed for %s", obj)
                results.append({"ok": False, "error": str(e)})

        return JSONResponse({"ok": True, "accepted": accepted, "skipped": skipped, "items": len(items), "results": results}, status_code=200)

    except Exception:
        # protect TV from any server error loops
        log.exception("unhandled /tv")
        return JSONResponse({"ok": True, "accepted": 0, "error": "unhandled"}, status_code=200)
