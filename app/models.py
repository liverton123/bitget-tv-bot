from pydantic import BaseModel
from typing import Optional

class TVSignal(BaseModel):
    # 알람 1개로 진입/종료 모두 다룸
    action: str        # "open" | "close"
    side: str          # "long" 고정
    symbol: str        # 예: "BINANCE:BTCUSDT.P"
    price: str         # "{{close}}"
    time: str          # "{{time}}"
    tag: Optional[str] = None
