from pydantic import BaseModel, Field
from typing import Optional

class TVSignal(BaseModel):
    action: str        # "open" | "close"
    side: str          # "long" (본 전략은 롱 전용)
    symbol: str        # TradingView 심볼 (예: "BINANCE:BTCUSDT.P")
    price: str
    time: str
    tag: Optional[str] = None
    # 필요시 추가 필드 허용
