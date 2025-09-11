from pydantic import BaseModel
from typing import Optional

class TVSignal(BaseModel):
    action: str        # "open" | "close"
    side: str          # "long"
    symbol: str        # e.g. "BINANCE:BTCUSDT.P"
    price: str         # "{{close}}"
    time: str          # "{{time}}"
    tag: Optional[str] = None
