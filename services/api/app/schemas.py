from pydantic import BaseModel, Field
from typing import Optional

class TradeIn(BaseModel):
    trade_id: str = Field(..., example="T-1000")
    timestamp: str = Field(..., example="2025-09-18T10:00:00Z")
    instrument: str = Field(..., example="AAPL")
    side: str = Field(..., example="BUY")
    quantity: int = Field(..., ge=1)
    price: float = Field(..., gt=0)
    trader_id: Optional[str]
    venue: Optional[str]
