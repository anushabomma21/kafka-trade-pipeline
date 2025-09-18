from fastapi import FastAPI, HTTPException
from schemas import TradeIn
from producer import publish_trade

app = FastAPI(title="Trade Ingest API")

@app.post("/trades", status_code=202)
async def post_trade(trade: TradeIn):
    try:
        record = trade.dict()
        publish_trade(record)
        return {"status": "accepted", "trade_id": trade.trade_id}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))
