import asyncio
import time
import uuid
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Body
import httpx

app = FastAPI()

LOGGING_SERVICE_URL = "http://logging-service:8000"
COUNTER_SERVICE_URL = "http://counter-service:8000"

http_client = httpx.AsyncClient(
    limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    timeout=httpx.Timeout(10.0)
)

metrics = {
    "logging_total_time": 0.0,
    "counter_total_time": 0.0,
    "request_count": 0
}

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.post("/transaction")
async def create_transaction(data: dict = Body(...)):
    user_id = data.get("user_id")
    amount = data.get("amount")
    
    if user_id is None or amount is None:
        raise HTTPException(status_code=400, detail="Invalid input")

    transaction_id = str(uuid.uuid4())
    payload = {
        "transaction_id": transaction_id,
        "user_id": user_id,
        "amount": amount
    }

    start_log = time.perf_counter()
    log_task = http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)
    
    start_count = time.perf_counter()
    count_task = http_client.post(f"{COUNTER_SERVICE_URL}/update", json=payload)

    try:
        log_res, count_res = await asyncio.gather(log_task, count_task)
        
        metrics["logging_total_time"] += (time.perf_counter() - start_log)
        metrics["counter_total_time"] += (time.perf_counter() - start_count)
        metrics["request_count"] += 1

        if count_res.status_code != 200:
            raise HTTPException(status_code=500, detail="Counter service error")

        balance_data = count_res.json()
        return {
            "transaction_id": transaction_id, 
            "balance": balance_data.get("balance")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal communication error: {str(e)}")

@app.get("/user/{user_id}")
async def get_user_info(user_id: str):
    log_task = http_client.get(f"{LOGGING_SERVICE_URL}/logs/{user_id}")
    count_task = http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}")
    
    log_res, count_res = await asyncio.gather(log_task, count_task)
    
    return {
        "balance": count_res.json().get("balance", 0),
        "transactions": log_res.json().get("transactions", [])
    }

@app.get("/accounts")
async def get_all_accounts():
    res = await http_client.get(f"{COUNTER_SERVICE_URL}/balances")
    return res.json()

@app.get("/stats")
async def get_stats():
    count = metrics["request_count"] or 1
    return {
        "total_requests": metrics["request_count"],
        "avg_logging_time_ms": (metrics["logging_total_time"] / count) * 1000,
        "avg_counter_time_ms": (metrics["counter_total_time"] / count) * 1000
    }

@app.post("/stats/reset")
async def reset_stats():
    metrics["logging_total_time"] = 0.0
    metrics["counter_total_time"] = 0.0
    metrics["request_count"] = 0
    return {"status": "metrics reset"}