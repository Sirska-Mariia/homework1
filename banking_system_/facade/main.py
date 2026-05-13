import os
import asyncio
import uuid
import time
from fastapi import FastAPI, HTTPException, Body
import httpx
import hazelcast

app = FastAPI()

HZ_ADDRESSES = os.getenv("HZ_ADDRESSES", "hazelcast-service:5701")
MQ_QUEUE_NAME = os.getenv("MQ_QUEUE_NAME", "transaction-queue")
LOGGING_SERVICE_URL = os.getenv("LOGGING_SERVICE_URL", "http://logging-service:8000")
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE_URL", "http://counter-service:8000")

hz_client = hazelcast.HazelcastClient(
    cluster_members=[HZ_ADDRESSES],
    cluster_name="dev",
    smart_routing=True
)
transaction_queue = hz_client.get_queue(MQ_QUEUE_NAME).blocking()
http_client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

stats = {
    "total_logging_time": 0.0,
    "total_counter_time": 0.0,
    "request_count": 0
}
stats_lock = asyncio.Lock()

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()
    hz_client.shutdown()

@app.post("/transaction")
async def create_transaction(data: dict = Body(...)):
    user_id = data.get("user_id")
    amount = data.get("amount")
    if user_id is None or amount is None:
        raise HTTPException(status_code=400, detail="Invalid input")

    transaction_id = str(uuid.uuid4())
    payload = {"transaction_id": transaction_id, "user_id": user_id, "amount": amount}

    t0 = time.perf_counter()
    try:
        asyncio.create_task(http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload))
    except Exception as e:
        print(f"Logging failed: {e}")
    t1 = time.perf_counter()
    logging_time = t1 - t0

    t2 = time.perf_counter()
    try:
        transaction_queue.put(payload) 
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MQ Error: {str(e)}")
    t3 = time.perf_counter()
    counter_time = t3 - t2

    async with stats_lock:
        stats["total_logging_time"] += logging_time
        stats["total_counter_time"] += counter_time
        stats["request_count"] += 1

    return {"status": "Transaction accepted (queued)", "transaction_id": transaction_id}

@app.get("/user/{user_id}")
async def get_user_info(user_id: str):
    try:
        log_res, count_res = await asyncio.gather(
            http_client.get(f"{LOGGING_SERVICE_URL}/logs/{user_id}"),
            http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}")
        )
        balance = count_res.json().get("balance") if count_res.status_code == 200 else None
        return {"balance": balance, "transactions": log_res.json().get("transactions", [])}
    except Exception:
         return {"balance": None, "transactions": [], "status": "Counter service unreachable"}

@app.get("/accounts")
async def get_all_accounts():
    try:
        res = await http_client.get(f"{COUNTER_SERVICE_URL}/balances")
        return res.json()
    except Exception as e:
        return {"error": f"Counter service not found or error: {str(e)}"}

@app.get("/stats")
async def get_stats():
    async with stats_lock:
        if stats["request_count"] == 0:
            return {"avg_logging_time_ms": 0, "avg_counter_time_ms": 0}
        avg_log = (stats["total_logging_time"] / stats["request_count"]) * 1000
        avg_cnt = (stats["total_counter_time"] / stats["request_count"]) * 1000
        return {"avg_logging_time_ms": avg_log, "avg_counter_time_ms": avg_cnt}

@app.post("/stats/reset")
async def reset_stats():
    async with stats_lock:
        stats["total_logging_time"] = 0.0
        stats["total_counter_time"] = 0.0
        stats["request_count"] = 0
    return {"status": "reset"}