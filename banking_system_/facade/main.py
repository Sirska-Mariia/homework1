import asyncio
import random
import uuid
from fastapi import FastAPI, HTTPException, Body
import httpx
import hazelcast

app = FastAPI()

CONFIG_SERVER_URL = "http://config-server:8000"

hz_client = hazelcast.HazelcastClient(cluster_members=["hazelcast:5701"])
transaction_queue = hz_client.get_queue("transaction-queue").blocking()

http_client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()
    hz_client.shutdown()

async def get_service_node(service_name: str):
    try:
        res = await http_client.get(f"{CONFIG_SERVER_URL}/nodes/{service_name}")
        nodes = res.json()
        if not nodes:
            return None
        return random.choice(nodes)
    except Exception:
        return None

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

    log_node = await get_service_node("logging-service")
    if log_node:
        try:
            asyncio.create_task(http_client.post(f"{log_node}/log", json=payload))
        except Exception as e:
            print(f"Logging failed: {e}")

    try:
        transaction_queue.put(payload) 
        
        return {
            "status": "Transaction accepted (queued)",
            "transaction_id": transaction_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MQ Error: {str(e)}")

@app.get("/user/{user_id}")
async def get_user_info(user_id: str):
    log_node = await get_service_node("logging-service")
    count_node = await get_service_node("counter-service")

    if not log_node or not count_node:
        raise HTTPException(status_code=503, detail="Services discovery failed")

    try:
        log_res, count_res = await asyncio.gather(
            http_client.get(f"{log_node}/logs/{user_id}"),
            http_client.get(f"{count_node}/balance/{user_id}")
        )
        
        balance = count_res.json().get("balance") if count_res.status_code == 200 else None
        
        return {
            "balance": balance,
            "transactions": log_res.json().get("transactions", [])
        }
    except Exception:
         return {"balance": None, "transactions": [], "status": "Counter service unreachable"}

@app.get("/accounts")
async def get_all_accounts():
    count_node = await get_service_node("counter-service")
    if not count_node:
        return {"error": "Counter service not found"}
    res = await http_client.get(f"{count_node}/balances")
    return res.json()