import os
import asyncio
import httpx
from fastapi import FastAPI, Body
import hazelcast

app = FastAPI()

hz_addr = os.getenv("HZ_ADDRESSES", "hazelcast:5701")
instance_name = os.getenv("SERVICE_NAME", "logging-default")
service_url = os.getenv("SERVICE_URL", "http://logging-1:8000") 

client = hazelcast.HazelcastClient(
    cluster_members=[hz_addr],
    cluster_name="dev",
    smart_routing=True
)

distributed_logs = client.get_map("all_transactions").blocking()

async def register_service():
    config_url = "http://config-server:8000/register"
    payload = {
        "service_name": "logging-service",
        "address": service_url
    }
    
    async with httpx.AsyncClient() as http_client:
        try:
            await http_client.post(config_url, json=payload)
            print(f"[{instance_name}] Successfully registered at {service_url} in Config Server")
        except Exception as e:
            print(f"[{instance_name}] Failed to register in Config Server: {e}")

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(register_service())

@app.post("/log")
async def log_transaction(data: dict = Body(...)):
    t_id = data.get("transaction_id")
    user_id = data.get("user_id")
    
    distributed_logs.put(t_id, data)
    
    print(f"[{instance_name}] Logged: {t_id} for user {user_id}")
    
    return {
        "status": "recorded", 
        "from": instance_name, 
        "transaction_id": t_id
    }

@app.get("/logs/{user_id}")
async def get_user_logs(user_id: str):
    all_entries = distributed_logs.entry_set()
    user_tx = [v for k, v in all_entries if v.get("user_id") == user_id]
    
    return {
        "user_id": user_id, 
        "transactions": user_tx,
        "source_node": instance_name
    }