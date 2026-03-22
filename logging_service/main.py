import os
from fastapi import FastAPI, Body
import hazelcast

app = FastAPI()

hz_addr = os.getenv("HZ_ADDRESSES", "hazelcast:5701")
instance_name = os.getenv("SERVICE_NAME", "logging-default")

client = hazelcast.HazelcastClient(
    cluster_members=[hz_addr],
    cluster_name="dev",
    smart_routing=True
)

distributed_logs = client.get_map("all_transactions").blocking()

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
