from fastapi import FastAPI, Body
from typing import List, Dict
from collections import defaultdict

app = FastAPI()
logs: Dict[str, dict] = {}

user_history = defaultdict(list)

@app.post("/log")
async def log_transaction(data: dict = Body(...)):
    t_id = data.get("transaction_id")
    user_id = data.get("user_id")
    
    logs[t_id] = data
    
    user_history[user_id].append(data)
    
    print(f"Logged: {t_id} for user {user_id}")
    
    return {"status": "recorded"}

@app.get("/logs/{user_id}")
async def get_user_logs(user_id: str):
    return {"user_id": user_id, "transactions": user_history.get(user_id, [])}