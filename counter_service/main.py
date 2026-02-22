from fastapi import FastAPI, Body
from typing import Dict
import asyncio

app = FastAPI()

balances: Dict[str, float] = {}

lock = asyncio.Lock()

@app.post("/update")
async def update_balance(data: dict = Body(...)):
    user_id = data.get("user_id")
    amount = data.get("amount")
    
    async with lock:
        if user_id not in balances:
            balances[user_id] = 0.0
        
        balances[user_id] += amount
        current_balance = balances[user_id]
    
    return {"user_id": user_id, "balance": current_balance}

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    return {"user_id": user_id, "balance": balances.get(user_id, 0.0)}

@app.get("/balances")
async def get_all_balances():
    return balances