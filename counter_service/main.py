import os
import asyncio
from fastapi import FastAPI, Body
from databases import Database

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres-db:5432/bank_db")
database = Database(DATABASE_URL)

app = FastAPI()

@app.on_event("startup")
async def startup():
    await database.connect()
    await database.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            user_id TEXT PRIMARY KEY,
            balance FLOAT DEFAULT 0.0
        )
    """)

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.post("/update")
async def update_balance(data: dict = Body(...)):
    user_id = data.get("user_id")
    amount = float(data.get("amount", 0))
    query = """
    INSERT INTO accounts (user_id, balance) 
    VALUES (:u, :a)
    ON CONFLICT (user_id) 
    DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
    RETURNING balance
    """
    new_balance = await database.fetch_val(query=query, values={"u": user_id, "a": amount})
    
    return {"user_id": user_id, "balance": new_balance}

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    query = "SELECT balance FROM accounts WHERE user_id = :u"
    res = await database.fetch_one(query=query, values={"u": user_id})
    return {"user_id": user_id, "balance": res['balance'] if res else 0.0}

@app.get("/balances")
async def get_all_balances():
    rows = await database.fetch_all("SELECT user_id, balance FROM accounts")
    return {row['user_id']: row['balance'] for row in rows}
