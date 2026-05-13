import os
import asyncio
import threading
import hazelcast
from fastapi import FastAPI
from databases import Database

DATABASE_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@bank-db:5432/bank")
HZ_ADDRESSES = os.getenv("HZ_ADDRESSES", "hazelcast-service:5701")
MQ_QUEUE_NAME = os.getenv("MQ_QUEUE_NAME", "transaction-queue")

database = Database(DATABASE_URL)
app = FastAPI()
event_loop = None

async def update_balance_in_db(user_id: str, amount: float):
    query = """
    INSERT INTO accounts (user_id, balance) 
    VALUES (:u, :a)
    ON CONFLICT (user_id) 
    DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
    """
    try:
        await database.execute(query=query, values={"u": user_id, "a": amount})
        print(f"Processed MQ update for {user_id}: +{amount}")
    except Exception as e:
        print(f"DB Update Error: {e}")

def consume_queue():
    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_members=[HZ_ADDRESSES],
            cluster_name="dev"
        )
        queue = hz_client.get_queue(MQ_QUEUE_NAME).blocking()
        print("MQ Consumer started. Waiting for messages...")
        
        while True:
            data = queue.take() 
            user_id = data.get("user_id")
            amount = float(data.get("amount", 0))
            
            if event_loop and not event_loop.is_closed():
                asyncio.run_coroutine_threadsafe(update_balance_in_db(user_id, amount), event_loop)
                
    except Exception as e:
        print(f"MQ Consumer Error: {e}")

@app.on_event("startup")
async def startup():
    global event_loop
    event_loop = asyncio.get_running_loop()
    
    await database.connect()
    await database.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            user_id TEXT PRIMARY KEY,
            balance FLOAT DEFAULT 0.0
        )
    """)
    
    threading.Thread(target=consume_queue, daemon=True).start()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    query = "SELECT balance FROM accounts WHERE user_id = :u"
    res = await database.fetch_one(query=query, values={"u": user_id})
    return {"user_id": user_id, "balance": res['balance'] if res else 0.0}

@app.get("/balances")
async def get_all_balances():
    rows = await database.fetch_all("SELECT user_id, balance FROM accounts")
    return {row['user_id']: row['balance'] for row in rows}