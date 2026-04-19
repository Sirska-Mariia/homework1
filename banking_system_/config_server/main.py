from fastapi import FastAPI, Body
from typing import Dict, List

app = FastAPI()
registry: Dict[str, List[str]] = {}

@app.post("/register")
async def register(data: dict = Body(...)):
    name = data.get("service_name")
    addr = data.get("address")
    if name not in registry: registry[name] = []
    if addr not in registry[name]: registry[name].append(addr)
    print(f"Registered: {name} at {addr}")
    return {"status": "ok"}

@app.get("/nodes/{service_name}")
async def get_nodes(service_name: str):
    return registry.get(service_name, [])