import asyncio
import httpx
import time

URL = "http://localhost:30080/transaction"
STATS_URL = "http://localhost:30080/stats"
RESET_URL = "http://localhost:30080/stats/reset"

async def send_requests(client, user_id, count):
    for _ in range(count):
        try:
            await client.post(URL, json={"user_id": user_id, "amount": 1})
        except Exception as e:
            print(f"Error: {e}")

async def run_scenario(name, num_clients, req_per_client, same_user=False):
    print(f"\nЗапуск: {name}")
    print(f"Налаштування: {num_clients} клієнтів по {req_per_client} запитів")
    
    async with httpx.AsyncClient() as client:
        await client.post(RESET_URL)

    start_time = time.perf_counter()
    
    async with httpx.AsyncClient(timeout=None) as client:
        tasks = []
        for i in range(num_clients):
            user_id = "target_user" if same_user else f"user_{i}"
            tasks.append(send_requests(client, user_id, req_per_client))
        
        await asyncio.gather(*tasks)

    end_time = time.perf_counter()
    total_time = end_time - start_time
    total_requests = num_clients * req_per_client
    rps = total_requests / total_time
    async with httpx.AsyncClient() as client:
        stats = (await client.get(STATS_URL)).json()

    print(f"Результати {name}:")
    print(f"   Загальний час: {total_time:.2f} сек")
    print(f"   RPS (запитів/сек): {rps:.2f}")

    print(f"   Сер. час виклику Logging: {stats['avg_logging_time_ms'] / 1000:.4f} сек")
    print(f"   Сер. час виклику Counter: {stats['avg_counter_time_ms'] / 1000:.4f} сек")

async def main():
    iters = 10000

    await run_scenario("Сценарій 1 (Різні рахунки)", 10, iters, same_user=False)
    await run_scenario("Сценарій 2 (Один рахунок)", 10, iters, same_user=True)

if __name__ == "__main__":
    asyncio.run(main())