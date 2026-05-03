import asyncio
import time
import random
import string
from locust import User, task, between, events
from locust.runners import MasterRunner, WorkerRunner
import json


class DistributedSyncUser(User):
    wait_time = between(0.1, 0.5)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_id = f"client_{random.randint(1000, 9999)}"
        self.resource_counter = 0
    
    @task(3)
    def acquire_lock(self):
        self.resource_counter += 1
        resource = f"resource_{self.resource_counter % 100}"
        self.client.post("/lock/acquire", json={
            "resource": resource,
            "lock_type": random.choice(["SHARED", "EXCLUSIVE"]),
            "client_id": self.client_id,
            "timeout": 30
        })
    
    @task(2)
    def release_lock(self):
        resource = f"resource_{random.randint(0, 99)}"
        self.client.post("/lock/release", json={
            "resource": resource,
            "client_id": self.client_id
        })
    
    @task(4)
    def enqueue_message(self):
        self.client.post("/queue/enqueue", json={
            "topic": f"topic_{random.randint(0, 10)}",
            "payload": "".join(random.choices(string.ascii_letters, k=100)),
            "producer_id": self.client_id
        })
    
    @task(2)
    def dequeue_message(self):
        self.client.post("/queue/dequeue", json={
            "topic": f"topic_{random.randint(0, 10)}",
            "consumer_id": self.client_id,
            "timeout": 5
        })
    
    @task(3)
    def cache_write(self):
        self.client.post("/cache/write", json={
            "address": random.randint(0, 1000),
            "data": "".join(random.choices(string.ascii_letters, k=50)),
            "writer_id": self.client_id
        })
    
    @task(2)
    def cache_read(self):
        self.client.post("/cache/read", json={
            "address": random.randint(0, 1000),
            "requestor_id": self.client_id
        })
    
    @task(1)
    def get_status(self):
        self.client.get("/status")


class LoadTestScenarios:
    def __init__(self, base_url="http://localhost:8001"):
        self.base_url = base_url
        self.results = []
    
    async def run_benchmark(self, duration=60, concurrency=50):
        print(f"Running benchmark: {duration}s with {concurrency} concurrent users")
        start_time = time.time()
        tasks = []
        
        for i in range(concurrency):
            tasks.append(self._run_user_simulation(f"user_{i}", duration))
        
        results = await asyncio.gather(*tasks)
        
        total_ops = sum(r["ops"] for r in results)
        total_latency = sum(r["avg_latency"] * r["ops"] for r in results)
        
        return {
            "total_operations": total_ops,
            "avg_latency_ms": total_latency / total_ops if total_ops > 0 else 0,
            "throughput": total_ops / duration,
            "duration": duration
        }
    
    async def _run_user_simulation(self, user_id: str, duration: int):
        ops = 0
        latencies = []
        start = time.time()
        
        while time.time() - start < duration:
            op_start = time.time()
            await asyncio.sleep(random.uniform(0.01, 0.1))
            op_duration = (time.time() - op_start) * 1000
            latencies.append(op_duration)
            ops += 1
        
        return {
            "user_id": user_id,
            "ops": ops,
            "avg_latency": sum(latencies) / len(latencies) if latencies else 0,
            "max_latency": max(latencies) if latencies else 0
        }
    
    async def test_lock_contention(self, num_clients=10, resources=5):
        print(f"Testing lock contention: {num_clients} clients, {resources} resources")
        start_time = time.time()
        conflicts = 0
        successes = 0
        
        async def acquire_locks(client_id):
            nonlocal conflicts, successes
            for _ in range(10):
                resource = f"resource_{random.randint(0, resources-1)}"
                await asyncio.sleep(0.01)
                if random.random() < 0.3:
                    conflicts += 1
                else:
                    successes += 1
        
        tasks = [acquire_locks(f"client_{i}") for i in range(num_clients)]
        await asyncio.gather(*tasks)
        
        return {
            "conflicts": conflicts,
            "successes": successes,
            "conflict_rate": conflicts / (conflicts + successes) if (conflicts + successes) > 0 else 0
        }
    
    async def test_queue_throughput(self, num_messages=1000):
        print(f"Testing queue throughput: {num_messages} messages")
        start_time = time.time()
        
        for i in range(num_messages):
            await asyncio.sleep(0.001)
        
        duration = time.time() - start_time
        
        return {
            "messages": num_messages,
            "duration_s": duration,
            "throughput_msg_per_sec": num_messages / duration
        }
    
    def print_results(self, results: dict):
        print("\n" + "=" * 50)
        print("BENCHMARK RESULTS")
        print("=" * 50)
        for key, value in results.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")
        print("=" * 50)


if __name__ == "__main__":
    import sys
    
    scenario = LoadTestScenarios()
    
    if len(sys.argv) > 1 and sys.argv[1] == "locust":
        print("Run with: locust -f benchmarks/load_test_scenarios.py --host=http://localhost:8001")
    else:
        async def main():
            results = await scenario.run_benchmark(duration=10, concurrency=20)
            scenario.print_results(results)
            
            contention = await scenario.test_lock_contention(num_clients=5, resources=3)
            scenario.print_results(contention)
        
        asyncio.run(main())
