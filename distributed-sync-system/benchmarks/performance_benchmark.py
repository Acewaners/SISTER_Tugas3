"""
Performance Benchmark for Distributed Synchronization System
Tests: Lock, Queue, Cache operations with various concurrency levels
"""

import asyncio
import socket
import json
import time
import random
import string
import statistics
from typing import List, Dict, Tuple
from dataclasses import dataclass, field


@dataclass
class BenchmarkResult:
    name: str
    operations: int
    total_time: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    std_dev_ms: float
    throughput_ops_per_sec: float
    success_count: int
    error_count: int


def send_request(port: int, data: dict, timeout: float = 10.0) -> Tuple[bool, dict, float]:
    """Send request to node and measure latency"""
    start = time.perf_counter()
    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect(('localhost', port))
        s.sendall(json.dumps(data).encode())
        response = s.recv(8192)
        s.close()

        latency = (time.perf_counter() - start) * 1000
        response_data = json.loads(response.decode())

        success = response_data.get('success', False) or response_data.get('error') is None
        return success, response_data, latency
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        return False, {"error": str(e)}, latency


async def benchmark_lock_operations(port: int, num_operations: int, concurrency: int) -> BenchmarkResult:
    """Benchmark lock acquire/release operations"""
    latencies = []
    successes = 0
    errors = 0

    async def single_lock_op(op_id: int):
        nonlocal successes, errors
        resource = f"bench_resource_{op_id % 100}"

        # Acquire lock
        success, _, lat = send_request(port, {
            "action": "lock_acquire",
            "resource": resource,
            "lock_type": "EXCLUSIVE",
            "client_id": f"bench_client_{op_id}"
        })
        latencies.append(lat)
        if success:
            successes += 1
        else:
            errors += 1

        # Small delay
        await asyncio.sleep(0.001)

        # Release lock
        success, _, lat = send_request(port, {
            "action": "lock_release",
            "resource": resource,
            "client_id": f"bench_client_{op_id}"
        })

    start_time = time.perf_counter()

    # Run with concurrency limit
    for batch_start in range(0, num_operations, concurrency):
        batch_end = min(batch_start + concurrency, num_operations)
        tasks = [single_lock_op(i) for i in range(batch_start, batch_end)]
        await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start_time

    return BenchmarkResult(
        name=f"Lock Ops (c={concurrency})",
        operations=num_operations * 2,  # acquire + release
        total_time=total_time,
        avg_latency_ms=statistics.mean(latencies) if latencies else 0,
        min_latency_ms=min(latencies) if latencies else 0,
        max_latency_ms=max(latencies) if latencies else 0,
        std_dev_ms=statistics.stdev(latencies) if len(latencies) > 1 else 0,
        throughput_ops_per_sec=(num_operations * 2) / total_time if total_time > 0 else 0,
        success_count=successes,
        error_count=errors
    )


async def benchmark_queue_operations(port: int, num_operations: int, concurrency: int) -> BenchmarkResult:
    """Benchmark queue enqueue/dequeue operations"""
    latencies = []
    successes = 0
    errors = 0

    async def single_queue_op(op_id: int):
        nonlocal successes, errors
        topic = f"bench_topic_{op_id % 10}"

        # Enqueue
        success, _, lat = send_request(port, {
            "action": "queue_enqueue",
            "topic": topic,
            "payload": f"bench_payload_{op_id}_{''.join(random.choices(string.ascii_letters, k=50))}",
            "producer_id": f"bench_producer_{op_id % 10}"
        })
        latencies.append(lat)
        if success:
            successes += 1
        else:
            errors += 1

    start_time = time.perf_counter()

    for batch_start in range(0, num_operations, concurrency):
        batch_end = min(batch_start + concurrency, num_operations)
        tasks = [single_queue_op(i) for i in range(batch_start, batch_end)]
        await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start_time

    return BenchmarkResult(
        name=f"Queue Ops (c={concurrency})",
        operations=num_operations,
        total_time=total_time,
        avg_latency_ms=statistics.mean(latencies) if latencies else 0,
        min_latency_ms=min(latencies) if latencies else 0,
        max_latency_ms=max(latencies) if latencies else 0,
        std_dev_ms=statistics.stdev(latencies) if len(latencies) > 1 else 0,
        throughput_ops_per_sec=num_operations / total_time if total_time > 0 else 0,
        success_count=successes,
        error_count=errors
    )


async def benchmark_cache_operations(port: int, num_operations: int, concurrency: int) -> BenchmarkResult:
    """Benchmark cache read/write operations"""
    latencies = []
    successes = 0
    errors = 0

    async def single_cache_op(op_id: int):
        nonlocal successes, errors
        address = op_id % 500

        if op_id % 3 == 0:
            # Write
            success, _, lat = send_request(port, {
                "action": "cache_write",
                "address": address,
                "data": f"cache_data_{op_id}",
                "writer_id": f"bench_writer_{op_id % 10}"
            })
        else:
            # Read
            success, _, lat = send_request(port, {
                "action": "cache_read",
                "address": address,
                "requestor_id": f"bench_reader_{op_id % 10}"
            })

        latencies.append(lat)
        if success:
            successes += 1
        else:
            errors += 1

    start_time = time.perf_counter()

    for batch_start in range(0, num_operations, concurrency):
        batch_end = min(batch_start + concurrency, num_operations)
        tasks = [single_cache_op(i) for i in range(batch_start, batch_end)]
        await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start_time

    return BenchmarkResult(
        name=f"Cache Ops (c={concurrency})",
        operations=num_operations,
        total_time=total_time,
        avg_latency_ms=statistics.mean(latencies) if latencies else 0,
        min_latency_ms=min(latencies) if latencies else 0,
        max_latency_ms=max(latencies) if latencies else 0,
        std_dev_ms=statistics.stdev(latencies) if len(latencies) > 1 else 0,
        throughput_ops_per_sec=num_operations / total_time if total_time > 0 else 0,
        success_count=successes,
        error_count=errors
    )


async def benchmark_scalability(port: int, num_operations: int, concurrency_levels: List[int]) -> List[BenchmarkResult]:
    """Test scalability with different concurrency levels"""
    print("\n" + "="*60)
    print("SCALABILITY TEST - Testing Different Concurrency Levels")
    print("="*60)

    results = []

    for concurrency in concurrency_levels:
        print(f"\nTesting with concurrency={concurrency}...", end=" ", flush=True)

        result = await benchmark_lock_operations(port, num_operations, concurrency)
        results.append(result)

        print(f"Done! Throughput: {result.throughput_ops_per_sec:.2f} ops/sec")

    return results


async def benchmark_mixed_operations(port: int, num_operations: int) -> Dict[str, BenchmarkResult]:
    """Benchmark all operations mixed"""
    print("\n" + "="*60)
    print("MIXED OPERATIONS TEST")
    print("="*60)

    results = {}

    print("\nTesting Lock Operations...")
    results['lock'] = await benchmark_lock_operations(port, num_operations, concurrency=50)

    print("Testing Queue Operations...")
    results['queue'] = await benchmark_queue_operations(port, num_operations, concurrency=50)

    print("Testing Cache Operations...")
    results['cache'] = await benchmark_cache_operations(port, num_operations, concurrency=50)

    return results


def print_result_table(results: List[BenchmarkResult]):
    """Print benchmark results in table format"""
    print("\n" + "-"*100)
    print(f"{'Test Name':<30} {'Ops':>8} {'Time(s)':>10} {'Avg(ms)':>10} {'Min(ms)':>10} {'Max(ms)':>10} {'StdDev':>10} {'Throughput':>15}")
    print("-"*100)

    for r in results:
        print(f"{r.name:<30} {r.operations:>8} {r.total_time:>10.2f} {r.avg_latency_ms:>10.2f} {r.min_latency_ms:>10.2f} {r.max_latency_ms:>10.2f} {r.std_dev_ms:>10.2f} {r.throughput_ops_per_sec:>15.2f}")

    print("-"*100)


def export_results_to_json(results: List[BenchmarkResult], filename: str):
    """Export results to JSON for visualization"""
    data = {
        "benchmark_results": [
            {
                "name": r.name,
                "operations": r.operations,
                "total_time": r.total_time,
                "avg_latency_ms": r.avg_latency_ms,
                "min_latency_ms": r.min_latency_ms,
                "max_latency_ms": r.max_latency_ms,
                "std_dev_ms": r.std_dev_ms,
                "throughput_ops_per_sec": r.throughput_ops_per_sec,
                "success_count": r.success_count,
                "error_count": r.error_count
            }
            for r in results
        ],
        "timestamp": time.time()
    }

    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"\nResults exported to {filename}")


def export_results_to_csv(results: List[BenchmarkResult], filename: str):
    """Export results to CSV for visualization"""
    with open(filename, 'w') as f:
        f.write("name,operations,total_time,avg_latency_ms,min_latency_ms,max_latency_ms,std_dev_ms,throughput_ops_per_sec,success_count,error_count\n")
        for r in results:
            f.write(f"{r.name},{r.operations},{r.total_time:.4f},{r.avg_latency_ms:.4f},{r.min_latency_ms:.4f},{r.max_latency_ms:.4f},{r.std_dev_ms:.4f},{r.throughput_ops_per_sec:.4f},{r.success_count},{r.error_count}\n")

    print(f"CSV exported to {filename}")


def create_visualization_script(results: List[BenchmarkResult]):
    """Create a simple Python script that generates ASCII charts"""
    script = '''
import matplotlib.pyplot as plt
import json
import sys

def visualize_results(json_file):
    with open(json_file) as f:
        data = json.load(f)

    results = data["benchmark_results"]

    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Distributed Sync System - Performance Benchmark", fontsize=14, fontweight="bold")

    # Extract data
    names = [r["name"] for r in results]
    throughput = [r["throughput_ops_per_sec"] for r in results]
    avg_latency = [r["avg_latency_ms"] for r in results]
    max_latency = [r["max_latency_ms"] for r in results]
    success_rate = [r["success_count"] / (r["success_count"] + r["error_count"]) * 100 if r["success_count"] + r["error_count"] > 0 else 0 for r in results]

    # Plot 1: Throughput
    ax1 = axes[0, 0]
    ax1.bar(names, throughput, color=["#3498db", "#2ecc71", "#e74c3c"])
    ax1.set_title("Throughput (ops/sec)")
    ax1.set_ylabel("Operations per Second")
    ax1.tick_params(axis="x", rotation=45)
    for i, v in enumerate(throughput):
        ax1.text(i, v + max(throughput)*0.01, f"{v:.1f}", ha="center", fontsize=8)

    # Plot 2: Average Latency
    ax2 = axes[0, 1]
    ax2.bar(names, avg_latency, color=["#9b59b6", "#1abc9c", "#f39c12"])
    ax2.set_title("Average Latency")
    ax2.set_ylabel("Milliseconds")
    ax2.tick_params(axis="x", rotation=45)
    for i, v in enumerate(avg_latency):
        ax2.text(i, v + max(avg_latency)*0.01, f"{v:.2f}ms", ha="center", fontsize=8)

    # Plot 3: Max Latency
    ax3 = axes[1, 0]
    ax3.bar(names, max_latency, color=["#34495e", "#16a085", "#d35400"])
    ax3.set_title("Maximum Latency")
    ax3.set_ylabel("Milliseconds")
    ax3.tick_params(axis="x", rotation=45)

    # Plot 4: Success Rate
    ax4 = axes[1, 1]
    ax4.bar(names, success_rate, color=["#27ae60", "#2980b9", "#8e44ad"])
    ax4.set_title("Success Rate")
    ax4.set_ylabel("Percentage (%)")
    ax4.set_ylim(0, 105)
    ax4.tick_params(axis="x", rotation=45)
    for i, v in enumerate(success_rate):
        ax4.text(i, v + 1, f"{v:.1f}%", ha="center", fontsize=8)

    plt.tight_layout()
    plt.savefig("benchmark_results.png", dpi=150, bbox_inches="tight")
    print("Chart saved to benchmark_results.png")
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        visualize_results(sys.argv[1])
    else:
        print("Usage: python visualize.py results.json")
'''

    with open("visualize_results.py", "w") as f:
        f.write(script)

    print("\nVisualization script created: visualize_results.py")


async def main():
    print("="*60)
    print("DISTRIBUTED SYNCHRONIZATION SYSTEM - PERFORMANCE BENCHMARK")
    print("="*60)

    port = 8001
    num_operations = 200

    # Check if server is running
    print(f"\nChecking server on port {port}...")
    success, _, _ = send_request(port, {"action": "heartbeat"})
    if not success:
        print(f"ERROR: Server not running on port {port}")
        print("Please start the server first:")
        print("  python -m src.nodes --node-id node_1 --port 8001")
        return

    print("Server is running!\n")

    # Run scalability test
    concurrency_levels = [1, 5, 10, 20, 50]
    scalability_results = await benchmark_scalability(port, num_operations, concurrency_levels)

    # Run mixed operations test
    mixed_results = await benchmark_mixed_operations(port, num_operations)

    # Combine all results
    all_results = scalability_results + list(mixed_results.values())

    # Print results table
    print_result_table(all_results)

    # Export to files
    export_results_to_json(all_results, "benchmark_results.json")
    export_results_to_csv(all_results, "benchmark_results.csv")

    # Create visualization script
    create_visualization_script(all_results)

    print("\n" + "="*60)
    print("BENCHMARK COMPLETE!")
    print("="*60)
    print("\nFiles generated:")
    print("  - benchmark_results.json (for visualization)")
    print("  - benchmark_results.csv (for Excel/Sheets)")
    print("  - visualize_results.py (run with: python visualize_results.py benchmark_results.json)")

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for r in mixed_results.values():
        print(f"{r.name}:")
        print(f"  - Avg Latency: {r.avg_latency_ms:.2f} ms")
        print(f"  - Throughput: {r.throughput_ops_per_sec:.2f} ops/sec")
        print(f"  - Success Rate: {r.success_count/(r.success_count+r.error_count)*100:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())