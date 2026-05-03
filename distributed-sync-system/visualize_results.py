
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
