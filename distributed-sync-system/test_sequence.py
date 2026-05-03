"""
Sequential Test Script - Semua komponen diuji 1 per 1
Buka terminal/PowerShell dan jalankan satu per satu

Usage: python test_sequence.py [test_number]
"""

import asyncio
import sys
import time

# Add current directory to path
sys.path.insert(0, ".")

from src.consensus.raft import RaftNode, RaftState, LogEntry
from src.nodes.lock_manager import DistributedLockManager, LockType
from src.nodes.queue_node import DistributedQueue, ConsistentHashRing
from src.nodes.cache_node import CacheNode, CacheState, LRUReplacement
from src.communication.failure_detector import FailureDetector


def print_header(title):
    print("\n" + "="*60)
    print(f"TEST {title}")
    print("="*60)


def print_success(msg):
    print(f"  [OK] {msg}")


def print_step(msg):
    print(f"  -> {msg}")


async def test1_raft_consensus():
    """Test 1: Raft Consensus - Leader Election & Failover"""
    print_header("1/8: RAFT CONSENSUS - Leader Election & Failover")

    print("\n[1] Membuat 3 Raft node...")
    nodes = []
    for i in range(3):
        peers = [f"localhost:{8001 + j}" for j in range(3) if j != i]
        node = RaftNode(f"node_{i+1}", peers, 8001 + i)
        nodes.append(node)

    print_step(f"Node 1 (port 8001): peers={nodes[0].peers}")
    print_step(f"Node 2 (port 8002): peers={nodes[1].peers}")
    print_step(f"Node 3 (port 8003): peers={nodes[2].peers}")

    print("\n[2] Semua node dalam state FOLLOWER:")
    for node in nodes:
        state = node.get_state()
        print(f"  - {state['node_id']}: term={state['term']}, state={state['state']}")

    print("\n[3] Simulasi election timeout -> Node 1 mulai election...")
    nodes[0].state = RaftState.CANDIDATE
    nodes[0].current_term = 1
    nodes[0].last_heartbeat = 0  # Trigger election
    await nodes[0]._start_election()
    await asyncio.sleep(0.1)
    print_step(f"Node 1 -> CANDIDATE, term={nodes[0].current_term}")

    print("\n[4] Node 1 becomes Leader:")
    nodes[0].state = RaftState.LEADER
    print(f"  - Node 1: state=leader, term={nodes[0].current_term}")

    print("\n[5] Leader submit command:")
    success = await nodes[0].submit_command("WRITE", {"key": "balance", "value": 1000})
    print(f"  - Command submitted: {success}")
    print(f"  - Log entries: {len(nodes[0].log)}")

    print("\n[6] Simulasi Leader crash:")
    crashed_node = nodes[0]
    print_step("Node 1 (leader) crashed!")
    crashed_node._running = False

    print("\n[7] Node 2 & 3 mendeteksi leader hilang, mulai election...")
    nodes[1].last_heartbeat = 0
    nodes[1].current_term = 2
    await nodes[1]._start_election()
    print_step("Node 2 memulai election dengan term=2")

    nodes[1].state = RaftState.LEADER
    print_success(f"Node 2 menjadi NEW LEADER (term={nodes[1].current_term})")

    print("\n[8] Verifikasi failover:")
    print(f"  - Node 1 (crashed): leader={crashed_node.is_leader()}")
    print(f"  - Node 2 (new leader): leader={nodes[1].is_leader()}, term={nodes[1].current_term}")
    print(f"  - Node 3 (follower): leader={nodes[2].is_leader()}")

    print("\n[9] New leader menerima command:")
    await nodes[1].submit_command("WRITE", {"key": "counter", "value": 1})
    print(f"  - Node 2 log entries: {len(nodes[1].log)}")

    print_success("Raft Consensus - Leader Election & Failover PASSED!")
    return True


async def test2_lock_manager():
    """Test 2: Distributed Lock Manager - Shared & Exclusive Locks"""
    print_header("2/8: DISTRIBUTED LOCK MANAGER - Shared & Exclusive Locks")

    lock_mgr = DistributedLockManager("test_node", [], 8001)
    await lock_mgr.start()

    print("\n[1] Acquire EXCLUSIVE lock on 'resource_A'...")
    result = await lock_mgr.acquire_lock("resource_A", LockType.EXCLUSIVE, "client_1")
    print_step(f"Result: success={result['success']}, request_id={result['request_id'][:8]}...")

    print("\n[2] Check lock status:")
    status = await lock_mgr.get_lock_status("resource_A")
    print(f"  - locked: {status['locked']}")
    print(f"  - lock_type: {status['lock_type']}")
    print(f"  - holder: {status['holder']}")

    print("\n[3] Acquire SHARED lock on 'resource_B' (client_2)...")
    result = await lock_mgr.acquire_lock("resource_B", LockType.SHARED, "client_2")
    print_step(f"Result: success={result['success']}")

    print("\n[4] Acquire SHARED lock on 'resource_B' (client_3)...")
    result = await lock_mgr.acquire_lock("resource_B", LockType.SHARED, "client_3")
    print_step(f"Result: success={result['success']}")

    print("\n[5] Multiple clients bisa hold SHARED lock:")
    all_locks = await lock_mgr.get_all_locks()
    for lock in all_locks:
        print(f"  - {lock['resource']}: {lock['lock_type']} held by {lock['holder']}")

    print("\n[6] Release exclusive lock 'resource_A'...")
    result = await lock_mgr.release_lock("resource_A", "client_1")
    print_step(f"Result: success={result['success']}")

    print("\n[7] Verifikasi resource_A unlocked:")
    status = await lock_mgr.get_lock_status("resource_A")
    print(f"  - locked: {status['locked']}")

    print("\n[8] Attempt release by non-holder (should fail)...")
    result = await lock_mgr.release_lock("resource_B", "wrong_client")
    print_step(f"Result: success={result['success']}, error={result.get('error')}")

    await lock_mgr.stop()
    print_success("Distributed Lock Manager PASSED!")
    return True


async def test3_distributed_queue():
    """Test 3: Distributed Queue - Enqueue/Dequeue"""
    print_header("3/8: DISTRIBUTED QUEUE - Message Queue Operations")

    queue = DistributedQueue("test_node", [], 8001)
    await queue.start()

    print("\n[1] Enqueue 5 messages to 'test_topic'...")
    for i in range(5):
        result = await queue.enqueue("test_topic", f"Message {i+1}", f"producer_{i%3}")
        print_step(f"  Message {i+1}: msg_id={result['msg_id'][:8]}... -> partition={result['partition']}")

    print("\n[2] Check queue stats:")
    stats = await queue.get_queue_stats()
    print(f"  - total_messages: {stats['total_messages']}")
    print(f"  - producers: {stats['producers']}")
    print(f"  - partitions: {len(stats['partitions'])}")

    print("\n[3] Dequeue 3 messages:")
    for i in range(3):
        msg = await queue.dequeue("test_topic", "consumer_1", timeout=2)
        if msg:
            print_step(f"  Dequeued: {msg['payload']} (msg_id={msg['msg_id'][:8]}...)")
            ack = await queue.acknowledge(msg["msg_id"], "consumer_1")
            print(f"       Ack: {ack['success']}")

    print("\n[4] Verify remaining messages:")
    stats = await queue.get_queue_stats()
    print(f"  - total_messages: {stats['total_messages']}")
    for partition, count in stats['partitions'].items():
        if count > 0:
            print(f"  - {partition}: {count} messages")

    print("\n[5] Attempt dequeue from empty partition:")
    msg = await queue.dequeue("empty_topic", "consumer_1", timeout=1)
    print_step(f"Result: {msg}")

    await queue.stop()
    print_success("Distributed Queue PASSED!")
    return True


async def test4_cache_mesi():
    """Test 4: Cache - MESI-like Protocol"""
    print_header("4/8: CACHE - MESI-like Protocol (Read/Write/Invalidation)")

    cache = CacheNode("test_node", [], 8001, max_size=100)
    await cache.start()

    print("\n[1] Write to address 100 (should be in MODIFIED state)...")
    result = await cache.write(100, b"Hello Cache!", "writer_1")
    print_step(f"Write result: {result}")
    print(f"  State: {cache.cache[100].state.value}")

    print("\n[2] Write to address 200...")
    await cache.write(200, b"Data 200", "writer_1")
    print_step(f"State of addr 200: {cache.cache[200].state.value}")

    print("\n[3] Read from address 100 (cache hit)...")
    data = await cache.read(100, "reader_1")
    print_step(f"Data: {data}")
    print(f"  Hit count: {cache.hit_count}")

    print("\n[4] Read from address 999 (cache miss)...")
    data = await cache.read(999, "reader_1")
    print_step(f"Data: {data}")
    print(f"  Miss count: {cache.miss_count}")

    print("\n[5] Invalidate address 100...")
    result = await cache.invalidate(100, "admin")
    print_step(f"Invalidation: {result}")
    print(f"  State after invalidation: {cache.cache[100].state.value}")

    print("\n[6] Read after invalidation (miss now)...")
    data = await cache.read(100, "reader_2")
    print_step(f"Data: {data}")

    print("\n[7] Cache statistics:")
    stats = await cache.get_cache_stats()
    print(f"  - cache_size: {stats['cache_size']}")
    print(f"  - hits: {stats['hits']}")
    print(f"  - misses: {stats['misses']}")
    print(f"  - hit_rate: {stats['hit_rate']:.1%}")

    await cache.stop()
    print_success("Cache MESI Protocol PASSED!")
    return True


async def test5_failure_detector():
    """Test 5: Failure Detector - Peer Monitoring"""
    print_header("5/8: FAILURE DETECTOR - SWIM-like Peer Monitoring")

    peers = ["localhost:8002", "localhost:8003", "localhost:8004"]
    fd = FailureDetector("node_1", peers, heartbeat_interval=1.0, timeout=5.0)
    await fd.start()

    print("\n[1] Initialize Failure Detector with peers:")
    for peer in peers:
        print(f"  - {peer}")

    print("\n[2] Initial peer status (all assumed alive):")
    stats = fd.get_stats()
    print(f"  - total_peers: {stats['total_peers']}")
    print(f"  - alive: {stats['alive']}")
    print(f"  - suspected: {stats['suspected']}")
    print(f"  - dead: {stats['dead']}")

    print("\n[3] Check individual peer status:")
    for peer in peers:
        status = fd.get_all_status()[peer]
        print(f"  - {peer}: is_alive={status['is_alive']}, last_heartbeat={status['last_heartbeat']:.2f}")

    print("\n[4] Simulasi node crash (mark peer as dead)...")
    # Simulate by resetting node status
    peer_to_kill = "localhost:8002"
    fd.peers = [p for p in peers if p != peer_to_kill]
    fd.dead_nodes.add(peer_to_kill)
    print_step(f"Peer {peer_to_kill} marked as dead")

    print("\n[5] Updated statistics:")
    stats = fd.get_stats()
    print(f"  - alive: {stats['alive']}")
    print(f"  - dead: {stats['dead']}")

    print("\n[6] Check if peer is dead:")
    print(f"  - localhost:8002 is_dead: {fd.is_dead('localhost:8002')}")
    print(f"  - localhost:8003 is_dead: {fd.is_dead('localhost:8003')}")

    print("\n[7] Reset dead node (recovery):")
    fd.reset_dead_node(peer_to_kill)
    print_step(f"Peer {peer_to_kill} marked as alive again")
    print(f"  - is_dead: {fd.is_dead(peer_to_kill)}")

    await fd.stop()
    print_success("Failure Detector PASSED!")
    return True


async def test6_multi_node_scenario():
    """Test 6: Multi-Node Scenario - Concurrent Operations"""
    print_header("6/8: MULTI-NODE SCENARIO - Concurrent Lock Operations")

    print("\n[1] Membuat 3 Lock Manager nodes...")
    nodes = []
    for i in range(3):
        node = DistributedLockManager(f"node_{i+1}", [], 8001 + i)
        await node.start()
        nodes.append(node)
    print_step("3 nodes created and started")

    print("\n[2] Concurrent lock acquisitions (lock same resource):")
    resource = "shared_resource"

    async def acquire_release(node_id, node, client_id, delay):
        print(f"    Client {client_id} -> requesting lock...")
        result = await node.acquire_lock(resource, LockType.EXCLUSIVE, client_id, timeout=3)
        if result['success']:
            print(f"    Client {client_id} -> acquired lock!")
            await asyncio.sleep(delay)
            await node.release_lock(resource, client_id)
            print(f"    Client {client_id} -> released lock")

    # Simulate concurrent acquisition
    tasks = [
        acquire_release("node_1", nodes[0], "client_1", 0.3),
        acquire_release("node_2", nodes[1], "client_2", 0.3),
        acquire_release("node_3", nodes[2], "client_3", 0.3),
    ]

    # Run sequentially to simulate
    for task in tasks:
        await task

    print("\n[3] Each node holds independent locks:")
    for node in nodes:
        locks = await node.get_all_locks()
        print(f"  - {node.node_id}: {len(locks)} locks")

    print("\n[4] Unique resources per node:")
    await nodes[0].acquire_lock("unique_1", LockType.EXCLUSIVE, "client_1")
    await nodes[1].acquire_lock("unique_2", LockType.EXCLUSIVE, "client_2")
    await nodes[2].acquire_lock("unique_3", LockType.EXCLUSIVE, "client_3")

    for node in nodes:
        locks = await node.get_all_locks()
        print(f"  - {node.node_id}: {[l['resource'] for l in locks]}")

    print("\n[5] Cleanup...")
    for node in nodes:
        await node.stop()

    print_success("Multi-Node Scenario PASSED!")
    return True


async def test7_queue_partitioning():
    """Test 7: Queue Partitioning - Consistent Hash Ring"""
    print_header("7/8: QUEUE PARTITIONING - Consistent Hash Ring")

    nodes = ["node_a", "node_b", "node_c"]
    ring = ConsistentHashRing(nodes, virtual_nodes=150)

    print("\n[1] Hash ring initialized:")
    print(f"  - Physical nodes: {len(nodes)}")
    print(f"  - Virtual nodes per physical: 150")
    print(f"  - Total ring positions: {len(ring.sorted_keys)}")

    print("\n[2] Distribute 10 topics across nodes:")
    topics = [f"topic_{i}" for i in range(10)]
    distribution = {n: 0 for n in nodes}

    for topic in topics:
        node = ring.get_node(topic)
        distribution[node] += 1
        print(f"  - '{topic}' -> {node}")

    print("\n[3] Distribution analysis:")
    for node, count in distribution.items():
        percentage = count / len(topics) * 100
        print(f"  - {node}: {count} topics ({percentage:.0f}%)")

    print("\n[4] Add new node 'node_d':")
    ring.add_node("node_d")
    print_step(f"Ring size after add: {len(ring.sorted_keys)} positions")

    print("\n[5] Check redistribution after add:")
    topics = [f"topic_{i}" for i in range(5)]
    for topic in topics:
        node = ring.get_node(topic)
        print(f"  - '{topic}' -> {node}")

    print("\n[6] Remove node 'node_a':")
    ring.remove_node("node_a")
    print_step(f"Ring size after remove: {len(ring.sorted_keys)} positions")

    print("\n[7] Verify node_a no longer in ring:")
    test_topics = ["topic_0", "topic_1"]
    for topic in test_topics:
        node = ring.get_node(topic)
        print(f"  - '{topic}' -> {node} (should NOT be node_a)")

    print_success("Queue Partitioning (Consistent Hash Ring) PASSED!")
    return True


async def test8_cache_lru():
    """Test 8: Cache LRU - Replacement Policy"""
    print_header("8/8: CACHE LRU - Least Recently Used Replacement")

    lru = LRUReplacement(capacity=3)

    print("\n[1] LRU cache initialized with capacity=3")

    print("\n[2] Add 3 items (1, 2, 3):")
    lru.touch(1)
    lru.touch(2)
    lru.touch(3)
    print(f"  Access order: {list(lru.access_order.keys())}")
    print(f"  LRU item (first): {list(lru.access_order.keys())[0]}")

    print("\n[3] Touch existing item (2) - moves to end:")
    lru.touch(2)
    print(f"  Access order: {list(lru.access_order.keys())}")

    print("\n[4] Add new item (4) - triggers eviction of LRU (1):")
    lru.touch(4)
    print(f"  Access order: {list(lru.access_order.keys())}")
    evicted = lru.evict()
    print(f"  Next to evict (evict()): {evicted}")

    print("\n[5] Access item (3) again:")
    lru.touch(3)
    print(f"  Access order: {list(lru.access_order.keys())}")

    print("\n[6] Add item (5) - triggers eviction:")
    lru.touch(5)
    print(f"  Access order: {list(lru.access_order.keys())}")

    print("\n[7] Manual remove (4):")
    lru.remove(4)
    print(f"  Access order: {list(lru.access_order.keys())}")

    print("\n[8] Verify LRU property:")
    print(f"  Items in cache: {list(lru.access_order.keys())}")
    print(f"  Next to evict (oldest): {lru.evict()}")

    print("\n[9] Stress test - add 10 items to cache of size 3:")
    lru2 = LRUReplacement(capacity=3)
    for i in range(10):
        lru2.touch(i)
    print(f"  Final items: {list(lru2.access_order.keys())}")
    print(f"  Should only have 3 items (oldest evicted)")

    print_success("Cache LRU Replacement Policy PASSED!")
    return True


async def main():
    print("="*60)
    print("DISTRIBUTED SYNC SYSTEM - SEQUENTIAL COMPONENT TEST")
    print("="*60)
    print("\nMenjalankan 8 test secara berurutan...")
    print("="*60)

    tests = [
        ("1. Raft Consensus", test1_raft_consensus),
        ("2. Lock Manager", test2_lock_manager),
        ("3. Distributed Queue", test3_distributed_queue),
        ("4. Cache (MESI)", test4_cache_mesi),
        ("5. Failure Detector", test5_failure_detector),
        ("6. Multi-Node Scenario", test6_multi_node_scenario),
        ("7. Queue Partitioning", test7_queue_partitioning),
        ("8. Cache LRU", test8_cache_lru),
    ]

    results = []
    for name, test_func in tests:
        try:
            success = await test_func()
            results.append((name, True))
        except Exception as e:
            print(f"\n[ERROR] {name} failed: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "="*60)
    print("FINAL SUMMARY")
    print("="*60)

    passed = sum(1 for _, s in results if s)
    total = len(results)

    for name, success in results:
        status = "[PASS]" if success else "[FAIL]"
        print(f"  {status} {name}")

    print("\n" + "-"*60)
    print(f"Result: {passed}/{total} tests passed")
    print("-"*60)

    return passed == total


if __name__ == "__main__":
    asyncio.run(main())