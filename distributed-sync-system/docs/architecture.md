# Distributed Synchronization System - Architecture

## Overview

This system implements a distributed synchronization mechanism with three main components:
1. **Distributed Lock Manager** - Based on Raft consensus
2. **Distributed Queue** - Using consistent hashing
3. **Cache Coherence** - Implementing MESI protocol with LRU eviction

## System Architecture

```
+-------------------+     +-------------------+     +-------------------+
|    Node 1         |     |    Node 2         |     |    Node 3         |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| | Raft Node     | |     | | Raft Node     | |     | | Raft Node     | |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| | Lock Manager  | |     | | Lock Manager  | |     | | Lock Manager  | |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| | Queue System  | |     | | Queue System  | |     | | Queue System  | |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| +---------------+ |     | +---------------+ |     | +---------------+ |
| | Cache (MESI)  | |     | | Cache (MESI)  | |     | | Cache (MESI)  | |
| +---------------+ |     | +---------------+ |     | +---------------+ |
+-------------------+     +-------------------+     +-------------------+
         |                       |                       |
         +-----------------------+-----------------------+
                                 |
                         +-------v-------+
                         |  Redis (Pub/Sub) |
                         +-----------------+
```

## Component Details

### 1. Raft Consensus

The Raft algorithm ensures leader election and log replication across all nodes.

**States:**
- Follower: Default state, waits for heartbeats
- Candidate: Initiates election when heartbeat timeout expires
- Leader: Handles all write requests and sends heartbeats

**Properties:**
- Strong leader: Writes go through leader
- Leader append-only: Leader never overwrites or deletes entries
- Log matching: Logs are consistent across nodes

### 2. Distributed Lock Manager

Implements shared and exclusive locks with deadlock detection.

**Features:**
- Shared locks: Multiple clients can hold simultaneously
- Exclusive locks: Only one client can hold
- Deadlock detection: Waits-for graph to detect cycles
- Lock expiration: Automatic cleanup of stale locks

### 3. Distributed Queue

Uses consistent hashing for message distribution across nodes.

**Features:**
- Consistent hashing: Even distribution of messages
- Partition replication: Messages replicated for fault tolerance
- At-least-once delivery: Acknowledgment-based delivery

### 4. Cache Coherence (MESI Protocol)

Implements Modified-Exclusive-Shared-Invalid states.

**States:**
- M (Modified): Cache line modified, exclusive ownership
- E (Exclusive): Cache line clean, exclusive ownership
- S (Shared): Cache line potentially shared with other caches
- I (Invalid): Cache line not valid

**LRU Replacement:**
- When cache is full, least recently used line is evicted
- Evicted Modified lines are flushed to main memory

## Communication

Nodes communicate via TCP connections using JSON messages:

**Message Types:**
- `heartbeat`: Periodic health check
- `request_vote`: Raft election voting
- `append_entries`: Raft log replication
- `lock_request`: Distributed lock operations
- `queue_message`: Queue operations

## Failure Detection

Nodes monitor each other using heartbeats:
- Normal heartbeat interval: 50ms
- Suspected failure after: 2 missed heartbeats
- Marked dead after: 5 missed heartbeats

## Scalability

The system scales horizontally by adding more nodes:
1. New node joins consistent hash ring
2. Raft consensus rebalances
3. Lock and queue ownership redistributed

## Performance Metrics

- Lock acquisition latency: ~10-50ms
- Queue enqueue latency: ~5-20ms
- Cache hit rate target: >80%
- System throughput: 1000+ ops/sec