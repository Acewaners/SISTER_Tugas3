# Distributed Synchronization System - Architecture

## Overview

This system implements a distributed synchronization mechanism with three main components:
1. **Distributed Lock Manager** - Based on Raft consensus
2. **Distributed Queue** - Using consistent hashing
3. **Cache Coherence** - Implementing MESI protocol with LRU eviction


## Diagram Format Mermaid
flowchart TB
    Client((Client Request))

    subgraph Docker_Network ["Docker Overlay Network"]
        direction TB

        subgraph Node_1 ["Node 1 (Leader/Follower)"]
            direction TB
            API_1["API Gateway / Base Node"]
            
            subgraph Services_1 ["Distributed Services"]
                LM_1["Lock Manager (Exclusive/Shared)"]
                QN_1["Queue Node (Consistent Hashing)"]
                CN_1["Cache Node (MESI Protocol)"]
            end
            
            subgraph Core_1 ["Core Components"]
                Raft_1["Raft Consensus (Log & State)"]
                Comm_1["Communication Layer & Failure Detector"]
            end
            
            API_1 --> Services_1
            Services_1 --> Raft_1
            Raft_1 --> Comm_1
        end

        subgraph Node_2 ["Node 2 (Follower)"]
            Comm_2["Communication Layer"]
            Raft_2["Raft Consensus"]
            Services_2["Services: Lock/Queue/Cache"]
            Comm_2 --- Raft_2 --- Services_2
        end

        subgraph Node_3 ["Node 3 (Follower)"]
            Comm_3["Communication Layer"]
            Raft_3["Raft Consensus"]
            Services_3["Services: Lock/Queue/Cache"]
            Comm_3 --- Raft_3 --- Services_3
        end
    end

    Client -->|HTTP / RPC| API_1
    Client -->|HTTP / RPC| Node_2
    Client -->|HTTP / RPC| Node_3

    Comm_1 <==>|"TCP/UDP (Heartbeats)"| Comm_2
    Comm_2 <==>|"TCP/UDP (Heartbeats)"| Comm_3
    Comm_3 <==>|"TCP/UDP (Heartbeats)"| Comm_1
    
    %% Styling
    classDef node fill:#f9f9f9,stroke:#333,stroke-width:2px;
    classDef core fill:#e1f5fe,stroke:#0288d1,stroke-width:2px;
    classDef service fill:#fff3e0,stroke:#f57c00,stroke-width:2px;
    
    class Node_1,Node_2,Node_3 node;
    class Raft_1,Comm_1,Raft_2,Comm_2,Raft_3,Comm_3 core;
    class LM_1,QN_1,CN_1,Services_2,Services_3 service;


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