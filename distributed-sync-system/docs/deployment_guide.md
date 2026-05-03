# Deployment Guide - Distributed Synchronization System

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Redis (optional, for pub/sub)

## Local Development

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run Without Docker

```bash
# Terminal 1 - Node 1
python -m src.nodes --node-id node_1 --port 8001

# Terminal 2 - Node 2
python -m src.nodes --node-id node_2 --port 8002

# Terminal 3 - Node 3
python -m src.nodes --node-id node_3 --port 8003
```

## Docker Deployment

### Build Images

```bash
cd distributed-sync-system
docker-compose build
```

### Start Cluster

```bash
docker-compose up -d
```

### Check Logs

```bash
docker-compose logs -f
```

### Scale Nodes

```bash
docker-compose up -d --scale node-1=2 --scale node-2=2 --scale node-3=2
```

### Stop Cluster

```bash
docker-compose down
```

## Configuration

Edit `.env` file or set environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| NODE_ID | node_1 | Node identifier |
| NODE_PORT | 8001 | TCP port |
| NODE_COUNT | 3 | Number of nodes |
| REDIS_HOST | localhost | Redis host |
| ELECTION_TIMEOUT_MIN | 150 | Min election timeout (ms) |
| ELECTION_TIMEOUT_MAX | 300 | Max election timeout (ms) |
| HEARTBEAT_INTERVAL | 50 | Heartbeat interval (ms) |
| LOCK_DEFAULT_TTL | 30 | Default lock TTL (seconds) |
| CACHE_MAX_SIZE | 1000 | Max cache entries |
| VIRTUAL_NODES | 150 | Consistent hash virtual nodes |

## Testing

### Unit Tests

```bash
pytest tests/unit/
```

### Integration Tests

```bash
pytest tests/integration/
```

### Performance Tests

```bash
pytest tests/performance/
locust -f benchmarks/load_test_scenarios.py
```

## Troubleshooting

### Node not starting

Check if port is already in use:
```bash
netstat -an | grep 8001
```

### Leader election issues

Check network connectivity between nodes:
```bash
docker-compose exec node-1 ping node-2
```

### Lock timeout

Increase lock TTL or check for deadlocks in the system.

### Cache miss rate high

Check cache size configuration and eviction policy.

## Monitoring

Access metrics via API:
```bash
curl http://localhost:8001/status
```

## API Examples

### Acquire Lock

```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"resource": "file1", "lock_type": "EXCLUSIVE", "client_id": "client1"}'
```

### Enqueue Message

```bash
curl -X POST http://localhost:8001/queue/enqueue \
  -H "Content-Type: application/json" \
  -d '{"topic": "orders", "payload": "order_data", "producer_id": "producer1"}'
```

### Cache Write

```bash
curl -X POST http://localhost:8001/cache/write \
  -H "Content-Type: application/json" \
  -d '{"address": 100, "data": "value", "writer_id": "writer1"}'
```
