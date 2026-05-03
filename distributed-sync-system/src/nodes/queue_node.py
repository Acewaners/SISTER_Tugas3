from dataclasses import dataclass
import asyncio
import hashlib
import time
import uuid
from typing import Dict, List, Optional, Set
import logging

logger = logging.getLogger(__name__)


class ConsistentHashRing:
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self._build_ring(nodes)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self, nodes: List[str]):
        self.ring.clear()
        self.sorted_keys = []
        for node in nodes:
            for i in range(self.virtual_nodes):
                key = self._hash(f"{node}:vn{i}")
                self.ring[key] = node
        self.sorted_keys = sorted(self.ring.keys())

    def get_node(self, key: str) -> str:
        if not self.sorted_keys:
            return None
        h = self._hash(key)
        for k in self.sorted_keys:
            if h <= k:
                return self.ring[k]
        return self.ring[self.sorted_keys[0]]

    def add_node(self, node: str):
        for i in range(self.virtual_nodes):
            key = self._hash(f"{node}:vn{i}")
            self.ring[key] = node
        self.sorted_keys = sorted(self.ring.keys())

    def remove_node(self, node: str):
        self.ring = {k: v for k, v in self.ring.items() if v != node}
        self.sorted_keys = sorted(self.ring.keys())


@dataclass
class Message:
    msg_id: str
    payload: str
    timestamp: float
    retry_count: int = 0
    acknowledged: bool = False


class DistributedQueue:
    def __init__(self, node_id: str, peers: List[str], port: int, partitions: int = 16):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.partitions = partitions
        self.ring = ConsistentHashRing(peers + [node_id])
        
        self.local_queues: Dict[str, List[Message]] = {}
        self.local_consumer_positions: Dict[str, int] = {}
        self.pending_ack: Dict[str, asyncio.Event] = {}
        
        self.producers: Set[str] = set()
        self.consumers: Set[str] = set()
        
        self.replication_factor = 3
        self.max_retry = 3
        
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self):
        self._running = True
        for i in range(self.partitions):
            self.local_queues[f"partition_{i}"] = []
        asyncio.create_task(self._message_recovery())
        asyncio.create_task(self._replication_sync())

    async def stop(self):
        self._running = False

    async def enqueue(self, topic: str, payload: str, producer_id: str) -> Dict:
        async with self._lock:
            msg_id = str(uuid.uuid4())
            partition_key = self.ring.get_node(topic)
            partition_idx = hashlib.md5(topic.encode()).hexdigest()
            partition_num = int(partition_idx, 16) % self.partitions
            partition_name = f"partition_{partition_num}"
            
            msg = Message(msg_id, payload, time.time())
            self.local_queues[partition_name].append(msg)
            self.producers.add(producer_id)
            
            asyncio.create_task(self._replicate_message(partition_name, msg))
            
            logger.info(f"Message {msg_id} enqueued to {partition_name} by {producer_id}")
            return {"success": True, "msg_id": msg_id, "partition": partition_name}

    async def _replicate_message(self, partition: str, message: Message):
        target_nodes = self.peers[:self.replication_factor]
        for node in target_nodes:
            asyncio.create_task(self._send_to_node(node, {"type": "replicate", "partition": partition, "message": {
                "msg_id": message.msg_id, "payload": message.payload, "timestamp": message.timestamp
            }}))

    async def _send_to_node(self, node: str, data: Dict):
        try:
            port = int(node.split(":")[1]) if ":" in node else 8000
            host = node.split(":")[0] if ":" in node else "localhost"
            r, w = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            import json
            w.write(json.dumps(data).encode())
            await w.drain()
            w.close()
        except Exception as e:
            logger.error(f"Failed to send to {node}: {e}")

    async def dequeue(self, topic: str, consumer_id: str, timeout: int = 30) -> Optional[Dict]:
        partition_num = int(hashlib.md5(topic.encode()).hexdigest(), 16) % self.partitions
        partition_name = f"partition_{partition_num}"
        
        async with self._lock:
            if partition_name not in self.local_queues:
                return None
            
            queue = self.local_queues[partition_name]
            if not queue:
                await asyncio.sleep(0.1)
                for _ in range(timeout * 10):
                    if queue:
                        break
                    await asyncio.sleep(0.1)
                if not queue:
                    return None
            
            message = queue.pop(0)
            self.consumers.add(consumer_id)
            
            ack_event = asyncio.Event()
            self.pending_ack[message.msg_id] = ack_event
            
            asyncio.create_task(self._wait_for_ack(message.msg_id, ack_event, partition_name, message))
            
            return {"msg_id": message.msg_id, "payload": message.payload, "timestamp": message.timestamp}

    async def _wait_for_ack(self, msg_id: str, event: asyncio.Event, partition: str, message: Message):
        try:
            await asyncio.wait_for(event.wait(), timeout=30)
        except:
            async with self._lock:
                if partition in self.local_queues and message not in self.local_queues[partition]:
                    self.local_queues[partition].insert(0, message)

    async def acknowledge(self, msg_id: str, consumer_id: str) -> Dict:
        async with self._lock:
            if msg_id in self.pending_ack:
                self.pending_ack[msg_id].set()
                del self.pending_ack[msg_id]
                logger.info(f"Message {msg_id} acknowledged by {consumer_id}")
                return {"success": True, "msg_id": msg_id}
            return {"success": False, "error": "message_not_found"}

    async def _message_recovery(self):
        while self._running:
            await asyncio.sleep(60)
            async with self._lock:
                for partition, queue in self.local_queues.items():
                    for msg in queue:
                        if time.time() - msg.timestamp > 300 and not msg.acknowledged:
                            if msg.retry_count < self.max_retry:
                                msg.retry_count += 1
                                asyncio.create_task(self._replicate_message(partition, msg))

    async def _replication_sync(self):
        while self._running:
            await asyncio.sleep(30)

    async def get_queue_stats(self) -> Dict:
        async with self._lock:
            total_messages = sum(len(q) for q in self.local_queues.values())
            return {
                "node_id": self.node_id,
                "total_messages": total_messages,
                "partitions": {k: len(v) for k, v in self.local_queues.items()},
                "producers": len(self.producers),
                "consumers": len(self.consumers),
                "pending_acks": len(self.pending_ack)
            }

    def get_state(self) -> Dict:
        return {"node_id": self.node_id, "partitions": self.partitions, "replication_factor": self.replication_factor}