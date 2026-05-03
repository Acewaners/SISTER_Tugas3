from dataclasses import dataclass
import asyncio
import time
from enum import Enum
from typing import Dict, Optional, List, Any
from collections import OrderedDict
import logging

logger = logging.getLogger(__name__)


class CacheState(Enum):
    MODIFIED = "M"
    OWNED = "O"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


@dataclass
class CacheLine:
    address: int
    state: CacheState
    data: Optional[bytes]
    last_updated: float
    version: int


class LRUReplacement:
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.access_order: OrderedDict[int, None] = OrderedDict()

    def touch(self, address: int):
        if address in self.access_order:
            del self.access_order[address]
        self.access_order[address] = None
        if len(self.access_order) > self.capacity:
            self.access_order.popitem(last=False)

    def evict(self) -> Optional[int]:
        if self.access_order:
            return list(self.access_order.keys())[0]
        return None

    def remove(self, address: int):
        if address in self.access_order:
            del self.access_order[address]


class CacheNode:
    def __init__(self, node_id: str, peers: List[str], port: int, max_size: int = 1000, ttl: int = 300):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.max_size = max_size
        self.ttl = ttl
        
        self.cache: Dict[int, CacheLine] = {}
        self.lru = LRUReplacement(max_size)
        
        self.pending_requests: Dict[str, asyncio.Event] = {}
        self.write_buffer: Dict[int, bytes] = {}
        
        self._running = False
        self._lock = asyncio.Lock()
        
        self.hit_count = 0
        self.miss_count = 0
        self.invalidation_count = 0

    async def start(self):
        self._running = True
        asyncio.create_task(self._cache_cleanup())
        asyncio.create_task(self._invalidation_handler())

    async def stop(self):
        self._running = False

    async def read(self, address: int, requestor_id: str) -> Optional[bytes]:
        async with self._lock:
            if address in self.cache:
                line = self.cache[address]
                if line.state != CacheState.INVALID:
                    self.lru.touch(address)
                    self.hit_count += 1
                    await self._send_snoop(address, "read_shared", requestor_id)
                    return line.data
            
            self.miss_count += 1
            msg_id = f"{address}_{time.time()}"
            
            await self._request_from_peer(address, requestor_id)
            
            if address in self.cache:
                return self.cache[address].data
            return None

    async def write(self, address: int, data: bytes, writer_id: str) -> bool:
        async with self._lock:
            if address in self.cache:
                line = self.cache[address]
                if line.state == CacheState.MODIFIED:
                    line.data = data
                    line.last_updated = time.time()
                    line.version += 1
                    self.lru.touch(address)
                    await self._broadcast_invalidation(address)
                    return True
            
            await self._broadcast_upgrade(address)
            
            if address not in self.cache and len(self.cache) >= self.max_size:
                evicted = self.lru.evict()
                if evicted is not None:
                    del self.cache[evicted]
                    self.lru.remove(evicted)
            
            self.cache[address] = CacheLine(address, CacheState.MODIFIED, data, time.time(), 1)
            self.lru.touch(address)
            await self._broadcast_invalidation(address)
            return True

    async def _request_from_peer(self, address: int, requestor_id: str):
        for peer in self.peers:
            asyncio.create_task(self._send_to_peer(peer, {
                "type": "cache_read_request", "address": address, "requestor": self.node_id
            }))

    async def _send_snoop(self, address: int, operation: str, requestor_id: str):
        for peer in self.peers:
            asyncio.create_task(self._send_to_peer(peer, {
                "type": "snoop", "address": address, "operation": operation, "source": self.node_id
            }))

    async def _broadcast_invalidation(self, address: int):
        self.invalidation_count += 1
        for peer in self.peers:
            asyncio.create_task(self._send_to_peer(peer, {
                "type": "invalidate", "address": address, "source": self.node_id
            }))

    async def _broadcast_upgrade(self, address: int):
        for peer in self.peers:
            asyncio.create_task(self._send_to_peer(peer, {
                "type": "upgrade_request", "address": address, "source": self.node_id
            }))

    async def _send_to_peer(self, peer: str, data: Dict):
        try:
            port = int(peer.split(":")[1]) if ":" in peer else 8000
            r, w = await asyncio.wait_for(asyncio.open_connection("localhost", port), timeout=5)
            import json
            w.write(json.dumps(data).encode())
            await w.drain()
            w.close()
        except Exception as e:
            logger.debug(f"Failed to send to {peer}: {e}")

    async def handle_snoop(self, address: int, operation: str):
        async with self._lock:
            if address in self.cache:
                line = self.cache[address]
                if operation == "read_shared":
                    if line.state == CacheState.MODIFIED:
                        line.state = CacheState.OWNED
                elif operation == "invalidate":
                    line.state = CacheState.INVALID

    async def _cache_cleanup(self):
        while self._running:
            await asyncio.sleep(60)
            async with self._lock:
                now = time.time()
                expired = [addr for addr, line in self.cache.items() if now - line.last_updated > self.ttl]
                for addr in expired:
                    if self.cache[addr].state == CacheState.MODIFIED:
                        await self._flush_to_memory(addr)
                    del self.cache[addr]
                    self.lru.remove(addr)

    async def _flush_to_memory(self, address: int):
        if address in self.cache:
            logger.debug(f"Flushing address {address} to memory")

    async def _invalidation_handler(self):
        while self._running:
            await asyncio.sleep(1)

    async def get_cache_stats(self) -> Dict:
        async with self._lock:
            return {
                "node_id": self.node_id,
                "cache_size": len(self.cache),
                "max_size": self.max_size,
                "hits": self.hit_count,
                "misses": self.miss_count,
                "hit_rate": self.hit_count / (self.hit_count + self.miss_count) if self.hit_count + self.miss_count > 0 else 0,
                "invalidations": self.invalidation_count,
                "states": {hex(addr): line.state.value for addr, line in self.cache.items()}
            }

    async def invalidate(self, address: int, source: str) -> bool:
        async with self._lock:
            if address in self.cache:
                self.cache[address].state = CacheState.INVALID
                logger.info(f"Address {address} invalidated by {source}")
                return True
            return False

    def get_state(self) -> Dict:
        return {
            "node_id": self.node_id,
            "cache_entries": len(self.cache),
            "lru_size": len(self.lru.access_order),
            "hit_rate": self.hit_count / (self.hit_count + self.miss_count) if self.hit_count + self.miss_count > 0 else 0
        }