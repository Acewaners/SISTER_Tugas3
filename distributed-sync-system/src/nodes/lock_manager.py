import asyncio
import time
import uuid
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class LockType(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class Lock:
    resource_id: str
    lock_type: LockType
    holder_id: str
    request_id: str
    created_at: float
    ttl: int


class DistributedLockManager:
    def __init__(self, node_id: str, peers: List[str], port: int):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.locks: Dict[str, Lock] = {}
        self.shared_locks: Dict[str, set] = {}
        self.waiting_queue: Dict[str, List[Dict]] = {}
        self.wait_graph: Dict[str, set] = {}
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self):
        self._running = True
        asyncio.create_task(self._deadlock_detector())
        asyncio.create_task(self._lock_expiry_checker())

    async def stop(self):
        self._running = False

    async def acquire_lock(self, resource_id: str, lock_type: LockType, client_id: str, 
                          timeout: int = 30) -> Dict:
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        async with self._lock:
            if resource_id in self.locks:
                existing = self.locks[resource_id]
                if lock_type == LockType.SHARED and existing.lock_type == LockType.SHARED:
                    if resource_id not in self.shared_locks:
                        self.shared_locks[resource_id] = set()
                    self.shared_locks[resource_id].add(client_id)
                    return {"success": True, "request_id": request_id, "resource": resource_id}
                
                if existing.holder_id == client_id and lock_type == LockType.EXCLUSIVE:
                    self.locks[resource_id] = Lock(resource_id, lock_type, client_id, request_id, time.time(), timeout)
                    return {"success": True, "request_id": request_id, "resource": resource_id}
                
                if resource_id not in self.waiting_queue:
                    self.waiting_queue[resource_id] = []
                self.waiting_queue[resource_id].append({"client_id": client_id, "lock_type": lock_type, "request_id": request_id})
                
                await asyncio.sleep(0.1)
                while time.time() - start_time < timeout:
                    if resource_id not in self.locks or self.locks[resource_id].holder_id == client_id:
                        break
                    await asyncio.sleep(0.1)
                
                if resource_id not in self.locks:
                    self.locks[resource_id] = Lock(resource_id, lock_type, client_id, request_id, time.time(), timeout)
                    return {"success": True, "request_id": request_id, "resource": resource_id}
                return {"success": False, "error": "timeout", "request_id": request_id}
            else:
                self.locks[resource_id] = Lock(resource_id, lock_type, client_id, request_id, time.time(), timeout)
                return {"success": True, "request_id": request_id, "resource": resource_id}

    async def release_lock(self, resource_id: str, client_id: str) -> Dict:
        async with self._lock:
            if resource_id not in self.locks:
                return {"success": False, "error": "lock_not_found"}
            if self.locks[resource_id].holder_id != client_id:
                return {"success": False, "error": "not_holder"}
            del self.locks[resource_id]
            if resource_id in self.shared_locks:
                del self.shared_locks[resource_id]
            await self._process_waiting(resource_id)
            return {"success": True, "resource": resource_id}

    async def _process_waiting(self, resource_id: str):
        if resource_id in self.waiting_queue and self.waiting_queue[resource_id]:
            req = self.waiting_queue[resource_id].pop(0)
            self.locks[resource_id] = Lock(resource_id, req["lock_type"], req["client_id"], req["request_id"], time.time(), 30)

    async def _deadlock_detector(self):
        while self._running:
            await asyncio.sleep(5)
            cycles = self._detect_cycles()
            for cycle in cycles:
                logger.warning(f"Deadlock detected: {cycle}")
                await self._resolve_deadlock(cycle)

    def _detect_cycles(self) -> List[List[str]]:
        cycles = []
        for waiter in self.wait_graph:
            for holder in self.wait_graph.get(waiter, []):
                if holder in self.wait_graph and waiter in self.wait_graph.get(holder, []):
                    cycles.append([waiter, holder, waiter])
        return cycles

    async def _resolve_deadlock(self, cycle: List[str]):
        victim = cycle[0]
        for rid, lock in list(self.locks.items()):
            if lock.holder_id == victim:
                del self.locks[rid]
                await self._process_waiting(rid)

    async def _lock_expiry_checker(self):
        while self._running:
            await asyncio.sleep(1)
            async with self._lock:
                expired = [rid for rid, lock in self.locks.items() if time.time() - lock.created_at > lock.ttl]
                for rid in expired:
                    del self.locks[rid]
                    await self._process_waiting(rid)

    async def get_lock_status(self, resource_id: str) -> Dict:
        async with self._lock:
            if resource_id in self.locks:
                lock = self.locks[resource_id]
                return {"resource": resource_id, "locked": True, "lock_type": lock.lock_type.value, "holder": lock.holder_id}
            return {"resource": resource_id, "locked": False}

    async def get_all_locks(self) -> List[Dict]:
        async with self._lock:
            return [{"resource": rid, "lock_type": l.lock_type.value, "holder": l.holder_id} for rid, l in self.locks.items()]

    def get_state(self) -> Dict:
        return {"node_id": self.node_id, "locks_held": len(self.locks), "waiting_queue_size": sum(len(v) for v in self.waiting_queue.values())}