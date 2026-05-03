import asyncio
import time
from typing import Dict, List, Set
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class NodeStatus:
    node_id: str
    is_alive: bool
    last_heartbeat: float
    consecutive_failures: int = 0
    is_suspected: bool = False


class FailureDetector:
    def __init__(self, node_id: str, peers: List[str], heartbeat_interval: float = 5.0, timeout: float = 15.0):
        self.node_id = node_id
        self.peers = peers
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout

        self.node_status: Dict[str, NodeStatus] = {}
        self.suspected_nodes: Set[str] = set()
        self.dead_nodes: Set[str] = set()

        self._running = False
        self._lock = asyncio.Lock()

        for peer in peers:
            self.node_status[peer] = NodeStatus(peer, True, time.time())

    async def start(self):
        if not self.peers:
            logger.info(f"No peers configured for {self.node_id}")
            return

        self._running = True
        asyncio.create_task(self._heartbeat_sender())
        asyncio.create_task(self._failure_checker())
        logger.info(f"FailureDetector started for {self.node_id} with peers: {self.peers}")

    async def stop(self):
        self._running = False

    async def _heartbeat_sender(self):
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            for peer in self.peers:
                if not self._running:
                    break
                asyncio.create_task(self._send_heartbeat(peer))

    async def _send_heartbeat(self, peer: str):
        try:
            import json
            port = int(peer.split(":")[1]) if ":" in peer else 8000
            host = peer.split(":")[0] if ":" in peer else "localhost"

            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=2)

            writer.write(json.dumps({"type": "heartbeat", "sender": self.node_id}).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

            await self._mark_alive(peer)
        except Exception as e:
            await self._mark_potential_failure(peer)

    async def _mark_alive(self, peer: str):
        async with self._lock:
            if peer in self.node_status:
                self.node_status[peer].is_alive = True
                self.node_status[peer].last_heartbeat = time.time()
                self.node_status[peer].consecutive_failures = 0
                self.node_status[peer].is_suspected = False
                self.suspected_nodes.discard(peer)
                self.dead_nodes.discard(peer)

    async def _mark_potential_failure(self, peer: str):
        async with self._lock:
            if peer not in self.node_status:
                return

            self.node_status[peer].consecutive_failures += 1

            if self.node_status[peer].consecutive_failures >= 2:
                if peer not in self.suspected_nodes:
                    self.suspected_nodes.add(peer)
                    logger.warning(f"Node {peer} suspected to be failed (failures: {self.node_status[peer].consecutive_failures})")

    async def _failure_checker(self):
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            async with self._lock:
                now = time.time()
                for peer, status in list(self.node_status.items()):
                    time_since_heartbeat = now - status.last_heartbeat

                    if time_since_heartbeat > self.timeout:
                        if not status.is_suspected:
                            status.is_suspected = True
                            self.suspected_nodes.add(peer)
                            logger.warning(f"Node {peer} marked as suspected (last heartbeat: {time_since_heartbeat:.1f}s ago)")

                        if time_since_heartbeat > self.timeout * 2:
                            if status.is_alive:
                                status.is_alive = False
                                self.dead_nodes.add(peer)
                                logger.error(f"Node {peer} marked as dead (last heartbeat: {time_since_heartbeat:.1f}s ago)")

    def is_alive(self, node_id: str) -> bool:
        status = self.node_status.get(node_id)
        return status.is_alive if status else False

    def is_suspected(self, node_id: str) -> bool:
        return node_id in self.suspected_nodes

    def is_dead(self, node_id: str) -> bool:
        return node_id in self.dead_nodes

    def get_quorum(self, required: int = None) -> List[str]:
        if required is None:
            required = (len(self.peers) + 1) // 2

        alive = [peer for peer in self.peers if self.is_alive(peer)]
        return alive if len(alive) >= required else []

    def get_all_status(self) -> Dict[str, Dict]:
        return {
            peer: {
                "is_alive": status.is_alive,
                "last_heartbeat": status.last_heartbeat,
                "consecutive_failures": status.consecutive_failures,
                "is_suspected": status.is_suspected
            }
            for peer, status in self.node_status.items()
        }

    def get_stats(self) -> Dict:
        return {
            "node_id": self.node_id,
            "total_peers": len(self.peers),
            "alive": len([p for p in self.peers if self.is_alive(p)]),
            "suspected": len(self.suspected_nodes),
            "dead": len(self.dead_nodes)
        }

    async def handle_heartbeat(self, sender: str):
        await self._mark_alive(sender)

    def reset_dead_node(self, node_id: str):
        if node_id in self.dead_nodes:
            self.dead_nodes.discard(node_id)
        if node_id in self.node_status:
            self.node_status[node_id].is_alive = True
            self.node_status[node_id].consecutive_failures = 0
            self.node_status[node_id].is_suspected = False
        logger.info(f"Node {node_id} marked as alive again")