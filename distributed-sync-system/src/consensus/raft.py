import asyncio
import random
import time
import uuid
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = __import__('logging').getLogger(__name__)


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class MessageType(Enum):
    REQUEST_VOTE = "request_vote"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    HEARTBEAT = "heartbeat"


@dataclass
class LogEntry:
    term: int
    index: int
    command: str
    data: Dict = field(default_factory=dict)


@dataclass
class Message:
    msg_type: MessageType
    sender_id: str
    receiver_id: str
    term: int
    data: Dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class RaftNode:
    def __init__(self, node_id: str, peers: List[str], port: int):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_heartbeat = time.time()
        self.election_timeout = random.randint(150, 300)
        self.heartbeat_interval = 50
        self.match_index: Dict[str, int] = {}
        self.next_index: Dict[str, int] = {}
        self.leader_id: Optional[str] = None
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self):
        self._running = True
        asyncio.create_task(self._election_loop())
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        self._running = False

    async def _election_loop(self):
        while self._running:
            await asyncio.sleep(0.1)
            elapsed = time.time() - self.last_heartbeat
            if elapsed > self.election_timeout / 1000:
                if self.state != RaftState.LEADER:
                    await self._start_election()

    async def _start_election(self):
        async with self._lock:
            self.state = RaftState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            last_idx = self.log[-1].index if self.log else 0
            last_term = self.log[-1].term if self.log else 0
            for peer in self.peers:
                asyncio.create_task(self._request_vote(peer, last_idx, last_term))

    async def _request_vote(self, peer: str, last_idx: int, last_term: int):
        try:
            port = int(peer.split(':')[1]) if ':' in peer else 8000
            r, w = await asyncio.wait_for(asyncio.open_connection('localhost', port), timeout=1)
            import json
            w.write(json.dumps({'type': 'request_vote', 'sender': self.node_id, 'term': self.current_term, 
                'data': {'candidate_id': self.node_id, 'last_log_index': last_idx, 'last_log_term': last_term}}).encode())
            await w.drain()
            w.close()
        except: pass

    async def _heartbeat_loop(self):
        while self._running:
            await asyncio.sleep(self.heartbeat_interval / 1000)
            if self.state == RaftState.LEADER:
                await self._send_heartbeats()

    async def _send_heartbeats(self):
        for peer in self.peers:
            try:
                port = int(peer.split(':')[1]) if ':' in peer else 8000
                r, w = await asyncio.wait_for(asyncio.open_connection('localhost', port), timeout=1)
                import json
                w.write(json.dumps({'type': 'heartbeat', 'sender': self.node_id, 'term': self.current_term,
                    'data': {'leader_commit': self.commit_index}}).encode())
                await w.drain()
                w.close()
            except: pass

    async def handle_message(self, msg: Dict):
        async with self._lock:
            term = msg.get('term', 0)
            if term > self.current_term:
                self.current_term = term
                self.state = RaftState.FOLLOWER
            if msg.get('type') == 'heartbeat':
                self.last_heartbeat = time.time()
                self.state = RaftState.FOLLOWER
                self.leader_id = msg.get('sender')
            elif msg.get('type') == 'request_vote':
                self.last_heartbeat = time.time()
                last_idx = msg['data'].get('last_log_index', 0)
                granted = (self.voted_for is None or self.voted_for == msg['sender']) and last_idx >= (self.log[-1].index if self.log else 0)
                if granted:
                    self.voted_for = msg['sender']

    async def submit_command(self, command: str, data: Dict = None) -> bool:
        async with self._lock:
            if self.state != RaftState.LEADER:
                return False
            entry = LogEntry(term=self.current_term, index=len(self.log) + 1, command=command, data=data or {})
            self.log.append(entry)
            return True

    def get_state(self) -> Dict:
        return {'node_id': self.node_id, 'state': self.state.value, 'term': self.current_term,
                'is_leader': self.state == RaftState.LEADER, 'log_length': len(self.log)}

    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def get_leader_id(self) -> Optional[str]:
        if self.state == RaftState.LEADER:
            return self.node_id
        return self.leader_id
