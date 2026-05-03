import asyncio
import random
import time
import json
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
        self._randomize_timeout()
        self.heartbeat_interval = 1.0
        self.votes_received: List[str] = []
        self.match_index: Dict[str, int] = {}
        self.next_index: Dict[str, int] = {}
        self.leader_id: Optional[str] = None
        self._running = False
        self._lock = asyncio.Lock()
        self._last_election_time = 0

    async def start(self):
        self._running = True
        asyncio.create_task(self._election_loop())
        asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        self._running = False

    def _randomize_timeout(self):
        self.election_timeout = random.uniform(3.0, 6.0)

    async def _election_loop(self):
        while self._running:
            await asyncio.sleep(0.5)
            if not self._running:
                break

            elapsed = time.time() - self.last_heartbeat

            if self.state != RaftState.LEADER and elapsed > self.election_timeout:
                print(f"[RAFT] {self.node_id}: Election timeout ({elapsed:.1f}s)")
                self._randomize_timeout()
                await self._start_election()

    async def _start_election(self):
        async with self._lock:
            self.state = RaftState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = [self.node_id]
            self.last_heartbeat = time.time()  # Reset to avoid immediate timeout
            print(f"[RAFT] {self.node_id}: Starting election for term {self.current_term}")

            last_idx = self.log[-1].index if self.log else 0
            last_term = self.log[-1].term if self.log else 0

            for peer in self.peers:
                asyncio.create_task(self._request_vote(peer, last_idx, last_term))

    async def _request_vote(self, peer: str, last_idx: int, last_term: int):
        try:
            port = int(peer.split(':')[1]) if ':' in peer else 8000
            print(f"[RAFT] {self.node_id}: Connecting to {peer}:{port}...")

            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port), timeout=2
            )
            print(f"[RAFT] {self.node_id}: Connected! Sending vote request...")

            msg = {
                'type': 'request_vote',
                'sender': f'127.0.0.1:{self.port}',
                'sender_id': self.node_id,
                'term': self.current_term,
                'data': {
                    'candidate_id': self.node_id,
                    'last_log_index': last_idx,
                    'last_log_term': last_term
                }
            }
            writer.write(json.dumps(msg).encode())
            await writer.drain()
            print(f"[RAFT] {self.node_id}: Sent vote request to {peer}")
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[RAFT] {self.node_id}: FAILED to send vote to {peer}: {e}")

    async def _heartbeat_loop(self):
        while self._running:
            await asyncio.sleep(self.heartbeat_interval)
            if self.state == RaftState.LEADER:
                await self._send_heartbeats()

    async def _send_heartbeats(self):
        for peer in self.peers:
            asyncio.create_task(self._send_heartbeat(peer))

    async def _send_heartbeat(self, peer: str):
        try:
            port = int(peer.split(':')[1]) if ':' in peer else 8000
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port), timeout=2
            )

            msg = {
                'type': 'heartbeat',
                'sender': f'127.0.0.1:{self.port}',
                'sender_id': self.node_id,
                'term': self.current_term,
                'data': {'leader_commit': self.commit_index}
            }
            writer.write(json.dumps(msg).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[RAFT] {self.node_id}: Failed to send heartbeat to {peer}")

    async def handle_message(self, msg: Dict):
        async with self._lock:
            term = msg.get('term', 0)

            if term > self.current_term:
                self.current_term = term
                if self.state == RaftState.LEADER:
                    print(f"[RAFT] {self.node_id}: Higher term received, stepping down")
                self.state = RaftState.FOLLOWER
                # Per Raft §5.1: entering a new term resets voted_for
                self.voted_for = None

            msg_type = msg.get('type', '')

            if msg_type == 'request_vote':
                await self._handle_vote_request(msg)
            elif msg_type == 'vote_response':
                await self._handle_vote_response(msg)
            elif msg_type == 'heartbeat' or msg_type == 'append_entries':
                await self._handle_heartbeat(msg)

    async def _handle_vote_request(self, msg: Dict):
        self.last_heartbeat = time.time()
        self._randomize_timeout()

        candidate_id = msg.get('data', {}).get('candidate_id', '')
        sender_addr = msg.get('sender', '')
        candidate_term = msg.get('term', 0)

        print(f"[RAFT] {self.node_id}: Got vote_request from {candidate_id} (term={candidate_term})")

        last_idx = self.log[-1].index if self.log else 0
        last_term = self.log[-1].term if self.log else 0
        cand_last_idx = msg.get('data', {}).get('last_log_index', 0)
        cand_last_term = msg.get('data', {}).get('last_log_term', 0)

        log_ok = (cand_last_term > last_term) or (cand_last_term == last_term and cand_last_idx >= last_idx)

        # Grant vote only if candidate's term is strictly greater OR same term but we haven't voted yet
        term_ok = (candidate_term > self.current_term) or (
            candidate_term == self.current_term and (self.voted_for is None or self.voted_for == candidate_id)
        )

        if term_ok and log_ok:
            self.voted_for = candidate_id
            self.current_term = candidate_term
            print(f"[RAFT] {self.node_id}: Granted vote to {candidate_id}")
            asyncio.create_task(self._send_vote_response(sender_addr, True))
        else:
            asyncio.create_task(self._send_vote_response(sender_addr, False))

    async def _send_vote_response(self, receiver: str, granted: bool):
        try:
            if not receiver:
                return
            # receiver should be like "127.0.0.1:8001"
            port = int(receiver.split(':')[1]) if ':' in receiver else 8000
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('127.0.0.1', port), timeout=2
            )

            msg = {
                'type': 'vote_response',
                'sender': f'127.0.0.1:{self.port}',
                'sender_id': self.node_id,
                'term': self.current_term,
                'granted': granted
            }
            writer.write(json.dumps(msg).encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"[RAFT] {self.node_id}: Failed to send vote response to {receiver}")

    async def _handle_vote_response(self, msg: Dict):
        self.last_heartbeat = time.time()
        self._randomize_timeout()

        granted = msg.get('granted', False)
        sender_id = msg.get('sender_id', '')
        sender_addr = msg.get('sender', '')

        print(f"[RAFT] {self.node_id}: Got vote_response from {sender_id}, granted={granted}")

        if granted and sender_id not in self.votes_received:
            self.votes_received.append(sender_id)

        self._check_election_winner()

    def _check_election_winner(self):
        if self.state != RaftState.CANDIDATE:
            return

        # Total cluster size = peers + self; majority = floor(total/2) + 1
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        print(f"[RAFT] {self.node_id}: Votes={len(self.votes_received)}/{total_nodes}, need={majority}")

        if len(self.votes_received) >= majority:
            print(f"[RAFT] {self.node_id}: WON ELECTION!")
            self.state = RaftState.LEADER
            self.leader_id = self.node_id

            self.next_index = {peer: len(self.log) + 1 for peer in self.peers}
            self.match_index = {peer: 0 for peer in self.peers}

            asyncio.create_task(self._send_heartbeats())

    async def _handle_heartbeat(self, msg: Dict):
        self.last_heartbeat = time.time()
        self._randomize_timeout()

        if self.state == RaftState.CANDIDATE:
            print(f"[RAFT] {self.node_id}: Received heartbeat, becoming follower")

        self.state = RaftState.FOLLOWER
        # NOTE: Do NOT reset voted_for here. The Raft spec requires voted_for to
        # persist for the current term so a node cannot vote for two candidates.
        # voted_for is only cleared when the term advances.
        self.leader_id = msg.get('sender_id', msg.get('sender', ''))

    async def submit_command(self, command: str, data: Dict = None) -> bool:
        async with self._lock:
            if self.state != RaftState.LEADER:
                return False

            entry = LogEntry(
                term=self.current_term,
                index=len(self.log) + 1,
                command=command,
                data=data or {}
            )
            self.log.append(entry)
            return True

    def get_state(self) -> Dict:
        return {
            'node_id': self.node_id,
            'state': self.state.value,
            'term': self.current_term,
            'is_leader': self.state == RaftState.LEADER,
            'log_length': len(self.log),
            'votes_received': len(self.votes_received),
            'total_peers': len(self.peers) + 1
        }

    def is_leader(self) -> bool:
        return self.state == RaftState.LEADER

    def get_leader_id(self) -> Optional[str]:
        if self.state == RaftState.LEADER:
            return self.node_id
        return getattr(self, 'leader_id', None)