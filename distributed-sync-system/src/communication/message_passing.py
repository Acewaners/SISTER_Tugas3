import asyncio
import json
import time
import uuid
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class Message:
    msg_id: str
    sender: str
    receiver: str
    msg_type: str
    payload: Dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3


class MessagePassing:
    def __init__(self, node_id: str, peers: List[str]):
        self.node_id = node_id
        self.peers = peers
        self.pending_messages: Dict[str, asyncio.Future] = {}
        self.message_history: List[Message] = []
        self.handlers: Dict[str, Callable] = {}
        self._lock = asyncio.Lock()

    async def send_message(self, receiver: str, msg_type: str, payload: Dict = None, timeout: int = 5) -> Optional[Dict]:
        msg_id = str(uuid.uuid4())
        msg = Message(msg_id, self.node_id, receiver, msg_type, payload or {})
        
        try:
            port = int(receiver.split(":")[1]) if ":" in receiver else 8000
            host = receiver.split(":")[0] if ":" in receiver else "localhost"
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
            
            writer.write(json.dumps({
                "msg_id": msg_id,
                "sender": self.node_id,
                "receiver": receiver,
                "type": msg_type,
                "payload": payload or {}
            }).encode())
            await writer.drain()
            
            response_data = await asyncio.wait_for(reader.read(4096), timeout=timeout)
            writer.close()
            
            if response_data:
                return json.loads(response_data.decode())
        except Exception as e:
            logger.error(f"Failed to send message to {receiver}: {e}")
            msg.retry_count += 1
            
            if msg.retry_count < msg.max_retries:
                await asyncio.sleep(0.5)
                return await self.send_message(receiver, msg_type, payload, timeout)
        
        return None

    async def broadcast(self, msg_type: str, payload: Dict = None) -> Dict:
        results = {}
        for peer in self.peers:
            result = await self.send_message(peer, msg_type, payload)
            results[peer] = result
        
        success_count = sum(1 for r in results.values() if r is not None)
        return {"total": len(self.peers), "success": success_count, "results": results}

    async def send_and_wait(self, receiver: str, msg_type: str, payload: Dict = None, timeout: int = 30) -> Optional[Dict]:
        future = asyncio.Future()
        self.pending_messages[msg_type] = future
        
        result = await self.send_message(receiver, msg_type, payload, timeout)
        
        if result:
            future.set_result(result)
        
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        except:
            return result

    def register_handler(self, msg_type: str, handler: Callable):
        self.handlers[msg_type] = handler

    async def handle_message(self, msg: Dict):
        msg_type = msg.get("type")
        if msg_type in self.handlers:
            handler = self.handlers[msg_type]
            if asyncio.iscoroutinefunction(handler):
                await handler(msg)
            else:
                handler(msg)
        
        self.message_history.append(Message(
            msg.get("msg_id", str(uuid.uuid4())),
            msg.get("sender", ""),
            msg.get("receiver", ""),
            msg_type,
            msg.get("payload", {})
        ))

    def get_history(self, limit: int = 100) -> List[Dict]:
        return [
            {"msg_id": m.msg_id, "sender": m.sender, "receiver": m.receiver, 
             "type": m.msg_type, "timestamp": m.timestamp}
            for m in self.message_history[-limit:]
        ]

    def get_stats(self) -> Dict:
        return {
            "node_id": self.node_id,
            "peers": len(self.peers),
            "pending": len(self.pending_messages),
            "history_size": len(self.message_history),
            "handlers": list(self.handlers.keys())
        }


from typing import List
