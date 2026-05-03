import asyncio
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class BaseNode(ABC):
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.server = None
        self._running = False
        self._message_queue: asyncio.Queue = asyncio.Queue()

    @abstractmethod
    async def handle_request(self, data: Dict) -> Dict:
        pass

    async def start_server(self):
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._running = True
        logger.info(f"{self.__class__.__name__} {self.node_id} listening on {self.host}:{self.port}")
        async with self.server:
            await self.server.serve_forever()

    async def stop_server(self):
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        try:
            data = await reader.read(4096)
            if data:
                import json
                request = json.loads(data.decode())
                response = await self.handle_request(request)
                writer.write(json.dumps(response).encode())
                await writer.drain()
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def send_to_peer(self, peer: str, data: Dict):
        try:
            port = int(peer.split(':')[1]) if ':' in peer else self.port
            host = peer.split(':')[0] if ':' in peer else 'localhost'
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            import json
            writer.write(json.dumps(data).encode())
            await writer.drain()
            response_data = await reader.read(4096)
            writer.close()
            await writer.wait_closed()
            return json.loads(response_data.decode()) if response_data else None
        except Exception as e:
            logger.error(f"Failed to send to peer {peer}: {e}")
            return None

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())
