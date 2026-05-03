import asyncio
import argparse
import os
import json

from src.utils.config import config
from src.consensus.raft import RaftNode, RaftState
from src.nodes.lock_manager import DistributedLockManager, LockType
from src.nodes.queue_node import DistributedQueue
from src.nodes.cache_node import CacheNode
from src.communication.failure_detector import FailureDetector
from src.communication.message_passing import MessagePassing


class NodeServer:
    def __init__(self, node_id: str, port: int, node_type: str):
        self.node_id = node_id
        self.port = port
        self.node_type = node_type

        all_ports = [8001, 8002, 8003]
        peer_ports = [p for p in all_ports if p != port]
        peer_ids = [f"localhost:{p}" for p in peer_ports]

        print(f"Initializing {node_id} on port {port}")
        print(f"Peers: {peer_ids}")

        self.raft = RaftNode(node_id, peer_ids, port)
        self.lock_manager = DistributedLockManager(node_id, peer_ids, port)
        self.queue = DistributedQueue(node_id, peer_ids, port)
        self.cache = CacheNode(node_id, peer_ids, port, config.cache_max_size, config.cache_default_ttl)
        self.failure_detector = FailureDetector(node_id, peer_ids, heartbeat_interval=10.0, timeout=30.0)
        self.message_passing = MessagePassing(node_id, peer_ids)

        self._running = False

    async def start(self):
        self._running = True

        await self.raft.start()
        await self.lock_manager.start()
        await self.queue.start()
        await self.cache.start()
        await self.failure_detector.start()

        server = await asyncio.start_server(self._handle_request, "0.0.0.0", self.port)
        print(f"Node {self.node_id} ({self.node_type}) started on port {self.port}")
        print(f"Ready to accept requests at http://localhost:{self.port}")

        async with server:
            await server.serve_forever()

    async def _handle_request(self, reader, writer):
        try:
            addr = writer.get_extra_info('peername')
            print(f"[NET] Connection from {addr}")
            data = await reader.read(8192)
            if data:
                request = json.loads(data.decode())
                msg_type = request.get("type", "")

                print(f"[NET] Received from {addr}: type={msg_type}, sender={request.get('sender', 'N/A')}, sender_id={request.get('sender_id', 'N/A')}")

                # Handle Raft messages (type-based)
                if msg_type in ["request_vote", "vote_response", "heartbeat", "append_entries"]:
                    await self.raft.handle_message(request)
                    writer.write(json.dumps({"raft_ok": True}).encode())
                    await writer.drain()
                    return

                # Handle normal actions
                response = await self.process_request(request)
                writer.write(json.dumps(response).encode())
                await writer.drain()
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass

    async def process_request(self, request: dict) -> dict:
        action = request.get("action")
        print(f"Processing request: {action}")

        # Handle Raft messages
        if "type" in request:
            msg_type = request.get("type")
            if msg_type in ["request_vote", "vote_response", "heartbeat", "append_entries"]:
                await self.raft.handle_message(request)
                return {"raft_response": True, "state": self.raft.get_state()}

        if action == "lock_acquire":
            resource = request.get("resource", "")
            lock_type_str = request.get("lock_type", "EXCLUSIVE")
            client_id = request.get("client_id", "")
            timeout = request.get("timeout", 30)

            try:
                lock_type = LockType[lock_type_str.upper()]
            except:
                lock_type = LockType.EXCLUSIVE

            result = await self.lock_manager.acquire_lock(resource, lock_type, client_id, timeout)
            return {"success": result.get("success", False), "resource": resource, "request_id": result.get("request_id", "")}

        elif action == "lock_release":
            resource = request.get("resource", "")
            client_id = request.get("client_id", "")
            result = await self.lock_manager.release_lock(resource, client_id)
            return {"success": result.get("success", False), "resource": resource}

        elif action == "lock_status":
            resource = request.get("resource", "")
            status = await self.lock_manager.get_lock_status(resource)
            return status

        elif action == "get_all_locks":
            locks = await self.lock_manager.get_all_locks()
            return {"locks": locks}

        elif action == "queue_enqueue":
            topic = request.get("topic", "")
            payload = request.get("payload", "")
            producer_id = request.get("producer_id", "")
            result = await self.queue.enqueue(topic, payload, producer_id)
            return result

        elif action == "queue_dequeue":
            topic = request.get("topic", "")
            consumer_id = request.get("consumer_id", "")
            timeout = request.get("timeout", 30)
            result = await self.queue.dequeue(topic, consumer_id, timeout)
            return result or {"empty": True, "msg_id": None, "payload": None}

        elif action == "queue_ack":
            msg_id = request.get("msg_id", "")
            consumer_id = request.get("consumer_id", "")
            result = await self.queue.acknowledge(msg_id, consumer_id)
            return result

        elif action == "queue_stats":
            stats = await self.queue.get_queue_stats()
            return stats

        elif action == "cache_read":
            address = request.get("address", 0)
            requestor_id = request.get("requestor_id", "")
            data = await self.cache.read(address, requestor_id)
            return {"address": address, "data": data.decode() if data else None}

        elif action == "cache_write":
            address = request.get("address", 0)
            data_str = request.get("data", "")
            writer_id = request.get("writer_id", "")
            result = await self.cache.write(address, data_str.encode(), writer_id)
            return {"success": result, "address": address}

        elif action == "cache_stats":
            stats = await self.cache.get_cache_stats()
            return stats

        elif action == "get_status":
            return {
                "node_id": self.node_id,
                "node_type": self.node_type,
                "raft_state": self.raft.get_state(),
                "locks_held": len(self.lock_manager.locks),
                "queue_stats": await self.queue.get_queue_stats(),
                "cache_stats": await self.cache.get_cache_stats(),
                "failure_stats": self.failure_detector.get_stats()
            }

        elif action == "heartbeat":
            return {"status": "alive", "node_id": self.node_id}

        return {"error": "unknown_action", "received_action": action}


async def main():
    parser = argparse.ArgumentParser(description="Distributed Sync System Node")
    parser.add_argument("--node-id", default=os.getenv("NODE_ID", "node_1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("NODE_PORT", "8001")))
    parser.add_argument("--type", default=os.getenv("NODE_TYPE", "lock_manager"))

    args = parser.parse_args()

    print(f"=" * 50)
    print(f"Starting node: {args.node_id}")
    print(f"Port: {args.port}")
    print(f"Type: {args.type}")
    print(f"=" * 50)

    server = NodeServer(args.node_id, args.port, args.type)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())