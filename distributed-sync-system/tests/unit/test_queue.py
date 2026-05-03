import pytest
import asyncio
from src.nodes.queue_node import DistributedQueue, ConsistentHashRing


@pytest.fixture
def hash_ring():
    return ConsistentHashRing(["node_1", "node_2", "node_3"])


def test_hash_ring_get_node(hash_ring):
    node = hash_ring.get_node("test_key")
    assert node in ["node_1", "node_2", "node_3"]


def test_hash_ring_consistency(hash_ring):
    nodes = [hash_ring.get_node("same_key") for _ in range(100)]
    assert len(set(nodes)) == 1


def test_hash_ring_add_node(hash_ring):
    hash_ring.add_node("node_4")
    
    node = hash_ring.get_node("new_key")
    assert node in ["node_1", "node_2", "node_3", "node_4"]


def test_hash_ring_remove_node(hash_ring):
    hash_ring.remove_node("node_1")
    
    node = hash_ring.get_node("test_key")
    assert node in ["node_2", "node_3"]


@pytest.fixture
def queue():
    return DistributedQueue("node_1", ["localhost:8002", "localhost:8003"], 8001)


@pytest.mark.asyncio
async def test_enqueue_message(queue):
    await queue.start()
    
    result = await queue.enqueue("test_topic", "test_payload", "producer_1")
    
    assert result["success"] == True
    assert "msg_id" in result
    assert result["partition"] is not None


@pytest.mark.asyncio
async def test_dequeue_message(queue):
    await queue.start()
    
    await queue.enqueue("test_topic", "test_payload", "producer_1")
    result = await queue.dequeue("test_topic", "consumer_1", timeout=2)
    
    assert result is not None
    assert "msg_id" in result
    assert result["payload"] == "test_payload"


@pytest.mark.asyncio
async def test_acknowledge_message(queue):
    await queue.start()
    
    await queue.enqueue("test_topic", "test_payload", "producer_1")
    dequeued = await queue.dequeue("test_topic", "consumer_1", timeout=2)
    
    result = await queue.acknowledge(dequeued["msg_id"], "consumer_1")
    
    assert result["success"] == True


@pytest.mark.asyncio
async def test_empty_dequeue(queue):
    await queue.start()
    
    result = await queue.dequeue("empty_topic", "consumer_1", timeout=1)
    
    assert result is None


@pytest.mark.asyncio
async def test_queue_stats(queue):
    await queue.start()
    
    await queue.enqueue("topic_1", "payload_1", "producer_1")
    await queue.enqueue("topic_2", "payload_2", "producer_2")
    
    stats = await queue.get_queue_stats()
    
    assert stats["node_id"] == "node_1"
    assert stats["total_messages"] >= 0
    assert stats["producers"] >= 1
