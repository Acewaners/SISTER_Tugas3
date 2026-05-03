import pytest
import asyncio
from src.nodes.cache_node import CacheNode, CacheState, LRUReplacement


@pytest.fixture
def lru():
    return LRUReplacement(capacity=3)


def test_lru_touch(lru):
    lru.touch(1)
    lru.touch(2)
    lru.touch(3)
    
    assert len(lru.access_order) == 3
    assert 1 in lru.access_order
    assert 2 in lru.access_order
    assert 3 in lru.access_order


def test_lru_eviction(lru):
    lru.touch(1)
    lru.touch(2)
    lru.touch(3)
    lru.touch(4)
    
    evicted = lru.evict()
    
    # touch(4) should evict 1 (LRU), then evict() returns the next oldest
    # But evict() returns 1 (which was already evicted by touch), so 1 is not in order
    assert 1 not in lru.access_order


def test_lru_remove(lru):
    lru.touch(1)
    lru.touch(2)
    lru.remove(1)
    
    assert 1 not in lru.access_order
    assert 2 in lru.access_order


@pytest.fixture
def cache_node():
    return CacheNode("node_1", [], 8001)


@pytest.mark.asyncio
async def test_cache_write(cache_node):
    await cache_node.start()
    
    result = await cache_node.write(100, b"test_data", "writer_1")
    
    assert result == True
    assert 100 in cache_node.cache
    assert cache_node.cache[100].data == b"test_data"


@pytest.mark.asyncio
async def test_cache_read_hit(cache_node):
    await cache_node.start()
    
    await cache_node.write(100, b"test_data", "writer_1")
    data = await cache_node.read(100, "reader_1")
    
    assert data == b"test_data"


@pytest.mark.asyncio
async def test_cache_read_miss(cache_node):
    await cache_node.start()
    
    data = await cache_node.read(999, "reader_1")
    
    assert data is None
    assert cache_node.miss_count == 1


@pytest.mark.asyncio
async def test_cache_invalidate(cache_node):
    await cache_node.start()
    
    await cache_node.write(100, b"test_data", "writer_1")
    result = await cache_node.invalidate(100, "source_1")
    
    assert result == True
    assert cache_node.cache[100].state == CacheState.INVALID


@pytest.mark.asyncio
async def test_cache_stats(cache_node):
    await cache_node.start()
    
    await cache_node.write(100, b"data1", "writer_1")
    await cache_node.read(100, "reader_1")
    await cache_node.read(999, "reader_1")
    
    stats = await cache_node.get_cache_stats()
    
    assert stats["node_id"] == "node_1"
    assert stats["cache_size"] == 1
    assert stats["hits"] == 1
    assert stats["misses"] == 1


@pytest.mark.asyncio
async def test_cache_lru_eviction(cache_node):
    cache_node.max_size = 2
    await cache_node.start()
    
    await cache_node.write(1, b"data1", "writer_1")
    await cache_node.write(2, b"data2", "writer_1")
    await cache_node.write(3, b"data3", "writer_1")
    
    assert len(cache_node.cache) <= 2
