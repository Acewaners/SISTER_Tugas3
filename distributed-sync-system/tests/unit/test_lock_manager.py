import pytest
import asyncio
from src.nodes.lock_manager import DistributedLockManager, LockType


@pytest.fixture
def lock_manager():
    return DistributedLockManager("node_1", [], 8001)


@pytest.mark.asyncio
async def test_acquire_exclusive_lock(lock_manager):
    await lock_manager.start()
    
    result = await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    
    assert result["success"] == True
    assert result["resource"] == "resource_1"
    assert "request_id" in result


@pytest.mark.asyncio
async def test_acquire_shared_lock(lock_manager):
    await lock_manager.start()
    
    result = await lock_manager.acquire_lock("resource_1", LockType.SHARED, "client_1")
    
    assert result["success"] == True
    assert result["resource"] == "resource_1"


@pytest.mark.asyncio
async def test_release_lock(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    result = await lock_manager.release_lock("resource_1", "client_1")
    
    assert result["success"] == True
    assert result["resource"] == "resource_1"


@pytest.mark.asyncio
async def test_release_nonexistent_lock(lock_manager):
    await lock_manager.start()
    
    result = await lock_manager.release_lock("nonexistent", "client_1")
    
    assert result["success"] == False
    assert result["error"] == "lock_not_found"


@pytest.mark.asyncio
async def test_release_lock_by_non_holder(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    result = await lock_manager.release_lock("resource_1", "client_2")
    
    assert result["success"] == False
    assert result["error"] == "not_holder"


@pytest.mark.asyncio
async def test_get_lock_status_locked(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    status = await lock_manager.get_lock_status("resource_1")
    
    assert status["locked"] == True
    assert status["lock_type"] == "exclusive"
    assert status["holder"] == "client_1"


@pytest.mark.asyncio
async def test_get_lock_status_unlocked(lock_manager):
    await lock_manager.start()
    
    status = await lock_manager.get_lock_status("resource_1")
    
    assert status["locked"] == False


@pytest.mark.asyncio
async def test_get_all_locks(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    await lock_manager.acquire_lock("resource_2", LockType.SHARED, "client_2")
    
    locks = await lock_manager.get_all_locks()
    
    assert len(locks) == 2


@pytest.mark.asyncio
async def test_lock_expiration(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1", timeout=1)
    
    await asyncio.sleep(2)
    
    status = await lock_manager.get_lock_status("resource_1")
    assert status["locked"] == False


@pytest.mark.asyncio
async def test_waiting_queue(lock_manager):
    await lock_manager.start()
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_1")
    
    await lock_manager.acquire_lock("resource_1", LockType.EXCLUSIVE, "client_2", timeout=2)
    
    await asyncio.sleep(0.5)
    
    await lock_manager.release_lock("resource_1", "client_1")
    
    await asyncio.sleep(0.5)
    
    status = await lock_manager.get_lock_status("resource_1")
    assert status["locked"] == True
