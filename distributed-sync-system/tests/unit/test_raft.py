import pytest
import asyncio
from src.consensus.raft import RaftNode, RaftState, MessageType


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def raft_node():
    return RaftNode("node_1", ["localhost:8002", "localhost:8003"], 8001)


def test_raft_initial_state(raft_node):
    assert raft_node.state == RaftState.FOLLOWER
    assert raft_node.current_term == 0
    assert raft_node.voted_for is None
    assert len(raft_node.log) == 0


def test_raft_get_state(raft_node):
    state = raft_node.get_state()
    assert state["node_id"] == "node_1"
    assert state["state"] == "follower"
    assert state["term"] == 0
    assert state["is_leader"] == False


def test_raft_is_not_leader(raft_node):
    assert raft_node.is_leader() == False


@pytest.mark.asyncio
async def test_raft_submit_command_before_leader():
    node = RaftNode("node_1", [], 8001)
    result = await node.submit_command("test_command", {"key": "value"})
    assert result == False


@pytest.mark.asyncio
async def test_raft_submit_command_after_leader():
    node = RaftNode("node_1", [], 8001)
    node.state = RaftState.LEADER
    node.current_term = 1
    
    result = await node.submit_command("test_command", {"key": "value"})
    assert result == True
    assert len(node.log) == 1
    assert node.log[0].command == "test_command"


@pytest.mark.asyncio
async def test_raft_get_leader_id_as_follower():
    node = RaftNode("node_1", [], 8001)
    node.leader_id = "node_2"
    
    assert node.get_leader_id() == "node_2"


@pytest.mark.asyncio
async def test_raft_get_leader_id_as_leader():
    node = RaftNode("node_1", [], 8001)
    node.state = RaftState.LEADER
    node.node_id = "node_1"
    
    assert node.get_leader_id() == "node_1"


@pytest.mark.asyncio
async def test_handle_heartbeat_message():
    node = RaftNode("node_1", [], 8001)
    
    msg = {
        "type": "heartbeat",
        "sender": "node_2",
        "term": 1,
        "data": {"leader_commit": 0}
    }
    
    await node.handle_message(msg)
    
    assert node.state == RaftState.FOLLOWER
    assert node.leader_id == "node_2"
    assert node.current_term == 1
