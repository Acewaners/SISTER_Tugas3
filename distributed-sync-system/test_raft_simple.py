import asyncio
from src.consensus.raft import RaftNode, RaftState

async def test():
    print('=== SIMPLE RAFT TEST ===')

    # Create 3 nodes
    n1 = RaftNode('node_1', ['localhost:8002', 'localhost:8003'], 8001)
    n2 = RaftNode('node_2', ['localhost:8001', 'localhost:8003'], 8002)
    n3 = RaftNode('node_3', ['localhost:8001', 'localhost:8002'], 8003)

    # Override election timeout for faster testing
    n1.election_timeout = 1.0
    n2.election_timeout = 1.5
    n3.election_timeout = 2.0

    print('Initial states:')
    print(f'  n1: {n1.get_state()}')
    print(f'  n2: {n2.get_state()}')
    print(f'  n3: {n3.get_state()}')

    # Simulate election timeout for n1
    print()
    print('Simulating election timeout for n1...')
    n1.last_heartbeat = 0
    await n1._start_election()
    print(f'n1 after start: {n1.get_state()}')
    print(f'Votes received: {n1.votes_received}')

    # Node 2 receives vote request and grants
    print()
    print('Node 2 receives vote request from n1...')
    msg = {
        'type': 'request_vote',
        'sender': 'node_1',
        'term': 1,
        'data': {'candidate_id': 'node_1', 'last_log_index': 0, 'last_log_term': 0}
    }
    await n2.handle_message(msg)
    print(f'n2 state after vote: voted_for={n2.voted_for}, term={n2.current_term}')

    # Node 3 receives vote request and grants
    print()
    print('Node 3 receives vote request from n1...')
    await n3.handle_message(msg)
    print(f'n3 state after vote: voted_for={n3.voted_for}, term={n3.current_term}')

    # Now simulate vote response back to n1
    print()
    print('Sending vote response back to n1...')
    await n1.handle_message({'type': 'vote_response', 'sender': 'node_2', 'term': 1, 'granted': True})
    print(f'n1 after vote from n2: {n1.get_state()}')

    await n1.handle_message({'type': 'vote_response', 'sender': 'node_3', 'term': 1, 'granted': True})
    print(f'n1 after vote from n3: {n1.get_state()}')

    print()
    if n1.is_leader():
        print('SUCCESS: n1 became LEADER!')
    else:
        print('FAILED: n1 did not become leader')
        print(f'n1 votes: {n1.votes_received}')

asyncio.run(test())