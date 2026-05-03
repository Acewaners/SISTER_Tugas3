import socket
import json
import argparse

def send_request(port, data):
    s = socket.socket()
    s.connect(('localhost', port))
    s.sendall(json.dumps(data).encode())
    response = s.recv(8192).decode()
    s.close()
    return response

def main():
    parser = argparse.ArgumentParser(description='Test Client for Distributed Sync System')
    parser.add_argument('--action', required=True, help='Action to perform')
    parser.add_argument('--port', type=int, default=8001, help='Port number')
    parser.add_argument('--resource', help='Resource name (for lock actions)')
    parser.add_argument('--lock_type', default='EXCLUSIVE', help='Lock type (SHARED or EXCLUSIVE)')
    parser.add_argument('--client_id', help='Client ID')
    parser.add_argument('--topic', help='Topic name (for queue actions)')
    parser.add_argument('--payload', help='Message payload (for queue actions)')
    parser.add_argument('--producer_id', help='Producer ID')
    parser.add_argument('--consumer_id', help='Consumer ID')
    parser.add_argument('--msg_id', help='Message ID (for ack)')
    parser.add_argument('--address', type=int, help='Cache address')
    parser.add_argument('--data', help='Data to write (for cache)')
    parser.add_argument('--writer_id', help='Writer ID')
    parser.add_argument('--requestor_id', help='Requestor ID')

    args = parser.parse_args()

    if args.action == 'get_status':
        data = {'action': 'get_status'}

    elif args.action == 'lock_acquire':
        data = {
            'action': 'lock_acquire',
            'resource': args.resource or 'default_resource',
            'lock_type': args.lock_type,
            'client_id': args.client_id or 'default_client'
        }

    elif args.action == 'lock_release':
        data = {
            'action': 'lock_release',
            'resource': args.resource or 'default_resource',
            'client_id': args.client_id or 'default_client'
        }

    elif args.action == 'get_all_locks':
        data = {'action': 'get_all_locks'}

    elif args.action == 'queue_enqueue':
        data = {
            'action': 'queue_enqueue',
            'topic': args.topic or 'default_topic',
            'payload': args.payload or '',
            'producer_id': args.producer_id or 'default_producer'
        }

    elif args.action == 'queue_dequeue':
        data = {
            'action': 'queue_dequeue',
            'topic': args.topic or 'default_topic',
            'consumer_id': args.consumer_id or 'default_consumer',
            'timeout': 30
        }

    elif args.action == 'queue_ack':
        data = {
            'action': 'queue_ack',
            'msg_id': args.msg_id or '',
            'consumer_id': args.consumer_id or 'default_consumer'
        }

    elif args.action == 'queue_stats':
        data = {'action': 'queue_stats'}

    elif args.action == 'cache_read':
        data = {
            'action': 'cache_read',
            'address': args.address or 0,
            'requestor_id': args.requestor_id or 'default_requestor'
        }

    elif args.action == 'cache_write':
        data = {
            'action': 'cache_write',
            'address': args.address or 0,
            'data': args.data or '',
            'writer_id': args.writer_id or 'default_writer'
        }

    elif args.action == 'cache_stats':
        data = {'action': 'cache_stats'}

    elif args.action == 'heartbeat':
        data = {'action': 'heartbeat'}

    else:
        print(f"Unknown action: {args.action}")
        return

    result = send_request(args.port, data)
    print(result)

if __name__ == '__main__':
    main()