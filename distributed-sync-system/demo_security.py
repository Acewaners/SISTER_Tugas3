import asyncio
import json

async def send_tcp_request(action, token, expected_status):
    print(f"\n[>] Mengirim aksi: '{action}' dengan token: '{token or 'KOSONG'}'")
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', 8001)
        payload = {"action": action, "token": token}
        
        writer.write(json.dumps(payload).encode())
        await writer.drain()
        
        data = await reader.read(1024)
        response = json.loads(data.decode())
        
        writer.close()
        await writer.wait_closed()
        
        if "error" in response:
            print(f"[X] DITOLAK (RBAC Active): {response['error']}")
        else:
            print(f"[V] DIIZINKAN: {response}")
            
    except ConnectionRefusedError:
        print("[!] Gagal terhubung. Pastikan Node 1 sudah nyala di port 8001!")

async def main():
    print("=== SIMULASI KEAMANAN RBAC & AUDIT LOG ===")
    
    # 1. Tanpa Token (Pasti ditolak)
    await send_tcp_request("cache_read", "", "DITOLAK")
    
    # 2. Token Consumer mencoba Enqueue (Pasti ditolak karena Role beda)
    await send_tcp_request("queue_enqueue", "consumer-key", "DITOLAK")
    
    # 3. Token Producer mencoba Enqueue (Harus berhasil)
    await send_tcp_request("queue_enqueue", "producer-key", "DIIZINKAN")
    
    # 4. Token Admin mencoba aksi apapun (Harus berhasil)
    await send_tcp_request("get_status", "admin-secret-key-change-this", "DIIZINKAN")

    print("\n✅ Simulasi Selesai! Coba buka file 'audit.log' sekarang.")

if __name__ == "__main__":
    asyncio.run(main())
