import socket
import time
import threading
import sys

NUM_REQUESTS = 10000
CONCURRENCY = 10
PORT = 7000 # Proxy port

def worker(worker_id, latencies):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", PORT))
    
    for i in range(NUM_REQUESTS // CONCURRENCY):
        key = f"bench_{worker_id}_{i}"
        cmd = f"SET {key} val_{i}\n".encode()
        
        start = time.time()
        s.sendall(cmd)
        s.recv(1024)
        latencies.append(time.time() - start)
        
    s.close()

def run_benchmark():
    print(f"Starting benchmark: {NUM_REQUESTS} requests with {CONCURRENCY} threads...")
    threads = []
    latencies = []
    
    start_time = time.time()
    for i in range(CONCURRENCY):
        t = threading.Thread(target=worker, args=(i, latencies))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    total_time = time.time() - start_time
    ops_sec = NUM_REQUESTS / total_time
    
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.5)] * 1000
    p99 = latencies[int(len(latencies) * 0.99)] * 1000
    
    print("\n--- Benchmark Results ---")
    print(f"Total Time : {total_time:.2f} seconds")
    print(f"Throughput : {ops_sec:.2f} ops/sec")
    print(f"p50 Latency: {p50:.2f} ms")
    print(f"p99 Latency: {p99:.2f} ms")

if __name__ == "__main__":
    run_benchmark()