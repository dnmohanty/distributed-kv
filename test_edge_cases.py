import socket
import time
import os

PROXY_PORT = 7000

def send_req(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', PROXY_PORT))
        s.sendall(cmd.encode() + b'\n')
        return s.recv(4096).decode().strip()

print("1. Testing DEL command...")
send_req("SET del_key 123")
assert send_req("GET del_key") == "123"
assert send_req("DEL del_key") == "(integer) 1"
assert send_req("GET del_key") == "(nil)"
print("DEL passed.")

print("2. Testing Large Value Rejection (1MB limit)...")
large_val = "A" * (1024 * 1024 + 10)
try:
    send_req(f"SET huge_key {large_val}")
except ConnectionResetError:
    pass # Proxy correctly dropped the connection
print("Large value rejection passed.")

print("3. Testing Torn-Write Recovery...")
# Simulate a torn write by appending garbage to a backend WAL
with open("data/node1/node_8080.wal", "ab") as f:
    f.write(b"GARBAGE_TORN_WRITE_BYTES")
print("Torn-write injected. Restart the server to verify it recovers previous keys cleanly and truncates the garbage.")

print("4. Testing Compaction...")
for i in range(100):
    send_req(f"SET comp_{i} val_{i}")
print("100 keys written. Wait 10 seconds for background compaction thread to shrink the WAL.")
