import os
import time
import socket
import signal
import subprocess
import sys

def send_cmd(port, cmd):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", port))
    s.sendall((cmd + "\n").encode())
    resp = s.recv(1024).decode().strip()
    s.close()
    return resp

def run_test():
    wal_file = "node_9999.wal"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    server = subprocess.Popen(["./kv_server", "9999", "127.0.0.1:9999"], stdout=subprocess.DEVNULL)
    time.sleep(0.5)

    send_cmd(9999, "SET crash_key1 data_1")
    send_cmd(9999, "SET crash_key2 data_2 EX 600")

    server.send_signal(signal.SIGKILL)
    server.wait()

    server = subprocess.Popen(["./kv_server", "9999", "127.0.0.1:9999"], stdout=subprocess.DEVNULL)
    time.sleep(0.5)

    res1 = send_cmd(9999, "GET crash_key1")
    res2 = send_cmd(9999, "GET crash_key2")

    server.send_signal(signal.SIGKILL)
    server.wait()

    if os.path.exists(wal_file):
        os.remove(wal_file)

    if res1 == "data_1" and res2 == "data_2":
        print("\033[92mPASS: Keys survived SIGKILL mid-write and recovered successfully.\033[0m")
        sys.exit(0)
    else:
        print(f"\033[91mFAIL: Data lost. Key1: {res1}, Key2: {res2}\033[0m")
        sys.exit(1)

if __name__ == "__main__":
    run_test()