# DistributedKV

A sharded in-memory KV store in C++20 with a consistent-hashing proxy and a durable write-ahead log.

## 1. Features
* **Non-Blocking I/O:** Core and proxy utilize Linux `epoll` (edge-triggered) to handle high-concurrency connections safely.
* **Smart Asynchronous Proxy:** Client requests hit a central gateway that manages a persistent connection per backend, routing commands without blocking the event loop.
* **Failure Detection & Ring Eviction:** The proxy background-monitors backend nodes. If it detects a closed connection, the node is temporarily evicted from the hash ring. 
* **Stable Consistent Hashing:** Implements FNV-1a with virtual nodes to map keys identically across proxy and server binaries.
* **Configurable WAL Durability:** Prevents data loss during power failures (strictly guaranteed with `--fsync=always`). Supports `--fsync=always` (fsync on every write), `--fsync=everysec` (background flush thread), and `--fsync=never`.
* **WAL Compaction:** A background thread monitors `.wal` file sizes and takes point-in-time lock-safe snapshots to reclaim disk space.
* **Lock-Striping:** The in-memory store is partitioned into 16 separate shards, each protected by a `std::shared_mutex`.
* **Time-to-Live (TTL):** Evaluates absolute expiration times lazily on `GET` requests, sweeps expired keys periodically, and persists expiries reliably to disk.

## 2. Architecture Diagram
```text
[ Client ] ---> [ Smart Proxy Gateway (Port 7000) ]
                         | (FNV-1a Consistent Hashing Ring + Connection Monitoring)
         +---------------+---------------+
         |               |               |
 [ Backend 8080 ] [ Backend 8081 ] [ Backend 8082 ]
         |               |               |
      (WAL)           (WAL)           (WAL)
    (Compactor)     (Compactor)     (Compactor)
```
## 3. Quick Start
**Via Docker (Recommended)**
```bash
git clone https://github.com/dnmohanty/distributed-kv.git
cd distributed-kv
docker compose up --build
```
Run Locally (No Docker)Note: Node names must perfectly match across all processes to avoid MOVED routing errors (e.g., use 127.0.0.1:8080, not localhost:8080).
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .
./kv_server --server 8080 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 &
./kv_server --server 8081 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 &
./kv_server --server 8082 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 &
./kv_server --proxy 7000 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 &
./kv_cli
```
## 4. Command Reference
**## 4. Command Reference**
```markdown
Connect using `telnet 127.0.0.1 7000`, the provided `./kv_cli`, or raw TCP sockets.

*   `SET <key> <value>` : Store data. Values may contain spaces. (Returns `OK`)
*   `SET <key> <value> EX <seconds>` : Store data with an absolute expiry TTL. (Returns `OK`)
*   `GET <key>` : Retrieve a value. (Returns value or `(nil)`)
*   `DEL <key>` : Delete a value. (Returns `(integer) 1` if successful, `0` if absent)

**Protocol Notes:**
* **Payload Limit:** Maximum request size is 1 MB. Larger requests will result in a closed connection.
* **Redirection:** If connecting directly to a backend node instead of the proxy, you may receive a `MOVED to <node_name>` response if the key belongs to a different shard.
```
## 5. Design Decisions & Guarantees
*    Durability: With --fsync=always, every acknowledged write is flushed to physical storage. Crash recovery stops at the first corrupt record and truncates the rest.
*    Consistency: The proxy guarantees requests for a given key always route to the same node according to the hash ring.
*    Torn-Write Protection: The WAL logs length and a CRC32 checksum before the payload, terminating recovery cleanly upon encountering a partial write (capped at 10MB to prevent bad allocation crashes).
*    Payload Limits: The proxy and server strictly enforce a 1MB payload limit per request to prevent malicious buffer exhaustion.
*    Decoupled Disk I/O: Using --fsync=everysec shifts the blocking fdatasync() system call to an isolated background std::thread, allowing the main epoll loop to process memory queries at maximum speed.
*    Atomic Compaction: Snapshotting locks the WAL during memory updates, preventing race conditions where acknowledged writes are dropped during background compaction.

## 6. Failure Behavior

| Scenario | System Reaction |
|---|---|
| Single Node Crash | In-flight requests routed to the node at the exact moment of failure will hang. Data in memory is wiped. The proxy eventually evicts the node and returns MOVED for its keys until it recovers. |
| Node Restart | Node recovers keys sequentially from its .wal file, dropping expired TTLs and verifying CRC32 checksums. |
| Power Loss Mid-Write | Recovery detects a checksum mismatch on the final entry, halts recovery cleanly, and ignores the torn write. |
| Proxy Crash | Clients disconnect. Backend nodes remain operational and durable. |
 
 ## 7. Benchmarks
Testing performed locally over TCP loopback on an Intel Core i5-1135G7 (8GB RAM, Windows Subsystem for Linux) using a Release build (`-O3`). 

**Workload:** 10-thread Python load generator executing 10,000 continuous `SET` operations with small values against a 4-process cluster (1 proxy, 3 backend nodes).

*(Note: Throughput is heavily bottlenecked by the Python client's GIL. Raw C++ event-loop capacity is significantly higher).*

| Fsync Mode | Throughput (ops/sec) | p50 Latency (ms) | p99 Latency (ms) |
|---|---|---|---|
| `--fsync=never` | ~12,400 | 0.52 | 2.10 |
| `--fsync=everysec` | ~12,158 | 0.58 | 2.89 |
| `--fsync=always` | ~4,200 | 2.80 | 18.50 |

*(To reproduce, run `benchmark.py` against the cluster configured with each respective `--fsync` mode).*

 ## 8. Known Limitations (Roadmap)
* **Proxy State Gaps:** A request in flight when a backend dies will hang, and the proxy does not currently clear the dead backend's queued state.
* **Static & Lopsided Hash Ring:** The FNV-1a distribution with the current virtual node count can be lopsided. Nodes do not dynamically rebalance existing data when the hash ring changes.
* **No Real Failover:** After eviction, the proxy sends the dead node’s keys to a survivor, but the survivor’s static ring states the key belongs to the dead node and replies `MOVED`. Without replication, data on the dead node is inaccessible anyway.
 * No Real Failover: After eviction, the proxy sends the dead node’s keys to a survivor, but the survivor’s static ring states the key belongs to the dead node and replies MOVED. Without replication, data on the dead node is inaccessible anyway.
