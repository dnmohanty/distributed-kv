# DistributedKV

A sharded in-memory KV store in C++20 with a consistent-hashing proxy and a durable write-ahead log.

## 1. Features
* **Non-Blocking I/O:** Core and proxy utilize Linux `epoll` (edge-triggered) to handle high-concurrency connections safely.
* **Smart Asynchronous Proxy:** Client requests hit a central gateway that manages a persistent connection per backend, routing commands without blocking the event loop.
* **Failure Detection & Ring Eviction:** The proxy background-monitors backend nodes. If it detects a closed connection, the node is temporarily evicted from the consistent hash ring to prevent client hangs, and automatically restored upon recovery.
* **Stable Consistent Hashing:** Implements FNV-1a with virtual nodes to map keys identically across proxy and server binaries.
* **Configurable WAL Durability:** Prevents data loss during power failures. Supports `--fsync=always` (fsync on every write), `--fsync=everysec` (background flush thread), and `--fsync=never`.
* **WAL Compaction:** A background thread monitors `.wal` file sizes and automatically takes point-in-time lock-safe snapshots to reclaim disk space, preventing unbounded file growth.
* **Lock-Striping:** The in-memory store is partitioned into 16 separate shards, each protected by a `std::shared_mutex`, structuring the data for future multi-threaded worker pool expansions.
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
```bash
git clone https://github.com/dnmohanty/distributed-kv.git
cd distributed-kv
docker compose up --build
```
## 4. Command Reference

Connect using `telnet 127.0.0.1 7000` or raw TCP sockets.

*   `SET <key> <value>` : Store data. (Returns `OK`)
*   `SET <key> <value> EX <seconds>` : Store data with an absolute expiry TTL. (Returns `OK`)
*   `GET <key>` : Retrieve a value. (Returns value or `(nil)`)
*   `DEL <key>` : Delete a value. (Returns `(integer) 1` if successful, `0` if absent)

## 5. Design Decisions & Guarantees
*    Edge-Triggered Epoll: Reduces system call overhead compared to level-triggered polling, paired with loop-until-EAGAIN reading to avoid    dropped bytes.
*    Torn-Write Protection: The WAL logs length and a CRC32 checksum before the payload, terminating recovery cleanly upon encountering a partial write caused by sudden power loss.
*     Decoupled Disk I/O: Using --fsync=everysec shifts the blocking fdatasync() system call to an isolated background std::thread, allowing the main epoll loop to process memory queries at maximum speed.
*     Atomic Compaction: Snapshotting locks the WAL during memory updates, preventing race conditions where acknowledged writes are dropped during background compaction.

## 6. Failure Behavior

| Scenario | System Reaction |
|---|---|
| Single Node Crash | Data in memory is wiped. Proxy routes to surviving nodes, but returns MOVED for the dead node's keys until it recovers.[cite: 7] |
| Node Restart | Node recovers keys sequentially from its `node_<port>.wal` file, dropping expired TTLs and verifying CRC32 checksums.[cite: 7] |
| Power Loss Mid-Write | Recovery detects a checksum mismatch on the final entry, halts recovery cleanly, and ignores the torn write.[cite: 7] |
| Proxy Crash | Clients disconnect. Backend nodes remain operational and durable.[cite: 7] |
 
 ## 7. Guarantees and Non-Goals
 * Durability: With --fsync=always, every acknowledged write is flushed to physical storage. Crash recovery skips corrupted CRC32 records.
 * Consistency: The proxy guarantees requests for a given key always route to the same node according to the hash ring.
 * Non-Goals: Multi-key transactions, secondary indexing, and strict CP/AP distributed consensus (e.g., Raft).

 ## 8. Benchmarks
 Testing performed locally over TCP loopback using a 10-thread Python load generator with 10,000 requests.
 * Configuration: 3 backend nodes + 1 Proxy (--fsync=everysec)
 * Throughput: ~16,385 ops/sec
 * p50 Latency: 0.49 ms
 * p99 Latency: 1.91 ms

 ## 9. Known Limitations (Roadmap)
 * No Real Failover: After eviction, the proxy sends the dead node’s keys to a survivor, but the survivor’s static ring states the key belongs to the dead node and replies MOVED. Without replication, the data on the dead node is inaccessible anyway.
 * Static Ring Rebalancing: Nodes do not dynamically rebalance or migrate existing data when the hash ring changes (e.g., when a node is permanently added or removed).