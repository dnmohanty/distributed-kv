# DistributedKV: Sharded in-memory KV store in C++20 with consistent-hashing proxy and a durable write-ahead log

A scalable, distributed, in-memory key-value database built from scratch. It relies on mathematical data sharding via consistent hashing, asynchronous edge-triggered I/O, lock-striping for concurrent reads, and disk-level durability. 

## 1. Features
* **Non-Blocking I/O:** Core and proxy utilize Linux `epoll` (edge-triggered) to handle high-concurrency connections safely.
* **Smart Asynchronous Proxy:** Client requests hit a central gateway that manages persistent backend connection pools, routing commands without blocking the event loop.
* **Health Checks & Ring Eviction:** The proxy background-pings backend nodes. If a node dies, it is temporarily evicted from the consistent hash ring to prevent client hangs, and automatically restored upon recovery.
* **Stable Consistent Hashing:** Implements FNV-1a with virtual nodes to map keys identically across proxy and server binaries.
* **Configurable WAL Durability:** Prevents data loss during power failures. Supports `--fsync=always` (strict ACID durability), `--fsync=everysec` (background flush thread), and `--fsync=never`.
* **WAL Compaction:** A background thread monitors `.wal` file sizes and automatically takes point-in-time lock-safe snapshots to reclaim disk space, preventing unbounded file growth.
* **Lock-Striping:** The in-memory store is partitioned into 16 separate shards, each protected by a `std::shared_mutex`, allowing massive read concurrency.
* **Time-to-Live (TTL):** Evaluates absolute expiration times lazily on `GET` requests and persists expiries reliably to disk.

## 2. Architecture Diagram
```text
[ Client ] ---> [ Smart Proxy Gateway (Port 7000) ]
                         | (FNV-1a Consistent Hashing Ring + Health Pings)
         +---------------+---------------+
         |               |               |
 [ Backend 8080 ] [ Backend 8081 ] [ Backend 8082 ]
         |               |               |
      (WAL)           (WAL)           (WAL)
    (Compactor)     (Compactor)     (Compactor)
```
## 3. Quick Start
git clone https://github.com/dnmohanty/distributed-kv.git
cd distributed-kv
docker compose up --build

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
 ## 6. Benchmarks
 Testing performed locally over TCP loopback using a 10-thread Python load generator with 10,000 requests.
 * Configuration: 3 backend nodes + 1 Proxy (--fsync=everysec)
 * Throughput: ~16,385 ops/sec
 * p50 Latency: 0.49 ms
 * p99 Latency: 1.91 ms

 ## 7. CI/CD & Testing
 The repository uses GitHub Actions to automatically run a suite of durability and crash-recovery scripts (test_crash.py) upon every push to the main branch, ensuring commits do not break CRC32 checksum generation or proxy routing logic.

 ## 8. Roadmap
 * Primary-to-Follower Replication (streaming WAL updates over TCP to standby nodes).
 * Dynamic data rebalancing upon node addition/eviction.
