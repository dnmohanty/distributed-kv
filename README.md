# DistributedKV

A sharded in-memory key-value store in C++20 with a consistent-hashing proxy and a durable write-ahead log.

## 1. Features

* **Event-Driven I/O:** The server and proxy use Linux `epoll` (edge-triggered) with non-blocking sockets, reading until `EAGAIN` and buffering partial requests per connection.
* **Routing Proxy:** Clients connect to one gateway that keeps a persistent connection to each backend and routes every command to the node that owns its key, without blocking the event loop.
* **Failure Detection & Ring Eviction:** A background thread checks backend connections every 5 seconds. A node whose connection has closed is evicted from the hash ring, and the proxy tries to reconnect on each check and restores the node when it succeeds.
* **Stable Consistent Hashing:** FNV-1a with virtual nodes, so the proxy and every server compute the same key-to-node mapping.
* **Configurable WAL Durability:** Every write is appended to a CRC32-checked write-ahead log before it is acknowledged. Supports `--fsync=always` (`fdatasync` on every write), `--fsync=everysec` (background flush thread) and `--fsync=never`.
* **WAL Compaction:** A background thread rewrites the log from a snapshot of live keys once it grows past 5 MB, using a temp file and an atomic rename.
* **Lock-Striping:** The in-memory store is split into 16 shards, each protected by a `std::shared_mutex`. The locks are needed because the compaction and TTL-sweeper threads access the store while the epoll thread serves requests.
* **Time-to-Live (TTL):** Expiry times are stored as absolute timestamps in the WAL, checked lazily on `GET`, and swept periodically to free memory.

## 2. Architecture

```text
[ Client ] ---> [ Proxy (port 7000) ]
                         | (FNV-1a consistent-hash ring + connection monitoring)
         +---------------+---------------+
         |               |               |
 [ Backend 8080 ] [ Backend 8081 ] [ Backend 8082 ]
         |               |               |
      (WAL)           (WAL)           (WAL)
    (Compactor)     (Compactor)     (Compactor)
```

## 3. Quick Start

### Via Docker (recommended)

```bash
git clone https://github.com/dnmohanty/distributed-kv.git
cd distributed-kv
docker compose up --build
```

Then connect to the proxy on port 7000, for example with `nc 127.0.0.1 7000` or `telnet 127.0.0.1 7000`.

### Run locally (no Docker)

Node names must match exactly across every process, or you will get `MOVED` errors (use `127.0.0.1:8080` everywhere, not `localhost:8080` in some places).

```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .

# Three backend nodes: kv_server <port> <all node addresses...> [--fsync=always|everysec|never]
./kv_server 8080 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 --fsync=everysec &
./kv_server 8081 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 --fsync=everysec &
./kv_server 8082 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 --fsync=everysec &

# Proxy: kv_server proxy <port> <all node addresses...>
./kv_server proxy 7000 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082 &

# Interactive client (connects to port 7000 by default)
./kv_cli
```

Each backend writes its log to `node_<port>.wal` in its working directory. The default fsync mode is `always`.

## 4. Command Reference

Connect with `telnet 127.0.0.1 7000`, the provided `./kv_cli`, or any raw TCP client. Commands are line-based and must be uppercase.

* `SET <key> <value>`: Store data. Values may contain spaces. Returns `OK`.
* `SET <key> <value> EX <seconds>`: Store data that expires after `<seconds>` seconds. Returns `OK`.
* `GET <key>`: Returns the value, or `(nil)` if the key is missing or expired.
* `DEL <key>`: Returns `(integer) 1` if the key existed, `(integer) 0` otherwise.

**Protocol notes**

* **Request size:** The maximum request size is 1 MB. A connection that buffers more than that is closed without a reply.
* **Errors:** `ERROR: Missing key or value`, `ERROR: Missing key`, `ERROR: Invalid TTL` (zero, negative or non-numeric), `ERROR: Unknown command`, `ERROR: Disk write failed`, and, from the proxy only, `ERROR: No backend nodes available`.
* **Redirection:** A backend replies `MOVED to <node>` when the key belongs to a different node. You see this when connecting to a backend directly, or through the proxy while a node is evicted.
* **Granularity:** TTLs have one-second resolution.

## 5. Design Decisions & Guarantees

* **Durability:** With `--fsync=always`, every acknowledged write has been flushed with `fdatasync`. With `everysec`, up to about one second of acknowledged writes can be lost on power failure. With `never`, the OS decides. A process crash alone loses nothing in any mode, because the data has already been written to the OS.
* **Torn-Write Protection:** Each WAL record is length-prefixed and carries a CRC32 checksum. Recovery stops at the first incomplete or corrupt record and truncates the rest of the file. Record length is capped at 10 MB so a corrupt header cannot trigger a huge allocation.
* **Binary Record Format:** Keys and values are stored as length-prefixed fields, so values containing spaces or leading whitespace are restored exactly.
* **Request Limits:** Server and proxy close any connection that buffers more than 1 MB of unprocessed input.
* **Write Failures:** If a log write or sync fails, the client gets `ERROR: Disk write failed` and the in-memory store is not updated.
* **Decoupled Disk I/O:** With `--fsync=everysec`, the blocking `fdatasync()` call runs on a background thread, not on the epoll thread.
* **Compaction Safety:** Appending to the log and updating memory run under a shared lock, and compaction takes that lock exclusively. A write can therefore never be missing from a rewritten log.
* **Consistency:** While ring membership is unchanged, the proxy always routes a given key to the same node.

## 6. Failure Behavior

| Scenario | System Reaction |
|---|---|
| Single node crash | Requests in flight to that node at the moment of failure hang. Its in-memory data is gone until it restarts. Within about 5 seconds the proxy evicts the node, and its keys then return `MOVED` until the node comes back. |
| Node restart | The node replays its `.wal` file, skipping expired keys and verifying every CRC32 checksum. The proxy reconnects on its next health check and restores the node to the ring. |
| Power loss mid-write | Recovery detects the incomplete or corrupt final record, stops cleanly and truncates it. Earlier records are kept. |
| Disk write fails | The write returns `ERROR: Disk write failed` and is not applied. |
| Proxy crash | Clients disconnect. Backends keep running and their data stays durable. |

## 7. Benchmarks

Tested locally over TCP loopback on an Intel Core i5-1135G7 (8 GB RAM, Windows Subsystem for Linux) with a Release build.

**Workload:** a 10-thread Python load generator sending 10,000 `SET` operations with small values to a 4-process cluster (1 proxy, 3 backends).

Throughput is limited by the Python client (the GIL), so the server's own capacity is higher than these numbers.

| Fsync mode | Throughput (ops/sec) | p50 latency (ms) | p99 latency (ms) |
|---|---|---|---|
| `--fsync=never` | ~12,400 | 0.52 | 2.10 |
| `--fsync=everysec` | ~12,158 | 0.58 | 2.89 |
| `--fsync=always` | ~4,200 | 2.80 | 18.50 |

To reproduce, run `benchmark.py` against a cluster started with each `--fsync` mode.

## 8. Known Limitations (Roadmap)

* **Proxy state gaps:** A request in flight when a backend dies will hang, and the proxy does not clear that backend's queued state.
* **Static, lopsided hash ring:** FNV-1a with the current number of virtual nodes can distribute keys unevenly. Nodes do not rebalance or migrate data when ring membership changes.
* **No real failover:** After a node is evicted, the proxy sends its keys to a surviving node, but that node's static ring says the key belongs to the dead node and replies `MOVED`. Without replication, the dead node's data is unavailable until it restarts.
* **Ephemeral Docker storage:** `docker-compose.yml` does not mount volumes, so WAL files are lost on `docker compose down`.
* **Testing coverage:** Automated tests cover basic crash recovery. Torn-write, large-value and compaction cases are not yet automated.
* **Single-threaded request handling:** One epoll thread serves all requests, so the 16 shards do not yet give parallel request handling.
