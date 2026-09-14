# Distributed Key-Value Store

A high-performance, distributed, in-memory key-value store written from scratch in C++. 

This project implements a custom database architecture heavily inspired by enterprise systems like Redis Cluster and Amazon DynamoDB. It features a smart reverse proxy, mathematical data sharding via Consistent Hashing, and disk durability.

## 🚀 Key Features

* **High-Concurrency Core:** Uses Linux `epoll` for non-blocking, asynchronous I/O, capable of handling thousands of simultaneous TCP connections.
* **Smart Proxy Gateway:** A dedicated routing layer that intercepts client requests and forwards them to the correct backend node, masking cluster complexity from the user.
* **Consistent Hashing:** Evenly distributes data across multiple database nodes. When a node goes down, the system dynamically reroutes traffic without full data rebalancing.
* **Thread-Safe Sharding:** Custom memory engine utilizing `std::shared_mutex` for granular read/write locking, preventing race conditions during high-volume operations.
* **Write-Ahead Logging (WAL):** Ensures data durability. Every `SET` operation is appended to a disk log before returning success, allowing full recovery in the event of a server crash.
* **TTL (Time-To-Live):** Supports automatic key expiration with a lazy-deletion strategy to optimize memory usage.
* **Custom CLI Client:** Includes `kv_cli`, a dedicated command-line interface for seamless interaction with the cluster.

## 🏗️ Architecture

1. **Client (`kv_cli`)** connects to the **Smart Proxy** (Port 7000).
2. The **Proxy** parses the command, applies the Consistent Hashing algorithm to the key, and determines which backend node owns the data.
3. The **Proxy** forwards the request to the correct **Backend Node** (e.g., Port 8080, 8081, or 8082).
4. The **Backend Node** writes to its WAL, updates memory, and sends the response back through the proxy.

## 🛠️ Getting Started

### Prerequisites
* C++17 Compiler (GCC/Clang)
* CMake (3.10+)
* Linux environment (for `epoll` support)

### Building the Project
```bash
git clone [https://github.com/dnmohanty/distributed-kv.git](https://github.com/yourusername/distributed-kv.git)
cd distributed-kv
mkdir build && cd build
cmake ..
cmake --build .