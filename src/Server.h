#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include "Store.h"
#include "ConsistentHash.h"
#include "WAL.h"

class Server {
private:
    int server_fd;
    int epoll_fd;
    int port;
    std::string node_name;
    
    Store& store;
    ConsistentHash hash_ring;
    WAL wal;
    
    std::unordered_map<int, std::string> client_buffers;

    std::thread compaction_thread;
    std::atomic<bool> stop_compaction;
    std::shared_mutex compaction_rwlock; // Prevents race condition during snapshots

    void set_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);
    void compaction_loop();

public:
    Server(int port, Store& store, const std::vector<std::string>& cluster_nodes, FsyncMode fsync_mode = FsyncMode::ALWAYS);
    ~Server();
    
    void start();
};