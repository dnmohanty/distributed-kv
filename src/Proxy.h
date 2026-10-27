#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <queue>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include "ConsistentHash.h"

class Proxy {
private:
    int proxy_fd;
    int epoll_fd;
    int port;

    std::shared_mutex state_mtx;
    ConsistentHash hash_ring;
    std::vector<std::string> nodes;

    std::unordered_map<int, std::string> client_buffers;
    std::unordered_map<int, std::string> backend_buffers;

    std::unordered_map<std::string, int> node_to_fd;
    std::unordered_map<int, std::queue<int>> backend_pending_clients;
    std::unordered_map<std::string, bool> node_status;

    std::thread health_check_thread;
    std::atomic<bool> stop_health_checks;

    void set_non_blocking(int socket_fd);
    void init_backend_connections();
    void connect_to_node(const std::string& node);
    void health_check_loop();
    void handle_new_connection();
    void handle_client_data(int client_fd);
    void handle_backend_data(int backend_fd);

public:
    Proxy(int port, const std::vector<std::string>& cluster_nodes);
    ~Proxy();

    void start();
};