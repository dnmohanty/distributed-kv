#pragma once
#include <string>
#include <vector>
#include "ConsistentHash.h"

class Proxy {
private:
    int proxy_fd;
    int epoll_fd;
    int port;
    
    ConsistentHash hash_ring;

    void set_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);
    std::string forward_request(const std::string& target_node, const std::string& request);

public:
    Proxy(int port, const std::vector<std::string>& cluster_nodes);
    ~Proxy();
    
    void start(); 
};