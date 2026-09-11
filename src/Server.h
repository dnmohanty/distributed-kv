#pragma once
#include <string>
#include <vector>
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

    void set_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);

public:
    Server(int port, Store& store, const std::vector<std::string>& cluster_nodes);
    ~Server();
    
    void start(); 
};