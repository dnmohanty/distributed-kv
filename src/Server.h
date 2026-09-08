#pragma once
#include <string>
#include "Store.h"

class Server {
private:
    int server_fd;
    int epoll_fd;
    int port;
    
    Store& store; 

    void set_non_blocking(int socket_fd);
    void handle_new_connection();
    void handle_client_data(int client_fd);

public:
    Server(int port, Store& store);
    ~Server();
    
    void start(); 
};