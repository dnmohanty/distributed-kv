#include "Server.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <stdexcept>
#include <sstream>

Server::Server(int port, Store& store, const std::vector<std::string>& cluster_nodes) 
    : port(port), store(store) {
    
    node_name = "127.0.0.1:" + std::to_string(port);

    for (const auto& node : cluster_nodes) {
        hash_ring.add_node(node);
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        throw std::runtime_error("Failed to bind to port");
    }

    if (listen(server_fd, SOMAXCONN) < 0) {
        throw std::runtime_error("Failed to listen on socket");
    }

    set_non_blocking(server_fd);

    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        throw std::runtime_error("Failed to create epoll");
    }
}

Server::~Server() {
    close(server_fd);
    close(epoll_fd);
}

void Server::set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

void Server::start() {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = server_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) == -1) {
        throw std::runtime_error("Failed to add server socket to epoll");
    }

    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    std::cout << "Database node " << node_name << " listening...\n";

    while (true) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        
        for (int i = 0; i < num_events; ++i) {
            if (events[i].data.fd == server_fd) {
                handle_new_connection();
            } else {
                handle_client_data(events[i].data.fd);
            }
        }
    }
}

void Server::handle_new_connection() {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    
    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd == -1) return;

    set_non_blocking(client_fd);

    epoll_event event{};
    event.events = EPOLLIN | EPOLLET; 
    event.data.fd = client_fd;

    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
}

void Server::handle_client_data(int client_fd) {
    char buffer[1024];
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);

    if (bytes_read <= 0) {
        close(client_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
        return;
    } 
    
    buffer[bytes_read] = '\0';
    std::string request(buffer);
    
    if (!request.empty() && request.back() == '\n') request.pop_back();
    if (!request.empty() && request.back() == '\r') request.pop_back();

    std::istringstream iss(request);
    std::string command, key, value;
    iss >> command >> key;

    std::string response;

    std::string owner = hash_ring.get_node(key);

    if (owner != node_name && !owner.empty()) {
        response = "MOVED to " + owner + "\n";
    } 
    else {
        if (command == "SET") {
            std::getline(iss >> std::ws, value);
            store.set(key, value);
            response = "OK\n";
        } 
        else if (command == "GET") {
            auto result = store.get(key);
            if (result.has_value()) {
                response = result.value() + "\n";
            } else {
                response = "(nil)\n";
            }
        } 
        else {
            response = "ERROR: Unknown command\n";
        }
    }

    write(client_fd, response.c_str(), response.length());
}