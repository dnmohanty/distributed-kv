#include "Proxy.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <stdexcept>
#include <sstream>

Proxy::Proxy(int port, const std::vector<std::string>& cluster_nodes) : port(port) {
    for (const auto& node : cluster_nodes) {
        hash_ring.add_node(node);
    }

    proxy_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (proxy_fd == -1) throw std::runtime_error("Failed to create socket");

    int opt = 1;
    setsockopt(proxy_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(proxy_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        throw std::runtime_error("Failed to bind to port");
    }

    if (listen(proxy_fd, SOMAXCONN) < 0) {
        throw std::runtime_error("Failed to listen on socket");
    }

    set_non_blocking(proxy_fd);
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) throw std::runtime_error("Failed to create epoll");
}

Proxy::~Proxy() {
    close(proxy_fd);
    close(epoll_fd);
}

void Proxy::set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

void Proxy::start() {
    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = proxy_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, proxy_fd, &event);

    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    std::cout << "Smart Proxy Gateway listening on port " << port << "...\n";

    while (true) {
        int num_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < num_events; ++i) {
            if (events[i].data.fd == proxy_fd) {
                handle_new_connection();
            } else {
                handle_client_data(events[i].data.fd);
            }
        }
    }
}

void Proxy::handle_new_connection() {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(proxy_fd, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd == -1) return;

    set_non_blocking(client_fd);
    epoll_event event{};
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = client_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
}

std::string Proxy::forward_request(const std::string& target_node, const std::string& request) {
    size_t colon_pos = target_node.find(':');
    if (colon_pos == std::string::npos) return "ERROR: Invalid node address\n";

    std::string ip = target_node.substr(0, colon_pos);
    int target_port = std::stoi(target_node.substr(colon_pos + 1));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return "ERROR: Proxy socket creation failed\n";

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_port);

    if (inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr) <= 0) {
        close(sock);
        return "ERROR: Invalid IP address\n";
    }

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sock);
        return "ERROR: Connection to backend node " + target_node + " failed\n";
    }

    std::string req_with_nl = request + "\n";
    send(sock, req_with_nl.c_str(), req_with_nl.length(), 0);

    char buffer[1024] = {0};
    int bytes_read = read(sock, buffer, sizeof(buffer) - 1);
    close(sock);

    if (bytes_read > 0) {
        return std::string(buffer, bytes_read);
    }
    return "ERROR: No response from backend\n";
}

void Proxy::handle_client_data(int client_fd) {
    char buffer[1024];
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);

    if (bytes_read <= 0) {
        close(client_fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
        return;
    }

    buffer[bytes_read] = '\0';
    std::string request(buffer);

    std::string clean_request = request;
    if (!clean_request.empty() && clean_request.back() == '\n') clean_request.pop_back();
    if (!clean_request.empty() && clean_request.back() == '\r') clean_request.pop_back();

    std::istringstream iss(clean_request);
    std::string command, key;
    iss >> command >> key;

    std::string response;

    if (key.empty()) {
        response = "ERROR: Missing key\n";
    } else {
        std::string owner = hash_ring.get_node(key);
        if (owner.empty()) {
            response = "ERROR: No backend nodes available\n";
        } else {
            response = forward_request(owner, clean_request);
        }
    }

    write(client_fd, response.c_str(), response.length());
}