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
#include <thread>
#include <chrono>

Proxy::Proxy(int port, const std::vector<std::string>& cluster_nodes) : port(port), nodes(cluster_nodes), stop_health_checks(false) {
    for (const auto& node : cluster_nodes) {
        hash_ring.add_node(node);
        node_status[node] = true;
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) throw std::runtime_error("Failed to create epoll");

    init_backend_connections();

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

    health_check_thread = std::thread(&Proxy::health_check_loop, this);
}

Proxy::~Proxy() {
    stop_health_checks = true;
    if (health_check_thread.joinable()) health_check_thread.join();

    for (auto const& [node, fd] : node_to_fd) {
        close(fd);
    }
    close(proxy_fd);
    close(epoll_fd);
}

void Proxy::set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

void Proxy::init_backend_connections() {
    for (const auto& node : nodes) {
        connect_to_node(node);
    }
}

void Proxy::connect_to_node(const std::string& node) {
    size_t colon_pos = node.find(':');
    if (colon_pos == std::string::npos) return;

    std::string ip = node.substr(0, colon_pos);
    int target_port = std::stoi(node.substr(colon_pos + 1));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return;

    sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(target_port);
    inet_pton(AF_INET, ip.c_str(), &serv_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == 0 || errno == EINPROGRESS) {
        set_non_blocking(sock);
        epoll_event event{};
        event.events = EPOLLIN | EPOLLET;
        event.data.fd = sock;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &event);
        node_to_fd[node] = sock;
    } else {
        close(sock);
    }
}

void Proxy::health_check_loop() {
    while (!stop_health_checks) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        for (const auto& node : nodes) {
            if (node_to_fd.find(node) == node_to_fd.end()) continue;
            
            int fd = node_to_fd[node];
            char buf[1];
            ssize_t res = recv(fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
            
            bool is_healthy = !(res == 0 || (res == -1 && errno != EAGAIN && errno != EWOULDBLOCK));
            
            if (is_healthy != node_status[node]) {
                node_status[node] = is_healthy;
                if (is_healthy) {
                    std::cout << "[Proxy] Node " << node << " recovered.\n";
                    hash_ring.add_node(node);
                } else {
                    std::cout << "[Proxy] Node " << node << " DOWN. Evicting from ring.\n";
                    hash_ring.remove_node(node);
                    close(fd);
                    node_to_fd.erase(node);
                    connect_to_node(node);
                }
            }
        }
    }
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
            int current_fd = events[i].data.fd;
            if (current_fd == proxy_fd) {
                handle_new_connection();
            } else {
                bool is_backend = false;
                for (auto const& [node, fd] : node_to_fd) {
                    if (fd == current_fd) {
                        is_backend = true;
                        break;
                    }
                }
                
                if (is_backend) {
                    handle_backend_data(current_fd);
                } else {
                    handle_client_data(current_fd);
                }
            }
        }
    }
}

void Proxy::handle_new_connection() {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(proxy_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return;
        }

        set_non_blocking(client_fd);
        epoll_event event{};
        event.events = EPOLLIN | EPOLLET;
        event.data.fd = client_fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &event);
    }
}

void Proxy::handle_client_data(int client_fd) {
    char buffer[1024];
    while (true) {
        ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer));
        if (bytes_read > 0) {
            client_buffers[client_fd].append(buffer, bytes_read);
        } else if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            close(client_fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
            client_buffers.erase(client_fd);
            return;
        } else {
            close(client_fd);
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
            client_buffers.erase(client_fd);
            return;
        }
    }

    auto& buf = client_buffers[client_fd];
    size_t pos;
    while ((pos = buf.find('\n')) != std::string::npos) {
        std::string request = buf.substr(0, pos);
        std::string full_request = request + "\n";
        buf.erase(0, pos + 1);

        if (!request.empty() && request.back() == '\r') request.pop_back();
        if (request.empty()) continue;

        std::string command, key;
        size_t first_space = request.find(' ');
        if (first_space != std::string::npos) {
            command = request.substr(0, first_space);
            size_t second_space = request.find(' ', first_space + 1);
            if (second_space != std::string::npos) {
                key = request.substr(first_space + 1, second_space - first_space - 1);
            } else {
                key = request.substr(first_space + 1);
            }
        } else {
            command = request;
        }

        if (key.empty()) {
            std::string err = "ERROR: Missing key\n";
            write(client_fd, err.c_str(), err.length());
        } else {
            std::string owner = hash_ring.get_node(key);
            if (owner.empty() || node_to_fd.find(owner) == node_to_fd.end()) {
                std::string err = "ERROR: No backend nodes available\n";
                write(client_fd, err.c_str(), err.length());
            } else {
                int backend_fd = node_to_fd[owner];
                backend_pending_clients[backend_fd].push(client_fd);
                write(backend_fd, full_request.c_str(), full_request.length());
            }
        }
    }
}

void Proxy::handle_backend_data(int backend_fd) {
    char buffer[1024];
    while (true) {
        ssize_t bytes_read = read(backend_fd, buffer, sizeof(buffer));
        if (bytes_read > 0) {
            backend_buffers[backend_fd].append(buffer, bytes_read);
        } else if (bytes_read == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            return; 
        } else {
            return; 
        }
    }

    auto& buf = backend_buffers[backend_fd];
    size_t pos;
    while ((pos = buf.find('\n')) != std::string::npos) {
        std::string response = buf.substr(0, pos + 1);
        buf.erase(0, pos + 1);

        if (!backend_pending_clients[backend_fd].empty()) {
            int client_fd = backend_pending_clients[backend_fd].front();
            backend_pending_clients[backend_fd].pop();
            write(client_fd, response.c_str(), response.length());
        }
    }
}