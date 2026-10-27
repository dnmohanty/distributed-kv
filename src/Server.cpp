#include "Server.h"
#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <cstring>
#include <stdexcept>
#include <sstream>

Server::Server(int port, Store& store, const std::vector<std::string>& cluster_nodes, FsyncMode fsync_mode) 
    : port(port), store(store), wal("node_" + std::to_string(port) + ".wal", fsync_mode) {
    
    std::string port_suffix = ":" + std::to_string(port);
    node_name = "127.0.0.1" + port_suffix; // Fallback
    
    for (const auto& node : cluster_nodes) {
        if (node.length() >= port_suffix.length() && 
            node.compare(node.length() - port_suffix.length(), port_suffix.length(), port_suffix) == 0) {
            node_name = node;
        }
        hash_ring.add_node(node);
    }

    wal.recover(store);

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

    stop_compaction = false;
    compaction_thread = std::thread(&Server::compaction_loop, this);
}

Server::~Server() {
    stop_compaction = true;
    if (compaction_thread.joinable()) compaction_thread.join();

    close(server_fd);
    close(epoll_fd);
}

void Server::set_non_blocking(int socket_fd) {
    int flags = fcntl(socket_fd, F_GETFL, 0);
    fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK);
}

void Server::compaction_loop() {
    while (!stop_compaction) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        store.sweep(); // Memory leak fix: Clear expired keys from RAM
        
        struct stat st;
        if (stat(wal.get_filepath().c_str(), &st) == 0) {
            if (st.st_size > 5 * 1024 * 1024) { 
                std::unique_lock<std::shared_mutex> lock(compaction_rwlock);
                wal.compact(store);
            }
        }
    }
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
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
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

void Server::handle_client_data(int client_fd) {
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
        buf.erase(0, pos + 1);

        if (!request.empty() && request.back() == '\r') request.pop_back();
        if (request.empty()) continue;

        std::string command, key, value;
        int ttl = 0;

        size_t first_space = request.find(' ');
        if (first_space != std::string::npos) {
            command = request.substr(0, first_space);
            size_t second_space = request.find(' ', first_space + 1);
            if (second_space != std::string::npos) {
                key = request.substr(first_space + 1, second_space - first_space - 1);
                std::string remainder = request.substr(second_space + 1);
                
                size_t ex_pos = remainder.rfind(" EX ");
                if (ex_pos != std::string::npos) {
                    value = remainder.substr(0, ex_pos);
                    try {
                        ttl = std::stoi(remainder.substr(ex_pos + 4));
                    } catch (...) {
                        ttl = 0;
                    }
                } else {
                    value = remainder;
                }
            } else {
                key = request.substr(first_space + 1);
            }
        } else {
            command = request;
        }

        std::string response;
        std::string owner = hash_ring.get_node(key);

        if (owner != node_name && !owner.empty()) {
            response = "MOVED to " + owner + "\n";
        } else {
            if (command == "SET") {
                if (key.empty() || value.empty()) {
                    response = "ERROR: Missing key or value\n";
                } else {
                    std::shared_lock<std::shared_mutex> lock(compaction_rwlock);
                    wal.append(key, value, ttl);
                    store.set(key, value, ttl);
                    response = "OK\n";
                }
            } else if (command == "GET") {
                auto result = store.get(key);
                if (result.has_value()) {
                    response = result.value() + "\n";
                } else {
                    response = "(nil)\n";
                }
            } else if (command == "DEL") {
                if (key.empty()) {
                    response = "ERROR: Missing key\n";
                } else {
                    std::shared_lock<std::shared_mutex> lock(compaction_rwlock);
                    wal.append_del(key);
                    if (store.del(key)) {
                        response = "(integer) 1\n";
                    } else {
                        response = "(integer) 0\n";
                    }
                }
            } else {
                response = "ERROR: Unknown command\n";
            }
        }
        write(client_fd, response.c_str(), response.length());
    }
}