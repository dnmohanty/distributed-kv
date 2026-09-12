#include "Store.h"
#include "Server.h"
#include "Proxy.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>

int main(int argc, char* argv[]) {
    std::vector<std::string> cluster_nodes = {
        "127.0.0.1:8080",
        "127.0.0.1:8081",
        "127.0.0.1:8082"
    };

    if (argc > 1 && std::string(argv[1]) == "proxy") {
        int port = 7000;
        if (argc > 2) port = std::atoi(argv[2]);
        
        try {
            Proxy proxy(port, cluster_nodes);
            proxy.start();
        } catch (const std::exception& e) {
            std::cerr << "Proxy Fatal error: " << e.what() << '\n';
            return 1;
        }
        return 0;
    }

    int port = 8080;
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }

    try {
        Store store(16);
        Server server(port, store, cluster_nodes);
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Server Fatal error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}