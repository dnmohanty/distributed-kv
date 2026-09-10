#include "Store.h"
#include "Server.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>

int main(int argc, char* argv[]) {
    int port = 8080;
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }

    try {
        std::vector<std::string> cluster_nodes = {
            "127.0.0.1:8080",
            "127.0.0.1:8081",
            "127.0.0.1:8082"
        };

        Store store(16);
        
        Server server(port, store, cluster_nodes);
        
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}