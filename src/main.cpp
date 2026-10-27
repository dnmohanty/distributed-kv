#include "Store.h"
#include "Server.h"
#include "Proxy.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include "WAL.h"

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: \n";
        std::cerr << "  Server: " << argv[0] << " <port> <node1> [node2 ...] [--fsync=always|everysec|never]\n";
        std::cerr << "  Proxy:  " << argv[0] << " proxy <port> <node1> [node2 ...]\n";
        return 1;
    }

    bool is_proxy = (std::string(argv[1]) == "proxy");
    int port = is_proxy ? std::atoi(argv[2]) : std::atoi(argv[1]);
    int node_start_idx = is_proxy ? 3 : 2;

    std::vector<std::string> cluster_nodes;
    FsyncMode fsync_mode = FsyncMode::ALWAYS;

    for (int i = node_start_idx; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg.find("--fsync=") == 0) {
            std::string mode_str = arg.substr(8);
            if (mode_str == "everysec") fsync_mode = FsyncMode::EVERYSEC;
            else if (mode_str == "never") fsync_mode = FsyncMode::NEVER;
            else fsync_mode = FsyncMode::ALWAYS;
        } else {
            cluster_nodes.push_back(arg);
        }
    }

    if (cluster_nodes.empty()) {
        std::cerr << "Error: Must specify at least one cluster node.\n";
        return 1;
    }

    try {
        if (is_proxy) {
            Proxy proxy(port, cluster_nodes);
            proxy.start();
        } else {
            Store store(16);
            Server server(port, store, cluster_nodes, fsync_mode);
            server.start();
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << '\n';
        return 1;
    }
    return 0;
}