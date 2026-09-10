#include "ConsistentHash.h"
#include <functional>
#include <mutex>

ConsistentHash::ConsistentHash(int virtual_nodes) : virtual_nodes(virtual_nodes) {}

size_t ConsistentHash::hash_key(const std::string& key) {
    return std::hash<std::string>{}(key);
}

void ConsistentHash::add_node(const std::string& node_name) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string virtual_node_name = node_name + "#" + std::to_string(i);
        size_t hash = hash_key(virtual_node_name);
        ring[hash] = node_name;
    }
}

void ConsistentHash::remove_node(const std::string& node_name) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string virtual_node_name = node_name + "#" + std::to_string(i);
        size_t hash = hash_key(virtual_node_name);
        ring.erase(hash);
    }
}

std::string ConsistentHash::get_node(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (ring.empty()) {
        return "";
    }

    size_t hash = hash_key(key);
    
    auto it = ring.lower_bound(hash);

    if (it == ring.end()) {
        it = ring.begin();
    }

    return it->second;
}