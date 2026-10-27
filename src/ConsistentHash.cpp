#include "ConsistentHash.h"
#include <mutex>

ConsistentHash::ConsistentHash(int virtual_nodes) : virtual_nodes(virtual_nodes) {}

uint32_t ConsistentHash::fnv1a_hash(const std::string& key) const {
    uint32_t hash = 2166136261u;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

void ConsistentHash::add_node(const std::string& node_name) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string virtual_node_name = node_name + "#" + std::to_string(i);
        uint32_t hash = fnv1a_hash(virtual_node_name);
        ring[hash] = node_name;
    }
}

void ConsistentHash::remove_node(const std::string& node_name) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    for (int i = 0; i < virtual_nodes; ++i) {
        std::string virtual_node_name = node_name + "#" + std::to_string(i);
        uint32_t hash = fnv1a_hash(virtual_node_name);
        ring.erase(hash);
    }
}

std::string ConsistentHash::get_node(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(mutex);
    if (ring.empty()) {
        return "";
    }

    uint32_t hash = fnv1a_hash(key);
    auto it = ring.lower_bound(hash);

    if (it == ring.end()) {
        it = ring.begin();
    }

    return it->second;
}