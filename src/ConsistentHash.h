#pragma once
#include <string>
#include <map>
#include <vector>
#include <shared_mutex>
#include <cstdint>

class ConsistentHash {
private:
    std::map<uint32_t, std::string> ring;
    int virtual_nodes; 
    mutable std::shared_mutex mutex;

    uint32_t fnv1a_hash(const std::string& key) const;

public:
    ConsistentHash(int virtual_nodes = 100);
    
    void add_node(const std::string& node_name);
    void remove_node(const std::string& node_name);
    
    std::string get_node(const std::string& key);
};