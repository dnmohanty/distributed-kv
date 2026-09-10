#pragma once
#include <string>
#include <map>
#include <vector>
#include <shared_mutex>

class ConsistentHash {
private:
    std::map<size_t, std::string> ring;
    
    int virtual_nodes; 
    
    std::shared_mutex mutex;

    size_t hash_key(const std::string& key);

public:
    ConsistentHash(int virtual_nodes = 100);
    
    void add_node(const std::string& node_name);
    void remove_node(const std::string& node_name);
    
    std::string get_node(const std::string& key);
};