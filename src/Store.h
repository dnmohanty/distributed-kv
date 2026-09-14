#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <optional>
#include <chrono>

struct CacheEntry {
    std::string value;
    uint64_t expiry_time; 
};

class Store {
private:
    struct Shard {
        std::unordered_map<std::string, CacheEntry> data;
        mutable std::shared_mutex mutex; 
    };

    size_t num_shards;
    std::vector<Shard> shards;

    size_t getShardIndex(const std::string& key) const;

public:
    explicit Store(size_t num_shards = 16);
    
    void set(const std::string& key, const std::string& value, int ttl = 0);
    std::optional<std::string> get(const std::string& key); 
};