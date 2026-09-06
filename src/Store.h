#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <vector>
#include <optional>

class Store {
private:
    struct Shard {
        std::unordered_map<std::string, std::string> data;
        mutable std::shared_mutex mutex; 
    };

    std::vector<Shard> shards;
    size_t num_shards;

    size_t getShardIndex(const std::string& key) const;

public:
    explicit Store(size_t num_shards = 16);
    
    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
};