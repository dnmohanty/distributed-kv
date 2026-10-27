#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <tuple>
#include <shared_mutex>
#include <optional>
#include <chrono>

struct Record {
    std::string value;
    uint64_t expiry_time; 
};

struct Shard {
    std::unordered_map<std::string, Record> data;
    mutable std::shared_mutex mutex;
};

class Store {
private:
    size_t num_shards;
    std::vector<Shard> shards;

    size_t getShardIndex(const std::string& key) const;

public:
    Store(size_t num_shards = 16);
    
    void set(const std::string& key, const std::string& value, int ttl = 0);
    void set_with_absolute_expiry(const std::string& key, const std::string& value, uint64_t expiry);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    
    std::vector<std::tuple<std::string, std::string, uint64_t>> get_snapshot();
    void sweep(); 
};