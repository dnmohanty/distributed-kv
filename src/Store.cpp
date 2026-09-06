#include "Store.h"
#include <functional>
#include <mutex>

Store::Store(size_t shards) : num_shards(shards) {
    this->shards = std::vector<Shard>(num_shards);
}

size_t Store::getShardIndex(const std::string& key) const {
    std::hash<std::string> hasher;
    return hasher(key) % num_shards;
}

void Store::set(const std::string& key, const std::string& value) {
    size_t index = getShardIndex(key);
    std::unique_lock lock(shards[index].mutex);
    shards[index].data[key] = value;
}

std::optional<std::string> Store::get(const std::string& key) const {
    size_t index = getShardIndex(key);
    std::shared_lock lock(shards[index].mutex);
    
    auto it = shards[index].data.find(key);
    if (it != shards[index].data.end()) {
        return it->second;
    }
    return std::nullopt;
}