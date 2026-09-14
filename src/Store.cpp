#include "Store.h"
#include <functional>
#include <mutex>

Store::Store(size_t num_shards) : num_shards(num_shards), shards(num_shards) {}

size_t Store::getShardIndex(const std::string& key) const {
    return std::hash<std::string>{}(key) % num_shards;
}

void Store::set(const std::string& key, const std::string& value, int ttl) {
    size_t index = getShardIndex(key);
    
    uint64_t expiry = 0;
    if (ttl > 0) {
        uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        expiry = now + ttl;
    }

    std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
    shards[index].data[key] = {value, expiry};
}

std::optional<std::string> Store::get(const std::string& key) {
    size_t index = getShardIndex(key);
    
    std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
    
    auto it = shards[index].data.find(key);
    if (it != shards[index].data.end()) {
        
        if (it->second.expiry_time > 0) {
            uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
            
            if (now > it->second.expiry_time) {
                shards[index].data.erase(it);
                return std::nullopt;
            }
        }
        
        return it->second.value;
    }
    return std::nullopt;
}