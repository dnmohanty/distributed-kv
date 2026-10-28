#include "Store.h"
#include <functional>
#include <mutex>

Store::Store(size_t num_shards) : num_shards(num_shards), shards(num_shards) {}

size_t Store::getShardIndex(const std::string& key) const {
    return std::hash<std::string>{}(key) % num_shards;
}

void Store::set(const std::string& key, const std::string& value, uint64_t expiry) {
    size_t index = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
    shards[index].data[key] = {value, expiry};
}

bool Store::del(const std::string& key) {
    size_t index = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(shards[index].mutex);
    return shards[index].data.erase(key) > 0;
}

std::optional<std::string> Store::get(const std::string& key) {
    size_t index = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(shards[index].mutex);
    
    auto it = shards[index].data.find(key);
    if (it != shards[index].data.end()) {
        if (it->second.expiry_time > 0) {
            uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
            
            if (now > it->second.expiry_time) {
                return std::nullopt;
            }
        }
        return it->second.value;
    }
    return std::nullopt;
}

std::vector<std::tuple<std::string, std::string, uint64_t>> Store::get_snapshot() {
    std::vector<std::tuple<std::string, std::string, uint64_t>> snapshot;
    uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());

    for (size_t i = 0; i < num_shards; ++i) {
        std::shared_lock<std::shared_mutex> lock(shards[i].mutex);
        for (const auto& [key, record] : shards[i].data) {
            if (record.expiry_time == 0 || record.expiry_time > now) {
                snapshot.emplace_back(key, record.value, record.expiry_time);
            }
        }
    }
    return snapshot;
}

void Store::sweep() {
    uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    
    for (size_t i = 0; i < num_shards; ++i) {
        std::unique_lock<std::shared_mutex> lock(shards[i].mutex);
        for (auto it = shards[i].data.begin(); it != shards[i].data.end(); ) {
            if (it->second.expiry_time > 0 && it->second.expiry_time <= now) {
                it = shards[i].data.erase(it);
            } else {
                ++it;
            }
        }
    }
}