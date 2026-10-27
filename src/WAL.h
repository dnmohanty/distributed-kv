#pragma once
#include <string>
#include <mutex>
#include <cstdint>
#include <thread>
#include <atomic>
#include <condition_variable>
#include "Store.h"

enum class FsyncMode {
    ALWAYS,
    EVERYSEC,
    NEVER
};

class WAL {
private:
    std::string filepath;
    int fd;
    std::mutex mtx;
    FsyncMode fsync_mode;
    
    std::thread flush_thread;
    std::atomic<bool> stop_thread;
    std::condition_variable cv;

    uint32_t calculate_crc32(const std::string& data);
    void background_flush();

public:
    WAL(const std::string& filepath, FsyncMode mode = FsyncMode::ALWAYS);
    ~WAL();

    void append(const std::string& key, const std::string& value, int ttl = 0);
    void append_del(const std::string& key);
    
    void recover(Store& store);
    void compact(Store& store);
    
    std::string get_filepath() const { return filepath; }
};