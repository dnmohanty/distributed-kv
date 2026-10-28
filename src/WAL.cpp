#include "WAL.h"
#include <iostream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstring>

WAL::WAL(const std::string& filepath, FsyncMode mode) : filepath(filepath), fsync_mode(mode), stop_thread(false) {
    fd = open(filepath.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    if (fd < 0) throw std::runtime_error("Failed to open WAL");
    if (fsync_mode == FsyncMode::EVERYSEC) flush_thread = std::thread(&WAL::background_flush, this);
}

WAL::~WAL() {
    if (fsync_mode == FsyncMode::EVERYSEC) {
        stop_thread = true;
        cv.notify_one();
        if (flush_thread.joinable()) flush_thread.join();
    }
    if (fd >= 0) {
        if (fsync_mode != FsyncMode::NEVER) {
            if (fdatasync(fd) < 0) {
                std::cerr << "[WAL] Sync failed on shutdown\n";
            }
        }
        close(fd);
    }
}

void WAL::background_flush() {
    std::mutex cv_m;
    std::unique_lock<std::mutex> lk(cv_m);
    while (!stop_thread) {
        cv.wait_for(lk, std::chrono::seconds(1));
        if (fd >= 0) {
            std::lock_guard<std::mutex> lock(mtx);
            if (fdatasync(fd) < 0) {
                std::cerr << "[WAL] Background sync failed\n";
            }
        }
    }
}

uint32_t WAL::calculate_crc32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFF;
    for (char c : data) {
        crc ^= static_cast<uint8_t>(c);
        for (int j = 0; j < 8; ++j) crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
    }
    return ~crc;
}

bool WAL::write_fully(int file_fd, const void* buf, size_t count) {
    const char* p = static_cast<const char*>(buf);
    size_t total = 0;
    while (total < count) {
        ssize_t written = write(file_fd, p + total, count - total);
        if (written <= 0) return false;
        total += written;
    }
    return true;
}

bool WAL::append(const std::string& key, const std::string& value, uint64_t expiry) {
    std::string payload;
    uint8_t op = 0;
    payload.append(reinterpret_cast<const char*>(&op), 1);
    
    uint32_t klen = key.size();
    payload.append(reinterpret_cast<const char*>(&klen), 4);
    payload.append(key);
    
    payload.append(reinterpret_cast<const char*>(&expiry), 8);
    uint32_t vlen = value.size();
    payload.append(reinterpret_cast<const char*>(&vlen), 4);
    payload.append(value);

    uint32_t len = payload.size();
    uint32_t crc = calculate_crc32(payload);

    std::lock_guard<std::mutex> lock(mtx);
    if (!write_fully(fd, &len, sizeof(len))) return false;
    if (!write_fully(fd, &crc, sizeof(crc))) return false;
    if (!write_fully(fd, payload.c_str(), len)) return false;
    
    if (fsync_mode == FsyncMode::ALWAYS) {
        if (fdatasync(fd) < 0) return false;
    }
    return true;
}

bool WAL::append_del(const std::string& key) {
    std::string payload;
    uint8_t op = 1;
    payload.append(reinterpret_cast<const char*>(&op), 1);
    
    uint32_t klen = key.size();
    payload.append(reinterpret_cast<const char*>(&klen), 4);
    payload.append(key);

    uint32_t len = payload.size();
    uint32_t crc = calculate_crc32(payload);

    std::lock_guard<std::mutex> lock(mtx);
    if (!write_fully(fd, &len, sizeof(len))) return false;
    if (!write_fully(fd, &crc, sizeof(crc))) return false;
    if (!write_fully(fd, payload.c_str(), len)) return false;
    
    if (fsync_mode == FsyncMode::ALWAYS) {
        if (fdatasync(fd) < 0) return false;
    }
    return true;
}

void WAL::recover(Store& store) {
    lseek(fd, 0, SEEK_SET);
    int recovered_count = 0;

    while (true) {
        off_t record_start = lseek(fd, 0, SEEK_CUR);
        uint32_t len = 0;
        uint32_t crc = 0;
        
        ssize_t rb = read(fd, &len, sizeof(len));
        if (rb <= 0) break;
        if (rb != sizeof(len) || len > 10 * 1024 * 1024) { 
            if (ftruncate(fd, record_start) < 0) std::cerr << "[WAL] Failed to truncate log\n"; 
            break; 
        }

        rb = read(fd, &crc, sizeof(crc));
        if (rb != sizeof(crc)) { 
            if (ftruncate(fd, record_start) < 0) std::cerr << "[WAL] Failed to truncate log\n"; 
            break; 
        }

        std::string payload(len, '\0');
        rb = read(fd, &payload[0], len);
        if (rb != len) { 
            if (ftruncate(fd, record_start) < 0) std::cerr << "[WAL] Failed to truncate log\n"; 
            break; 
        }

        if (calculate_crc32(payload) != crc) {
            if (ftruncate(fd, record_start) < 0) std::cerr << "[WAL] Failed to truncate log\n";
            break;
        }

        uint8_t op = payload[0];
        size_t offset = 1;
        uint32_t klen = *reinterpret_cast<const uint32_t*>(payload.data() + offset);
        offset += 4;
        std::string key = payload.substr(offset, klen);
        offset += klen;

        if (op == 0) {
            uint64_t expiry = *reinterpret_cast<const uint64_t*>(payload.data() + offset);
            offset += 8;
            uint32_t vlen = *reinterpret_cast<const uint32_t*>(payload.data() + offset);
            offset += 4;
            std::string value = payload.substr(offset, vlen);
            
            uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
            
            if (expiry == 0 || expiry > now) {
                store.set(key, value, expiry);
                recovered_count++;
            }
        } else if (op == 1) {
            store.del(key);
        }
    }
}

void WAL::compact(Store& store) {
    std::lock_guard<std::mutex> lock(mtx);

    std::string tmp_filepath = filepath + ".tmp";
    int tmp_fd = open(tmp_filepath.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (tmp_fd < 0) return;

    auto snapshot = store.get_snapshot();
    
    for (const auto& [key, value, expiry] : snapshot) {
        std::string payload;
        uint8_t op = 0;
        payload.append(reinterpret_cast<const char*>(&op), 1);
        uint32_t klen = key.size();
        payload.append(reinterpret_cast<const char*>(&klen), 4);
        payload.append(key);
        payload.append(reinterpret_cast<const char*>(&expiry), 8);
        uint32_t vlen = value.size();
        payload.append(reinterpret_cast<const char*>(&vlen), 4);
        payload.append(value);

        uint32_t len = payload.size();
        uint32_t crc = calculate_crc32(payload);

        if (!write_fully(tmp_fd, &len, sizeof(len)) ||
            !write_fully(tmp_fd, &crc, sizeof(crc)) ||
            !write_fully(tmp_fd, payload.c_str(), len)) {
            close(tmp_fd);
            unlink(tmp_filepath.c_str());
            return;
        }
    }
    
    if (fdatasync(tmp_fd) < 0) {
        close(tmp_fd);
        unlink(tmp_filepath.c_str());
        return;
    }
    close(tmp_fd);

    if (rename(tmp_filepath.c_str(), filepath.c_str()) == 0) {
        close(fd);
        fd = open(filepath.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    }
}