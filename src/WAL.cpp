#include "WAL.h"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

WAL::WAL(const std::string& filepath, FsyncMode mode) : filepath(filepath), fsync_mode(mode), stop_thread(false) {
    fd = open(filepath.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    if (fd < 0) throw std::runtime_error("Failed to open WAL file: " + filepath);
    if (fsync_mode == FsyncMode::EVERYSEC) flush_thread = std::thread(&WAL::background_flush, this);
}

WAL::~WAL() {
    if (fsync_mode == FsyncMode::EVERYSEC) {
        stop_thread = true;
        cv.notify_one();
        if (flush_thread.joinable()) flush_thread.join();
    }
    if (fd >= 0) {
        fdatasync(fd);
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
            fdatasync(fd);
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

void WAL::append(const std::string& key, const std::string& value, int ttl) {
    uint64_t expiry = 0;
    if (ttl > 0) {
        uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        expiry = now + ttl;
    }

    std::string payload = "SET " + key + " " + std::to_string(expiry) + " " + value;
    uint32_t len = payload.size();
    uint32_t crc = calculate_crc32(payload);

    std::lock_guard<std::mutex> lock(mtx);
    write(fd, &len, sizeof(len));
    write(fd, &crc, sizeof(crc));
    write(fd, payload.c_str(), len);
    if (fsync_mode == FsyncMode::ALWAYS) fdatasync(fd);
}

void WAL::append_del(const std::string& key) {
    std::string payload = "DEL " + key;
    uint32_t len = payload.size();
    uint32_t crc = calculate_crc32(payload);

    std::lock_guard<std::mutex> lock(mtx);
    write(fd, &len, sizeof(len));
    write(fd, &crc, sizeof(crc));
    write(fd, payload.c_str(), len);
    if (fsync_mode == FsyncMode::ALWAYS) fdatasync(fd);
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
        // Cap untrusted length header to 10MB to prevent bad alloc crash
        if (rb != sizeof(len) || len > 10 * 1024 * 1024) { ftruncate(fd, record_start); break; }

        rb = read(fd, &crc, sizeof(crc));
        if (rb != sizeof(crc)) { ftruncate(fd, record_start); break; }

        std::string payload(len, '\0');
        rb = read(fd, &payload[0], len);
        if (rb != len) { ftruncate(fd, record_start); break; }

        if (calculate_crc32(payload) != crc) {
            ftruncate(fd, record_start);
            break;
        }

        std::istringstream iss(payload);
        std::string command, key;
        iss >> command >> key;

        if (command == "SET") {
            uint64_t expiry = 0;
            iss >> expiry;
            std::string value;
            std::getline(iss >> std::ws, value);
            
            uint64_t now = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
            
            if (expiry == 0 || expiry > now) {
                store.set_with_absolute_expiry(key, value, expiry);
                recovered_count++;
            }
        } else if (command == "DEL") {
            store.del(key);
        }
    }
    
    if (recovered_count > 0) std::cout << "Recovered " << recovered_count << " keys from disk.\n";
}

void WAL::compact(Store& store) {
    std::lock_guard<std::mutex> lock(mtx);

    std::string tmp_filepath = filepath + ".tmp";
    int tmp_fd = open(tmp_filepath.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (tmp_fd < 0) return;

    auto snapshot = store.get_snapshot();
    
    for (const auto& [key, value, expiry] : snapshot) {
        std::string payload = "SET " + key + " " + std::to_string(expiry) + " " + value;
        uint32_t len = payload.size();
        uint32_t crc = calculate_crc32(payload);

        write(tmp_fd, &len, sizeof(len));
        write(tmp_fd, &crc, sizeof(crc));
        write(tmp_fd, payload.c_str(), len);
    }
    
    fdatasync(tmp_fd);
    close(tmp_fd);
    rename(tmp_filepath.c_str(), filepath.c_str());
    
    close(fd);
    fd = open(filepath.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    std::cout << "[WAL] Compaction complete. Rewrote " << snapshot.size() << " active keys.\n";
}