#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include "Store.h"

class WAL {
private:
    std::string filepath;
    std::ofstream log_file;
    std::mutex mtx; 

public:
    WAL(const std::string& filepath);
    ~WAL();

    void append(const std::string& key, const std::string& value);

    void recover(Store& store);
};