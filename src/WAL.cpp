#include "WAL.h"
#include <iostream>
#include <sstream>
#include <stdexcept>

WAL::WAL(const std::string& filepath) : filepath(filepath) {
    log_file.open(filepath, std::ios::app);
    if (!log_file.is_open()) {
        throw std::runtime_error("Failed to open WAL file: " + filepath);
    }
}

WAL::~WAL() {
    if (log_file.is_open()) {
        log_file.close();
    }
}

void WAL::append(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mtx);
    
    log_file << "SET " << key << " " << value << "\n";
    log_file.flush(); 
}

void WAL::recover(Store& store) {
    std::ifstream infile(filepath);
    if (!infile.is_open()) {
        return; 
    }

    std::string line;
    int recovered_count = 0;
    
    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string command, key, value;
        
        iss >> command >> key;
        if (command == "SET") {
            std::getline(iss >> std::ws, value);
            store.set(key, value);
            recovered_count++;
        }
    }
    
    if (recovered_count > 0) {
        std::cout << "Recovered " << recovered_count << " keys from disk.\n";
    }
}