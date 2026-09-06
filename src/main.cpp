#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include "Store.h"

int main() {
    Store kv_store(16); 
    std::vector<std::thread> threads;

    std::cout << "Firing up 100 concurrent threads...\n";

    for (int i = 0; i < 50; ++i) {
        threads.emplace_back([&kv_store, i]() {
            for (int j = 0; j < 1000; ++j) {
                kv_store.set("key_" + std::to_string(i) + "_" + std::to_string(j), "val");
            }
        });
    }

    for (int i = 0; i < 50; ++i) {
        threads.emplace_back([&kv_store, i]() {
            for (int j = 0; j < 1000; ++j) {
                kv_store.get("key_" + std::to_string(i) + "_" + std::to_string(j));
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "Success! 100,000 concurrent operations executed safely with no crashes.\n";
    return 0;
}