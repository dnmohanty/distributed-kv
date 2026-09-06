#pragma once
#include <vector>
#include <cstddef>
#include <mutex>

class SlabAllocator {
private:
    size_t chunk_size;
    size_t total_chunks;
    char* memory_block;
    std::vector<void*> free_list;
    std::mutex mtx;

public:
    SlabAllocator(size_t chunk_size, size_t total_chunks);
    ~SlabAllocator();
    void* allocate();
    void deallocate(void* ptr);
};