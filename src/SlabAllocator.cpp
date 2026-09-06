#include "SlabAllocator.h"
#include <new>
#include <stdexcept>

SlabAllocator::SlabAllocator(size_t chunk_size, size_t total_chunks) 
    : chunk_size(chunk_size), total_chunks(total_chunks) {
    memory_block = new char[chunk_size * total_chunks];
    for (size_t i = 0; i < total_chunks; ++i) {
        free_list.push_back(memory_block + (i * chunk_size));
    }
}

SlabAllocator::~SlabAllocator() {
    delete[] memory_block;
}

void* SlabAllocator::allocate() {
    std::lock_guard<std::mutex> lock(mtx);
    if (free_list.empty()) throw std::bad_alloc();
    void* ptr = free_list.back();
    free_list.pop_back();
    return ptr;
}

void SlabAllocator::deallocate(void* ptr) {
    std::lock_guard<std::mutex> lock(mtx);
    free_list.push_back(ptr);
}