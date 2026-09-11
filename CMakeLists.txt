cmake_minimum_required(VERSION 3.10)
project(DistributedKV VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -pthread")

add_executable(kv_server src/main.cpp src/Store.cpp src/SlabAllocator.cpp src/Server.cpp src/ConsistentHash.cpp src/WAL.cpp)