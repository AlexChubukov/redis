cmake_minimum_required(VERSION 3.4)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include("hunter/gate/cmake/HunterGate.cmake")

huntergate(
  URL "https://github.com/ruslo/hunter/archive/v0.23.34.tar.gz"
  SHA1 "70287b1ffa810ee4e952052a9adff9b4856d0d54"
)

project(my_agent)
#string(APPEND CMAKE_CXX_FLAGS " -pedantic -Werror -Wall -Wextra")
#string(APPEND CMAKE_CXX_FLAGS " -Wno-unused-command-line-argument")
#string(APPEND CMAKE_CXX_FLAGS " -Wshadow -Wnon-virtual-dtor")

hunter_add_package(cpp_redis)
find_package(cpp_redis CONFIG REQUIRED)

hunter_add_package(rocksdb)
find_package(RocksDB CONFIG REQUIRED)

add_executable(my_agent methods.cpp)

target_link_libraries(my_agent PUBLIC RocksDB::rocksdb cpp_redis::cpp_redis)
