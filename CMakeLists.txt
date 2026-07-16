cmake_minimum_required(VERSION 3.16)

project(QuorumDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(quorumdb_core STATIC
    src/query/query_engine.cpp
    src/query/table.cpp
    src/storage/bplus_tree.cpp
    src/storage/buffer_pool_manager.cpp
    src/storage/disk_manager.cpp
)
target_include_directories(quorumdb_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(quorumdb_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(quorumdb src/main.cpp)
target_link_libraries(quorumdb PRIVATE quorumdb_core)
target_compile_options(quorumdb PRIVATE -Wall -Wextra -Wpedantic)

add_executable(test_disk_manager test/test_disk_manager.cpp)
target_link_libraries(test_disk_manager PRIVATE quorumdb_core)
target_compile_options(test_disk_manager PRIVATE -Wall -Wextra -Wpedantic)

add_executable(test_buffer_pool_manager test/test_buffer_pool_manager.cpp)
target_link_libraries(test_buffer_pool_manager PRIVATE quorumdb_core)
target_compile_options(test_buffer_pool_manager PRIVATE -Wall -Wextra -Wpedantic)

add_executable(test_bplus_tree test/test_bplus_tree.cpp)
target_link_libraries(test_bplus_tree PRIVATE quorumdb_core)
target_compile_options(test_bplus_tree PRIVATE -Wall -Wextra -Wpedantic)

add_executable(test_query_engine test/test_query_engine.cpp)
target_link_libraries(test_query_engine PRIVATE quorumdb_core)
target_compile_options(test_query_engine PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
add_test(NAME disk_manager COMMAND test_disk_manager)
add_test(NAME buffer_pool_manager COMMAND test_buffer_pool_manager)
add_test(NAME bplus_tree COMMAND test_bplus_tree)
add_test(NAME query_engine COMMAND test_query_engine)
