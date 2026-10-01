cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)
project(QuackSpike LANGUAGES CXX)

option(TENZIR_ENABLE_STATIC_EXECUTABLE "Link DuckDB's static archives" OFF)
list(APPEND CMAKE_MODULE_PATH
     "${CMAKE_CURRENT_LIST_DIR}/../../plugins/duckdb/cmake")
find_package(DuckDB MODULE REQUIRED)
add_executable(quack-probe probe.cpp)
target_compile_features(quack-probe PRIVATE cxx_std_20)
target_link_libraries(quack-probe PRIVATE DuckDB::duckdb)
