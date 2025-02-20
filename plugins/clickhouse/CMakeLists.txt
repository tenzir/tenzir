cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  clickhouse
  DESCRIPTION "Clickhouse Integration for Tenzir"
  LANGUAGES CXX)

find_package(Tenzir REQUIRED)

set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "")
set(WITH_SYSTEM_CITYHASH OFF CACHE BOOL "")
set(WITH_SYSTEM_LZ4 ON CACHE BOOL "")
set(WITH_SYSTEM_ZSTD ON CACHE BOOL "")

add_subdirectory(aux/clickhouse-cpp)
TenzirSystemizeTarget(clickhouse-cpp-lib)

TenzirRegisterPlugin(
  TARGET clickhouse
  ENTRYPOINT builtins/stub.cpp
  SOURCES GLOB src/*.cpp
  BUILTINS builtins/to_clickhouse.cpp
  INCLUDE_DIRECTORIES include
)

target_include_directories(clickhouse-cpp-lib PUBLIC aux/clickhouse-cpp)
target_link_libraries(clickhouse PUBLIC clickhouse-cpp-lib)
