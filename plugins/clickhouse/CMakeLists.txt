cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  clickhouse
  DESCRIPTION "Clickhouse integration for Tenzir"
  LANGUAGES CXX)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET clickhouse
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB src/*.cpp
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp"
  INCLUDE_DIRECTORIES include
)

option(TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP "Always use the bundled tsl-robin-map"
       OFF)
add_feature_info(
  "TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP" TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP
  "always use the bundled clickhouse-cpp library.")
if (NOT TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP)
  find_package(clickhouse-cpp REQUIRED)
  if (NOT TARGET lz4::lz4)
    add_library(lz4::lz4 ALIAS LZ4::lz4)
  endif ()
  if (NOT TARGET zstd::zstd)
    add_library(zstd::zstd ALIAS zstd::libzstd)
  endif ()
else ()
  set(WITH_OPENSSL ON CACHE BOOL "")
  set(WITH_SYSTEM_ABSEIL ON CACHE BOOL "")
  set(WITH_SYSTEM_CITYHASH OFF CACHE BOOL "")
  set(WITH_SYSTEM_LZ4 ON CACHE BOOL "")
  set(WITH_SYSTEM_ZSTD ON CACHE BOOL "")

  add_subdirectory(aux/clickhouse-cpp)
  TenzirSystemizeTarget(clickhouse-cpp-lib)

  target_include_directories(clickhouse-cpp-lib PUBLIC aux/clickhouse-cpp)
endif ()

dependency_summary("clickhouse-cpp" clickhouse-cpp-lib "Dependencies")
target_link_libraries(clickhouse PUBLIC clickhouse-cpp-lib)
