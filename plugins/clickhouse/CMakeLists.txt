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
  INCLUDE_DIRECTORIES include)

option(TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP
       "Always use the bundled tsl-robin-map" OFF)
if (NOT TENZIR_ENABLE_BUNDLED_CLICKHOUSE_CPP)
  find_package(clickhouse-cpp QUIET)
endif ()

if (clickhouse-cpp_FOUND)
  dependency_summary("clickhouse-cpp" clickhouse-cpp-lib "Dependencies")
  if (NOT TARGET lz4::lz4)
    add_library(lz4::lz4 ALIAS LZ4::lz4)
  endif ()
  if (NOT TARGET zstd::zstd)
    add_library(zstd::zstd ALIAS zstd::libzstd)
  endif ()
else ()
  # Add a fallback to the find modules in clickhouse-cpp.
  list(APPEND CMAKE_MODULE_PATH
       "${CMAKE_CURRENT_SOURCE_DIR}/aux/clickhouse-cpp/cmake")
  find_package(lz4 REQUIRED)
  # The targets in Findlz4 and Findzstd use different names from the upstream
  # CMake setups.
  if (NOT TARGET lz4::lz4)
    add_library(lz4::lz4 ALIAS LZ4::lz4)
    set(DEBUG_DEPENDENCIES
        OFF
        CACHE BOOL "")
  endif ()
  find_package(zstd REQUIRED)
  if (NOT TARGET zstd::zstd)
    add_library(zstd::zstd ALIAS zstd::libzstd)
    set(DEBUG_DEPENDENCIES
        OFF
        CACHE BOOL "")
  endif ()

  set(WITH_OPENSSL
      ON
      CACHE BOOL "")
  set(WITH_SYSTEM_ABSEIL
      ON
      CACHE BOOL "")
  set(WITH_SYSTEM_CITYHASH
      OFF
      CACHE BOOL "")
  set(WITH_SYSTEM_LZ4
      ON
      CACHE BOOL "")
  set(WITH_SYSTEM_ZSTD
      ON
      CACHE BOOL "")

  set(_build_shared_libs_before "${BUILD_SHARED_LIBS}")
  set(BUILD_SHARED_LIBS OFF)
  add_subdirectory(aux/clickhouse-cpp)
  TenzirSystemizeTarget(clickhouse-cpp-lib)
  set(BUILD_SHARED_LIBS "${_build_shared_libs_before}")
  unset(_build_shared_libs_before)
  set_target_properties(clickhouse-cpp-lib PROPERTIES POSITION_INDEPENDENT_CODE
                                                      ON)

  dependency_summary(
    "clickhouse-cpp" "${CMAKE_CURRENT_SOURCE_DIR}/aux/clickhouse-cpp"
    "Dependencies")
endif ()

if (NOT TARGET clickhouse-cpp-lib)
  message(ERROR "The ClickHouse plugin requires clickhouse-cpp-lib")
  return()
endif ()

target_link_libraries(clickhouse PUBLIC clickhouse-cpp-lib)
