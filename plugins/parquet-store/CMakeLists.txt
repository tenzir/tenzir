cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)

project(
  parquet-store
  VERSION 1.0.0
  DESCRIPTION "Parquet store plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET parquet-store
  ENTRYPOINT parquet_store.cpp
  TEST_SOURCES tests/parquet_store.cpp)

if (BUILD_SHARED_LIBS)
  set(ARROW_LIBRARY arrow_shared)
  set(PARQUET_LIBRARY parquet_shared)
else ()
  set(ARROW_LIBRARY arrow_static)
  set(PARQUET_LIBRARY parquet_static)
endif ()

find_package(Arrow REQUIRED)
get_target_property(PARQUET_PATH "${ARROW_LIBRARY}" LOCATION)
get_filename_component(PARQUET_PATH "${PARQUET_PATH}" DIRECTORY)

find_package(Parquet CONFIG REQUIRED PATHS "${PARQUET_PATH}/cmake/arrow"
             NO_DEFAULT_PATH)
target_link_libraries(parquet-store PRIVATE "${PARQUET_LIBRARY}")
