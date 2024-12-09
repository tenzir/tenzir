cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  parquet
  DESCRIPTION "Parquet store plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET parquet
  ENTRYPOINT parquet.cpp
  SOURCES GLOB "src/*.cpp"
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

if (BUILD_SHARED_LIBS)
  set(PARQUET_LIBRARY Parquet::parquet_shared)
else ()
  set(PARQUET_LIBRARY Parquet::parquet_static)
endif ()

find_package(Parquet)
if (Parquet_VERSION_MAJOR LESS 12)
  message(
    FATAL_ERROR
      "The Parquet plugin requires at least Parquet version 12.0, but found ${PARQUET_VERSION}"
  )
endif ()

target_link_libraries(parquet PRIVATE "${PARQUET_LIBRARY}")

if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  # Work around missing dependency links in the Parquet target
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(Thrift REQUIRED)
  target_link_libraries(parquet PRIVATE thrift::thrift)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()
