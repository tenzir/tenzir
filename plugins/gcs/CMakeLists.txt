cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  gcs
  DESCRIPTION "GCS plugin for Tenzir"
  LANGUAGES CXX)

find_package(Arrow QUIET REQUIRED CONFIG)
# ARROW_GCS is "ON" or "TRUE" depending on how Arrow was configured; test
# truthiness, not one exact spelling.
if (NOT ARROW_GCS)
  message(
    WARNING
      "Disabling gcs plugin because the ARROW_GCS flag is not found/enabled. You probably need to enable ARROW_GCS when installing Apache Arrow."
  )
  return()
endif ()

include(CTest)

find_package(Tenzir REQUIRED)
find_package(google_cloud_cpp_storage REQUIRED CONFIG)

TenzirRegisterPlugin(
  TARGET gcs
  ENTRYPOINT src/plugin.cpp
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

target_link_libraries(gcs PUBLIC google-cloud-cpp::storage)
