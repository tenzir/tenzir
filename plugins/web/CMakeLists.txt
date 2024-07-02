cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

if (APPLE)
  message(
    WARNING
      "The http_parser library is unmaintained and no longer available on macOS; disabling the web plugin"
  )
  return()
endif ()

project(
  web
  DESCRIPTION "Web plugin for Tenzir"
  LANGUAGES CXX)

include(FeatureSummary)
include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET web
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
          "${CMAKE_CURRENT_SOURCE_DIR}/include/web/*.hpp"
  TEST_SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
  FLATBUFFERS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/fbs/*.fbs"
  INCLUDE_DIRECTORIES include)

find_package(OpenSSL REQUIRED)

option(TENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
       "Always use the bundled and patched restinio" ON)
add_feature_info(
  "TENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO"
  TENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
  "always use the bundled and patched restinio.")
if (NOT TENZIR_ENABLE_BUNDLED_AND_PATCHED_RESTINIO)
  find_package(fmt REQUIRED)
  find_package(restinio REQUIRED)
  # OpenSSL link is missing from restinio.
  find_package(OpenSSL REQUIRED)
  target_link_libraries(web PUBLIC OpenSSL::SSL OpenSSL::Crypto)
else ()
  # Setup dependency code from submodules.
  list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  add_subdirectory(aux)
  list(POP_FRONT CMAKE_MODULE_PATH)
endif ()
dependency_summary("restinio" restinio::restinio "Dependencies")
target_link_libraries(web PRIVATE restinio::restinio)
