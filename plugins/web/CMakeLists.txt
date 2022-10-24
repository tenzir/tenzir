cmake_minimum_required(VERSION 3.18...3.24 FATAL_ERROR)

project(
  web
  VERSION 1.0.0
  DESCRIPTION "Web plugin for VAST"
  LANGUAGES CXX)

include(FeatureSummary)
include(CTest)

file(GLOB_RECURSE web_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/web/*.hpp")

file(GLOB_RECURSE web_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)

VASTRegisterPlugin(
  TARGET web
  ENTRYPOINT src/plugin.cpp
  SOURCES ${web_sources}
  TEST_SOURCES ${web_tests}
  INCLUDE_DIRECTORIES include)

find_package(OpenSSL REQUIRED)

option(VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
       "Always use the bundled and patched restinio" ON)
add_feature_info(
  "VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO"
  VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO
  "always use the bundled and patched restinio.")
if (NOT VAST_ENABLE_BUNDLED_AND_PATCHED_RESTINIO)
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
target_link_libraries(web PRIVATE restinio::restinio)

# Compile the web FlatBuffers schemas.
file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/fbs/*.fbs")

VASTCompileFlatBuffers(
  TARGET web-fbs
  SCHEMAS ${flatbuffers_schemas}
  INCLUDE_DIRECTORY "web/fbs")

target_link_libraries(web PUBLIC web-fbs)
