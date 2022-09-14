cmake_minimum_required(VERSION 3.18...3.24 FATAL_ERROR)

project(
  rest
  VERSION 1.0.0
  DESCRIPTION "REST plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE rest_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")

file(GLOB_RECURSE rest_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET rest
  ENTRYPOINT src/plugin.cpp
  SOURCES ${rest_sources}
  TEST_SOURCES ${rest_tests}
  INCLUDE_DIRECTORIES include)

find_package(fmt REQUIRED)

find_package(restinio REQUIRED)
target_link_libraries(rest PRIVATE restinio::restinio)

# Compile the rest FlatBuffers schemas.
file(GLOB flatbuffers_schemas CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/fbs/*.fbs")

VASTCompileFlatBuffers(
  TARGET rest-fbs
  SCHEMAS ${flatbuffers_schemas}
  INCLUDE_DIRECTORY "rest/fbs")

target_link_libraries(rest PUBLIC rest-fbs)
