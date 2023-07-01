cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  kafka
  DESCRIPTION "kafka plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(VAST REQUIRED)

file(GLOB_RECURSE kafka_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

VASTRegisterPlugin(
  TARGET kafka
  ENTRYPOINT src/plugin.cpp
  SOURCES ${kafka_sources}
  INCLUDE_DIRECTORIES include)

find_package(RdKafka QUIET)
if (NOT RdKafka_FOUND)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(RdKafka QUIET REQUIRED)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()

dependency_summary("librdkafka" RdKafka::rdkafka++ "Dependencies")
target_link_libraries(kafka PUBLIC RdKafka::rdkafka++)
target_link_libraries(kafka-shared PUBLIC RdKafka::rdkafka++)
target_link_libraries(kafka-static PUBLIC RdKafka::rdkafka++)
