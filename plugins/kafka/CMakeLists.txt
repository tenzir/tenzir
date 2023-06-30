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

option(VAST_ENABLE_BUNDLED_RDKAFKA "Always use the bundled RdKafka" OFF)
add_feature_info("VAST_ENABLE_BUNDLED_RDKAFKA" VAST_ENABLE_BUNDLED_RDKAFKA
                 "always use the bundled RdKafka.")
if (NOT VAST_ENABLE_BUNDLED_RDKAFKA)
  find_package(RdKafka QUIET)
  if (RdKafka_FOUND)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(RdKafka REQUIRED)")
    dependency_summary("librdkafka" RdKafka::rdkafka++ "Dependencies")
  endif ()
endif ()
if (NOT RdKafka_FOUND)
  if (NOT VAST_ENABLE_BUNDLED_RDKAFKA)
    message(
      STATUS
        "Cannot find installed RdKafka; falling back to bundled version")
  endif ()
  set(RDKAFKA_BUILD_EXAMPLES OFF)
  set(RDKAFKA_BUILD_TESTS OFF)
  if (VAST_ENABLE_STATIC_EXECUTABLE)
    set(RDKAFKA_BUILD_STATIC ON)
  endif ()
  add_subdirectory(aux/librdkafka)
  dependency_summary("librdkafka" "${CMAKE_CURRENT_SOURCE_DIR}/aux/librdkafka"
                     "Dependencies")
  add_library(RdKafka::rdkafka++ ALIAS rdkafka++)
endif ()

target_link_libraries(kafka PUBLIC vast::libvast vast::internal)
target_link_libraries(kafka PUBLIC RdKafka::rdkafka++)
