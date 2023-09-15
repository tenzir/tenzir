cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  fluent-bit
  DESCRIPTION "fluent-bit plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE fluentbit_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET fluent-bit
  ENTRYPOINT src/plugin.cpp
  SOURCES ${fluentbit_sources}
  INCLUDE_DIRECTORIES include)

find_package(fluentbit QUIET)
if (NOT fluentbit_FOUND)
  list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
  find_package(fluentbit QUIET REQUIRED)
  list(POP_BACK CMAKE_MODULE_PATH)
endif ()

get_filename_component(FLUENT_BIT_LOCATION ${FLUENT_BIT_LIB} PATH)
get_filename_component(FLUENT_BIT_LOCATION ${FLUENT_BIT_LOCATION} PATH)
dependency_summary("fluent-bit" ${FLUENT_BIT_LOCATION} "Dependencies")
target_link_libraries(fluent-bit PUBLIC fluentbit::fluentbit)
