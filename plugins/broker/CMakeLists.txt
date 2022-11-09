cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)
message(WARNING "broker plugin disabled due to lack of compatibility with CAF 18.6")
return()

project(
  broker
  VERSION 1.0.0
  DESCRIPTION "Broker plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE broker_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")

file(GLOB_RECURSE broker_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)

if (VAST_ENABLE_BUNDLED_CAF)
  message(WARNING "Cannot use bundled CAF with Broker plugin")
  return()
endif ()

VASTRegisterPlugin(
  TARGET broker
  ENTRYPOINT src/plugin.cpp
  SOURCES ${broker_sources}
  TEST_SOURCES ${broker_tests}
  INCLUDE_DIRECTORIES include)

# Link against libbroker.
set(_CMAKE_MODULE_PATH_OLD "${CMAKE_MODULE_PATH}")
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(Broker REQUIRED)
set(CMAKE_MODULE_PATH "${_CMAKE_MODULE_PATH_OLD}")
target_link_libraries(broker PUBLIC zeek::broker)
