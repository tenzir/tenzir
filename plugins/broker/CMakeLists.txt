cmake_minimum_required(VERSION 3.18...3.21 FATAL_ERROR)

project(
  broker
  DESCRIPTION "Broker plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE broker_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")

file(GLOB_RECURSE broker_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET broker
  ENTRYPOINT src/plugin.cpp
  SOURCES ${broker_sources}
  TEST_SOURCES ${broker_tests}
  INCLUDE_DIRECTORIES include)

if (VAST_ENABLE_BUNDLED_CAF)
  message(FATAL_ERROR "Cannot use bundled CAF with Broker plugin")
endif ()

# Link against libbroker.
set(_CMAKE_MODULE_PATH_OLD "${CMAKE_MODULE_PATH}")
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(Broker REQUIRED)
set(CMAKE_MODULE_PATH "${_CMAKE_MODULE_PATH_OLD}")
target_link_libraries(broker PUBLIC zeek::broker)
