cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)

project(
  cef
  VERSION 0.1.0
  DESCRIPTION "CEF reader plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE cef_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")

file(GLOB_RECURSE cef_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET cef
  ENTRYPOINT src/plugin.cpp
  SOURCES ${cef_sources}
  TEST_SOURCES ${cef_tests}
  INCLUDE_DIRECTORIES include)
