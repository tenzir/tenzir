cmake_minimum_required(VERSION 3.14...3.20 FATAL_ERROR)

project(example
  DESCRIPTION "Example plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

# Unless built alongside VAST, we must find an installed VAST.
if (NOT "${CMAKE_PROJECT_NAME}" STREQUAL "VAST")
  find_package(VAST REQUIRED)
endif ()

VASTRegisterPlugin(
  TARGET example
  SOURCES
    example.cpp
  TEST_SOURCES
    tests/example.cpp)
