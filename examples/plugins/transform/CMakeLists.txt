cmake_minimum_required(VERSION 3.18...3.21 FATAL_ERROR)

project(
  example-transform
  DESCRIPTION "Example transform plugin for VAST"
  VERSION 0.1
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET example-transform
  ENTRYPOINT transform_plugin.cpp
  # SOURCES
  #   list additional source files here
  TEST_SOURCES tests/transform_plugin.cpp)
