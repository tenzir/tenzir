cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)

project(
  example-pipeline-operator
  VERSION 1.0.0
  DESCRIPTION "Example pipeline operator plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET example-pipeline-operator
  ENTRYPOINT pipeline_operator_plugin.cpp
  # SOURCES
  #   list additional source files here
  TEST_SOURCES tests/pipeline_operator_plugin.cpp)
