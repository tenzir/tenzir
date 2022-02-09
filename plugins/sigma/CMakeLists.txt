cmake_minimum_required(VERSION 3.18...3.22 FATAL_ERROR)

project(
  sigma
  VERSION 0.1.0
  DESCRIPTION "Sigma query language plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

# FIXME: migrate unit tests
find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET sigma
  ENTRYPOINT sigma.cpp
  TEST_SOURCES tests.cpp)
