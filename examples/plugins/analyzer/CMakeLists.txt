cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  example-analyzer
  VERSION 1.0.0
  DESCRIPTION "Example analyzer plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET example-analyzer
  ENTRYPOINT example_analyzer.cpp # SOURCES
                                  #   list additional source files here
  TEST_SOURCES tests/example_analyzer.cpp)
