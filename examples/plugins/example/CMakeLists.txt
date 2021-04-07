cmake_minimum_required(VERSION 3.14...3.20 FATAL_ERROR)

project(example
  DESCRIPTION "Example plugin for VAST"
  LANGUAGES CXX)

if (CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
  include(CTest)
  find_package(VAST REQUIRED)
endif ()

VASTRegisterPlugin(
  TARGET example
  SOURCES
    example.cpp
  TEST_SOURCES
    tests/example.cpp)
