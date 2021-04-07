cmake_minimum_required(VERSION 3.14...3.20 FATAL_ERROR)

project(example
  DESCRIPTION "example plugin for VAST"
  LANGUAGES CXX)

if (NOT TARGET vast::libvast)
  find_package(VAST REQUIRED)
endif ()

VASTRegisterPlugin(
  TARGET example
  SOURCES
    example.cpp
  TEST_SOURCES
    tests/example.cpp)
