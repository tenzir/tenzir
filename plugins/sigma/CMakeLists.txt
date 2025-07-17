cmake_minimum_required(VERSION 3.30...4.0 FATAL_ERROR)

project(
  sigma
  DESCRIPTION "Sigma operator plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET sigma
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
          "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp"
  TEST_SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp"
  INCLUDE_DIRECTORIES include)
