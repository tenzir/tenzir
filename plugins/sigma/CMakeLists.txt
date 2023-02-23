cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  sigma
  DESCRIPTION "Sigma query language plugin for VAST"
  LANGUAGES CXX)

include(CTest)

file(GLOB_RECURSE sigma_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
     "${CMAKE_CURRENT_SOURCE_DIR}/include/*.hpp")

file(GLOB_RECURSE sigma_tests CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET sigma
  ENTRYPOINT src/plugin.cpp
  SOURCES ${sigma_sources}
  TEST_SOURCES ${sigma_tests}
  INCLUDE_DIRECTORIES include)
