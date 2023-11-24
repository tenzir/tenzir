cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  lookup-table-context
  DESCRIPTION "Lookup table context plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
TenzirRegisterPlugin(
  TARGET lookup-table-context
  ENTRYPOINT src/plugin.cpp
  SOURCES ${sources})

set_target_properties(lookup-table-context PROPERTIES LINKER_LANGUAGE CXX)
