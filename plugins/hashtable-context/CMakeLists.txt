cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  hashtable-context
  DESCRIPTION "HashTable context plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE lookup_tables_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET hashtable-context
  ENTRYPOINT src/plugin.cpp
  SOURCES ${lookup_tables_sources})
