cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  explore
  DESCRIPTION "explore plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE explore_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET explore
  ENTRYPOINT src/plugin.cpp
  SOURCES ${explore_sources}
  INCLUDE_DIRECTORIES include)

add_subdirectory(../../libtenzir_tui
                 "${CMAKE_CURRENT_BINARY_DIR}/libtenzir_tui")

target_link_libraries(explore PUBLIC tenzir::libtenzir tenzir::internal
                                     libtenzir_tui)
