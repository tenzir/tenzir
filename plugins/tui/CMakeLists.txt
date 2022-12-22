cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  tui
  VERSION 0.0.1
  DESCRIPTION "TUI plugin for VAST"
  LANGUAGES CXX)

include(CTest)

find_package(VAST REQUIRED)

file(GLOB_RECURSE tui_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

VASTRegisterPlugin(
  TARGET tui
  ENTRYPOINT src/plugin.cpp
  SOURCES ${tui_sources}
  INCLUDE_DIRECTORIES include)

find_package(ftxui REQUIRED)

target_link_libraries(tui PRIVATE vast::libvast vast::internal)
target_link_libraries(tui PRIVATE ftxui::screen ftxui::dom ftxui::component)
