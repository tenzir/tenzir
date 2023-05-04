cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  tui
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

if (NOT EXISTS "${CMAKE_CURRENT_LIST_DIR}/aux/ftxui/.git")
  message(
    FATAL_ERROR
      "submodule '${CMAKE_CURRENT_LIST_DIR}/aux/ftxui' is not initialized")
endif ()

add_subdirectory(aux/ftxui)

target_link_libraries(tui PUBLIC vast::libvast vast::internal)
target_link_libraries(tui PUBLIC ftxui::screen ftxui::dom ftxui::component)
