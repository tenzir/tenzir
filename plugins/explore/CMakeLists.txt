cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  explore
  DESCRIPTION "explore plugin for VAST"
  LANGUAGES CXX)

include(CTest)

find_package(VAST REQUIRED)

file(GLOB_RECURSE explore_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

VASTRegisterPlugin(
  TARGET explore
  ENTRYPOINT src/plugin.cpp
  SOURCES ${explore_sources}
  INCLUDE_DIRECTORIES include)

option(VAST_ENABLE_BUNDLED_FTXUI "Always use the bundled ftxui" OFF)
add_feature_info("VAST_ENABLE_BUNDLED_FTXUI" VAST_ENABLE_BUNDLED_FTXUI
                 "always use the bundled ftxui.")
  if (NOT VAST_ENABLE_BUNDLED_FTXUI)
  find_package(ftxui QUIET)
  if (ftxui_FOUND)
    string(APPEND VAST_FIND_DEPENDENCY_LIST
           "\nfind_package(ftxui REQUIRED)")
    dependency_summary("ftxui" ftxui::screen "Dependencies")
  endif ()
endif ()
if (NOT ftxui_FOUND)
  if (NOT VAST_ENABLE_BUNDLED_FTXUI)
    message(
      STATUS
        "Cannot find installed ftxui; falling back to bundled version")
  endif ()
  set(CMAKE_CXX_STANDARD 20)
  add_subdirectory(aux/ftxui)
  unset(CMAKE_CXX_STANDARD)
  dependency_summary("ftxui" "${CMAKE_CURRENT_SOURCE_DIR}/aux/ftxui"
                     "Dependencies")
endif ()

target_link_libraries(explore PUBLIC vast::libvast vast::internal)
target_link_libraries(explore PUBLIC ftxui::screen ftxui::dom ftxui::component)
