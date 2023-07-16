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

option(TENZIR_ENABLE_BUNDLED_FTXUI "Always use the bundled ftxui" OFF)
add_feature_info("TENZIR_ENABLE_BUNDLED_FTXUI" TENZIR_ENABLE_BUNDLED_FTXUI
                 "always use the bundled ftxui.")
  if (NOT TENZIR_ENABLE_BUNDLED_FTXUI)
  find_package(ftxui QUIET)
  if (ftxui_FOUND)
    string(APPEND TENZIR_FIND_DEPENDENCY_LIST
           "\nfind_package(ftxui REQUIRED)")
    dependency_summary("ftxui" ftxui::screen "Dependencies")
  endif ()
endif ()
if (NOT ftxui_FOUND)
  if (NOT TENZIR_ENABLE_BUNDLED_FTXUI)
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

target_link_libraries(explore PUBLIC tenzir::libtenzir tenzir::internal)
target_link_libraries(explore PUBLIC ftxui::screen ftxui::dom ftxui::component)
