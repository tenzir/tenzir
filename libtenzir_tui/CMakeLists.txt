if (TARGET libtenzir_tui)
  return()
endif()

find_package(Tenzir REQUIRED)

option(TENZIR_ENABLE_BUNDLED_FTXUI "Always use the bundled ftxui" OFF)
add_feature_info("TENZIR_ENABLE_BUNDLED_FTXUI" TENZIR_ENABLE_BUNDLED_FTXUI
                 "always use the bundled ftxui.")
if (NOT TENZIR_ENABLE_BUNDLED_FTXUI)
  find_package(ftxui QUIET)
  if (ftxui_FOUND)
    string(APPEND TENZIR_FIND_DEPENDENCY_LIST "\nfind_package(ftxui REQUIRED)")
    dependency_summary("ftxui" ftxui::screen "Dependencies")
  endif ()
endif ()
if (NOT ftxui_FOUND)
  if (NOT TENZIR_ENABLE_BUNDLED_FTXUI)
    message(
      STATUS "Cannot find installed ftxui; falling back to bundled version")
  endif ()
  set(CMAKE_CXX_STANDARD 20)
  add_subdirectory(aux/ftxui)
  unset(CMAKE_CXX_STANDARD)
  dependency_summary("ftxui" "${CMAKE_CURRENT_SOURCE_DIR}/aux/ftxui"
                     "Dependencies")
endif ()

add_library(libtenzir_tui)

file(GLOB_RECURSE tui_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")
target_sources(libtenzir_tui PRIVATE ${tui_sources})

target_include_directories(libtenzir_tui
  PUBLIC "${CMAKE_CURRENT_LIST_DIR}/include")

target_link_libraries(
  libtenzir_tui PUBLIC tenzir::libtenzir tenzir::internal ftxui::screen
                       ftxui::dom ftxui::component)
