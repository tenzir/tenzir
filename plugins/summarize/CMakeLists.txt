cmake_minimum_required(VERSION 3.18...3.23 FATAL_ERROR)

project(
  summarize
  VERSION 2.0.0
  DESCRIPTION "Summarize transform plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

find_package(VAST REQUIRED)

string(MAKE_C_IDENTIFIER "vast_plugin_summarize_version"
                         PLUGIN_TARGET_IDENTIFIER)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/config.cpp.in"
     "const char* ${PLUGIN_TARGET_IDENTIFIER} = \"@VAST_PLUGIN_VERSION@\";\n")
set(VAST_PLUGIN_VERSION "va.b.c")
configure_file("${CMAKE_CURRENT_BINARY_DIR}/config.cpp.in"
              "${CMAKE_CURRENT_BINARY_DIR}/config.cpp" @ONLY)
add_library(summarize-static STATIC summarize.cpp ${CMAKE_CURRENT_BINARY_DIR}/config.cpp)
target_link_libraries(summarize-static PRIVATE libvast)
target_link_libraries(summarize-static INTERFACE -static-libgcc
                                                 -static-libstdc++ -static)
target_compile_features(summarize-static PRIVATE cxx_std_20)
target_compile_definitions(summarize-static
                           PRIVATE VAST_ENABLE_STATIC_PLUGINS)
set_source_files_properties(
  summarize.cpp
  PROPERTIES COMPILE_DEFINITIONS
             "VAST_PLUGIN_VERSION=${PLUGIN_TARGET_IDENTIFIER}")

target_compile_options(summarize-static PRIVATE
    "-g" "-fno-lto" "-fcoroutines"
)
VASTTargetLinkWholeArchive(vast PRIVATE summarize-static)
