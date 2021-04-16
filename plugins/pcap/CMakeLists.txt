cmake_minimum_required(VERSION 3.14...3.20 FATAL_ERROR)

project(pcap
  DESCRIPTION "PCAP plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

# Unless built alongside VAST, we must find an installed VAST.
if (NOT COMMAND VASTRegisterPlugin)
  find_package(VAST REQUIRED)
endif ()

VASTRegisterPlugin(
  TARGET pcap
  ENTRYPOINT main.cpp
  TEST_SOURCES
    tests.cpp)

# Adjust the CMake Module Path to pick up our custom FindPCAP.cmake script.
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Link pcap plugin against libpcap.
find_package(PCAP REQUIRED)
target_link_libraries(pcap PUBLIC pcap::pcap)
