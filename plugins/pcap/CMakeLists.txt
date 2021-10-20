cmake_minimum_required(VERSION 3.18...3.21 FATAL_ERROR)

project(
  pcap
  DESCRIPTION "PCAP plugin for VAST"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside VAST, it is necessary to specify this
# line manually so plugins can be linked against an installed VAST.
include(CTest)

find_package(VAST REQUIRED)
VASTRegisterPlugin(
  TARGET pcap
  ENTRYPOINT main.cpp
  TEST_SOURCES tests.cpp)

# Link pcap plugin against libpcap.
set(_CMAKE_MODULE_PATH_OLD "${CMAKE_MODULE_PATH}")
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(PCAP REQUIRED)
set(CMAKE_MODULE_PATH "${_CMAKE_MODULE_PATH_OLD}")
target_link_libraries(pcap PUBLIC pcap::pcap)
