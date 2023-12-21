cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  nic
  DESCRIPTION "NIC plugin for Tenzir"
  LANGUAGES CXX)

# Enable unit testing. Note that it is necessary to include CTest in the
# top-level CMakeLists.txt file for it to create a test target, so while
# optional for plugins built alongside Tenzir, it is necessary to specify this
# line manually so plugins can be linked against an installed Tenzir.
include(CTest)

find_package(Tenzir REQUIRED)
TenzirRegisterPlugin(
  TARGET nic ENTRYPOINT src/plugin.cpp #TEST_SOURCES tests.cpp
)

# Link nic plugin against libpcap.
find_package(PkgConfig REQUIRED QUIET)
pkg_check_modules(pcap REQUIRED IMPORTED_TARGET libpcap)
dependency_summary("libpcap" PkgConfig::pcap "Dependencies")
target_link_libraries(nic PUBLIC PkgConfig::pcap)
