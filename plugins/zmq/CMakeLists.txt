cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  zmq
  DESCRIPTION "ZeroMQ plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE zmq_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

TenzirRegisterPlugin(
  TARGET zmq
  ENTRYPOINT src/plugin.cpp
  SOURCES ${zmq_sources}
  INCLUDE_DIRECTORIES include)

find_package(cppzmq QUIET)

dependency_summary("cppzmq" cppzmq "Dependencies")
if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  target_link_libraries(zmq PUBLIC cppzmq-static)
else ()
  target_link_libraries(zmq PUBLIC cppzmq)
endif ()
