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

if (TARGET libzmq)
  # libzmq declares its link dependencies as public, but in reality they are all
  # private. Clearing the link libraries fixes execution when those dependencies
  # are not installed in a "standard location", such as in a Nix managed build
  # environment.
  set_target_properties(libzmq PROPERTIES INTERFACE_LINK_LIBRARIES "")
endif ()

if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  target_link_libraries(zmq PUBLIC cppzmq-static)
else ()
  target_link_libraries(zmq PUBLIC cppzmq)
endif ()
