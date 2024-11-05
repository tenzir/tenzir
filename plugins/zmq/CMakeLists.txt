cmake_minimum_required(VERSION 3.19...3.28 FATAL_ERROR)

project(
  zmq
  DESCRIPTION "ZeroMQ plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

TenzirRegisterPlugin(
  TARGET zmq
  ENTRYPOINT src/plugin.cpp
  SOURCES GLOB "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  INCLUDE_DIRECTORIES include
  BUILTINS GLOB "${CMAKE_CURRENT_SOURCE_DIR}/builtins/*.cpp")

find_package(cppzmq QUIET)

if (TARGET libzmq)
  # libzmq declares its link dependencies as public, but in reality they are all
  # private. Clearing the link libraries fixes execution when those dependencies
  # are not installed in a "standard location", such as in a Nix managed build
  # environment.
  set_target_properties(libzmq PROPERTIES INTERFACE_LINK_LIBRARIES "")
endif ()

if (TENZIR_ENABLE_STATIC_EXECUTABLE)
  set(cppzmq_target cppzmq-static)
else ()
  set(cppzmq_target cppzmq)
endif ()

dependency_summary("cppzmq" ${cppzmq_target} "Dependencies")
target_link_libraries(zmq PUBLIC ${cppzmq_target})
