cmake_minimum_required(VERSION 3.19...3.24 FATAL_ERROR)

project(
  velociraptor
  DESCRIPTION "velociraptor plugin for Tenzir"
  LANGUAGES CXX)

include(CTest)

find_package(Tenzir REQUIRED)

file(GLOB_RECURSE velociraptor_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.c*")

TenzirRegisterPlugin(
  TARGET velociraptor
  ENTRYPOINT src/plugin.cpp
  SOURCES ${velociraptor_sources}
  INCLUDE_DIRECTORIES include)

find_package(gRPC REQUIRED)
target_link_libraries(velociraptor PUBLIC gRPC::grpc++)
